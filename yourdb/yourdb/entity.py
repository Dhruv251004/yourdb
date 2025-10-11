import os
import json
from multiprocessing.dummy import Pool
from functools import partial
from .utils import YourDBEncoder, yourdb_decoder, SERIALIZABLE_CLASSES
from .compaction import Compactor
import operator

class Entity:
    def __init__(self, entity_path, name, schema=None, num_partitions=10):
        self.name = name
        self.schema = schema
        self.num_partitions = num_partitions
        self.entity_path = entity_path
        print(f"this is the entity path------------->   {entity_path}")
        self.schema_path = os.path.join(entity_path, 'schema.json')
        print(f"this is the schema path------------->   {self.schema_path}")
        self.file_paths = [
            os.path.join(entity_path, f"{name}_shard_{i}.log")
            for i in range(num_partitions)
        ]

        self.data = {i: {} for i in range(num_partitions)} #  It's a copy of the final state of data held in
                                                        #  computer's RAM for extremely fast reading.
        self.primary_key = None
        self.primary_key_set = set()

        self.COMPACTION_THRESHOLD=1000
        self.write_counts={i: 0 for i in range(num_partitions)}

        self.indexes = {}
        os.makedirs(entity_path, exist_ok=True)

        if os.path.exists(self.schema_path):
            self._load_schema()
            self.primary_key = self.schema.get('primary_key')

            # Initializing the empty indexes based on the schema
            for indexed_field in self.schema.get('indexes', []):
                self.indexes[indexed_field] = {}

            self._load_from_logs()
        else:
            if schema is None:
                raise Exception("Schema must be provided when creating a new entity.")
            self._save_schema()
            self.primary_key = self.schema.get('primary_key')
            for indexed_field in self.schema.get('indexes', []):
                self.indexes[indexed_field] = {}
            for fp in self.file_paths:
                open(fp, 'a').close()
        print(f"Entity '{self.name}' initialized with schema: {self.schema}")


    def _check_and_compact(self,partition_index: int):
        """ if the partition has hit the write threshold ,do the compaction"""

        if self.write_counts[partition_index] >=self.COMPACTION_THRESHOLD:
            print(f"Compaction triggered for partition {partition_index} of entity '{self.name}'")
            compactor=Compactor(self.file_paths[partition_index],self.primary_key)
            compactor.compact()

            self.write_counts[partition_index]=0 # reseting the write count file after compaction

    def _save_schema(self):
        with open(self.schema_path, 'w') as f:
            # Added custom encoder for consistency ---
            json.dump(self.schema, f, indent=4, cls=YourDBEncoder)

    def _load_schema(self):
        with open(self.schema_path, 'r') as f:
            # Added custom decoder for consistency ---
            self.schema = json.load(f, object_hook=yourdb_decoder)

    def _load_from_logs(self):
        with Pool() as pool:
            pool.map(self._replay_partition, range(self.num_partitions))

    def _replay_partition(self, i):
        partition_data = {}
        with open(self.file_paths[i], 'r') as f:
            for line in f:
                if not line.strip(): continue
                log_entry = json.loads(line, object_hook=yourdb_decoder)
                op = log_entry.get('op')

                if op == 'INSERT':
                    obj = log_entry['data']
                    pk_val = getattr(obj, self.primary_key)
                    partition_data[pk_val] = obj
                    self.primary_key_set.add(pk_val)
                    for field_name, index in self.indexes.items():
                        value = getattr(obj, field_name)
                        if value not in index:
                            index[value] = set()
                        index[value].add(pk_val)

                elif op == 'UPDATE':
                    pk_to_update = log_entry['pk']
                    if pk_to_update in partition_data:
                        original_object = partition_data[pk_to_update]
                        update_data = log_entry.get('data', {})
                        # For each indexed field, check if its value is changing
                        for field_name, index in self.indexes.items():
                            if field_name in update_data:
                                old_value = getattr(original_object, field_name)
                                new_value = update_data[field_name]
                                if old_value != new_value:
                                    # Remove from old index entry
                                    if old_value in index:
                                        index[old_value].discard(pk_to_update)
                                    # Add to new index entry
                                    if new_value not in index:
                                        index[new_value] = set()
                                    index[new_value].add(pk_to_update)
                        # Apply the update to the in-memory object
                        for key, value in update_data.items():
                           setattr(original_object, key, value)

                elif op == 'DELETE':
                    pk_to_delete = log_entry['pk']
                    if pk_to_delete in partition_data:
                        obj_to_delete = partition_data[pk_to_delete]
                        # Remove from all indexes before deleting
                        for field_name, index in self.indexes.items():
                            value = getattr(obj_to_delete, field_name)
                            if value in index:
                                index[value].discard(pk_to_delete)
                        # Delete the object
                        del partition_data[pk_to_delete]
                        self.primary_key_set.discard(pk_to_delete)
        self.data[i] = partition_data

    def hash_partition(self, key):
        res = hash(key) % self.num_partitions
        return res

    def is_valid_entity(self, entity):
        if not hasattr(entity, '__dict__'):
            raise TypeError("Entity to be saved must be a class object.")
        entity_dict = entity.__dict__
        type_mapping = {"str": str, "int": int, "bool": bool, "float": float}

        for key, value in entity_dict.items():
            if key in self.schema:
                expected_type_str = self.schema[key]
                expected_type = type_mapping.get(expected_type_str)
                if expected_type is None and expected_type_str in SERIALIZABLE_CLASSES:
                    expected_type = SERIALIZABLE_CLASSES[expected_type_str]
                if expected_type and not isinstance(value, expected_type):
                    raise TypeError(f"Field '{key}' expects type {expected_type.__name__} but got {type(value).__name__}.")

        primary_value = entity_dict.get(self.primary_key)
        if primary_value is None:
            raise Exception(f"Primary key '{self.primary_key}' cannot be None.")
        if primary_value in self.primary_key_set:
            raise Exception(f"Duplicate primary key '{primary_value}' found.")
        return True

    def insert(self, entity):
        if self.is_valid_entity(entity):
            pk_val = getattr(entity, self.primary_key)
            partition = self.hash_partition(pk_val)
            log_entry = {"op": "INSERT", "data": entity}
            with open(self.file_paths[partition], 'a') as f:
                f.write(json.dumps(log_entry, cls=YourDBEncoder) + '\n')
            self.data[partition][pk_val] = entity
            self.primary_key_set.add(pk_val)

            for field_name, index in self.indexes.items():
                value = getattr(entity, field_name)
                if value not in index:
                    index[value] = set()
                index[value].add(pk_val)

            self.write_counts[partition] += 1
            self._check_and_compact(partition)

            return True
        return False

    # def get_data(self, filter_dict: dict = None):
    #     if not filter_dict:
    #     # No filter, return all data (full scan)
    #         all_results = []
    #         for partition_data in self.data.values():
    #             all_results.extend(partition_data.values())
    #         return all_results

    #     indexed_field = None
    #     for field in filter_dict.keys():
    #         if field in self.indexes:
    #             indexed_field = field
    #             break # Use the first index we find

    #     if indexed_field:
    #     #  Path 1: Use the Index
    #         print(f"Querying using index on '{indexed_field}'...")
    #         value_to_find = filter_dict[indexed_field]
    #         index = self.indexes[indexed_field]

    #         # Get the set of primary keys from the index (very fast)
    #         primary_keys = index.get(value_to_find, set())

    #         # Retrieve only the matching records directly
    #         results = []
    #         for pk in primary_keys:
    #             partition_index = self.hash_partition(pk)
    #             record = self.data[partition_index].get(pk)
    #             if record:
    #                 # (A full implementation would apply other filters here if necessary)
    #                 results.append(record)
    #         return results

    #     else:
    #     # --- Path 2: Full Scan ---
    #         print("No suitable index found. Performing full scan...")
    #         results = []
    #         for partition_data in self.data.values():
    #             for record in partition_data.values():
    #                 if all(getattr(record, k, None) == v for k, v in filter_dict.items()):
    #                     results.append(record)
    #         return results
    def get_data(self, filter_dict: dict = None):
        """
        Retrieve records matching the filter_dict.
        Supports MongoDB-style operators and index-based optimization.
        Example filters:
            {'department': 'Engineering'}
            {'salary': {'$gt': 80000}}
            {'department': 'Engineering', 'emp_id': {'$gt': 101}}
        """
        # --- No filter: return all data ---
        if not filter_dict:
            all_results = []
            for partition_data in self.data.values():
                all_results.extend(partition_data.values())
            return all_results

        # ---  Find all indexed fields used in the filter ---
        indexed_fields = [f for f in filter_dict if f in self.indexes]
        candidates = None

        # ---  Use indexes to narrow down search space ---
        if indexed_fields:
            print(f"Using indexes on fields: {indexed_fields}")

            # For each indexed field, find primary keys that match
            index_sets = []
            for field in indexed_fields:
                condition = filter_dict[field]
                index = self.indexes[field]

                # Handle only equality for indexes (non-range)
                if not isinstance(condition, dict) or "$eq" in condition:
                    value = condition if not isinstance(condition, dict) else condition["$eq"]
                    index_sets.append(index.get(value, set()))
                else:
                    # If range condition, fall back to scanning all index entries
                    matched_keys = set()
                    for val, pks in index.items():
                        if self._match_condition(val, condition):
                            matched_keys.update(pks)
                    index_sets.append(matched_keys)

            # Intersect sets for multi-index queries (AND logic)
            candidates = set.intersection(*index_sets) if index_sets else None
        else:
            print("No suitable index found. Performing full scan...")

        # --- Collect candidate records (from narrowed partitions if possible) ---
        results = []
        if candidates is not None:
            # Indexed path: direct lookup
            for pk in candidates:
                partition_index = self.hash_partition(pk)
                record = self.data[partition_index].get(pk)
                if record and self._matches_filter(record, filter_dict):
                    results.append(record)
        else:
            # Full scan path
            for partition_data in self.data.values():
                for record in partition_data.values():
                    if self._matches_filter(record, filter_dict):
                        results.append(record)

        return results
    
    def _match_condition(self, field_value, condition):
        """Evaluate a single field condition, supporting MongoDB-style operators."""
        if isinstance(condition, dict):
            ops = {
                '$gt': lambda a, b: a > b,
                '$lt': lambda a, b: a < b,
                '$gte': lambda a, b: a >= b,
                '$lte': lambda a, b: a <= b,
                '$eq': lambda a, b: a == b,
                '$ne': lambda a, b: a != b,
            }
            # All operators in the dict must hold true
            return all(ops[op](field_value, val) for op, val in condition.items() if op in ops)
        else:
            # Simple equality
            return field_value == condition


    def _matches_filter(self, record, filter_dict):
        """Check if a record satisfies all filter conditions."""
        for field, condition in filter_dict.items():
            field_value = getattr(record, field, None)
            if not self._match_condition(field_value, condition):
                return False  # short-circuit
        return True



    

    # --- REWRITTEN: Delete and its helper now use the append-only log ---
    def _delete_from_partition(self, i, condition_fn):
        records_to_delete = [
            record for pk, record in self.data[i].items() if condition_fn(record)
        ]
        if not records_to_delete:
            return

        with open(self.file_paths[i], 'a') as f:
            for record in records_to_delete:
                pk_val = getattr(record, self.primary_key)

                for field_name, index in self.indexes.items():
                    value = getattr(record, field_name)
                    if value in index:
                        index[value].discard(pk_val)

                log_entry = {"op": "DELETE", "pk": pk_val}
                f.write(json.dumps(log_entry) + '\n')
                # Update in-memory state
                del self.data[i][pk_val]
                self.primary_key_set.discard(pk_val)

                self.write_counts[i] += 1
                self._check_and_compact(i)

    def delete(self, filter_dict:dict):
        records_to_delete = self.get_data(filter_dict)
        if not records_to_delete:
            return

        for record in records_to_delete:
            pk_val = getattr(record, self.primary_key)
            partition_index = self.hash_partition(pk_val)

            # 1. Update indexes
            for field_name, index in self.indexes.items():
                value = getattr(record, field_name)
                if value in index:
                    index[value].discard(pk_val)

            # 2. Write to log
            with open(self.file_paths[partition_index], 'a') as f:
                log_entry = {"op": "DELETE", "pk": pk_val}
                f.write(json.dumps(log_entry) + '\n')

            # 3. Update in-memory state
            if pk_val in self.data[partition_index]:
                del self.data[partition_index][pk_val]
            self.primary_key_set.discard(pk_val)

            # 4. Trigger compaction check
            self.write_counts[partition_index] += 1
            self._check_and_compact(partition_index)


    # Update and its helper now use the append-only log ---
    def _update_partition(self, i, condition_fn, update_fn):
        records_to_update = [
            record for pk, record in self.data[i].items() if condition_fn(record)
        ]
        if not records_to_update:
            return

        with open(self.file_paths[i], 'a') as f:
            for record in records_to_update:
                pk_val = getattr(record, self.primary_key)
                old_index_values = {
                    field_name: getattr(record, field_name)
                    for field_name in self.indexes.keys()
                }

                # Apply the update function to a copy to see what changed
                updated_record = update_fn(record)
                for field_name, index in self.indexes.items():
                    old_value = old_index_values[field_name]
                    new_value = getattr(updated_record, field_name)
                    if old_value != new_value:
                        # Remove from old index entry
                        if old_value in index:
                            index[old_value].discard(pk_val)
                        # Add to new index entry
                        if new_value not in index:
                            index[new_value] = set()
                        index[new_value].add(pk_val)

                update_payload = {
                    k: v for k, v in updated_record.__dict__.items()
                    if v != old_index_values.get(k)
                }


                if update_payload:
                    log_entry = {"op": "UPDATE", "pk": pk_val, "data": update_payload}
                    f.write(json.dumps(log_entry, cls=YourDBEncoder) + '\n')
                    # The in-memory object is already updated by reference via update_fn

                    self.write_counts[i] += 1
                    self._check_and_compact(i)

    def update(self, filter_dict: dict, update_fn):
        """Finds records using get_data (which uses indexes) and then updates them."""
        records_to_update = self.get_data(filter_dict)

        if not records_to_update:
            return

        for record in records_to_update:
            pk_val = getattr(record, self.primary_key)
            partition_index = self.hash_partition(pk_val)

            # 1. Store old index values before updating
            old_index_values = {
                field_name: getattr(record, field_name)
                for field_name in self.indexes.keys()
            }

            # 2. Apply update function (modifies the object by reference)
            updated_record = update_fn(record)

            # 3. Update indexes if any indexed fields changed
            for field_name, index in self.indexes.items():
                old_value = old_index_values[field_name]
                new_value = getattr(updated_record, field_name)
                if old_value != new_value:
                    if old_value in index:
                        index[old_value].discard(pk_val)
                    if new_value not in index:
                        index[new_value] = set()
                    index[new_value].add(pk_val)

            # 4. Create a minimal log and write to disk
            update_payload = { k: v for k, v in updated_record.__dict__.items() if v != old_index_values.get(k) }

            if update_payload:
                with open(self.file_paths[partition_index], 'a') as f:
                    log_entry = {"op": "UPDATE", "pk": pk_val, "data": update_payload}
                    f.write(json.dumps(log_entry, cls=YourDBEncoder) + '\n')

                    # 5. Trigger compaction check
                    self.write_counts[partition_index] += 1
                    self._check_and_compact(partition_index)
        return True
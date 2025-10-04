import os
import json
from multiprocessing.dummy import Pool
from functools import partial
from .utils import YourDBEncoder, yourdb_decoder, SERIALIZABLE_CLASSES

class Entity:
    def __init__(self, entity_path, name, schema=None, num_partitions=10):
        self.name = name
        self.schema = schema
        self.num_partitions = num_partitions
        self.entity_path = entity_path
        self.schema_path = os.path.join(entity_path, 'schema.json')
        self.file_paths = [
            os.path.join(entity_path, f"{name}_shard_{i}.log")
            for i in range(num_partitions)
        ]
        self.data = {i: {} for i in range(num_partitions)} #  It's a copy of the final state of data held in
                                                        #  computer's RAM for extremely fast reading.
        self.primary_key = None
        self.primary_key_set = set()

        os.makedirs(entity_path, exist_ok=True)

        if os.path.exists(self.schema_path):
            self._load_schema()
            self.primary_key = self.schema.get('primary_key')
            self._load_from_logs()
        else:
            if schema is None:
                raise Exception("Schema must be provided when creating a new entity.")
            self._save_schema()
            self.primary_key = self.schema.get('primary_key')
            for fp in self.file_paths:
                open(fp, 'a').close()
        print(f"Entity '{self.name}' initialized with schema: {self.schema}")

    def _save_schema(self):
        with open(self.schema_path, 'w') as f:
            # --- CHANGED: Added custom encoder for consistency ---
            json.dump(self.schema, f, indent=4, cls=YourDBEncoder)

    def _load_schema(self):
        with open(self.schema_path, 'r') as f:
            # --- CHANGED: Added custom decoder for consistency ---
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

                if log_entry['op'] == 'INSERT':
                    pk_val = getattr(log_entry['data'], self.primary_key)
                    partition_data[pk_val] = log_entry['data']
                    self.primary_key_set.add(pk_val)

                elif log_entry['op'] == 'UPDATE':
                    pk_to_update = log_entry['pk']
                    if pk_to_update in partition_data:
                        for key, value in log_entry['data'].items():
                           setattr(partition_data[pk_to_update], key, value)

                elif log_entry['op'] == 'DELETE':
                    pk_to_delete = log_entry['pk']
                    if pk_to_delete in partition_data:
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
            return True
        return False

    def get_data(self, condition_fn=None):
        all_results = []
        for partition_index in range(self.num_partitions):
            for record_object in self.data[partition_index].values():
                if condition_fn is None or condition_fn(record_object):
                    all_results.append(record_object)
        return all_results

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
                log_entry = {"op": "DELETE", "pk": pk_val}
                f.write(json.dumps(log_entry) + '\n')
                # Update in-memory state
                del self.data[i][pk_val]
                self.primary_key_set.discard(pk_val)

    def delete(self, condition_fn):
        # The by_id flag is removed as this scan-and-delete is the primary method now
        with Pool() as pool:
            pool.map(partial(self._delete_from_partition,
                     condition_fn=condition_fn), range(self.num_partitions))

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
                original_record_dict = record.__dict__.copy()

                # Apply the update function to a copy to see what changed
                updated_record = update_fn(record)

                update_payload = {
                    k: updated_record.__dict__[k] for k in updated_record.__dict__
                    if updated_record.__dict__[k] != original_record_dict.get(k)
                }

                if update_payload:
                    log_entry = {"op": "UPDATE", "pk": pk_val, "data": update_payload}
                    f.write(json.dumps(log_entry, cls=YourDBEncoder) + '\n')
                    # The in-memory object is already updated by reference via update_fn

    def update(self, condition_fn, update_fn):
        with Pool() as pool:
            pool.map(partial(self._update_partition, condition_fn=condition_fn,
                     update_fn=update_fn), range(self.num_partitions))
        return True

    # --- REMOVED: All old pickle-based methods are now gone ---
    # _init_empty_shards
    # _save_partition
    # save_data
    # load_data
    # _load_partition
    # _get_from_partition
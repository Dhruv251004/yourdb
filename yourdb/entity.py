import os
import pickle
from multiprocessing.dummy import Pool  # Threading-based Pool
from functools import partial


class Entity:
    def __init__(self, entity_path, name, schema=None, num_partitions=10):
        self.name = name
        self.schema = schema
        self.primary_key = None
        self.num_partitions = num_partitions
        self.entity_path = entity_path
        self.schema_path = os.path.join(entity_path, 'schema.pkl')
        self.file_paths = [
            os.path.join(entity_path, f"{name}_shard_{i}.entity")
            for i in range(num_partitions)
        ]

        self.operation_count = 0
        self.data = {i: [] for i in range(num_partitions)}  # In-memory data
        self.primary_key_set = set() # Set for fast primary key lookups

        # Create dir if needed
        os.makedirs(entity_path, exist_ok=True)

        # Load schema and data if files exist, otherwise initialize
        if all(os.path.exists(fp) for fp in self.file_paths) and os.path.exists(self.schema_path):
            self._load_schema()
            self.load_data()  # Load data once into memory

            if self.schema.get('primary_key'):
                pk = self.schema.get('primary_key')
                for partition_data in self.data.values():
                    for entity in partition_data:
                        self.primary_key_set.add(entity.get(pk))
        else:
            if schema is None:
                raise Exception(
                    "Schema must be provided when creating a new entity.")
            self._save_schema()
            self._init_empty_shards()

        self.primary_key = self.schema.get('primary_key')
        print(f"Entity '{self.name}' initialized with schema: {self.schema}")

    def _save_schema(self):
        with open(self.schema_path, 'wb') as f:
            pickle.dump(self.schema, f)   # Serialization

    def _load_schema(self):
        with open(self.schema_path, 'rb') as f:
            self.schema = pickle.load(f)  # Deserialization
        self.primary_key = self.schema.get('primary_key')

    def _init_empty_shards(self):
        for i in range(self.num_partitions):
            with open(self.file_paths[i], 'wb') as f:
                pickle.dump([], f)

    def hash_partition(self, key):
        # print(key)
        # print(self.primary_key)
        res = hash(key) % self.num_partitions    # quick access of the correct partition while CRUD operations
        # print(f"Hash partition for key '{key}': {res}")
        return res
        # return key % self.num_partitions

    def is_valid_entity(self, entity):
        if not isinstance(entity, dict):
            raise Exception("Entity must be a dictionary.")

        for key, value in entity.items():
            if key not in self.schema:
                raise Exception(f"Invalid field '{key}' in entity.")
            expected_type = self.schema[key]
            if isinstance(expected_type, dict):
                expected_type = expected_type['type']
            if not isinstance(value, expected_type):
                raise Exception(
                    f"Field '{key}' must be of type {expected_type.__name__}.")

        if self.primary_key:
            primary_value = entity.get(self.primary_key)
            if primary_value is None:
                raise Exception(
                    f"Primary key '{self.primary_key}' cannot be None.")
            # for partition_data in self.data.values():
            #     for existing_entity in partition_data:
            #         if existing_entity.get(self.primary_key) == primary_value:
            #             raise Exception(
            #                 f"Duplicate primary key '{primary_value}' found.")

            if primary_value in self.primary_key_set:
                raise Exception(
                    f"Duplicate primary key '{primary_value}' found.")
        return True

    def _save_partition(self, i):
        with open(self.file_paths[i], 'wb') as f:
            pickle.dump(self.data[i], f)

    def save_data(self):
        with Pool() as pool:
            pool.map(self._save_partition, range(self.num_partitions))

    def load_data(self):
        # Load all partitions once into memory
        with Pool() as pool:
            loaded_data = pool.map(self._load_partition,
                                   range(self.num_partitions))
        for i in range(self.num_partitions):
            self.data[i] = loaded_data[i]

    def _load_partition(self, i):
        with open(self.file_paths[i], 'rb') as f:
            return pickle.load(f)

    def insert(self, entity):
        if self.is_valid_entity(entity):
            partition = self.hash_partition(entity.get(self.primary_key))
            self.data[partition].append(entity)  # In-memory insertion

            self.primary_key_set.add(entity.get(self.primary_key)) # Add to primary key set


            self._save_partition(partition)  # Save to disk immediately
            return True
        return False

    def _delete_from_partition(self, i, condition_fn):
        # Perform in-memory deletion first
        data = self.data[i]

        if self.primary_key:
            keys_to_delete = {e[self.primary_key] for e in data if condition_fn(e)}


        filtered = [e for e in data if not condition_fn(e)]
        self.data[i] = filtered  # Update in-memory data

        if self.primary_key:
            self.primary_key_set.difference_update(keys_to_delete) # Remove from primary key set

        # Now save the partition to disk
        with open(self.file_paths[i], 'wb') as f:
            pickle.dump(filtered, f)

    def delete(self, condition_fn, by_id=False):
        if by_id:
            # Perform delete by ID using a single thread
            partition = self.hash_partition(condition_fn(self.primary_key))
            self._delete_from_partition(partition, condition_fn)
        else:
            # Perform delete for non-ID using multiple threads
            with Pool() as pool:
                pool.map(partial(self._delete_from_partition,
                         condition_fn=condition_fn), range(self.num_partitions))

    def _get_from_partition(self, i, condition_fn):
        data = self.data[i]  # In-memory data
        if condition_fn:
            return [e for e in data if condition_fn(e)]
        return data

    def get_data(self, condition_fn=None, by_id=False):
        if by_id:
            # Perform search by ID using a single thread
            partition = self.hash_partition(condition_fn(self.primary_key))
            return [e for e in self.data[partition] if condition_fn(e)]
        else:
            # Perform search for non-ID using multiple threads
            with Pool() as pool:
                results = pool.map(partial(
                    self._get_from_partition, condition_fn=condition_fn), range(self.num_partitions))
            return [item for sublist in results for item in sublist]

    def _update_partition(self, i, condition_fn, update_fn):
        # Perform in-memory update first
        data = self.data[i]
        updated = [update_fn(e) if condition_fn(e) else e for e in data]
        self.data[i] = updated  # Update in-memory data

        # Now save the partition to disk
        with open(self.file_paths[i], 'wb') as f:
            pickle.dump(updated, f)

    def update(self, condition_fn, update_fn, by_id=False):
        if by_id:
            # Perform update by ID using a single thread
            partition = self.hash_partition(condition_fn(self.primary_key))
            self._update_partition(partition, condition_fn, update_fn)
        else:
            # Perform update for non-ID using multiple threads
            with Pool() as pool:
                pool.map(partial(self._update_partition, condition_fn=condition_fn,
                         update_fn=update_fn), range(self.num_partitions))
        return True



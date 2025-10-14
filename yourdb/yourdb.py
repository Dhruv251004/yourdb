import os
import shutil
import types
import json
from typing import Dict
from .utils import is_valid_entity_name, is_valid_schema,YourDBEncoder, yourdb_decoder
from .entity import Entity
from multiprocessing import Pool

class YourDB:
    """
    A lightweight Python-based database engine that supports basic entity-based operations,
    including creation, deletion, insertion, querying, and updates with persistence.
    """

    def __init__(self, db_name):
        """
        Initializes a new or existing database.

        Args:
            db_name (str): Name of the database.
        """
        self.db_name = db_name
        self.db_path = os.path.join(os.getcwd(), db_name+'.yourdb')
        self.entities: Dict[str, Entity] = {}


        if not os.path.exists(self.db_path):
            os.makedirs(self.db_path)
        else:
            # Load existing entities from file
            for entity_folder in os.listdir(self.db_path):
                entity_path = os.path.join(self.db_path, entity_folder)
                if os.path.isdir(entity_path):
                    self.entities[entity_folder] = Entity(
                        entity_path, entity_folder)  # no schema passed

    def is_valid_entity(self, entity_name, schema):
        """
        Validates the entity name and schema.

        Args:
            entity_name (str): Name of the entity.
            schema (dict): Dictionary of field names and their types.

        Raises:
            Exception: If the entity name or schema is invalid.

        Returns:
            bool: True if valid.
        """
        if not is_valid_entity_name(entity_name):
            raise Exception(
                f"Invalid entity name: {entity_name}. Name must only contain alphanumeric characters and underscores.")

        if not is_valid_schema(schema):
            raise Exception(
                f"Invalid schema for entity: {entity_name}. Ensure the schema contains valid types and the primary key is defined.")

        return True

    def check_entity_existence(self, entity_name):
        """
        Checks whether the entity exists.

        Args:
            entity_name (str): Name of the entity.

        Raises:
            Exception: If the entity does not exist.

        Returns:
            bool: True if entity exists.
        """
        if entity_name not in self.entities:
            raise Exception(f"Entity {entity_name} does not exist.")
        return True

    def create_entity(self, entity_name, entity_schema):
        """
        Creates a new entity (table) with the given schema.

        Args:
            entity_name (str): Name of the entity.
            entity_schema (dict): Dictionary of field names and their types.

        Raises:
            Exception: If entity name is invalid or already exists.

        Returns:
            bool: True if creation is successful.
        """
        self.is_valid_entity(entity_name, entity_schema)

        if entity_name in self.entities:
            raise Exception(f"Entity {entity_name} already exists.")

        entity_path = os.path.join(self.db_path, entity_name)
        os.makedirs(entity_path)
        self.entities[entity_name] = Entity(
            entity_path, entity_name, entity_schema)
        return True

    def drop_entity(self, entity_name):
        """
        Drops an existing entity (table).

        Args:
            entity_name (str): Name of the entity to remove.

        Raises:
            Exception: If entity does not exist.

        Returns:
            bool: True if successfully deleted.
        """
        self.check_entity_existence(entity_name)
        entity_path = os.path.join(self.db_path, entity_name)
        shutil.rmtree(entity_path) # Use this instead of os.remove
        del self.entities[entity_name]
        return True


    def insert_into(self, entity_name, entity):
        """
        Inserts a new record into the specified entity.

        Args:
            entity_name (str): Name of the entity.
            entity (dict): The record to insert.

        Raises:
            Exception: If the entity does not exist or insertion fails.

        Returns:
            bool: True if inserted successfully.
        """
        self.check_entity_existence(entity_name)
        self.entities[entity_name].insert(entity)
        return True

    def list_entities(self):
        """
        Lists all entities in the database.

        Returns:
            list: A list of entity names.
        """
        return list(self.entities.keys())

    def select_from(self, entity_name, filter_dict: dict = None):
        """
        Selects records from an entity that match a given filter dictionary.
        Example: filter_dict={'department': 'Retail'}
        """
        return self.entities[entity_name].get_data(filter_dict)


    def delete_from(self, entity_name, filter_dict: dict):
        """
        Deletes records from an entity that satisfy the condition.

        Args:
            entity_name (str): Name of the entity.
            condition_fn (callable): A function that returns True for records to delete.

        Raises:
            Exception: If the entity does not exist.
        """
        self.check_entity_existence(entity_name)
        self.entities[entity_name].delete(filter_dict)

    def update_entity(self, entity_name, filter_dict: dict , update_fn):
        """
        Updates records in an entity that match the condition using the provided update function.

        Args:
            entity_name (str): Name of the entity.
            condition_fn (callable): Function to identify records to update.
            update_fn (callable): Function that modifies the record.

        Raises:
            Exception: If the entity does not exist.
        """
        self.check_entity_existence(entity_name)
        self.entities[entity_name].update(filter_dict, update_fn)

        #--- Aliases for consistency with test naming ---

    def update_into(self, entity_name, filter_dict, update_fn):
        """Alias for update_entity for API consistency."""
        return self.update_entity(entity_name, filter_dict, update_fn)


    def optimize_entity(self, entity_name):
        """
        Performs an eager migration on an entity.

        This reads all data, applies all necessary schema upgrades, and writes
        the fully upgraded data to new, clean log files. This process is safe
        and atomic.
        """

        self.check_entity_existence(entity_name)
        entity = self.entities[entity_name]
        print(f"--- Starting optimization for entity '{entity_name}' ---")

        # Acquire a write lock to prevent any other operations during optimization.
        with entity.lock.write():
            # 1. Get all data. The `get_data` call will trigger the lazy-read
            #    upgraders, giving us a fully modern set of objects in memory.
            all_records = entity._get_data_unlocked()

            # 2. Prepare temporary new log files.
            temp_files = [fp + ".optimizing" for fp in entity.file_paths]
            temp_file_handles = [open(fp, 'w') for fp in temp_files]

            try:
                # 3. Re-partition the upgraded data and write to temp files.
                for record in all_records:
                    pk_val = getattr(record, entity.primary_key)
                    partition = entity.hash_partition(pk_val)

                    log_entry = {"op": "INSERT", "data": record}
                    json_string = json.dumps(log_entry, cls=YourDBEncoder)
                    temp_file_handles[partition].write(json_string + '\n')

            except Exception as e:
                print(f"ERROR during optimization: {e}. Aborting. No changes were made.")
                # Clean up temp files on failure
                for h in temp_file_handles: h.close()
                for fp in temp_files: os.remove(fp)
                return False
            finally:
                for h in temp_file_handles: h.close()

            # 4. ATOMIC SWAP: Replace old logs with new ones.
            print("Optimization successful. Swapping to new log files...")
            for i in range(entity.num_partitions):
                os.replace(temp_files[i], entity.file_paths[i])

            # 5. Reload the entity's in-memory state from the new, clean logs.
            entity._load_from_logs()
            print(f"--- Optimization for '{entity_name}' complete. ---")
            return True
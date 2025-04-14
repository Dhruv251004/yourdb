import os
import pickle


class Entity:
    """
    Represents a single entity (table) in the database. Handles data storage, schema validation,
    and supports CRUD operations on in-memory data with persistence via pickle.
    """

    def __init__(self, db_path, name, schema=None):
        """
        Initializes an Entity. Loads existing data if the file exists, or creates a new file.

        Args:
            db_path (str): Path to the database directory.
            name (str): Name of the entity.
            schema (dict, optional): Schema for the entity. Required only during creation.
        """
        self.name = name
        self.schema = schema
        self.data = []
        self.file_path = os.path.join(db_path, f"{name}.entity")

        if os.path.exists(self.file_path):
            self.load_data()
        else:
            os.makedirs(os.path.dirname(self.file_path), exist_ok=True)
            self.save_data()

    def save_data(self):
        """
        Saves the current entity schema and data to a file using pickle.
        """
        with open(self.file_path, 'wb') as f:
            pickle.dump({
                'schema': self.schema,
                'data': self.data
            }, f)

    def load_data(self):
        """
        Loads entity schema and data from the corresponding pickle file.
        """
        with open(self.file_path, 'rb') as f:
            saved = pickle.load(f)
            self.schema = saved['schema']
            self.data = saved['data']

    def is_valid_entity(self, entity):
        """
        Validates that an entity matches the schema.

        Args:
            entity (dict): The data record to validate.

        Raises:
            Exception: If the format is invalid, a field is missing, or a value is of incorrect type.

        Returns:
            bool: True if the entity is valid.
        """
        if not isinstance(entity, dict):
            raise Exception("Invalid entity format. Entity should be a dictionary.")

        for key, value in entity.items():
            if key not in self.schema:
                raise Exception(f"Invalid field '{key}' in entity.")
            if not isinstance(value, self.schema[key]):
                raise Exception(f"Field '{key}' should be of type {self.schema[key].__name__}.")

        return True

    def insert(self, entity):
        """
        Inserts a new entity after validation.

        Args:
            entity (dict): The record to insert.

        Raises:
            Exception: If the record is invalid or a duplicate.

        Returns:
            bool: True if insertion is successful.
        """
        if self.is_valid_entity(entity):
            if entity in self.data:
                raise Exception("Duplicate entity.")
            self.data.append(entity)
            self.save_data()
            return True
        return False

    def delete(self, condition_fn):
        """
        Deletes records that match the given condition.

        Args:
            condition_fn (callable): A function that returns True for records to be deleted.
        """
        self.data = [entity for entity in self.data if not condition_fn(entity)]
        self.save_data()

    def get_data(self, condition_fn):
        """
        Retrieves records based on a filtering condition.

        Args:
            condition_fn (callable or None): A function that returns True for matching records.

        Returns:
            list: List of matching records. Returns all records if condition_fn is None.
        """
        if condition_fn is None:
            return self.data
        return [entity for entity in self.data if condition_fn(entity)]

    def update(self, condition_fn, update_fn):
        """
        Updates records that satisfy the condition using the update function.

        Args:
            condition_fn (callable): A function that selects records to update.
            update_fn (callable): A function that returns the modified version of the record.

        Returns:
            bool: True after successful update and persistence.
        """
        for i, entity in enumerate(self.data):
            if condition_fn(entity):
                self.data[i] = update_fn(entity)
        self.save_data()
        return True

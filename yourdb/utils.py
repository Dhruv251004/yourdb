import re
import json
import sys

SERIALIZABLE_CLASSES = {}

def register_class(cls):
    """A decorator that registers a class for automatic serialization."""
    SERIALIZABLE_CLASSES[cls.__name__] = cls
    return cls

#  Custom JSONEncoder to handle any Python class object
class YourDBEncoder(json.JSONEncoder):
    """
    Teaches the json module how to serialize custom objects.
    It converts an object into a dictionary with a special '__class__' marker.
    """
    def default(self, obj):
        if hasattr(obj, '__dict__'): # A simple way to check if it's a custom object
            return {
                "__class__": obj.__class__.__name__,
                "__data__": obj.__dict__
            }
        # Let the base class handle standard types (str, int, etc.)
        return json.JSONEncoder.default(self, obj)

# Custom decoder function to reconstruct Python objects from JSON ---
def yourdb_decoder(dct):
    """
    Checks for the '__class__' marker during JSON loading. If found,
    it looks up the class in our registry and reconstructs the original object.
    """
    if "__class__" in dct:
        class_name = dct["__class__"]
        cls = SERIALIZABLE_CLASSES.get(class_name)
        if cls:
            # create a blank instance of the class.
            obj = cls.__new__(cls)
            obj.__dict__.update(dct["__data__"])
            # Create a new instance of the class using its data
            return obj
    return dct

def is_valid_entity_name(entity_name: str) -> bool:
    """
    Check if the entity name is valid.
    :param entity_name: Name of the entity to check.
    :return: True if valid, False otherwise.
    """
    # Entity name should only contain alphanumeric characters and underscores and should not start with a number
    return bool(re.match(r"^[A-Za-z_][A-Za-z0-9_]*$", entity_name))


def is_valid_schema(entity_schema: dict) -> bool:
    """
    Check if the entity schema is a valid dictionary, ensuring there are valid field types.
    :param entity_schema: Schema of the entity to check.
    :param primary_key: The field to be considered as the primary key.
    :return: True if valid, False otherwise.
    """
    if not isinstance(entity_schema, dict) or not entity_schema:
        return False
    print(entity_schema)
    # Check if primary key exists in the schema
    if 'primary_key' not in entity_schema:
        return False
    # print(entity_schema)

    if entity_schema['primary_key'] not in entity_schema:
        return False
    # print(entity_schema)

    for field, field_type in entity_schema.items():
        if field == 'primary_key':
            continue

    return True

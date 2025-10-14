import re
import json
import sys

SERIALIZABLE_CLASSES = {}

# Stores the latest version of each registered class
# Format: { "ClassName": (class_object, latest_version) }
_CLASS_REGISTRY = {}

# Stores the functions that upgrade data from one version to the next
# Format: { ("ClassName", from_version, to_version): upgrade_function }
_UPGRADE_REGISTRY = {}


def register_class(cls):
    """
        A decorator that registers a class for serialization and version tracking.
        It automatically reads the `__version__` attribute from the class
    """
    class_name=cls.__name__
    version = getattr(cls, '__version__', 1)

    _CLASS_REGISTRY[class_name] = (cls, version)

    SERIALIZABLE_CLASSES[cls.__name__] = cls
    return cls

def register_upgrade(class_name, from_version, to_version):
    """
    A decorator factory to register a function that upgrades data for a class
    from one version to the next.
    """
    def decorator(func):
        key = (class_name, from_version, to_version)
        if key in _UPGRADE_REGISTRY:
            print(f"WARNING: Overwriting upgrade function for {key}")
        _UPGRADE_REGISTRY[key] = func
        return func
    return decorator


#  Custom JSONEncoder to handle any Python class object
class YourDBEncoder(json.JSONEncoder):
    """
    Teaches the json module how to serialize custom objects.
    It converts an object into a dictionary with a special '__class__' marker.
    """
    def default(self, obj):
        if hasattr(obj, '__dict__'):# A simple way to check if it's a custom object
            class_name = obj.__class__.__name__
            if class_name not in SERIALIZABLE_CLASSES:
                raise TypeError(f"Object of type {class_name} is not registered for serialization.Please use the @register_class decorator.")

            version = getattr(obj, '__version__', 1)


            return {
                "__class__": obj.__class__.__name__,
                "__version__": version,
                "__data__": obj.__dict__
            }
        # Let the base class handle standard types (str, int, etc.)
        return json.JSONEncoder.default(self, obj)

# Custom decoder function to reconstruct Python objects from JSON ---
def yourdb_decoder(dct):
    """
    Custom JSON decoder that handles object reconstruction and on-the-fly
    schema evolution by applying registered upgrade functions.
    """
    if "__class__" in dct:
        class_name = dct["__class__"]

        if class_name not in _CLASS_REGISTRY:
            raise TypeError(f"Unknown class '{class_name}' found during deserialization. Is it registered with @register_class?")

        cls, latest_version = _CLASS_REGISTRY[class_name]
        stored_version = dct.get("__version__", 1)
        data=dct["__data__"]

        if stored_version < latest_version:
            print(f"Found old '{class_name}' object (v{stored_version}). Upgrading to v{latest_version}...")
            current_version = stored_version

        # Upgrading data to the latest version if necessary
            while current_version < latest_version:
                next_version = current_version + 1
                upgrade_key = (class_name, current_version, next_version)

                 # Find the required upgrade function
                upgrader_func = _UPGRADE_REGISTRY.get(upgrade_key)
                if not upgrader_func:
                    raise RuntimeError(f"Missing upgrade path for '{class_name}' from v{current_version} to v{next_version}. Please register an upgrader.")

                # Apply the upgrade and move to the next version
                data = upgrader_func(data)
                current_version = next_version

        obj=cls.__new__(cls)
        obj.__dict__.update(data)
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
    if not isinstance(entity_schema, dict) or not entity_schema: return False
    if 'primary_key' not in entity_schema:  return False
    if entity_schema['primary_key'] not in entity_schema: return False

    for field, field_type in entity_schema.items():
        if field == 'primary_key':
            continue

    return True

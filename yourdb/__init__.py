"""
YourDB: A lightweight Python-native object database.

Modules:
    - YourDB: main database class
    - Entity: database entity class
    - utils: helper functions
"""

__version__ = "0.1.0"

# Import main classes
from .yourdb import YourDB
from .entity import Entity
from .utils import is_valid_entity_name, is_valid_schema, register_class

# Explicitly define exports
__all__ = ['YourDB', 'Entity', 'is_valid_entity_name',
           'is_valid_schema', 'register_class', 'register_upgrade']

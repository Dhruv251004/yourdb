# __init__.py for your DB module

# Import the main DB class
from .yourdb import YourDB

# Import utility functions for entity validation
from .utils import is_valid_entity_name, is_valid_schema

# Import the Entity class
from .entity import Entity

# Optional: Expose other helper functions or constants
__all__ = ['YourDB', 'Entity', 'is_valid_entity_name', 'is_valid_schema']

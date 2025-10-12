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


# Explicitly define exports
__all__ = ['YourDB', 'Entity']

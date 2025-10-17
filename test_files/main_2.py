import sys, os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))


from yourdb.utils import register_class
from yourdb import YourDB
import shutil
import os
import time


db = YourDB("test_db")

schema = {
    'primary_key': 'id',
    'id': "int",
    'name': "str",
    'age': "int",
    'is_active': "bool",
    'indexes': ['age', 'is_active']
}

db.create_entity("users", schema)
db.insert_into("users", {'id': 1, 'name': 'Alice',
               'age': 30, 'is_active': True})

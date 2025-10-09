# YourDB

**YourDB** is a lightweight, Python-native object database designed to persist and query Python objects with schema validation and SQL-like querying capabilities.

It allows developers to define entities using Python dictionaries (or class-like schemas), insert objects, and perform filtering, updating, or deleting — all using native Python.

---

## 🔍 Features

- 🧱 Define custom entities with schema validation
- 📦 Store any Python dictionary or object (pickle-backed)
- 🧠 Functional querying with lambda conditions
- 🛠 Update & delete data using custom logic
- 💾 Persistent storage using `pickle` under the hood
- 🔍 Future extensibility for SQL-like syntax and class-based schemas

---

## 📦 Installation

```bash
git clone https://github.com/Dhruv251004/yourdb
cd yourdb
pip install .
```


## 🏁 Quickstart

```python
# 1. Import the necessary components from yourdb
from yourdb import YourDB, register_class

# 2. Define your data model as a standard Python class
# The @register_class decorator is essential for the database to handle your object.
@register_class
class User:
    def __init__(self, user_id, name, city, is_active=True):
        self.user_id = user_id
        self.name = name
        self.city = city
        self.is_active = is_active

    def __repr__(self):
        # A nice string representation for printing the object
        return f"User(id={self.user_id}, name='{self.name}', city='{self.city}', active={self.is_active})"

# 3. Initialize the database (this will create a 'my_app.yourdb' directory)
db = YourDB("my_app")

# 4. Define a schema for your entity, including which fields to index
user_schema = {
    'primary_key': 'user_id',
    'user_id': "int",
    'name': "str",
    'city': "str",
    'is_active': "bool",
    'indexes': ['city'] # We'll create an index on the 'city' field for fast lookups
}

# 5. Create an entity (similar to a table)
db.create_entity("users", user_schema)

# 6. Insert your custom objects directly (no .to_dict() needed)
print("--> Inserting 3 users...")
db.insert_into("users", User(user_id=101, name="Alice", city="New York"))
db.insert_into("users", User(user_id=102, name="Bob", city="London"))
db.insert_into("users", User(user_id=103, name="Charlie", city="New York"))

# 7. Query data using an index for high performance
print("\n--> Fetching users from 'New York' (uses the 'city' index)...")
ny_users = db.select_from("users", filter_dict={'city': 'New York'})
print(ny_users)

# 8. Update data using a filter dictionary
print("\n--> Deactivating Bob...")
def deactivate_user(user):
    user.is_active = False
    return user

# The update operation will use a full scan because 'name' is not indexed
db.update_entity("users", filter_dict={'name': 'Bob'}, update_fn=deactivate_user)

# Verify the update by fetching the record again
bob = db.select_from("users", filter_dict={'user_id': 102})[0]
print(f"Verified update: {bob}")

# 9. Delete data using a filter
print("\n--> Deleting user 101...")
db.delete_from("users", filter_dict={'user_id': 101})

# Verify the deletion by fetching all remaining users
all_users = db.select_from("users")
print(f"Final users in DB: {all_users}")
```

## 📁 Directory Structure

<pre>
yourdb/
├── .gitignore
├── LICENSE
├── MANIFEST.in
├── pyproject.toml      # Project configuration (replaces requirements.txt)
├── Readme.md
│
├── test_files/         # Contains benchmark and test scripts
│   ├── fetch_test.py
│   └── main.py
│
└── yourdb/             # The core source code of the database package
    ├── __init__.py     # Makes 'yourdb' a Python package
    ├── compaction.py   # Handles log file compaction logic
    ├── entity.py       # Core storage engine and entity-level logic
    ├── utils.py        # Serialization, validation, and helper functions
    └── yourdb.py       # Main public API and DB interface
</pre>

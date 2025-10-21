# YourDB

**YourDB** is a lightweight, Python-native object database designed to persist and query Python objects with schema validation and SQL-like querying capabilities.

It allows developers to define entities using Python dictionaries (or class-like schemas), insert objects, and perform filtering, updating, or deleting — all using native Python.

---
**➡️ View the live documentation site deployed on Vercel:** [https://vercel.com/aayushman-guptas-projects/your-db-official-docs](https://your-db-official-docs.vercel.app/)
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


## 🏁 Follow the Quickstart guide

```python
# 1. Import the necessary components from yourdb
from yourdb import YourDB, register_class

# 2. Define your data model as a standard Python class
# The @register_class decorator is essential for the database to handle your object.
@register_class
class User:
    def __init__(self, user_id, name, city, age):
        self.user_id = user_id
        self.name = name
        self.city = city
        self.age = age
    
    def __repr__(self):
        return f"User(id={self.user_id}, name='{self.name}', city='{self.city}', age={self.age})"

# 3. Initialize the database
db = YourDB("my_app")

# 4. Define a schema, including which fields to index
user_schema = {
    'primary_key': 'user_id',
    'user_id': "int",
    'name': "str",
    'city': "str",
    'age': "int",
    'indexes': ['city'] # Create an index on 'city' for fast lookups
}

# 5. Create an entity (like a table)
db.create_entity("users", user_schema)

# 6. Insert your custom objects directly
print("--> Inserting users...")
db.insert_into("users", User(user_id=101, name="Alice", city="New York", age=28))
db.insert_into("users", User(user_id=102, name="Bob", city="London", age=35))
db.insert_into("users", User(user_id=103, name="Charlie", city="New York", age=42))

# 7. Query data using an index for high performance
print("\n--> Fetching users from 'New York' (uses the 'city' index)...")
ny_users = db.select_from("users", filter_dict={'city': 'New York'})
print(ny_users)

# 8. Perform an advanced query with operators
print("\n--> Fetching users older than 30 (uses a full scan)...")
older_users = db.select_from("users", filter_dict={'age': {'$gt': 30}})
print(older_users)

# 9. Update data using a filter
print("\n--> Updating Charlie's city to 'Tokyo'...")
def update_city(user):
    user.city = "Tokyo"
    return user
db.update_entity("users", filter_dict={'name': 'Charlie'}, update_fn=update_city)

# 10. Delete data using a filter
print("\n--> Deleting user 102...")
db.delete_from("users", filter_dict={'user_id': 102})

# Verify by fetching all remaining users
all_users = db.select_from("users")
print(f"\nFinal users in DB: {all_users}")
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

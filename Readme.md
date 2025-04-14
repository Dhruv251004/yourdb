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

git clone https://github.com/Dhruv251004/yourdb
cd yourdb
pip install .

## 🏁 Quickstart

from yourdb.yourdb import YourDB

# Create or connect to a DB

db = YourDB("my_database")

# Define entity schema (like a table schema)

schema = {
"id": int,
"name": str,
"is_active": bool
}
db.create_entity("users", schema)

# Insert data

user1 = {"id": 1, "name": "Alice", "is_active": True}
db.insert_into("users", user1)

# Query data

results = db.select_from("users", lambda u: u["is_active"])
print(results)

# Update data

db.update_entity("users", lambda u: u["name"] == "Alice", lambda u: {\*\*u, "is_active": False})

# Delete data

db.delete_from("users", lambda u: u["id"] == 1)

## 📁 Directory Structure

<pre>
yourdb/
│
├── yourdb/ # Core module
│ ├── **init**.py
│ ├── yourdb.py # Main DB interface
│ ├── entity.py # Entity-level logic
│ ├── utils.py # Schema validation
│ └── test.py # Basic tests
│
├── LICENSE
├── MANIFEST.in
├── Readme.md
└── requirements.txt
</pre>

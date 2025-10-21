# YourDB

**YourDB** is a lightweight, Python-native object database designed to persist and query Python objects with schema validation and SQL-like querying capabilities.

It allows developers to define entities using Python dictionaries (or class-like schemas), insert objects, and perform filtering, updating, or deleting — all using native Python.

---
**➡️ View the live documentation site deployed on Vercel:** [https://vercel.com/aayushman-guptas-projects/your-db-official-docs](https://your-db-official-docs.vercel.app/)
---

## 🔍 Features

* 🚀 **Object-Native Persistence**: Your classes *are* the database. Use the `@register_class` decorator on your Python classes and store/retrieve instances directly. `yourdb` handles serialization automatically.
* 🧬 **Hybrid Schema Evolution**: Effortlessly evolve your data models over time without risky migration scripts!
    * **Lazy Read:** Automatically upgrades old data objects *in memory* on-the-fly using simple `@register_upgrade` functions. Your application code only ever sees the latest version.
    * **Eager Migration:** An optional `db.optimize_entity()` tool safely rewrites data files *on disk* to the latest version for performance tuning.
* ⚡ **High-Performance & Thread-Safe**: Built on an append-only log for fast writes and an in-memory cache for rapid reads. A robust **writer-preference lock** ensures data integrity under high concurrency.
* 🧠 **Advanced Querying**: Go beyond simple lookups. Use a `filter_dict` with MongoDB-style operators like `$gt`, `$lt`, `$gte`, `$lte`, and `$ne` for expressive queries.
* 🔍 **Indexing**: Define indexes on specific fields within your schema to significantly accelerate lookups for common queries.
* ⚙️ **Automatic Compaction**: A background process automatically cleans up log files, removing redundant data to save space and speed up load times.
* 🌐 **Zero-Dependency**: Pure Python. No external database servers to install or manage. Perfect for serverless, edge, desktop apps, or simplifying your stack.

---

## 📦 Installation

```bash
git clone https://github.com/Dhruv251004/yourdb
cd yourdb
pip install .
```


## 🏁 Follow the Quickstart guide

```python
from yourdb import YourDB, register_class, register_upgrade

# --- 1. Define Your Data Model (Initial Version) ---
@register_class
class User:
    __version__ = 1
    def __init__(self, name, email):
        self.user_id = None
        self.name = name
        self.email = email
    def __repr__(self):
        return f"User(v{self.__version__}, id={self.user_id}, name='{self.name}', email='{self.email}')"

# --- 2. Initialize Database & Entity ---
db = YourDB("my_app_db")
user_schema = {
    'primary_key': 'user_id',
    'user_id': "int",
    'name': "str",
    'email': "str",
    'indexes': ['email'] # Index the email field
}
# Creates entity if it doesn't exist, loads if it does
db.create_entity("users", user_schema)

# --- 3. Insert Data ---
alice = User(name="Alice Smith", email="alice@example.com")
alice.user_id = 101
db.insert_into("users", alice)

# --- 4. Select Data ---
retrieved_alice = db.select_from("users", {'email': 'alice@example.com'})[0]
print(f"Found: {retrieved_alice}")

# --- 5. Evolve Schema (Example) ---
# Imagine you deploy new code with User v2 and an upgrader

@register_upgrade("User", from_version=1, to_version=2)
def upgrade_v1_to_v2(data):
    data['status'] = 'active' # Add a new field with a default
    return data

@register_class
class User: # Redefine the class in your new code
    __version__ = 2
    def __init__(self, name, email, status='active'):
        self.user_id = None
        self.name = name
        self.email = email
        self.status = status
    def __repr__(self):
         return f"User(v{self.__version__}, id={self.user_id}, name='{self.name}', email='{self.email}', status='{self.status}')"

# --- 6. Read Data After Evolution ---
# Re-initialize DB instance simulates app restart with new code
db_reloaded = YourDB("my_app_db")
alice_v2 = db_reloaded.select_from("users", {'user_id': 101})[0]
print(f"After Evolution: {alice_v2}") # Alice is now a v2 object with status='active'!

# --- 7. Optimize (Optional) ---
# db_reloaded.optimize_entity("users") # Rewrites log files to contain only v2 data
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
    ├── __init__.py     # Package definition & public exports
    ├── compaction.py   # Log file compaction logic
    ├── entity.py       # Core storage engine, in-memory cache, indexing
    ├── locking.py      # Reader-Writer lock for concurrency
    ├── utils.py        # Serialization, validation, schema evolution helpers
    └── yourdb.py       # Main public API (YourDB class)
</pre>

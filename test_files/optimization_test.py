import sys, os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
import shutil
import json
from yourdb import YourDB
from yourdb.utils import register_class, register_upgrade

# --- Test Setup ---
DB_NAME = "optimization_test_db"
DB_DIR = f"{DB_NAME}.yourdb"
if os.path.exists(DB_DIR):
    shutil.rmtree(DB_DIR)

# ==============================================================================
# PHASE 1: Create a "messy" database with mixed versions ockear
# n disk
# ==============================================================================
print("--- PHASE 1: Setting up an inconsistent database state... ---")

# The app starts at v1
@register_class
class Item:
    __version__ = 1
    def __init__(self, name, quantity):
        self.item_id = None
        self.name = name
        self.quantity = quantity

db = YourDB(DB_NAME)
db.create_entity("inventory", {'primary_key': 'item_id', 'item_id': "int", 'name': "str", 'quantity': 'int'})
item1 = Item("Apple", 50); item1.item_id = 1; db.insert_into("inventory", item1)
item2 = Item("Banana", 75); item2.item_id = 2; db.insert_into("inventory", item2)
del Item # Simulate app restart

# The app evolves to v2 (adds 'category')
@register_upgrade("Item", from_version=1, to_version=2)
def upgrade_v1_to_v2(data):
    data["category"] = "Unknown" # Default category for old items
    return data
@register_class
class Item:
    __version__ = 2
    def __init__(self, name, quantity, category="Fruit"):
        self.item_id = None
        self.name = name
        self.quantity = quantity
        self.category = category

db_v2 = YourDB(DB_NAME)
item3 = Item("Carrot", 30, "Vegetable"); item3.item_id = 3; db_v2.insert_into("inventory", item3)
print("Setup complete. ON-DISK STATE: item1 & item2 are v1, item3 is v2.\n")
del Item

# Helper function to peek at the raw, untranslated data on disk
def peek_at_raw_data(database):
    final_state = {}
    entity = database.entities["inventory"]
    for fp in entity.file_paths:
        if not os.path.exists(fp): continue
        with open(fp, 'r') as f:
            for line in f:
                if not line.strip(): continue
                log_entry = json.loads(line)
                if log_entry['op'] == 'INSERT':
                    pk = log_entry['data']['__data__'][entity.primary_key]
                    final_state[pk] = log_entry['data']
    print("Peeking at raw on-disk data:")
    for data in sorted(final_state.values(), key=lambda d: d['__data__']['item_id']):
        version = data.get('__version__', 1)
        print(f"   -> Raw Object (v{version}) | Data: {data['__data__']}")

# Verify the "before" state
peek_at_raw_data(db_v2)
items_before = sorted(db_v2.select_from("inventory"), key=lambda i: i.item_id)
count_before = len(items_before)
print("\n" + "="*70 + "\n")


# ==============================================================================
# PHASE 2: Run the Eager Migration (Optimization)
# ==============================================================================
print(f"--- PHASE 2: Running optimize_entity() on 'inventory' ---")
# The application code is still at v2 when we run this
@register_class
class Item:
    __version__ = 2
    def __init__(self, name, quantity, category="Fruit"):
        self.item_id, self.name, self.quantity, self.category = None, name, quantity, category

db_v2.optimize_entity("inventory")
print("\n" + "="*70 + "\n")


# ==============================================================================
# PHASE 3: Verify the "after" state
# ==============================================================================
print(f"--- PHASE 3: Verifying the results ---")
# The in-memory data should be reloaded and still correct
items_after = sorted(db_v2.select_from("inventory"), key=lambda i: i.item_id)
count_after = len(items_after)

# The raw data on disk should now be clean and consistent
peek_at_raw_data(db_v2)

# Assertions to prove correctness
assert count_before == count_after, "Data was lost during optimization!"
assert items_before[0].name == items_after[0].name, "Data was corrupted!"
assert items_after[0].category == "Unknown", "Lazy upgrade during optimization failed!"

print("\n✅ PASSED: Eager migration successful!")
print("   - The number of records is unchanged.")
print("   - The on-disk data is now consistently at the latest version (v2).")
print("   - Data integrity was maintained throughout the process.")
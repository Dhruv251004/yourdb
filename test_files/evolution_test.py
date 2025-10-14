import sys, os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
import shutil
import json
from yourdb import YourDB
from yourdb.utils import register_class, register_upgrade

# --- Test Setup ---
DB_NAME = "evolution_stress_test_db"
DB_DIR = f"{DB_NAME}.yourdb"
if os.path.exists(DB_DIR):
    shutil.rmtree(DB_DIR)

# --- Define a Full History of the 'User' Class and its Upgraders ---

# Upgrade v1 -> v2: Adds an optional middle name
@register_upgrade("User", from_version=1, to_version=2)
def upgrade_user_v1_to_v2(old_data):
    """Adds a 'middle_name' field with a default None value."""
    
    print("   -> Running v1 to v2 upgrader...")
    old_data["middle_name"] = None
    print("Added 'middle_name' field with default None")
    return old_data
print("----------------------------------------------------------------------------------------------")
# Upgrade v2 -> v3: Splits the full name into first and last names
@register_upgrade("User", from_version=2, to_version=3)
def upgrade_user_v2_to_v3(old_data):
    """Splits the 'name' field into 'first_name' and 'last_name'."""
    print("   -> Running v2 to v3 upgrader...")
    full_name = old_data.pop("name", "")
    parts = full_name.split(" ", 1)
    first = parts[0] if parts else ""
    last = parts[1] if len(parts) > 1 else ""

    print(f"Split '{full_name}' into first='{first}' and last='{last}'")

    print("-------------------------------------------------------------------------------------------------")

    # Reconstruct the data dictionary for the new class version
    return {
        "user_id": old_data["user_id"],
        "first_name": first,
        "last_name": last,
        "middle_name": old_data.get("middle_name")
    }

# This simulates the CURRENT state of your code (Version 3)
@register_class
class User:
    __version__ = 3

    def __init__(self, first_name, last_name, middle_name=None):
        self.user_id = None # Will be set during insertion
        self.first_name = first_name
        self.last_name = last_name
        self.middle_name = middle_name

    def __repr__(self):
        middle = f"'{self.middle_name}'" if self.middle_name else "None"
        return f"User(v{self.__version__}, id={self.user_id}, name='{self.first_name} {self.last_name}', middle={middle})"

# --- Test Execution ---

print("--- Phase 1: Storing data from multiple eras ---")
db = YourDB(DB_NAME)
schema = {
    'primary_key': 'user_id',
    'user_id': "int",
    'first_name': "str",
    'last_name': "str",
    'middle_name': "str"
}
db.create_entity("users", schema)

# 1. Insert a brand new user with the v3 class
user_v3 = User(first_name="Alice", last_name="Williams", middle_name="Marie")
user_v3.user_id = 301
db.insert_into("users", user_v3)
print(f"Stored modern v3 user: {user_v3}")

# 2. Manually inject a v2 user (has 'name' and 'middle_name')
user_v2_data = json.dumps({
    "op": "INSERT", "data": {
        "__class__": "User", "__version__": 2,
        "__data__": {"user_id": 201, "name": "Bob Johnson", "middle_name": "Jay"}
    }
})

# 3. Manually inject a v1 user (only has 'name')
user_v1_data = json.dumps({
    "op": "INSERT", "data": {
        "__class__": "User", # No version, defaults to 1
        "__data__": {"user_id": 101, "name": "Charlie Brown"}
    }
})

# Append old data to log files
log_file_path = os.path.join(DB_DIR, "users", "users_shard_1.log") # for user_id 101 and 201
with open(log_file_path, "a") as f:
    f.write(user_v1_data + "\n")
    f.write(user_v2_data + "\n")
print("Manually injected legacy v1 and v2 users into the log file.\n")


# --- Verification ---
print("--- Phase 2: Reloading database with current code (User v3) ---")
reloaded_db = YourDB(DB_NAME)
all_users = sorted(reloaded_db.select_from("users"), key=lambda u: u.user_id)

print("\n--- Verifying Data ---")
print("Objects retrieved from DB after on-the-fly upgrades:")
for user in all_users:
    print(f"   {user}")

# Assertions to prove it worked
v1_upgraded = all_users[0]
v2_upgraded = all_users[1]
v3_original = all_users[2]

# Verify the v1 user, which was upgraded TWICE (v1->v2, then v2->v3)
assert v1_upgraded.user_id == 101
assert v1_upgraded.first_name == "Charlie"
assert v1_upgraded.last_name == "Brown"
assert v1_upgraded.middle_name is None, "v1->v2 upgrader should have added middle_name=None"

# Verify the v2 user, which was upgraded ONCE (v2->v3)
assert v2_upgraded.user_id == 201
assert v2_upgraded.first_name == "Bob"
assert v2_upgraded.last_name == "Johnson"
assert v2_upgraded.middle_name == "Jay", "v2->v3 upgrader should have preserved the middle name"

# Verify the v3 user, which was not upgraded
assert v3_original.user_id == 301
assert v3_original.first_name == "Alice"
assert v3_original.middle_name == "Marie"

print("\n✅ PASSED: Chained schema evolution test successful. Legacy data was correctly transformed on read.")
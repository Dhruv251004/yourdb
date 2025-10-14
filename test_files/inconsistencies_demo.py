import sys, os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
import shutil
from yourdb import YourDB
from yourdb.utils import register_class, register_upgrade

# --- Test Setup ---
DB_NAME = "real_inconsistency_db"
DB_DIR = f"{DB_NAME}.yourdb"
if os.path.exists(DB_DIR):
    shutil.rmtree(DB_DIR)

# ==============================================================================
# ERA 1: The application is at v1.
# ==============================================================================
print("--- ERA 1: Application at v1 ---")

@register_class
class User:
    __version__ = 1
    def __init__(self, name):
        self.user_id = None
        self.name = name
    def __repr__(self):
        return f"User(v{self.__version__}, id={self.user_id}, name='{self.name}')"

db = YourDB(DB_NAME)
db.create_entity("users", {'primary_key': 'user_id', 'user_id': "int", 'name': "str"})

# We store two v1 users. On disk, they are v1.
# FIX: Create object, then set its ID in a separate step for clarity.
alice = User("Alice Smith")
alice.user_id = 101
db.insert_into("users", alice)

bob = User("Bob Johnson")
bob.user_id = 102
db.insert_into("users", bob)

print("Initial state: Alice and Bob are v1 on disk.")
del User  # Simulate app restart
print("\n" + "="*70 + "\n")

# ==============================================================================
# ERA 2: App evolves to v2 (adds 'middle_name').
# ==============================================================================
print("--- ERA 2: Application is now at v2 ---")

@register_upgrade("User", from_version=1, to_version=2)
def upgrade_v1_to_v2(data):
    data["middle_name"] = None # Default value for old users
    return data

@register_class
class User:
    __version__ = 2
    def __init__(self, name, middle_name=None):
        self.user_id = None
        self.name = name
        self.middle_name = middle_name
    def __repr__(self):
        middle = f", middle='{self.middle_name}'" if self.middle_name is not None else ""
        return f"User(v{self.__version__}, id={self.user_id}, name='{self.name}'{middle})"

db_v2 = YourDB(DB_NAME)
    
# We update Bob, so his record is physically written to disk as a v2 object.
print("Updating Bob (102) to add a middle name. This writes a v2 record to the log.")
db_v2.update_entity("users", {'user_id': 102}, lambda u: setattr(u, 'middle_name', 'Danger') or u)

# We add Charlie, who is born as a v2 object.
charlie = User("Charlie Brown", "Noel")
charlie.user_id = 103
db_v2.insert_into("users", charlie)
print("Adding Charlie (103) as a new v2 user.")

print("\n--- Reading data from the v2 application's perspective ---")
print("ON-DISK STATE: Alice is v1, Bob is v2, Charlie is v2.")
print("IN-MEMORY RESULT: YourDB upgrades Alice to v2 on-the-fly.")
for user in sorted(db_v2.select_from("users"), key=lambda u: u.user_id):
    print(f"   -> {user}")
del User # Simulate app restart
print("\n" + "="*70 + "\n")


# ==============================================================================
# ERA 3: App evolves to v3 (splits 'name').
# ==============================================================================
print("--- ERA 3: Application is now at v3 ---")

@register_upgrade("User", from_version=2, to_version=3)
def upgrade_v2_to_v3(data):
    name = data.pop("name", "")
    parts = name.split(" ", 1)
    return {
        "user_id": data["user_id"],
        "first_name": parts[0] if parts else "",
        "last_name": parts[1] if len(parts) > 1 else "",
        "middle_name": data.get("middle_name")
    }

@register_class
class User:
    __version__ = 3
    def __init__(self, first_name, last_name, middle_name=None):
        self.user_id = None
        self.first_name = first_name
        self.last_name = last_name
        self.middle_name = middle_name
    def __repr__(self):
        middle = f", middle='{self.middle_name}'" if self.middle_name else ""
        return f"User(v{self.__version__}, id={self.user_id}, first='{self.first_name}', last='{self.last_name}'{middle})"

db_v3 = YourDB(DB_NAME)
print("\n--- Reading data from the v3 application's perspective ---")
print("ON-DISK STATE: Alice (v1), Bob (v2), Charlie (v2).")
print("IN-MEMORY RESULT: YourDB performs chained upgrades (v1->v2->v3) as needed.")
for user in sorted(db_v3.select_from("users"), key=lambda u: u.user_id):
    print(f"   -> {user}")

print("\n--- Final Analysis ---")
print("The inconsistency of multiple versions stored on disk is resolved during the read,")
print("giving your application a perfectly consistent and up-to-date view of the data.")
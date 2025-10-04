import time
import os
import shutil
from yourdb import YourDB
from yourdb.utils import register_class

# --- Both custom classes must be registered ---
@register_class
class ContactDetails:
    def __init__(self, email, phone):
        self.email = email
        self.phone = phone

    def __repr__(self):
        return f"ContactDetails(Email='{self.email}', Phone='{self.phone}')"

@register_class
class BankEmployee:
    def __init__(self, emp_id, name, department, email, phone, is_manager=False):
        self.emp_id = emp_id
        self.name = name
        self.department = department
        self.is_manager = is_manager
        self.contact_details = ContactDetails(email, phone)

    def __repr__(self):
        return f"BankEmployee(ID={self.emp_id}, Name='{self.name}')"

# --- Configuration ---
DB_NAME = "fetch_test_db"
ENTITY_NAME = "employees"
NUM_OBJECTS = 10000

# --- Schema to match the BankEmployee class ---
EMPLOYEE_SCHEMA = {
    'primary_key': 'emp_id',
    'emp_id': "int",
    'name': "str",
    'department': "str",
    'is_manager': "bool",
    'contact_details': "ContactDetails"
}

def setup_database():
    """A silent setup function to populate the database with test data."""
    print("--- Phase 1: Setting up the database ---")
    db_path = os.path.join(os.getcwd(), DB_NAME + '.yourdb')
    if os.path.exists(db_path):
        shutil.rmtree(db_path)

    db = YourDB(DB_NAME)
    db.create_entity(ENTITY_NAME, EMPLOYEE_SCHEMA)

    employees_to_insert = [
        BankEmployee(
            emp_id=1000 + i,
            name=f'employee_{i}',
            department='retail',
            email=f'emp_{i}@bank.com',
            phone=f'555-0100-{i:04d}',
            is_manager=(i % 100 == 0) # Every 100th employee is a manager
        ) for i in range(NUM_OBJECTS)
    ]

    for employee in employees_to_insert:
        db.insert_into(ENTITY_NAME, employee)
    print(f"{NUM_OBJECTS} objects inserted. Setup complete.\n")

def run_fetch_benchmark():
    """Times various fetch (select) operations."""
    print(f"--- Phase 2: Starting YourDB Fetch Benchmark ---")

    # --- 1. Time Reloading from Disk ---
    # This simulates a "cold start" for your application
    start_time = time.time()
    db = YourDB(DB_NAME)
    duration_reload = time.time() - start_time

    # --- 2. Time Fetching a SINGLE Object by ID ---
    id_to_find = 1777 # An arbitrary ID we know exists
    condition_id = lambda emp: emp.emp_id == id_to_find
    start_time = time.time()
    found_employee = db.select_from(ENTITY_NAME, condition_fn=condition_id)
    duration_fetch_one = time.time() - start_time

    # --- 3. Time Fetching a SUBSET of Objects with a filter ---
    condition_managers = lambda emp: emp.is_manager is True
    start_time = time.time()
    managers = db.select_from(ENTITY_NAME, condition_fn=condition_managers)
    duration_fetch_filtered = time.time() - start_time

    # --- 4. Display Results ---
    print("\n--- Fetch Benchmark Results ---")
    print(f"Time to Reload {NUM_OBJECTS} objects from disk: {duration_reload:.4f} seconds")
    print("--------------------------------------------------")
    print(f"Time to Fetch 1 object by ID: {(duration_fetch_one * 1000):.4f} ms")
    print(f"Time to Fetch {len(managers)} manager objects: {(duration_fetch_filtered * 1000):.4f} ms")
    print("--------------------------------------------------")

    # Verification to ensure the fetched data is correct
    if found_employee:
        print(f"Verified found employee: {found_employee[0]}")
    else:
        print("Verification FAILED: Did not find employee by ID.")

if __name__ == "__main__":
    setup_database()
    run_fetch_benchmark()
import sys, os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

import time
import os
import shutil
from yourdb import YourDB
from yourdb.utils import register_class

# --- Define and Register All Custom Classes ---
@register_class
class ContactDetails:
    def __init__(self, email, phone):
        self.email = email
        self.phone = phone

@register_class
class BankEmployee:
    def __init__(self, emp_id, name, department, email, phone, is_manager=False):
        self.emp_id = emp_id
        self.name = name
        self.department = department
        self.is_manager = is_manager
        self.contact_details = ContactDetails(email, phone)

# --- Configuration ---
DB_NAME = "final_fetch_db"
ENTITY_NAME = "employees"
NUM_OBJECTS = 10000

# --- Define Schema, including the fields to be indexed ---
EMPLOYEE_SCHEMA = {
    'primary_key': 'emp_id',
    'emp_id': "int", 'name': "str", 'department': "str",
    'is_manager': "bool", 'contact_details': "ContactDetails",
    'indexes': ['department'] # We will index the 'department' field
}

def setup_database():
    """A silent setup function to populate the database with test data."""
    print("--- Phase 1: Setting up the database for fetch tests ---")
    db_path = os.path.join(os.getcwd(), DB_NAME + '.yourdb')
    if os.path.exists(db_path):
        shutil.rmtree(db_path)

    db = YourDB(DB_NAME)
    db.create_entity(ENTITY_NAME, EMPLOYEE_SCHEMA)

    employees_to_insert = [
        BankEmployee(
            emp_id=1000 + i, name=f'employee_{i}', department=f'dept_{i % 10}',
            email=f'emp_{i}@bank.com', phone=f'555-0100-{i:04d}',
            is_manager=(i % 100 == 0)
        ) for i in range(NUM_OBJECTS)
    ]
    for employee in employees_to_insert:
        db.insert_into(ENTITY_NAME, employee)
    print(f"{NUM_OBJECTS} objects inserted. Setup complete.\n")

def run_fetch_benchmark():
    """Times various fetch scenarios to test performance."""
    print(f"--- Phase 2: Starting Final Fetch Benchmark ---")

    # --- 1. Time Reloading from Disk (Cold Start) ---
    start_time = time.time()
    db = YourDB(DB_NAME)
    duration_reload = time.time() - start_time

    # --- 2. Time an Indexed Query (Fast Path) ---
    # We are filtering on 'department', which is an indexed field.
    indexed_filter = {'department': 'dept_5'}
    start_time = time.time()
    indexed_results = db.select_from(ENTITY_NAME, filter_dict=indexed_filter)
    duration_indexed = time.time() - start_time

    # --- 3. Time a Non-Indexed Query (Full Scan) ---
    # We are filtering on 'name', which is NOT indexed.
    non_indexed_filter = {'name': 'employee_777'}
    start_time = time.time()
    scan_results = db.select_from(ENTITY_NAME, filter_dict=non_indexed_filter)
    duration_scan = time.time() - start_time

    # --- 4. Display Results ---
    print("\n--- Fetch Benchmark Results ---")
    print(f"Time to Reload {NUM_OBJECTS} objects from disk: {duration_reload:.4f} seconds")
    print("--------------------------------------------------")
    print(f"Indexed Query ({len(indexed_results)} records found): {(duration_indexed * 1000):.4f} ms")
    print(f"Full Scan Query ({len(scan_results)} record found): {(duration_scan * 1000):.4f} ms")
    print("--------------------------------------------------")

    # Verification
    assert len(indexed_results) == NUM_OBJECTS / 10
    assert scan_results[0].emp_id == 1777
    print("Verification successful.")

if __name__ == "__main__":
    setup_database()
    run_fetch_benchmark()
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
DB_NAME = "final_insert_db"
ENTITY_NAME = "employees"
NUM_OBJECTS = 10000

# --- Define Schema, including the fields to be indexed ---
EMPLOYEE_SCHEMA = {
    'primary_key': 'emp_id',
    'emp_id': "int",
    'name': "str",
    'department': "str",
    'is_manager': "bool",
    'contact_details': "ContactDetails",
    'indexes': ['department', 'is_manager'] # Specify fields to index
}

def run_insertion_benchmark():
    """Initializes the DB and benchmarks the insertion of 10,000 objects."""
    # --- 1. Cleanup ---
    db_path = os.path.join(os.getcwd(), DB_NAME + '.yourdb')
    if os.path.exists(db_path):
        shutil.rmtree(db_path)

    print(f"\n--- Starting Final Insertion Benchmark ---")

    # --- 2. Setup ---
    db = YourDB(DB_NAME)
    db.create_entity(ENTITY_NAME, EMPLOYEE_SCHEMA)
    print(f"Inserting {NUM_OBJECTS} objects...")

    employees_to_insert = [
        BankEmployee(
            emp_id=1000 + i, name=f'employee_{i}', department=f'dept_{i % 10}',
            email=f'emp_{i}@bank.com', phone=f'555-0100-{i:04d}',
            is_manager=(i % 100 == 0)
        ) for i in range(NUM_OBJECTS)
    ]

    # --- 3. Run and Time the Insertion ---
    start_time = time.time()
    for employee in employees_to_insert:
        db.insert_into(ENTITY_NAME, employee)
    duration = time.time() - start_time
    print("Insertion complete.\n")

    # --- 4. Display Results ---
    avg_time_ms = (duration / NUM_OBJECTS) * 1000 if NUM_OBJECTS > 0 else 0

    print("--- Insertion Benchmark Results ---")
    print(f"Total objects inserted: {NUM_OBJECTS}")
    print(f"Total time taken: {duration:.4f} seconds")
    print(f"Average time per insert (including indexing/compaction): {avg_time_ms:.4f} ms")
    print("-----------------------------------")

if __name__ == "__main__":
    run_insertion_benchmark()
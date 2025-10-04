import time
import os
import shutil
from yourdb import YourDB
# --- NEW: Import the class registration decorator ---
from yourdb.utils import register_class




class ContactDetails:
    def __init__(self, email, phone):
        self.email = email
        self.phone = phone

    def __repr__(self):
        return f"ContactDetails(Email='{self.email}', Phone='{self.phone}')"

# --- NEW: Define and register a custom class to be stored in the DB ---
@register_class
class BankEmployee:
    def __init__(self, emp_id, name, department, email_id,phone_no,is_manager=False):
        self.emp_id = emp_id
        self.name = name
        self.department = department
        self.is_manager = is_manager
        self.contact_details=ContactDetails(email_id,phone_no)

    def __repr__(self):
        # This provides a nice print output for the object
        return f"BankEmployee(ID={self.emp_id}, Name='{self.name}')"

# --- Configuration ---
DB_NAME = "json_object_benchmark_db"
ENTITY_NAME = "employees"
NUM_OBJECTS = 10000

# --- The schema describes the attributes of our BankEmployee object ---
EMPLOYEE_SCHEMA = {
    'primary_key': 'emp_id',
    'emp_id': "int",
    'name': "str",
    'department': "str",
    'is_manager': "bool",
    'contact_details': "ContactDetails"
}

def run_benchmark():
    """
    Initializes the database, inserts 10,000 custom objects, and measures the time.
    """
    # --- 1. Cleanup and Setup ---
    db_path = os.path.join(os.getcwd(), DB_NAME + '.yourdb')
    if os.path.exists(db_path):
        shutil.rmtree(db_path)

    print(f"\n--- Starting YourDB JSON + Object Insertion Benchmark ---")
    print(f"Number of objects to insert: {NUM_OBJECTS}\n")

    try:
        db = YourDB(DB_NAME)
        db.create_entity(ENTITY_NAME, EMPLOYEE_SCHEMA)
        print("Database and entity created successfully.")
    except Exception as e:
        print(f"Error during setup: {e}")
        return

    # --- 2. Generate Test Data as custom objects ---
    print("Generating test objects...")
    employees_to_insert = [
        BankEmployee(
            emp_id=1000 + i,
            name=f'employee_{i}',
            department='digital_banking',
            email_id='kyu',
            phone_no='1234567890',
            is_manager=(i % 100 == 0)
        ) for i in range(NUM_OBJECTS)
    ]
    print(f"Generated {len(employees_to_insert)} objects.")

    # --- 3. Run and Time the Insertion ---
    print("Starting insertion...")
    start_time = time.time()

    for employee in employees_to_insert:
        try:
            # Insert the custom object directly
            db.insert_into(ENTITY_NAME, employee)
        except Exception as e:
            print(f"Failed to insert employee {getattr(employee, 'emp_id', 'N/A')}: {e}")

    end_time = time.time()
    duration = end_time - start_time
    print("Insertion complete.\n")

    # --- 4. Display Results ---
    avg_time_ms = (duration / NUM_OBJECTS) * 1000 if NUM_OBJECTS > 0 else 0


    print("--- Benchmark Results ---")
    print("## New JSON + Object Serialization Method ##")
    print(f"Total objects inserted: {NUM_OBJECTS}")
    print(f"Total time taken: {duration:.4f} seconds")
    print(f"Average time per insert: {avg_time_ms:.4f} ms")
    print("-------------------------------------------")

if __name__ == "__main__":
    run_benchmark()
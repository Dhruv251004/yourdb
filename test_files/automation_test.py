import os
import shutil
import time
from yourdb.yourdb import YourDB
from yourdb.utils import register_class

DB_NAME = "company_db"
DB_DIR = f"{DB_NAME}.yourdb"

# Clean up previous runs
if os.path.exists(DB_DIR):
    shutil.rmtree(DB_DIR)

# Define the data model
@register_class
class Employee:
    def __init__(self, emp_id, name, department, salary):
        self.emp_id = emp_id
        self.name = name
        self.department = department
        self.salary = salary

    def __repr__(self):
        return f"Employee(id={self.emp_id}, name='{self.name}', dept='{self.department}', salary=${self.salary})"

# Initialize the database
db = YourDB(DB_NAME)

# Define the schema
employee_schema = {
    'primary_key': 'emp_id',
    'emp_id': "int",
    'name': "str",
    'department': "str",
    'salary': "int",
    'indexes': ['department', 'salary']  # Added salary index for range testing
}

# Create the entity
db.create_entity("employees", employee_schema)

# Insert sample data
print("\n--> Inserting employees...")
employees = [
    Employee(101, "Alice", "Engineering", 90000),
    Employee(102, "Bob", "Sales", 75000),
    Employee(103, "Charlie", "Engineering", 110000),
    Employee(104, "Diana", "Sales", 82000),
    Employee(105, "Eve", "HR", 95000),
    Employee(106, "Frank", "Engineering", 70000),
    Employee(107, "Grace", "HR", 120000),
]

for emp in employees:
    db.insert_into("employees", emp)

# Verify data insertion
print("\n--> Verifying inserted data...")
all_employees = db.select_from("employees")
print(f"Total employees in DB: {len(all_employees)}\n")
for emp in all_employees:
    print(emp)

# Define helper to time queries
def timed_query(description, entity, filter_dict):
    print(f"\n{description}")
    start = time.time()
    results = db.select_from(entity, filter_dict)
    duration = (time.time() - start) * 1000
    print(f"Query took {duration:.2f} ms | Found {len(results)} result(s):")
    for r in results:
        print(f"   {r}")
    return results


# Run Advanced Queries

# A. Simple greater-than
timed_query("Employees with salary > $80,000", "employees", {'salary': {'$gt': 80000}})

# B. Less-than-or-equal
timed_query("Employees with salary <= $82,000", "employees", {'salary': {'$lte': 82000}})

# C. Not equal
timed_query("Employees not in Sales", "employees", {'department': {'$ne': 'Sales'}})

# D. Combined index-assisted query
timed_query(
    "Engineering employees with ID > 101",
    "employees",
    {'department': 'Engineering', 'emp_id': {'$gt': 101}}
)

# E. Multi-operator on same field
timed_query("Employees with salary between 80k and 100k", "employees", {
    'salary': {'$gte': 80000, '$lte': 100000}
})

# F. Complex multi-condition query
timed_query(
    "Engineering employees earning > $85,000 but not Charlie",
    "employees",
    {'department': 'Engineering', 'salary': {'$gt': 85000}, 'name': {'$ne': 'Charlie'}}
)

# Summary
print("\n All tests completed successfully.")

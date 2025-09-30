import time
import os
import shutil
from yourdb.yourdb import YourDB  # Import the main class from your project

# --- Configuration ---
DB_NAME = "performance_test_db"
ENTITY_NAME = "users"
NUM_OBJECTS = 10000

# Define the schema for our test entity. This must match your schema rules.
USER_SCHEMA = {
    'primary_key': 'user_id',
    'user_id': int,
    'username': str,
    'email': str,
    'is_active': bool
}

# --- Main Benchmark Logic ---
def run_benchmark():
    """
    Initializes the database, inserts 10,000 objects, and measures the time.
    """
    print(f"--- Starting YourDB Benchmark ---")
    print(f"Database: '{DB_NAME}', Entity: '{ENTITY_NAME}'")
    print(f"Number of objects to insert: {NUM_OBJECTS}\n")

    # 1. Initialize the database and create the entity
    try:
        db = YourDB(DB_NAME)
        db.create_entity(ENTITY_NAME, USER_SCHEMA)
        print("Database and entity created successfully.")
    except Exception as e:
        print(f"Error during setup: {e}")
        return

    # 2. Generate a list of 10,000 user objects to insert
    print("Generating test objects...")
    users_to_insert = []
    for i in range(NUM_OBJECTS):
        user = {
            'user_id': 1000 + i,  # Unique ID for each user
            'username': f'user_{i}',
            'email': f'user_{i}@example.com',
            'is_active': (i % 2 == 0)
        }
        users_to_insert.append(user)
    print(f"{len(users_to_insert)} objects generated.\n")

    # 3. Perform and time the insertion process
    print("Starting insertion process...")
    start_time = time.time()

    # Loop through the generated data and insert each object one by one
    for user in users_to_insert:
        try:
            db.insert_into(ENTITY_NAME, user)
        except Exception as e:
            # If an error occurs, print it but don't stop the whole benchmark
            print(f"Failed to insert user {user.get('user_id')}: {e}")
            pass

    end_time = time.time()
    print("Insertion process finished.\n")

    # 4. Calculate and display the performance results
    duration = end_time - start_time
    # Calculate average time in milliseconds for better readability
    avg_time_per_insert = (duration / NUM_OBJECTS) * 1000

    print("--- Benchmark Results ---")
    print(f"Total objects inserted: {NUM_OBJECTS}")
    print(f"Total time taken: {duration:.4f} seconds")
    print(f"Average time per insert: {avg_time_per_insert:.4f} ms")
    print("-------------------------")


if __name__ == "__main__":
    run_benchmark()

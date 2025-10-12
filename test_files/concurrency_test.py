import sys, os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

import shutil
import time
import threading
import random
from yourdb.yourdb import YourDB
from yourdb.utils import register_class

# --- Configuration ---
DB_NAME = "concurrency_stress_db"
DB_DIR = f"{DB_NAME}.yourdb"
ENTITY_NAME = "test_items"

NUM_WRITER_THREADS = 5
NUM_READER_THREADS = 3
NUM_UPDATER_THREADS = 2
NUM_DELETER_THREADS = 1

RECORDS_PER_WRITER = 200
TOTAL_RECORDS = NUM_WRITER_THREADS * RECORDS_PER_WRITER

# --- Cleanup ---
if os.path.exists(DB_DIR):
    shutil.rmtree(DB_DIR)

# --- Define Entity Class ---
@register_class
class TestObject:
    def __init__(self, item_id, thread_name, value):
        self.item_id = item_id
        self.thread_name = thread_name
        self.value = value

    def __repr__(self):
        return f"TestObject(id={self.item_id}, thread='{self.thread_name}', value={self.value})"


# --- Initialize DB ---
db = YourDB(DB_NAME)
schema = {
    'primary_key': 'item_id',
    'item_id': 'int',
    'thread_name': 'str',
    'value': 'int',
    'indexes': ['value']
}

db.create_entity(ENTITY_NAME, schema)

stop_readers = threading.Event()


# --- Writer Threads ---
def writer_task(tid):
    name = f"Writer-{tid}"
    base = tid * 1000
    for i in range(RECORDS_PER_WRITER):
        item = TestObject(item_id=base + i, thread_name=name, value=i)
        try:
            db.insert_into(ENTITY_NAME, item)
            if i % 50 == 0:
                time.sleep(0.01)
        except Exception as e:
            print(f"[{name}] Insert error: {e}")


# --- Reader Threads ---
def reader_task(tid):
    name = f"Reader-{tid}"
    print(f"[{name}] Started.")
    while not stop_readers.is_set():
        try:
            all_count = len(db.select_from(ENTITY_NAME))
            _ = db.select_from(ENTITY_NAME, {"value": {"$lt": 50}})
            time.sleep(0.02)
        except Exception as e:
            print(f"[{name}] Read error: {e}")
    print(f"[{name}] Stopped.")


# --- Updater Threads ---
def updater_task(tid):
    name = f"Updater-{tid}"
    print(f"[{name}] Started.")
    while not stop_readers.is_set():
        try:
            # Randomly update a few items
            records = db.select_from(ENTITY_NAME)
            if not records:
                time.sleep(0.02)
                continue
            sample = random.sample(records, min(5, len(records)))
            for rec in sample:
                db.update_into(
                    ENTITY_NAME,
                    {"item_id": rec.item_id},
                    lambda obj: setattr(obj, "value", obj.value + 1000) or obj
                )
            time.sleep(0.03)
        except Exception as e:
            print(f"[{name}] Update error: {e}")
    print(f"[{name}] Stopped.")


# --- Deleter Threads ---
def deleter_task(tid):
    name = f"Deleter-{tid}"
    print(f"[{name}] Started.")
    while not stop_readers.is_set():
        try:
            # Delete a few low-valued records occasionally
            db.delete_from(ENTITY_NAME, {"value": {"$lt": 3}})
            time.sleep(0.05)
        except Exception as e:
            print(f"[{name}] Delete error: {e}")
    print(f"[{name}] Stopped.")


# --- Main Test Execution ---
def run_concurrency_stress_test():
    print("\n--- Starting Concurrency Stress Test ---")
    print(f"{NUM_WRITER_THREADS} writers, {NUM_READER_THREADS} readers, "
          f"{NUM_UPDATER_THREADS} updaters, {NUM_DELETER_THREADS} deleters")

    threads = []
    start = time.time()

    # Start writers first
    for i in range(NUM_WRITER_THREADS):
        t = threading.Thread(target=writer_task, args=(i,))
        threads.append(t)
        t.start()

    # Start readers
    for i in range(NUM_READER_THREADS):
        t = threading.Thread(target=reader_task, args=(i,))
        threads.append(t)
        t.start()

    # Start updaters
    for i in range(NUM_UPDATER_THREADS):
        t = threading.Thread(target=updater_task, args=(i,))
        threads.append(t)
        t.start()

    # Start deleter
    for i in range(NUM_DELETER_THREADS):
        t = threading.Thread(target=deleter_task, args=(i,))
        threads.append(t)
        t.start()

    # Wait for writers to finish
    for i in range(NUM_WRITER_THREADS):
        threads[i].join()

    print(f"\nAll writers finished in {time.time() - start:.2f}s")

    # Let readers/updaters/deleters run for a bit longer
    time.sleep(1.0)
    stop_readers.set()

    # Wait for remaining threads
    for t in threads[NUM_WRITER_THREADS:]:
        t.join()

    print("\n--- Verification Phase ---")
    final_db = YourDB(DB_NAME)
    items = final_db.select_from(ENTITY_NAME)
    ids = [x.item_id for x in items]
    duplicates = len(ids) - len(set(ids))

    print(f"Final count: {len(items)} / Expected ≤ {TOTAL_RECORDS}")
    print(f"Duplicate keys: {duplicates}")

    if duplicates == 0 and len(items) <= TOTAL_RECORDS:
        print("\n✅ PASSED: Locking integrity holds under mixed concurrency.")
    else:
        print("\n❌ FAILED: Data inconsistency or duplicate keys detected.")


if __name__ == "__main__":
    run_concurrency_stress_test()

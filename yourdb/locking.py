# In yourdb/locking.py
import threading

class RWLock:
    """
    A Reader-Writer Lock with a writer-preference policy.

    This lock prevents writer starvation by making new readers wait if a
    writer is already waiting to acquire the lock.
    """
    def __init__(self):
        self.mutex = threading.Lock()

        # Condition for readers to wait on
        self.readers_condition = threading.Condition(self.mutex)
        # Condition for writers to wait on
        self.writers_condition = threading.Condition(self.mutex)

        self.readers_count = 0
        self.writers_waiting = 0

    def acquire_read(self):
        """Acquire a read lock. Blocks if a writer is waiting."""
        self.mutex.acquire()
        try:
            # Readers must wait if a writer is in the queue. This is the key
            # to giving writers preference.
            while self.writers_waiting > 0:
                self.readers_condition.wait()
            self.readers_count += 1
        finally:
            self.mutex.release()

    def release_read(self):
        """Release a read lock."""
        self.mutex.acquire()
        try:
            self.readers_count -= 1
            # If this was the last reader, signal to the waiting writers
            # that the coast is clear.
            if self.readers_count == 0 and self.writers_waiting > 0:
                self.writers_condition.notify_all()
        finally:
            self.mutex.release()

    def acquire_write(self):
        """Acquire a write lock. Blocks if there are active readers."""
        self.mutex.acquire() # The lock is held throughout the write operation
        try:
            self.writers_waiting += 1
            # Wait until there are no active readers.
            while self.readers_count > 0:
                self.writers_condition.wait()
        except:
            # In case of an error during wait, release the mutex
            self.mutex.release()
            raise


    def release_write(self):
        """Release a write lock."""
        try:
            self.writers_waiting -= 1
            # A write operation has finished. We must wake up other threads.
            # Priority is given to other waiting writers.
            if self.writers_waiting > 0:
                self.writers_condition.notify_all()
            else:
                # If no writers are waiting, wake up all waiting readers.
                self.readers_condition.notify_all()
        finally:
            self.mutex.release() # The lock is released only after the write is done

    # --- Context Managers (No changes needed here) ---
    class _ReadLock:
        def __init__(self, lock): self.lock = lock
        def __enter__(self): self.lock.acquire_read()
        def __exit__(self, exc_type, exc_val, exc_tb): self.lock.release_read()

    class _WriteLock:
        def __init__(self, lock): self.lock = lock
        def __enter__(self): self.lock.acquire_write()
        def __exit__(self, exc_type, exc_val, exc_tb): self.lock.release_write()

    def read(self):
        return self._ReadLock(self)

    def write(self):
        return self._WriteLock(self)
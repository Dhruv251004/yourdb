import os
import json
from .utils import YourDBEncoder, yourdb_decoder

class Compactor:
    """
    This class handles the compaction of the database by removing
    deleted and outdated entries for a single partition log file.
    """
    def __init__(self, log_file_path: str, primary_key: str):
        self.log_file_path = log_file_path
        self.primary_key = primary_key
        self.temp_file_path = log_file_path + '.compacting'

    def compact(self):
        """
        Reading the log file, computing the final state of the data,
        writes it to a new file, and atomically replaces the old log.
        """
        print(f"Starting compaction for {os.path.basename(self.log_file_path)}...")

        final_state = {}
        try:
            with open(self.log_file_path, 'r') as log_file:
                for line in log_file:
                    if not line.strip(): continue
                    log_entry = json.loads(line, object_hook=yourdb_decoder)

                    op = log_entry.get('op')

                    if op == 'INSERT':
                        obj = log_entry.get('data')
                        if obj:
                            pk_value = getattr(obj, self.primary_key)
                            final_state[pk_value] = obj

                    elif op == 'UPDATE':
                        pk_to_update = log_entry.get('pk')
                        if pk_to_update in final_state:
                            update_data = log_entry.get('data', {})
                            for key, value in update_data.items():
                                setattr(final_state[pk_to_update], key, value)

                    elif op == 'DELETE':
                        pk_to_delete = log_entry.get('pk')
                        if pk_to_delete in final_state:
                            del final_state[pk_to_delete]
        except (IOError, json.JSONDecodeError) as e:
            print(f"ERROR: Could not read log file during compaction: {e}. Aborting.")
            return False

        # Writing the final state to a new temporary file
        try:
            with open(self.temp_file_path, 'w') as temp_file:
                for obj in final_state.values():
                    log_entry = {"op": "INSERT", "data": obj}

                    # FIX: Use json.dumps() to get a string
                    # FIX: Use the ENCODER (YourDBEncoder), not the decoder
                    json_string = json.dumps(log_entry, cls=YourDBEncoder)
                    temp_file.write(json_string + '\n')
        except IOError as e:
            print(f"ERROR: Could not write to temp file: {e}. Aborting.")
            return False

        # Atomically replace the old log file with the new one
        os.replace(self.temp_file_path, self.log_file_path)

        print(f"Compaction for {os.path.basename(self.log_file_path)} complete.")
        return True
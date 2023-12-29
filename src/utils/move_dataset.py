import os
import time
import hashlib
import shutil
import json

def load_settings():
    with open('settings.json', 'r') as file:
        return json.load(file)

settings = load_settings()

def calculate_directory_size(path):
    total_size = 0
    for dirpath, dirnames, filenames in os.walk(path):
        for f in filenames:
            fp = os.path.join(dirpath, f)
            total_size += os.path.getsize(fp)
    return total_size

def has_dataset_stabilized(path):
    interval = settings["monitor_interval"]
    stable_duration = settings["stable_duration"]

    last_size = -1
    stable_time = 0

    while stable_time < stable_duration:
        current_size = calculate_directory_size(path)
        if current_size == last_size:
            stable_time += interval
        else:
            stable_time = 0
        last_size = current_size
        time.sleep(interval)

    return True

def hide_dataset(path):
    parent_dir = os.path.dirname(path)
    hidden_path = os.path.join(parent_dir, '.' + os.path.basename(path))
    os.rename(path, hidden_path)
    return hidden_path

def hash_directory(path):
    hash_algo = hashlib.sha256()

    for dirpath, dirnames, filenames in os.walk(path):
        for fname in sorted(filenames):
            file_path = os.path.join(dirpath, fname)
            with open(file_path, "rb") as f:
                for chunk in iter(lambda: f.read(4096), b""):
                    hash_algo.update(chunk)

    return hash_algo.hexdigest()

def copy_to_staging(src, dest):
    shutil.copytree(src, dest)

def verify_data_integrity(src, dest):
    return hash_directory(src) == hash_directory(dest)

def move_dataset(dataset_path, staging_path):
    if has_dataset_stabilized(dataset_path):
        hidden_path = hide_dataset(dataset_path)
        original_hash = hash_directory(hidden_path)
        copy_to_staging(hidden_path, staging_path)
        copied_hash = hash_directory(staging_path)
        
        if original_hash == copied_hash:
            print("Dataset successfully moved and verified.")
            return True
        else:
            print("Data integrity check failed.")
            return False
    else:
        print("Dataset is still changing and cannot be moved yet.")
        return False

if __name__ == "__main__":
    dataset_path = settings["dataset_path"]
    staging_path = settings["staging_path"]
    move_dataset(dataset_path, staging_path)
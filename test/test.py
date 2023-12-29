import os
import json
import random
import time
import subprocess
from pathlib import Path

CONFIG_PATH = "config/settings.json"
TEST_DATA_PATH = "test/test_L_Drive"

def create_folders(base_path, groups):
    print("Creating group and user folders...")
    for group, users in groups.items():
        for user in users:
            user_folder = os.path.join(base_path, group, user)
            os.makedirs(user_folder, exist_ok=True)
            print(f"Created folder: {user_folder}")

def create_random_files(folder_path, depth=0):
    if depth > 5:
        return
    num_subfolders = random.randint(1, 5)
    num_files = random.randint(1, 5)

    for _ in range(num_files):
        file_name = f"file{random.randint(1, 100)}.txt"
        with open(os.path.join(folder_path, file_name), 'w') as f:
            f.write("Test content\n")

    for _ in range(num_subfolders):
        subfolder_name = f"folder{random.randint(1, 100)}"
        subfolder_path = os.path.join(folder_path, subfolder_name)
        os.makedirs(subfolder_path, exist_ok=True)
        create_random_files(subfolder_path, depth+1)

def generate_test_data(base_path, group_folders):
    print("Generating test datasets...")
    for _ in range(10):
        group, users = random.choice(list(group_folders.items()))
        user = random.choice(users)
        dataset_name = f"dataset{random.randint(1, 100)}"
        dataset_path = os.path.join(base_path, group, user, dataset_name)
        os.makedirs(dataset_path, exist_ok=True)
        create_random_files(dataset_path)
        print(f"Created dataset: {dataset_path}")
        time.sleep(random.uniform(0, 3))  # Random delay up to 3 seconds

if __name__ == "__main__":
    # Load JSON configuration
    with open(CONFIG_PATH, 'r') as f:
        config = json.load(f)

    group_folders = {group: users['membersOf'] for group, users in config['Groups'].items()}

    # Create base folders
    create_folders(TEST_DATA_PATH, group_folders)

    # Run the main script
    print("Starting main.py script...")
    process = subprocess.Popen(["python", "src/main.py", TEST_DATA_PATH])

    # Wait for a few seconds to ensure main.py is up and running
    print("Waiting for main.py to initialize...")
    time.sleep(5)

    # Generate test datasets
    generate_test_data(TEST_DATA_PATH, group_folders)

    # Keep the script running to allow time for testing
    print("Test data generation complete. Keeping the script running for 30 seconds...")
    time.sleep(30)

    # Optionally terminate the main.py process after testing
    print("Terminating main.py script...")
    process.terminate()
    print("Test script completed.")

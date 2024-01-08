#test.py

import os
import json
import random
import time
import yaml
import subprocess
from pathlib import Path

from ..utils.config import load_settings, load_json

# Configuration
CONFIG_PATH = "config/test_settings.yml"

# Load YAML configuration
config = load_settings(CONFIG_PATH)

# Load JSON directory_structure
DIRECTORY_STRUCTURE_PATH = config['directory_structure_file_path']
directory_structure = load_json(DIRECTORY_STRUCTURE_PATH)

group_folders = {group: users['membersOf'] for group, users in directory_structure['Groups'].items()}


def create_folders(base_path, groups):
    """
    Creates a directory structure based on the provided groups.
    Each group will have a directory, and within each group directory, 
    a directory for each user will be created.
    """
    print("Creating group and user folders...")
    for group, users in groups.items():
        for user in users:
            user_folder = os.path.join(base_path, group, user)
            os.makedirs(user_folder, exist_ok=True)
            print(f"Created folder: {user_folder}")

def create_random_files(folder_path, depth=0, file_num=1, folder_num=1, max_depth=3):
    """
    Creates a random number of files and subdirectories within the provided folder_path.
    The depth parameter controls how many levels of subdirectories to create.
    """
    if depth > max_depth:
        return
    num_subfolders = random.randint(1, 1)
    num_files = random.randint(1, 3)

    for _ in range(num_files):
        file_name = f"file{file_num}.txt"
        with open(os.path.join(folder_path, file_name), 'w') as f:
            f.write("Test content\n")
        file_num += 1

    for _ in range(num_subfolders):
        if random.choice([True, False]):  # Randomly decide to create a subfolder or not
            subfolder_name = f"folder{folder_num}"
            subfolder_path = os.path.join(folder_path, subfolder_name)
            os.makedirs(subfolder_path, exist_ok=True)
            create_random_files(subfolder_path, depth+1, file_num, folder_num+1, max_depth)

def generate_test_data(base_path, group_folders):
    """
    Generates test data by creating a specific number of datasets within the directory structure.
    Each dataset is a directory with a specific number of files and subdirectories.
    """
    print("Generating test datasets...")
    num_datasets = random.randint(2, 5)
    for d in range(num_datasets):
        group, users = random.choice(list(group_folders.items()))
        user = random.choice(users)
        dataset_name = f"dataset{d+1}"
        dataset_path = os.path.join(base_path, group, user, dataset_name)
        os.makedirs(dataset_path, exist_ok=True)
        num_folders = random.randint(1, 2)
        for i in range(num_folders):
            folder_name = f"folder{i+1}"
            folder_path = os.path.join(dataset_path, folder_name)
            os.makedirs(folder_path, exist_ok=True)
            max_depth = random.randint(0, 3)
            create_random_files(folder_path, max_depth=max_depth, file_num=1)  # Reset file_num for each folder
        print(f"Created dataset: {dataset_path}")
        time.sleep(random.uniform(0, 3))  # Random delay up to 3 seconds

def print_tree(directory, file_output=False, indents=0):
    """
    Prints the directory tree structure starting from the given directory.
    """
    print('  ' * indents + os.path.basename(directory))
    if file_output:
        for file in os.listdir(directory):
            if os.path.isfile(os.path.join(directory, file)):
                print('  ' * (indents + 1) + file)
    for folder in os.listdir(directory):
        if os.path.isdir(os.path.join(directory, folder)):
            print_tree(os.path.join(directory, folder), file_output, indents + 1)

if __name__ == "__main__":
    """
    The main function of the script. It loads the configuration, creates the base folders,
    starts the main.py script, generates test data, and then keeps the script running for testing.
    """
    # Create base folders in landing_dir_base_path
    create_folders(config['landing_dir_base_path'], group_folders)

    # Create base folders in staging_dir_path
    create_folders(config['staging_dir_path'], group_folders)

    # Run the main script
    print("Starting main.py script...")
    process = subprocess.Popen(["python", "src/main.py", CONFIG_PATH, config['landing_dir_base_path']])

    # Wait for a few seconds to ensure main.py is up and running
    print("Waiting for main.py to initialize...")
    time.sleep(5)

    # Generate test datasets
    generate_test_data(config['landing_dir_base_path'], group_folders)

    # Print the directory tree structure
    print("Printing the directory tree structure:")
    print_tree(config['landing_dir_base_path'], file_output=True)

    # Keep the script running to allow time for testing
    print("Test data generation complete. Keeping the script running for 30 seconds...")
    time.sleep(30)

    # Optionally terminate the main.py process after testing
    print("Terminating main.py script...")
    process.terminate()
    print("Test script completed.")
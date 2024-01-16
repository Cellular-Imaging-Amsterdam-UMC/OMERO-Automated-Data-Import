#test.py

import os
import shutil
import time
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

def clean_user_directories(base_path, groups):
    """
    Cleans out all files and subdirectories in each user directory.
    """
    print("Cleaning user directories...")
    for group, users in groups.items():
        for user in users:
            user_folder = os.path.join(base_path, group, user)
            if os.path.exists(user_folder):
                for item in os.listdir(user_folder):
                    item_path = os.path.join(user_folder, item)
                    if os.path.isfile(item_path):
                        os.remove(item_path)
                    elif os.path.isdir(item_path):
                        shutil.rmtree(item_path)
                print(f"Cleaned folder: {user_folder}")


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

def generate_test_data(base_path, group_folders, data_package_path):
    """
    Generates test data by copying a sample data package into each user's directory.
    """
    print("Generating test datasets...")
    for group, users in group_folders.items():
        for user in users:
            user_folder = os.path.join(base_path, group, user)
            # Copy the data package into the user's directory
            shutil.copytree(data_package_path, os.path.join(user_folder, "Test_DataPackage"))
            print(f"Copied data package to: {os.path.join(user_folder, 'Test_DataPackage')}")
            time.sleep(1) 

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
    starts the main.py script, copies the test data, and then keeps the script running for testing.
    """
    
    # Clean existing user directories
    clean_user_directories(config['landing_dir_base_path'], group_folders)
    clean_user_directories(config['staging_dir_path'], group_folders)


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

    # Copy test data packages
    generate_test_data(config['landing_dir_base_path'], group_folders, "test/Test_DataPackage")

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
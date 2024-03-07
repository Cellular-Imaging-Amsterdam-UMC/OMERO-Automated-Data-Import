#test.py

import os
import shutil
import time
import subprocess
from pathlib import Path

from ..utils.config_manager import load_settings, load_json

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


#config['landing_dir_base_path'], group_folders, "test_mnt/Test_DataPackage
def generate_test_data(base_path, group_folders, data_package_path):
    """
    Generates test data by copying a sample data package into each user's directory.
    """
    print("Generating test datasets...")

    # Print the contents of test_mnt before copying
    print("Contents of test_mnt before copying:")
    print_tree(data_package_path, file_output=True)

    for group, users in group_folders.items():
        for user in users:
            user_folder = os.path.join(base_path, group, user)
            destination_path = os.path.join(user_folder, "Test_DataPackage")
            try:
                # Ensure the destination directory exists
                os.makedirs(destination_path, exist_ok=True)
                # Copy each item in the data package individually to the destination
                for item in os.listdir(data_package_path):
                    s_item = os.path.join(data_package_path, item)
                    d_item = os.path.join(destination_path, item)
                    if os.path.isdir(s_item):
                        shutil.copytree(s_item, d_item)
                    else:
                        shutil.copy2(s_item, d_item)
                print(f"Copied data package to: {destination_path}")
            except Exception as e:
                print(f"Error copying data package to {destination_path}: {e}")
            time.sleep(1) 

    # Print the contents of test_mnt after copying
    print("Contents of test_mnt after copying:")
    print_tree(data_package_path, file_output=True)

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
    generate_test_data(config['landing_dir_base_path'], group_folders, "test_mnt/Test_DataPackage")

    # Print the directory tree structure
    print("Printing the directory tree structure:")
    print_tree(config['landing_dir_base_path'], file_output=True)

    # Keep the script running to allow time for testing
    print("Test data generation complete. Keeping the script running for 10 seconds...")
    time.sleep(10)

    # Optionally terminate the main.py process after testing
    print("Terminating main.py script...")
    process.terminate()
    print("Test script completed.")
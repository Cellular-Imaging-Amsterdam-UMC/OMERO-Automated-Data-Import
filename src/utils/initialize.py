import os
import shutil
import sys
import json
from .ingest_tracker import initialize_database
from .logger import setup_logger

def load_json(file_path):
    """
    Load JSON data from a file.
    """
    with open(file_path, 'r') as file:
        return json.load(file)

def check_directory_access(path, log, test_file_name='access_test_file.tmp'):
    """
    Checks if the application has read, write, and delete access to the specified path.
    It tries to create, read, and delete a temporary file in the directory.
    """
    try:
        # Test write access
        test_file_path = os.path.join(path, test_file_name)
        with open(test_file_path, 'w') as test_file:
            test_file.write('Access test.')
        
        # Test read access
        with open(test_file_path, 'r') as test_file:
            if test_file.read() != 'Access test.':
                raise Exception("Failed to read the test file correctly.")
        
        # Test delete access
        os.remove(test_file_path)

        # Test ability to delete a subdirectory (simulate user directory deletion)
        test_dir_path = os.path.join(path, 'access_test_dir')
        os.makedirs(test_dir_path, exist_ok=True)
        shutil.rmtree(test_dir_path)

        log.info(f"Access check successful for {path}")
        return True
    except Exception as e:
        log.error(f"Access check failed for {path}: {e}")
        return False

def initialize_system(config):
    """
    Performs initial system checks and setups, including directory access checks and database initialization.
    """
    # Setup logger
    logger = setup_logger('initialize_system', config['log_file_path'])

    # Load the directory structure
    directory_structure_path = config['directory_structure_file_path']
    directory_structure = load_json(directory_structure_path)

    # Check access to user directories under landing_dir_base_path
    base_path = config['landing_dir_base_path']
    access_checks_passed = True
    for group, details in directory_structure['Groups'].items():
        for user in details['membersOf']:
            user_dir_path = os.path.join(base_path, group, user)
            if not check_directory_access(user_dir_path, logger):
                access_checks_passed = False

    if not access_checks_passed:
        logger.error("Insufficient access to one or more required directories. Please check permissions.")
        sys.exit(1)

    # Initialize the database for ingest tracking
    initialize_database()
    logger.info("System initialization complete.")
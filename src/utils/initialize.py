# Copyright 2023 Rodrigo Rosas-Bertolini
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

#initialize.py

import os
import shutil
import sys
import json
from .ingest_tracker import initialize_database
from .logger import setup_logger
from .config_manager import load_json

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

    # Check access to directories for each group
    access_checks_passed = True
    for group_info in load_json(config['group_list']):
        # Use 'core_grp_name' as the directory name
        group_base_path = os.path.join(config['base_dir'], group_info['core_grp_name'])
        
        # Check access for each specific directory within the group directory
        for dir_name in [config['upload_orders_dir_name'], config['completed_orders_dir_name'], config['failed_uploads_directory_name']]:
            dir_path = os.path.join(group_base_path, dir_name)
            if not check_directory_access(dir_path, logger):
                access_checks_passed = False

    if not access_checks_passed:
        logger.error("Insufficient access to one or more required directories. Please check permissions.")
        sys.exit(1)

    # Initialize the database for ingest tracking
    initialize_database()
    logger.info("Database has been successfully created and initialized.")

    logger.info("System initialization complete.")
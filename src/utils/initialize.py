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

# initialize.py

import os
import shutil
import sys
import yaml
import json
from pathlib import Path
from .logger import LoggerManager
from .ingest_tracker import initialize_ingest_tracker

def check_directory_access(path, test_file_name='access_test_file.tmp'):
    """
    Checks if the application has read, write, and delete access to the specified path.
    
    Args:
        path (str): Directory path to check
        test_file_name (str): Name of temporary test file
    
    Returns:
        bool: True if all access checks pass, False otherwise
    """
    if not LoggerManager.is_initialized():
        raise RuntimeError("LoggerManager must be initialized before checking directory access")
    logger = LoggerManager.get_module_logger(__name__)
    
    try:
        # Test write access
        test_file_path = os.path.join(path, test_file_name)
        logger.debug(f"Testing write access to {path}")
        with open(test_file_path, 'w') as test_file:
            test_file.write('Access test.')
        
        # Test read access
        logger.debug(f"Testing read access to {path}")
        with open(test_file_path, 'r') as test_file:
            if test_file.read() != 'Access test.':
                raise Exception("Failed to read the test file correctly.")
        
        # Test delete access
        logger.debug(f"Testing delete access to {path}")
        os.remove(test_file_path)

        # Test subdirectory creation and deletion
        test_dir_path = os.path.join(path, 'access_test_dir')
        logger.debug(f"Testing directory creation/deletion in {path}")
        os.makedirs(test_dir_path, exist_ok=True)
        shutil.rmtree(test_dir_path)

        logger.info(f"Access check successful for {path}")
        return True
        
    except Exception as e:
        logger.error(f"Access check failed for {path}: {str(e)}")
        return False

def load_settings(file_path):
    """
    Load settings from either a YAML or JSON file.
    
    Args:
        file_path (str): Path to the settings file
    
    Returns:
        dict: Loaded settings
    
    Raises:
        FileNotFoundError: If settings file doesn't exist
        ValueError: If file format is not supported
    """
    if not LoggerManager.is_initialized():
        raise RuntimeError("LoggerManager must be initialized before loading settings")
    logger = LoggerManager.get_module_logger(__name__)
    
    try:
        file_path = Path(file_path)  # Convert to Path object if it's not already
        
        if not file_path.exists():
            logger.error(f"Settings file not found: {file_path}")
            raise FileNotFoundError(f"Settings file not found: {file_path}")
        
        logger.debug(f"Loading settings from {file_path}")
        with file_path.open('r') as file:
            if file_path.suffix in ['.yml', '.yaml']:
                settings = yaml.safe_load(file)
                logger.debug("Successfully loaded YAML settings")
                return settings
            elif file_path.suffix == '.json':
                settings = json.load(file)
                logger.debug("Successfully loaded JSON settings")
                return settings
            else:
                logger.error(f"Unsupported file format: {file_path}")
                raise ValueError(f"Unsupported file format: {file_path}")
                
    except Exception as e:
        logger.error(f"Failed to load settings from {file_path}: {str(e)}")
        raise

def initialize_system(config):
    """
    Performs initial system checks and setups.
    
    Args:
        config (dict): Configuration dictionary containing system settings
    
    Raises:
        RuntimeError: If logger is not initialized
        SystemExit: If critical initialization steps fail
    """
    if not LoggerManager.is_initialized():
        raise RuntimeError("LoggerManager must be initialized before system initialization")
    logger = LoggerManager.get_module_logger(__name__)

    try:
        logger.info("Starting system initialization...")
        # Check access to directories for each group
        access_checks_passed = True
        logger.debug("Loading group settings...")
        groups = load_settings(config['group_list'])
        
        for group_info in groups:
            group_base_path = os.path.join(config['base_dir'], group_info['core_grp_name'])
            logger.debug(f"Checking access for group: {group_info['core_grp_name']}")
            
            for dir_name in [
                config['upload_orders_dir_name'], 
                config['completed_orders_dir_name'], 
                config['failed_uploads_directory_name']
            ]:
                dir_path = os.path.join(group_base_path, dir_name)
                logger.debug(f"Creating directory if not exists: {dir_path}")
                os.makedirs(dir_path, exist_ok=True)
                
                if not check_directory_access(dir_path):
                    logger.error(f"Access check failed for directory: {dir_path}")
                    access_checks_passed = False

        if not access_checks_passed:
            logger.error("Insufficient access to one or more required directories. Please check permissions.")
            sys.exit(1)

        # Initialize the ingest tracking database
        logger.debug("Initializing ingest tracking database...")
        initialize_ingest_tracker(config)

        logger.info("System initialization completed successfully.")
        
    except Exception as e:
        logger.error(f"System initialization failed: {str(e)}", exc_info=True)
        sys.exit(1)
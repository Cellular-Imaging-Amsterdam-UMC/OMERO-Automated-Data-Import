# main.py

import sys
import json
import os
import time
import logging
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler
import subprocess
from concurrent.futures import ProcessPoolExecutor

#Modules
from utils.data_mover import move_dataset
from utils.config import load_settings, load_json

# Configuration
CONFIG_PATH = sys.argv[1] if len(sys.argv) > 1 else "config/settings.yml"

# Load YAML configuration
config = load_settings(CONFIG_PATH)

# Load JSON directory_structure
DIRECTORY_STRUCTURE_PATH = config['directory_structure_file_path']
directory_structure = load_json(DIRECTORY_STRUCTURE_PATH)

# ProcessPoolExecutor with 4 workers
executor = ProcessPoolExecutor(max_workers=config['max_workers'])

# Set up logging
logging.basicConfig(level=logging.INFO, filename=config['log_file_path'], filemode='a',
                    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')

logger = logging.getLogger(__name__)

class DataPackage:
    def __init__(self, group, user, dataset):
        self.group = group
        self.user = user
        self.dataset = dataset
        self.path = os.path.join(group, user, dataset)

def ingest(data_package, config):
    """
    This is the ingestion process:

    >>> data_mover.py
    Determines when data has finished copying to the "dropbox" measuring the size of the dataset.
    Hides the dataset by prefixing it with '.'
    Copies the dataset to the staging with a hash check
    Deletes the dataset in in the "dropbox"

    >>> stager.py
    Creates a json file describing for each file:
        Destination user, group, project, and dataset    
        Screen or simple data

    >>> importer.py
    Creates the Projects and Datasets described by the jsaon created by the stager module
    Uploads the images/screens
    Append the indicated metadata

    """
    try:
        logger.info(f"Starting ingestion for group: {data_package.group}, user: {data_package.user}, dataset: {data_package.dataset}")

        # data_mover.py
        move_dataset(data_package, config)
        
        # stager.py

        # importer.py

        logger.info(f"Completed ingestion for group: {data_package.group}, user: {data_package.user}, dataset: {data_package.dataset}")
    except Exception as e:
        logger.error(f"Error during ingestion for group: {data_package.group}, user: {data_package.user}, dataset: {data_package.dataset}: {e}")

# Handler class
class DatasetHandler(FileSystemEventHandler):
    """
    Event handler class for the watchdog observer. 
    It checks for directory creation events and triggers the ingest function when a new directory is created.
    """
    def __init__(self, base_directory, group_folders, executor):
        self.base_directory = base_directory
        self.group_folders = group_folders
        self.executor = executor
        
    def on_created(self, event):
        """
        Event hook for when a new directory or file is created. 
        Checks if the created directory or the parent directory of the created file is a dataset 
        and triggers the ingest function if it is.
        """
        try:
            created_dir = os.path.normpath(event.src_path)
        
            # If a new file is created, set created_dir to its parent directory
            if not event.is_directory:
                created_dir = os.path.dirname(created_dir)
        
            for group, users in self.group_folders.items():
                for user in users:
                    user_folder = os.path.normpath(os.path.join(self.base_directory, group, user))
                    # Check if the created directory or the parent directory of the created file is directly under the user's folder
                    if os.path.dirname(created_dir) == user_folder:
                        logger.info(f"Dataset detected: {os.path.basename(created_dir)} for user: {user} in group: {group}")
                        data_package = DataPackage(group, user, os.path.basename(created_dir))
                        self.executor.submit(ingest, data_package, config)
        except Exception as e:
            logger.error(f"Error during on_created event handling: {e}")



def main(directory):
    """
    Main function of the script. 
    It sets up the directory structure, event loop, and observer, and starts the folder monitoring service.
    """
    # Load JSON directory_structure
    with open(DIRECTORY_STRUCTURE_PATH, 'r') as f:
        directory_structure = json.load(f)

    group_folders = {group: users['membersOf'] for group, users in directory_structure['Groups'].items()}

    # ProcessPoolExecutor with 4 workers
    executor = ProcessPoolExecutor(max_workers=4)

    # Set up the observer
    observer = Observer()
    for group in group_folders.keys():
        event_handler = DatasetHandler(directory, group_folders, executor)
        group_path = os.path.normpath(os.path.join(directory, group))
        observer.schedule(event_handler, path=group_path, recursive=True)
    observer.start()

    logger.info("Starting the folder monitoring service.")

    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        observer.stop()
        logger.info("Stopping the folder monitoring service.")
    observer.join()
    executor.shutdown()

if __name__ == "__main__":
    if len(sys.argv) > 3:
        print("Usage: python main.py [config_file] [directory]")
        sys.exit(1)

    main(sys.argv[2])
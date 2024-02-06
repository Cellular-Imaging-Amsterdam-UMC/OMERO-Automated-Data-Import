# main.py

import sys
import json
from pathlib import Path
import time
from utils.logger import setup_logger
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler
from concurrent.futures import ProcessPoolExecutor

#Modules
from utils.config import load_settings, load_json
from utils.data_mover import MoveDataPackage
from utils.stager import DataPackageStager
from utils.importer import DataPackageImporter

# Configuration
CONFIG_PATH = sys.argv[1] if len(sys.argv) > 1 else "config/settings.yml"

# Load YAML configuration
config = load_settings(CONFIG_PATH)

# Load JSON directory_structure
DIRECTORY_STRUCTURE_PATH = config['directory_structure_file_path']
directory_structure = load_json(DIRECTORY_STRUCTURE_PATH)

# ProcessPoolExecutor with 4 workers
executor = ProcessPoolExecutor(max_workers=config['max_workers'])

# Set up logging using setup_logger instead of basicConfig
logger = setup_logger(__name__, config['log_file_path'])

class DataPackage:
    def __init__(self, landing_dir_base_path, group, user, project):
        self.group = group
        self.user = user
        self.project = project
        self.original_path = Path(landing_dir_base_path) / group / user / project
        self.hidden_path = None
        self.datasets = {}

def ingest(data_package, config, active_ingestions, ):
    """
    This is the ingestion process:

    >>> data_mover.py -- Step 1: Move Data Package
    Determines when data has finished copying to the "dropbox" measuring the size of the dataset.
    Hides the dataset by prefixing it with '.'
    Copies the dataset to the staging with a hash check
    Deletes the dataset in in the "dropbox"

    >>> stager.py -- Step 2: Categorize Datasets
    Creates a json file describing for each file:
        Destination user, group, project, and dataset    
        Screen or simple data

    >>> importer.py -- Step 3: Import Data Package
    Creates the Projects and Datasets described by the jsaon created by the stager module
    Uploads the images/screens
    Append the indicated metadata

    """
    try:
        # Step 1: Move Data Package
        move_result, hidden_path = MoveDataPackage(data_package, config).move_result
        if not move_result:
            raise Exception("Failed to move data package.")

        # Step 2: Categorize Datasets
        stager = DataPackageStager(config)  # Instantiate the DatasetStager class
        data_package.datasets = stager.identify_datasets(data_package)  # Use the identify_datasets method


        # Step 3: Import Data Package
        importer = DataPackageImporter(config)
        importer.import_data_package(data_package) 


        # Remove the data package identifier from active ingestions upon successful ingestion
        data_package_identifier = f"{data_package.group}_{data_package.user}_{data_package.project}"
        active_ingestions.remove(data_package_identifier)
    except Exception as e:
        logger.error(f"Error during ingestion for group: {data_package.group}, user: {data_package.user}, project: {data_package.project}: {e}")
    finally:
        # Ensure the identifier is removed from active_ingestions once processing is complete or fails
        data_package_identifier = f"{data_package.group}_{data_package.user}_{data_package.project}"
        active_ingestions.remove(data_package_identifier)

# Handler class
class DataPackageHandler(FileSystemEventHandler):
    """
    Event handler class for the watchdog observer. 
    It checks for directory creation events and triggers the ingest function when a new directory is created.
    """
    def __init__(self, base_directory, group_folders, executor, active_ingestions):
        self.base_directory = Path(base_directory)
        self.group_folders = group_folders
        self.executor = executor
        self.active_ingestions = active_ingestions

    def on_created(self, event):
        """
        Event hook for when a new directory or file is created. 
        Checks if the created directory or the parent directory of the created file is a dataset 
        and triggers the ingest function if it is.
        """
        try:
            created_dir = Path(event.src_path)

            # If a new file is created, set created_dir to its parent directory
            if not event.is_directory:
                created_dir = created_dir.parent

            for group, users in self.group_folders.items():
                for user in users:
                    user_folder = self.base_directory / group / user
                    # Check if the created directory or the parent directory of the created file is directly under the user's folder
                    if created_dir.parent == user_folder:
                        logger.info(f"DataPackge detected: {config['landing_dir_base_path']}, {group}, {user}, {created_dir.name}")
                        data_package = DataPackage(config['landing_dir_base_path'], group, user, created_dir.name)

                        # Submit the ingest task to the executor and add a callback for error logging
                        future = self.executor.submit(ingest, data_package, config, self.active_ingestions)
                        future.add_done_callback(self.log_future_exception)
        except Exception as e:
            logger.error(f"Error during on_created event handling: {e}")

    def log_future_exception(self, future):
        """
        Callback function to log exceptions from futures.
        """
        try:
            future.result()  # This will raise any exceptions caught during the execution of the task
        except Exception as e:
            logger.error(f"Error in background task: {e}")


def main(directory):
    """
    Main function of the script. 
    It sets up the directory structure, event loop, and observer, and starts the folder monitoring service.
    """
    active_ingestions = set()
    
    # Load JSON directory_structure
    with open(DIRECTORY_STRUCTURE_PATH, 'r') as f:
        directory_structure = json.load(f)

    group_folders = {group: users['membersOf'] for group, users in directory_structure['Groups'].items()}

    # ProcessPoolExecutor with 4 workers
    executor = ProcessPoolExecutor(max_workers=4)

    # Set up the observer
    observer = Observer()
    for group in group_folders.keys():
        event_handler = DataPackageHandler(directory, group_folders, executor, active_ingestions)
        group_path = Path(directory) / group
        observer.schedule(event_handler, path=str(group_path), recursive=True)
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
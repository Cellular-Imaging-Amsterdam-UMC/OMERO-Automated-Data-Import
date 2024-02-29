# main.py

import sys
import json
from pathlib import Path
import time
import uuid
from utils.logger import setup_logger
from concurrent.futures import ProcessPoolExecutor
import signal
from threading import Event, Thread

#Modules
from utils.config import load_settings, load_json
from utils.initialize import initialize_system
from utils.data_mover import DataPackageMover
from utils.stager import DataPackageStager
from utils.importer import DataPackageImporter
from utils.ingest_tracker import log_ingestion_step
from utils.failure_handler import UploadFailureHandler

# Setup Configuration
CONFIG_PATH = sys.argv[1] if len(sys.argv) > 1 else "config/settings.yml" # Configuration
config = load_settings(CONFIG_PATH) # Load YAML configuration
DIRECTORY_STRUCTURE_PATH = config['directory_structure_file_path'] # Load JSON directory_structure
directory_structure = load_json(DIRECTORY_STRUCTURE_PATH)
executor = ProcessPoolExecutor(max_workers=config['max_workers']) # ProcessPoolExecutor with 4 workers
logger = setup_logger(__name__, config['log_file_path']) # Set up logging using setup_logger instead of basicConfig

class DataPackage:
    def __init__(self, landing_dir_base_path, staging_dir_base_path, group, user, project):
        self.group = group
        self.user = user
        self.project = project
        self.landing_dir_base_path = Path(landing_dir_base_path) / group / user / project
        self.staging_dir_base_path = Path(staging_dir_base_path) / group / user / project
        self.hidden_path = None
        self.datasets = {}

def ingest(data_package, config, ingestion_id):
    """
    Ingests a data package through various stages and logs each step.
    """
    try:
        # Step 1: Move Data Package
        mover = DataPackageMover(data_package, config)
        move_result = mover.move_data_package()

        if not move_result:
            logger.error(f"Failed to move data package for project {data_package.project}. Check logs for details.")
            log_ingestion_step(data_package.group, data_package.user, data_package.project, "Move Failed", ingestion_id)
            return
        logger.info(f"Data package {data_package.project} moved successfully.")
        log_ingestion_step(data_package.group, data_package.user, data_package.project, "Data Moved", ingestion_id)

        # Step 2: Categorize Datasets
        stager = DataPackageStager(config)
        data_package.datasets = stager.identify_datasets(data_package)
        logger.info(f"Datasets categorized for data package {data_package.project}.")
        log_ingestion_step(data_package.group, data_package.user, data_package.project, "Datasets Categorized", ingestion_id)

        # Step 3: Import Data Package
        importer = DataPackageImporter(config)
        successful_uploads, failed_uploads = importer.import_data_package(data_package)
        logger.info(f"Data package {data_package.project} processed successfully with {len(successful_uploads)} successful uploads and {len(failed_uploads)} failed uploads.")
        log_ingestion_step(data_package.group, data_package.user, data_package.project, "Data Imported", ingestion_id)
                
        # Step 4: Handling Failure
        failure_handler = UploadFailureHandler(config)
        failure_handler.move_failed_uploads(failed_uploads, data_package.user)  # Pass user name to the method
        logger.info(f"Failed uploads for data package {data_package.project} have been handled.")
        log_ingestion_step(data_package.group, data_package.user, data_package.project, "Failed Uploads Handled", ingestion_id)

    except Exception as e:
        logger.error(f"Error during ingestion for group: {data_package.group}, user: {data_package.user}, project: {data_package.project}: {e}")
        log_ingestion_step(data_package.group, data_package.user, data_package.project, "Ingestion Error", ingestion_id)

# Handler class
class DirectoryPoller:
    def __init__(self, landing_dir_base_path, staging_dir_base_path, group_folders, executor, logger, interval=10):
        self.landing_dir_base_path = Path(landing_dir_base_path)
        self.staging_dir_base_path = Path(staging_dir_base_path)
        self.group_folders = group_folders
        self.executor = executor
        self.logger = logger
        self.interval = interval
        self.shutdown_event = Event()

    def start(self):
        self.polling_thread = Thread(target=self.poll_directory_changes)
        self.polling_thread.start()

    def stop(self):
        self.shutdown_event.set()
        self.polling_thread.join()

    def poll_directory_changes(self):
        last_checked = {}
        while not self.shutdown_event.is_set():
            for group, users in self.group_folders.items():
                for user in users:
                    user_folder = self.landing_dir_base_path / group / user
                    if not user_folder.exists():
                        continue
                    for item in user_folder.iterdir():
                        # Check if the item is a directory or a file
                        if item.is_dir() or item.is_file():
                            package_name = item.name
                            # Determine if the item is new or has been modified since last checked
                            if package_name not in last_checked or item.stat().st_mtime > last_checked.get(package_name, 0):
                                self.logger.info(f"Change detected in package: {package_name}")
                                self.process_event(group, user, package_name, item)
                                last_checked[package_name] = item.stat().st_mtime
            time.sleep(self.interval)

    def process_event(self, group, user, package_name, created_path):
        ingestion_id = str(uuid.uuid4())
        self.logger.info(f"DataPackage detected - Path: {created_path}, Group: {group}, User: {user}, Package Name: {package_name}, Ingestion ID: {ingestion_id}")
        log_ingestion_step(group, user, package_name, "Data Package Detected", ingestion_id)
        data_package = DataPackage(self.landing_dir_base_path, self.staging_dir_base_path, group, user, package_name)
        future = self.executor.submit(ingest, data_package, config, ingestion_id)
        future.add_done_callback(self.log_future_exception)

    def log_future_exception(self, future):
        try:
            future.result()  # This will raise any exceptions caught during the execution of the task
        except Exception as e:
            self.logger.error(f"Error in background task: {e}")


def main():
    # Initialize system configurations and logging
    initialize_system(config)
    
    # Define a global shutdown event to manage the graceful shutdown of the application
    global shutdown_event
    shutdown_event = Event()

    # Function to handle graceful exit signals (SIGINT/SIGTERM)
    def graceful_exit(signum, frame):
        """
        Signal handler for graceful shutdown.
        Sets the shutdown event to signal the polling loop to exit.
        """
        logger.info("Graceful shutdown initiated.")
        shutdown_event.set()

    # Register signal handlers for graceful shutdown
    signal.signal(signal.SIGINT, graceful_exit)
    signal.signal(signal.SIGTERM, graceful_exit)

    # Load the directory structure from the configuration
    try:
        with open(DIRECTORY_STRUCTURE_PATH, 'r') as f:
            directory_structure = json.load(f)
        group_folders = {group: users['membersOf'] for group, users in directory_structure['Groups'].items()}
    except Exception as e:
        logger.error(f"Failed to load or parse the directory structure JSON: {e}")
        sys.exit(1)  # Exit if the directory structure cannot be loaded

    # Initialize the DirectoryPoller with the loaded configuration
    poller = DirectoryPoller(config['landing_dir_base_path'], config['staging_dir_path'], group_folders, executor, logger)
    
    # Start the DirectoryPoller to begin monitoring for changes
    poller.start()
    logger.info("Starting the folder monitoring service using polling.")

    # Main loop waits for the shutdown event
    try:
        while not shutdown_event.is_set():
            time.sleep(1)
    finally:
        # Cleanup operations
        logger.info("Stopping the folder monitoring service.")
        poller.stop()  # Stop the DirectoryPoller
        executor.shutdown(wait=True)  # Shutdown the ProcessPoolExecutor
        logger.info("Folder monitoring service stopped.")

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python main.py [config_file]")
        sys.exit(1)
    CONFIG_PATH = sys.argv[1]
    config = load_settings(CONFIG_PATH)  # Load the configuration early
    main()
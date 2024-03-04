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
import datetime

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
        self.id = str(uuid.uuid4())
        self.group = group
        self.user = user
        self.project = project
        self.landing_path = Path(landing_dir_base_path) / group / user / project
        self.staging_path = Path(staging_dir_base_path) / group / user / project
        self.hidden_path = None
        self.datasets = {}

    def update_datapackage_data(self, **kwargs):
        for key, value in kwargs.items():
            if hasattr(self, key):
                setattr(self, key, value)

class IngestionProcess:
    def __init__(self, data_package, config, ingestion_id):
        self.data_package = data_package
        self.config = config
        self.ingestion_id = ingestion_id
        self.failure_handler = UploadFailureHandler(config)
        self.process_steps = [
            self.move_data_package,
            self.categorize_datasets,
            self.import_data_package,
        ]
    
    def execute(self):
        for step in self.process_steps:
            if not self.run_step(step):
                self.handle_failure()
                break
    
    def run_step(self, step_function):
        try:
            step_function()
            return True
        except Exception as e:
            logger.error(f"Error during {step_function.__name__} step: {e}")
            return False
        
    def move_data_package(self):
        mover = DataPackageMover(self.data_package, self.config)
        move_result = mover.move_data_package()
        if not move_result:
            raise Exception(f"Failed to move data package for project {self.data_package.project}. Check logs for details.")
        self.log_ingestion_step("Data Moved")
        
        # Log each attribute of the DataPackage class
        logger.info("Logging DataPackage attributes post-move:")
        for attr, value in self.data_package.__dict__.items():
            logger.info(f"{attr}: {value}")
    
    def categorize_datasets(self):
        stager = DataPackageStager(self.config)
        self.data_package.datasets = stager.identify_datasets(self.data_package)
        logger.info(f"Datasets categorized for data package {self.data_package.project}.")
        self.log_ingestion_step("Datasets Categorized")
        
    def import_data_package(self):
        importer = DataPackageImporter(self.config)
        successful_uploads, failed_uploads, importer_failed = importer.import_data_package(self.data_package)
        if importer_failed:
            raise Exception(f"Import process failed for data package {self.data_package.project}.")
        logger.info(f"Data package {self.data_package.project} processed successfully with {len(successful_uploads)} successful uploads and {len(failed_uploads)} failed uploads.")
        self.log_ingestion_step("Data Imported")
        if failed_uploads:
            self.failure_handler.move_failed_uploads(failed_uploads, self.data_package.user)
            logger.info(f"Failed uploads for data package {self.data_package.project} have been handled.")
            self.log_ingestion_step("Failed Uploads Handled")

    def handle_failure(self):
        #TODO check why if I am giving both datapackage and one of its attributes for a reason or a mistake
        self.failure_handler.move_entire_data_package(self.data_package, self.data_package.staging_path)
        logger.error(f"Due to errors, the entire data package {self.data_package.project} has been moved to failed uploads.")
        self.log_ingestion_step("Process Failed - Moved to Failed Uploads")

    def log_ingestion_step(self, step_description):
        log_ingestion_step(self.data_package.group, self.data_package.user, self.data_package.project, step_description, self.ingestion_id)

# Handler class
class DirectoryPoller:
    def __init__(self, landing_dir_base_path, staging_dir_base_path, group_folders, executor, logger, interval=10):
        self.landing_path = Path(landing_dir_base_path)
        self.staging_path = Path(staging_dir_base_path)
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
                    user_folder = self.landing_path / group / user
                    if not user_folder.exists():
                        continue
                    for item in user_folder.iterdir():
                        # Check if the item is a directory or a file
                        if item.is_dir() or item.is_file():
                            package_name = item.name
                            # Determine if the item is new or has been modified since last checked
                            if package_name not in last_checked or item.stat().st_mtime > last_checked.get(package_name, 0):
                                self.process_event(group, user, package_name, item)
                                last_checked[package_name] = item.stat().st_mtime
            time.sleep(self.interval)

    def process_event(self, group, user, package_name, created_path):
        data_package = DataPackage(self.landing_path, self.staging_path, group, user, package_name)
        self.logger.info(
            f"  DataPackage detected:\n"
            f"  Path: {created_path},\n"
            f"  Group: {group},\n"
            f"  User: {user},\n"
            f"  Package Name: {package_name},\n"
            f"  Ingestion ID: {data_package.id}"
        )
        log_ingestion_step(group, user, package_name, "Data Package Detected", data_package.id)
        ingestion_process = IngestionProcess(data_package, config, data_package.id)
        future = self.executor.submit(ingestion_process.execute)
        future.add_done_callback(self.log_future_exception)

    def log_future_exception(self, future):
        try:
            future.result()  # This will raise any exceptions caught during the execution of the task
        except Exception as e:
            self.logger.error(f"Error in background task: {e}")

def log_ready_flag(logger):
    """
    Logs a decorative flag indicating the system is ready to upload data to OMERO.
    """
    line_pattern = "/\\/\\/\\/\\/\\/\\/\\/\\/\\/\\/\\/\\/\\/\\/\\/\\/\\/\\/\\/\\/\\/\\/\\/\\/"
    logger.info("\n" + line_pattern + "\n         READY TO UPLOAD DATA TO OMERO\n" + line_pattern)

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
    poller = DirectoryPoller(config['landing_dir_base_path'], config['staging_dir_base_path'], group_folders, executor, logger)
    
    # Start the DirectoryPoller to begin monitoring for changes
    poller.start()
    logger.info("Starting the folder monitoring service using polling.")

    # Log the ready flag
    log_ready_flag(logger)

    # Main loop waits for the shutdown event
    start_time = datetime.datetime.now()
    try:
        while not shutdown_event.is_set():
            time.sleep(1)
    finally:
        # Cleanup operations
        logger.info("Stopping the folder monitoring service.")
        poller.stop()  # Stop the DirectoryPoller
        executor.shutdown(wait=True)  # Shutdown the ProcessPoolExecutor
        logger.info("Folder monitoring service stopped.")
        end_time = datetime.datetime.now()
        runtime = end_time - start_time
        logger.info(f"Program completed. Total runtime: {runtime}")

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python main.py [config_file]")
        sys.exit(1)
    CONFIG_PATH = sys.argv[1]
    config = load_settings(CONFIG_PATH)  # Load the configuration early
    main()
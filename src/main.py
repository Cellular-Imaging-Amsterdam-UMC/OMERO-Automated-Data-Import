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
from utils.config_manager import load_settings, load_json
from utils.initialize import initialize_system
from utils.upload_order_manager import UploadOrderManager
from utils.importer import DataPackageImporter
from utils.ingest_tracker import log_ingestion_step
from utils.failure_handler import UploadFailureHandler

# Setup Configuration
config = load_settings(sys.argv[1] if len(sys.argv) > 1 else "config/settings.yml")
groups_info = load_json(config['group_list'])
executor = ProcessPoolExecutor(max_workers=config['max_workers'])
logger = setup_logger(__name__, config['log_file_path'])

class DataPackage:
    def __init__(self, uuid, base_dir, group, user, project, dataset):
        self.uuid = uuid
        self.base_dir = base_dir
        self.group = group
        self.user = user
        self.project = project
        self.dataset = dataset # dir name, cannot be single file

    def update_datapackage_attributes(self, **kwargs):
        for key, value in kwargs.items():
            if hasattr(self, key):
                setattr(self, key, value)

class IngestionProcess:
    def __init__(self, data_package, config, ingestion_uuid):
        self.data_package = data_package
        self.config = config
        self.ingestion_uuid = ingestion_uuid
        self.failure_handler = UploadFailureHandler(config)
    
    def import_data_package(self):
        try:
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
                
        except Exception as e:
            self.handle_failure()
            logger.error(f"Error during import_data_package: {e}")

    def handle_failure(self):
        self.failure_handler.move_entire_data_package(self.data_package, self.data_package.staging_path)
        logger.error(f"Due to errors, the entire data package {self.data_package.project} has been moved to failed uploads.")
        self.log_ingestion_step("Process Failed - Moved to Failed Uploads")

    def log_ingestion_step(self, step_description):
        log_ingestion_step(self.data_package.group, self.data_package.user, self.data_package.project, step_description, self.ingestion_id)

# Handler class
class DirectoryPoller:
    def __init__(self, config, executor, logger, interval=10):
        self.base_dir = Path(config['base_dir'])
        self.core_grp_names = [group["core_grp_name"] for group in load_json(config['group_list']) if "core_grp_name" in group]
        self.upload_orders_dir_name = config['upload_orders_dir_name']
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
            for core_grp_name in self.core_grp_names:
                group_folder = self.base_dir / core_grp_name / self.upload_orders_dir_name
                if not group_folder.exists():
                    continue
                for item in group_folder.iterdir():
                    # Check if the item is a directory or a file
                    if item.is_dir() or item.is_file():
                        package_name = item.name
                        # Determine if the item is new or has been modified since last checked
                        if package_name not in last_checked or item.stat().st_mtime > last_checked.get(package_name, 0):
                            self.process_event(core_grp_name, package_name, item)
                            last_checked[package_name] = item.stat().st_mtime
            time.sleep(self.interval)

    def process_event(self, core_grp_name, package_name, created_path):
        if created_path.suffix == '.txt':  # Adjust based on your file naming/extension
            order_manager = UploadOrderManager(str(created_path), 'config/settings.yml')
            group, user, project, dataset = order_manager.get_order_info()  # Unpack the returned tuple

            # Create a DataPackage instance with the unpacked information
            data_package = DataPackage(uuid.uuid4(), self.base_dir, group, user, project, dataset)
            self.logger.info(
                f"DataPackage detected:\n"
                f"Group: {group},\n"
                f"User: {user},\n"
                f"Project: {project},\n"
                f"Dataset: {dataset},\n"
                f"Ingestion UUID: {data_package.uuid}"
            )
            log_ingestion_step(group, user, project, "Data Package Detected", data_package.uuid)
            ingestion_process = IngestionProcess(data_package, self.config, data_package.uuid)
            future = self.executor.submit(ingestion_process.import_data_package)
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
    line_pattern = "/\\/\\/\\/\\/\\/\\/\\/\\/\\/\\/\\/\\/\\/\\/\\/\\/\\/\\/\\/\\/\\/\\/"
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

    # Initialize the DirectoryPoller with the loaded configuration
    poller = DirectoryPoller(config, executor, logger)
    
    # Start the DirectoryPoller to begin monitoring for changes
    poller.start()
    log_ready_flag(logger) # Log the ready flag
    start_time = datetime.datetime.now() # Main loop waits for the shutdown event
    try:
        while not shutdown_event.is_set():
            time.sleep(1)
    finally:
        # Cleanup operations
        logger.info("/\\/\\/ STOPPING aUTOMATIC UPLOAD SERVICE /\\/\\/")
        poller.stop()  # Stop the DirectoryPoller
        executor.shutdown(wait=True)  # Shutdown the ProcessPoolExecutor
        end_time = datetime.datetime.now()
        runtime = end_time - start_time
        logger.info(f"Program completed. Total runtime: {runtime}")

if __name__ == "__main__":
    try:
        CONFIG_PATH = sys.argv[1]
    except IndexError:
        print("Usage: python main.py [config_file]")
        sys.exit(1)
    config = load_settings(CONFIG_PATH)  # Load the configuration early
    main()
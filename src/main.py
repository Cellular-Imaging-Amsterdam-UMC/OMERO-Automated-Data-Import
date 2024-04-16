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

# main.py

from pathlib import Path
import time
from concurrent.futures import ProcessPoolExecutor
import signal
from threading import Event, Thread
import datetime

#Modules
from utils.config_manager import load_settings, load_json
from utils.logger import setup_logger, log_flag
from utils.initialize import initialize_system
from utils.upload_order_manager import UploadOrderManager
from utils.importer import DataPackageImporter
from utils.ingest_tracker import log_ingestion_step

# Setup Configuration
config = load_settings("config/settings.yml")
groups_info = load_json(config['group_list'])
executor = ProcessPoolExecutor(max_workers=config['max_workers'])
logger = setup_logger(__name__, config['log_file_path'])

class DataPackage:
    def __init__(self, uuid, base_dir, group, username, dataset, path, files, upload_order_name):
        self.uuid = uuid
        self.base_dir = base_dir
        self.group = group
        self.username = username
        self.dataset = dataset
        self.path = path
        self.files = files
        self.upload_order_name = upload_order_name

#TODO add a function to move the upload order to the ".failed_uploads" or ".Uploaded" directories (in upload_order_manager) 
class IngestionProcess:
    def __init__(self, data_package, config, uuid, order_manager):
        self.data_package = data_package
        self.config = config
        self.uuid = uuid
        self.order_manager = order_manager
    
    def import_data_package(self):
        try:
            importer = DataPackageImporter(self.config)
            successful_uploads, failed_uploads, importer_failed = importer.import_data_package(self.data_package)
            
            if importer_failed or failed_uploads:
                # Handle failed uploads
                self.order_manager.move_upload_order('failed')
                logger.error(f"Import process failed for data package in {self.data_package.dataset} due to failed uploads or importer failure.")
                self.log_ingestion_step("Process Failed - Moved to Failed Uploads")
                return
            
            # Handle successful uploads
            self.order_manager.move_upload_order('completed')
            logger.info(f"Data package in {self.data_package.dataset} processed successfully with {len(successful_uploads)} successful uploads.")
                
        except Exception as e:
            logger.error(f"Error during import_data_package: {e}")

    def log_ingestion_step(self, step_description):
        log_ingestion_step(self.data_package.group, self.data_package.username, self.data_package.dataset, step_description, str(self.uuid))

# Handler class
class DirectoryPoller:
    def __init__(self, config, executor, logger, interval=10):
        self.config = config
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
                            self.process_event(item)
                            last_checked[package_name] = item.stat().st_mtime
            time.sleep(self.interval)

    def process_event(self, created_path):
        if created_path.suffix == '.txt': 
            order_manager = UploadOrderManager(str(created_path), self.config)
            uuid, group, username, dataset, path, files = order_manager.get_order_info()

            # Create a DataPackage instance with the unpacked information
            data_package = DataPackage(uuid, self.base_dir, group, username, dataset, path, files, created_path.name)
            self.logger.info(
                f"  DataPackage detected:\n"
                f"  UUID: {uuid}\n"
                f"  Group: {group}\n"
                f"  Username: {username}\n"
                f"  Dataset: {dataset}\n"
                f"  Path: {path}\n"
                f"  Files: {files}\n"
                f"  Upload Order Name: {created_path.name}"
            )
            log_ingestion_step(group, username, dataset, "Data Package Detected", str(uuid))
            
            # Pass the existing UploadOrderManager instance to IngestionProcess
            ingestion_process = IngestionProcess(data_package, self.config, uuid, order_manager)
            future = self.executor.submit(ingestion_process.import_data_package)
            future.add_done_callback(self.log_future_exception)

    def log_future_exception(self, future):
        try:
            future.result()
        except Exception as e:
            self.logger.error(f"Error in background task: {e}")

def main():
    # Initialize system configurations and logging
    initialize_system(config)
    
    # Define a global shutdown event to manage the graceful shutdown of the application
    # TODO review this shutdown mechanism, I find it somewhat excessive.
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
    
    # Start the DirectoryPoller to begin monitoring for changes
    poller = DirectoryPoller(config, executor, logger)
    poller.start()
    log_flag(logger, 'start')
    start_time = datetime.datetime.now() # Main loop waits for the shutdown event
    try:
        while not shutdown_event.is_set():
            time.sleep(1)
    finally:
        # Cleanup operations
        log_flag(logger, 'end') 
        poller.stop()  # Stop the DirectoryPoller
        executor.shutdown(wait=True)  # Shutdown the ProcessPoolExecutor
        end_time = datetime.datetime.now()
        logger.info(f"Program completed. Total runtime: {end_time - start_time}")

if __name__ == "__main__":
    main()
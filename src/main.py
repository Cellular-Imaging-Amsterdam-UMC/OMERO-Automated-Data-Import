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

# Standard library imports
from pathlib import Path
import time
from concurrent.futures import ProcessPoolExecutor
import signal
from threading import Event, Thread
import datetime

# Local module imports
from utils.config_manager import load_settings, load_json
from utils.logger import setup_logger, log_flag
from utils.initialize import initialize_system
from utils.upload_order_manager import UploadOrderManager
from utils.importer import DataPackageImporter
from utils.ingest_tracker import STAGE_DETECTED, log_ingestion_step

# Setup Configuration
config = load_settings("config/settings.yml")
groups_info = load_json(config['group_list'])
executor = ProcessPoolExecutor(max_workers=config['max_workers'])
logger = setup_logger(__name__, config['log_file_path'])

class DataPackage:
    """
    Represents a data package containing information from the upload order.
    """
    def __init__(self, order_data, base_dir):
        """
        Initialize the DataPackage with order data and base directory.
        
        :param order_data: Dictionary containing order information
        :param base_dir: Base directory for the data package
        """
        self.__dict__.update(order_data)
        self.base_dir = base_dir
        
        # Verify that ID fields are integers
        for field in ['UserID', 'GroupID', 'ProjectID', 'DatasetID']:
            if field in self.__dict__:
                if not isinstance(self.__dict__[field], int):
                    logger.warning(f"{field} is not an integer: {self.__dict__[field]}")

    def __str__(self):
        """
        Return a string representation of the DataPackage.
        """
        attributes = [f"{key}: {value}" for key, value in self.__dict__.items() if key != 'Files']
        file_count = len(self.Files) if hasattr(self, 'Files') else 0
        return f"DataPackage({', '.join(attributes)}, Files: {file_count} files)"

    def get(self, key, default=None):
        """
        Safely get an attribute value with a default if not found.
        
        :param key: Attribute name to retrieve
        :param default: Default value if attribute is not found
        :return: Attribute value or default
        """
        return self.__dict__.get(key, default)

class IngestionProcess:
    """
    Handles the ingestion process for a data package.
    """
    def __init__(self, data_package, config, order_manager):
        """
        Initialize the IngestionProcess with a data package, config, and order manager.
        
        :param data_package: DataPackage object to be ingested
        :param config: Configuration dictionary
        :param order_manager: UploadOrderManager object
        """
        self.data_package = data_package
        self.config = config
        self.order_manager = order_manager
    
    def import_data_package(self):
        """
        Import the data package and handle the outcome.
        """
        try:
            importer = DataPackageImporter(self.config)
            successful_uploads, failed_uploads, import_failed = importer.import_data_package(self.data_package)
            
            if import_failed or failed_uploads:
                self.order_manager.move_upload_order('failed')
                logger.error(f"Import process failed for data package in {self.data_package.get('Dataset', 'Unknown')} due to failed uploads or importer failure.")
                self.log_ingestion_step("Process Failed - Moved to Failed Uploads")
            else:
                self.order_manager.move_upload_order('completed')
                logger.info(f"Data package in {self.data_package.get('Dataset', 'Unknown')} processed successfully with {len(successful_uploads)} successful uploads.")
                self.log_ingestion_step("Process Completed - Moved to Completed Uploads")
            
            return successful_uploads, failed_uploads, import_failed
        except Exception as e:
            logger.error(f"Error during import_data_package: {e}, {type(e)}")
            self.order_manager.move_upload_order('failed')
            self.log_ingestion_step("Process Failed - Unexpected Error")
            return [], [], True

    def log_ingestion_step(self, step_description):
        """
        Log an ingestion step to the database.
        
        :param step_description: Description of the ingestion step
        """
        log_ingestion_step(
            self.data_package.get('Group', 'Unknown'),
            self.data_package.get('Username', 'Unknown'),
            self.data_package.get('DatasetID', 'Unknown'),
            step_description,
            str(self.data_package.get('UUID', 'Unknown')),
            self.data_package.get('Files', 'Unknown')
        )

class DirectoryPoller:
    """
    Polls directories for new upload order files and processes them.
    """
    def __init__(self, config, executor, logger, interval=3):
        """
        Initialize the DirectoryPoller.
        
        :param config: Configuration dictionary
        :param executor: ProcessPoolExecutor object
        :param logger: Logger object
        :param interval: Polling interval in seconds
        """
        self.config = config
        self.base_dir = Path(config['base_dir'])
        self.core_grp_names = [group["core_grp_name"] for group in load_json(config['group_list']) if "core_grp_name" in group]
        self.upload_orders_dir_name = config['upload_orders_dir_name']
        self.executor = executor
        self.logger = logger
        self.interval = interval
        self.shutdown_event = Event()
        self.processing_orders = set()

    def start(self):
        """Start the directory polling thread."""
        self.polling_thread = Thread(target=self.poll_directory_changes)
        self.polling_thread.start()

    def stop(self):
        """Stop the directory polling thread."""
        self.shutdown_event.set()
        self.polling_thread.join()

    def poll_directory_changes(self):
        """
        Continuously poll directories for changes and process new files.
        """
        while not self.shutdown_event.is_set():
            for core_grp_name in self.core_grp_names:
                group_folder = self.base_dir / core_grp_name / self.upload_orders_dir_name
                for order_file in sorted(group_folder.iterdir(), key=lambda x: x.stat().st_mtime):
                    if order_file.is_file() and self.is_valid_order_file(order_file):
                        self.process_event(order_file)
            time.sleep(self.interval)

    def is_valid_order_file(self, order_file):
        """
        Check if the file name is a valid order file name.
        
        :param order_file: Path to the file
        :return: True if the file name is valid, False otherwise
        """
        try:
            datetime.datetime.strptime(order_file.stem, "%Y_%m_%d_%H-%M-%S")
            return True
        except ValueError:
            return False

    def process_event(self, order_file):
        """
        Process a newly created or modified file.
        
        :param order_file: Path to the new or modified file
        """
        if str(order_file) in self.processing_orders:
            return  # Order is already being processed

        self.logger.info(f"Processing new upload order: {order_file}")
        try:
            order_manager = UploadOrderManager(str(order_file), self.config)
            order_info = order_manager.get_order_info()
            
            data_package = DataPackage(order_info, self.base_dir)
            self.logger.info(f"DataPackage detected: {data_package}")
            log_ingestion_step(
                data_package.get('Group', 'Unknown'),
                data_package.get('Username', 'Unknown'),
                data_package.get('Dataset', 'Unknown'),
                STAGE_DETECTED,
                str(data_package.get('UUID', 'Unknown')),
                data_package.get('Files', ['Unknown'])
            )
            
            ingestion_process = IngestionProcess(data_package, self.config, order_manager)
            future = self.executor.submit(ingestion_process.import_data_package)
            future.add_done_callback(self.order_completed)
            
            self.processing_orders.add(str(order_file))
        except Exception as e:
            self.logger.error(f"Error processing upload order {order_file}: {e}")

    def order_completed(self, future):
        """
        Handle the completion of an upload order.
        
        :param future: Future object representing the completed order
        """
        try:
            result = future.result()
            # Handle the result if needed
        except Exception as e:
            self.logger.error(f"Error in processing order: {e}")
        finally:
            # Remove the completed order from processing_orders
            completed_orders = [order for order in self.processing_orders if order not in self.executor._pending_work_items]
            for order in completed_orders:
                self.processing_orders.remove(order)
                self.logger.info(f"Order completed and removed from processing list: {order}")

    def log_future_exception(self, future):
        """
        Log any exceptions that occur in the future tasks.
        
        :param future: Future object to check for exceptions
        """
        try:
            future.result()
        except Exception as e:
            self.logger.error(f"Error in background task: {e}")

def main():
    """
    Main function to run the OMERO Automated Data Import system.
    """
    # Initialize system configurations and logging
    initialize_system(config)
    
    # Define a global shutdown event to manage the graceful shutdown of the application
    global shutdown_event
    shutdown_event = Event()

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
    start_time = datetime.datetime.now()

    # Main loop waits for the shutdown event
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
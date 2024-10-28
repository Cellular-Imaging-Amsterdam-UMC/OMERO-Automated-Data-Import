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
from threading import Event, Thread, Lock
import datetime
import yaml
import json
import sys

# Local module imports
from utils.logger import setup_logger, log_flag
from utils.initialize import initialize_system
from utils.upload_order_manager import UploadOrderManager
from utils.importer import DataPackageImporter
from utils.ingest_tracker import STAGE_DETECTED, STAGE_MOVED_COMPLETED, STAGE_MOVED_FAILED, log_ingestion_step
from utils.logger import LoggerManager

def load_settings(file_path):
    """
    Load settings from either a YAML or JSON file.
    
    :param file_path: Path to the settings file
    :return: Loaded settings as a dictionary
    """
    with open(file_path, 'r') as file:
        if file_path.endswith('.yml') or file_path.endswith('.yaml'):
            return yaml.safe_load(file)
        elif file_path.endswith('.json'):
            return json.load(file)
        else:
            raise ValueError(f"Unsupported file format: {file_path}")

def load_config(settings_path="config/settings.yml"):
    config = load_settings(settings_path)
    groups_info = load_settings(config['group_list'])
    return config, groups_info

def create_executor(config):
    return ProcessPoolExecutor(max_workers=config['max_workers'])

def setup_logging(config):
    """
    Setup logging with error handling and resource management.
    """
    try:
        return LoggerManager.setup_logger(__name__, config['log_file_path'])
    except Exception as e:
        print(f"Critical error setting up logger: {e}")
        sys.exit(1)

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
        Initialize the IngestionProcess with a data package, config, order manager, and logger.
        
        :param data_package: DataPackage object to be ingested
        :param config: Configuration dictionary
        :param order_manager: UploadOrderManager object
        :param logger: Logger instance to log messages
        """
        self.data_package = data_package
        self.config = config
        self.order_manager = order_manager
        self.logger = LoggerManager.get_module_logger(__name__)
        
    def import_data_package(self):
        """
        Import the data package and handle the outcome.
        """
        try:
            importer = DataPackageImporter(self.config, self.data_package)
            successful_uploads, failed_uploads, import_failed = importer.import_data_package()
            parent_id = self.data_package.get('DatasetID', self.data_package.get('ScreenID','Unknown'))
            if import_failed or failed_uploads:
                self.order_manager.move_upload_order('failed')
                self.logger.error(f"Import process failed for data package {self.data_package.get('UUID')} in {parent_id} due to failed uploads or importer failure.")
            else:
                self.order_manager.move_upload_order('completed')
                self.logger.info(f"Data package {self.data_package.get('UUID')} in {parent_id} processed successfully with {len(successful_uploads)} successful uploads.")            
            return successful_uploads, failed_uploads, import_failed
        except Exception as e:
            self.logger.error(f"Error during import_data_package: {e}")
            self.order_manager.move_upload_order('failed')
            return [], [], True

class DirectoryPoller:
    """
    Polls directories for new upload order files and processes them.
    """
    def __init__(self, config, executor, interval=3):
        """
        Initialize the DirectoryPoller.
        
        :param config: Configuration dictionary
        :param executor: ProcessPoolExecutor object
        :param logger: Logger object
        :param interval: Polling interval in seconds
        """
        self.config = config
        self.base_dir = Path(config['base_dir'])
        self.core_grp_names = [group["core_grp_name"] for group in load_settings(config['group_list']) if "core_grp_name" in group]
        self.upload_orders_dir_name = config['upload_orders_dir_name']
        self.executor = executor
        self.logger = LoggerManager.get_module_logger(__name__)
        self.interval = interval
        self.shutdown_event = Event()
        self.processing_orders = set()
        self.processing_lock = Lock()

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
        with self.processing_lock:
            if str(order_file) in self.processing_orders:
                return  # Order is already being processed

        self.logger.info(f"Processing new upload order: {order_file}")
        try:
            order_manager = UploadOrderManager(str(order_file), self.config)
            order_info = order_manager.get_order_info()
            
            data_package = DataPackage(order_info, self.base_dir)
            self.logger.info(f"DataPackage detected: {data_package}")
            
            # Update this part to use the new log_ingestion_step signature
            log_ingestion_step(data_package.__dict__, STAGE_DETECTED)
            
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
            with self.processing_lock:
                result = future.result() # TODO: Handle the result if needed
        except Exception as e:
            self.logger.error(f"Error in processing order", exc_info=True)
        finally:
            with self.processing_lock:
                # Remove the completed order from processing_orders
                completed_orders = [order for order in self.processing_orders if order not in self.executor._pending_work_items]
                for order in completed_orders:
                    self.processing_orders.remove(order)
                    self.logger.info(f"Order completed and removed from processing list: {order}")
                self.processing_orders.clear()

    def log_future_exception(self, future):
        """
        Log any exceptions that occur in the future tasks.
        
        :param future: Future object to check for exceptions
        """
        try:
            future.result()
        except Exception as e:
            self.logger.error(f"Error in background task: {e}")

def run_application(config, groups_info, executor):
    # Initialize system configurations and logging
    if not LoggerManager.is_initialized():
        raise RuntimeError("Logger must be initialized before running application")
    
    logger = LoggerManager.get_module_logger(__name__)
    initialize_system(config)
    
    shutdown_event = Event()
    shutdown_timeout = config.get('shutdown_timeout', 30)

    def graceful_exit(signum, frame):
        logger.info("Graceful shutdown initiated.")
        shutdown_event.set()

    # Register signal handlers for graceful shutdown
    signal.signal(signal.SIGINT, graceful_exit)
    signal.signal(signal.SIGTERM, graceful_exit)
    
    # Start the DirectoryPoller to begin monitoring for changes
    poller = DirectoryPoller(config, executor)
    poller.start()
    log_flag(logger, 'start')
    start_time = datetime.datetime.now()

    # Main loop waits for the shutdown event
    try:
        while not shutdown_event.is_set():
            time.sleep(1)
    finally:
        # Cleanup operations
        logger.info("Initiating shutdown sequence...")
        log_flag(logger, 'end') 
        poller.stop()
        executor.shutdown(timeout=shutdown_timeout)
        LoggerManager.cleanup(timeout=shutdown_timeout)
        end_time = datetime.datetime.now()
        logger.info(f"Program completed. Total runtime: {end_time - start_time}")

def main():
    try:
        config, groups_info = load_config()
        # Setup logging first
        LoggerManager.setup_logger(__name__, config['log_file_path'])
        logger = LoggerManager.get_module_logger(__name__)
        
        # Then create executor with logging
        logger.info("Creating process executor...")
        executor = create_executor(config)
        
        run_application(config, groups_info, executor)
    except Exception as e:
        print(f"Fatal error in main: {e}")
        sys.exit(1)
    finally:
        LoggerManager.cleanup()

if __name__ == "__main__":
    main()

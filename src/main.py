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
import logging
from collections import UserDict

# Local module imports
from utils.initialize import initialize_system
from utils.upload_order_manager import UploadOrderManager
from utils.importer import DataPackageImporter
from utils.ingest_tracker import log_ingestion_step, STAGE_DETECTED #, STAGE_MOVED_COMPLETED, STAGE_MOVED_FAILED

def load_settings(file_path):
    """
    Load settings from either a YAML or JSON file.
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
    """Create a ProcessPoolExecutor with proper logging initialization."""
    def init_worker():
        # Initialize logging in worker processes
        import logging
        import sys

        log_level = config.get('log_level', 'INFO').upper()
        log_file = config['log_file_path']

        logging.basicConfig(
            level=getattr(logging, log_level, logging.INFO),
            format='%(asctime)s - PID:%(process)d - %(name)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler(log_file),
                logging.StreamHandler(sys.stdout)
            ]
        )

    return ProcessPoolExecutor(
        max_workers=config['max_workers'],
        initializer=init_worker
    )

def log_flag(logger, flag_type):
    line_pattern = "    /\\/\\/\\/\\/\\/\\/\\/\\/\\/\\/\\/\\/\\/\\/\\/\\/\\/\\/\\/\\/\\/\\/"
    if flag_type == 'start':
        logger.info("\n" + line_pattern + "\n           READY TO UPLOAD DATA TO OMERO\n" + line_pattern)
    elif flag_type == 'end':
        logger.info("\n" + line_pattern + "\n           STOPPING AUTOMATIC UPLOAD SERVICE\n" + line_pattern)

class DataPackage(UserDict):
    """
    Represents a data package containing information from the upload order.
    """
    def __init__(self, order_data, base_dir, order_file):
        """
        Initialize the DataPackage with order data and base directory.
        
        :param order_data: Dictionary containing order information
        :param base_dir: Base directory for the data package
        """
        self.data = order_data
        self.base_dir = base_dir
        self.order_file = str(order_file)

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
        self.logger = logging.getLogger(__name__)
        
    def import_data_package(self):
        """
        Import the data package and handle the outcome.
        """
        try:
            importer = DataPackageImporter(self.config, self.data_package)
            successful_uploads, failed_uploads, import_failed = importer.import_data_package()
            parent_id = self.data_package.get('DatasetID', self.data_package.get('ScreenID', 'Unknown'))
            if import_failed or failed_uploads:
                self.order_manager.move_upload_order('failed')
                self.logger.error(f"Import process failed for data package {self.data_package.get('UUID')} in {parent_id} due to failed uploads or importer failure.")
            else:
                self.order_manager.move_upload_order('completed')
                self.logger.info(f"Data package {self.data_package.get('UUID')} in {parent_id} processed successfully with {len(successful_uploads)} successful uploads.")
            return self.data_package.order_file
        except Exception as e:
            self.logger.error(f"Error during import_data_package: {e}", exc_info=True)
            self.order_manager.move_upload_order('failed')
            return self.data_package.order_file

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
        self.logger = logging.getLogger(f"{__name__}.poller")
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
        #TODO: Check that it is version 3(or current version of the order file format)
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
            else:
                self.processing_orders.add(str(order_file))
        self.logger.info(f"Processing new upload order: {order_file}")

        try:
            order_manager = UploadOrderManager(str(order_file), self.config)
            order_info = order_manager.get_order_info()

            data_package = DataPackage(order_info, self.base_dir, order_file)
            self.logger.info(f"DataPackage detected: {data_package}")

            log_ingestion_step(data_package, STAGE_DETECTED)

            # Subprocessor function called
            ingestion_process = IngestionProcess(data_package, self.config, order_manager)
            future = self.executor.submit(ingestion_process.import_data_package)

            # When subprocessor function finishes, order_completed is called
            future.add_done_callback(self.order_completed)

        except Exception as e:
            self.logger.error(f"Error processing upload order {order_file}: {e}", exc_info=True)

    def order_completed(self, future):
        """
        Handle the completion of an upload order by removing the completed order from processing_orders
        """
        with self.processing_lock:
            completed_order = future.result()
            self.processing_orders.remove(completed_order)
            self.logger.info(f"Order completed and removed from processing list: {completed_order}.")

def run_application(config, groups_info, executor):
    logger = logging.getLogger(__name__)
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
        executor.shutdown(wait=True, timeout=shutdown_timeout)
        end_time = datetime.datetime.now()
        logger.info(f"Program completed. Total runtime: {end_time - start_time}")

def main():
    try:
        # Load configuration first
        config, groups_info = load_config()

        # Setup logging
        log_level = config.get('log_level', 'DEBUG').upper()
        log_file = config['log_file_path']

        logging.basicConfig(
            level=getattr(logging, log_level, logging.INFO),
            format='%(asctime)s - PID:%(process)d - %(name)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler(log_file),
                logging.StreamHandler(sys.stdout)
            ]
        )

        logger = logging.getLogger(__name__)
        logger.info("Starting application...")

        # Initialize System (which includes IngestTracker)
        initialize_system(config)

        # Create executor with logging
        logger.info("Creating process executor...")
        executor = create_executor(config)

        run_application(config, groups_info, executor)
    except Exception as e:
        print(f"Fatal error in main: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()

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

# Local module importsa
from utils.initialize import initialize_system
from utils.upload_order_manager import UploadOrderManager
from utils.importer import DataPackageImporter
from utils.ingest_tracker import log_ingestion_step, STAGE_DETECTED, STAGE_MOVED_COMPLETED, STAGE_MOVED_FAILED, STAGE_NEW_ORDER

# --------------------------------------------------
# Utility function to load settings
# --------------------------------------------------
def load_config(settings_path="config/settings.yml"):
    """Load settings from either a YAML or JSON file."""
    with open(settings_path, 'r') as file:
        if settings_path.endswith(('.yml', '.yaml')):
            return yaml.safe_load(file)
        elif settings_path.endswith('.json'):
            return json.load(file)
        else:
            raise ValueError(f"Unsupported file format: {settings_path}")

# --------------------------------------------------
# ProcessPoolExecutor creation remains the same.
# --------------------------------------------------
def create_executor(config):
    """Create a ProcessPoolExecutor with proper logging initialization."""
    def init_worker():
        import logging
        import sys
        log_level = config.get('log_level', 'INFO').upper()
        log_file = config.get('log_file_path', 'logs/app.logs')
        logging.basicConfig(
            level=getattr(logging, log_level, logging.INFO),
            format='%(asctime)s - PID:%(process)d - %(name)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler(log_file),
                logging.StreamHandler(sys.stdout)
            ]
        )
    return ProcessPoolExecutor(
        max_workers=config.get('max_workers', 4),
        initializer=init_worker
    )

def log_flag(logger, flag_type):
    line_pattern = "    /\\/\\/\\/\\/\\/\\/\\/\\/\\/\\/\\/\\/\\/\\/\\/\\/\\/\\/\\/\\/\\/\\/"
    if flag_type == 'start':
        logger.info("\n" + line_pattern + "\n           READY TO UPLOAD DATA TO OMERO\n" + line_pattern)
    elif flag_type == 'end':
        logger.info("\n" + line_pattern + "\n           STOPPING AUTOMATIC UPLOAD SERVICE\n" + line_pattern)

# --------------------------------------------------
# DataPackage and IngestionProcess are refactored to work with DB orders
# --------------------------------------------------
class DataPackage(UserDict):
    """
    Represents a data package containing order information.
    """
    def __init__(self, order_data, base_dir=None, order_identifier=None):
        self.data = order_data
        self.base_dir = base_dir
        self.order_identifier = order_identifier

    def get(self, key, default=None):
        return self.data.get(key, default)

class IngestionProcess:
    """
    Handles the ingestion process for a data package.
    """
    def __init__(self, data_package, config, order_manager):
        """
        :param data_package: DataPackage object to be ingested
        :param config: Configuration dictionary
        :param order_manager: UploadOrderManager object
        """
        self.data_package = data_package
        self.config = config
        self.order_manager = order_manager
        self.logger = logging.getLogger(__name__)
        
    def import_data_package(self):
        """
        Import the data package and log the outcome.
        Instead of moving files, we log the step.
        """
        try:
            importer = DataPackageImporter(self.config, self.data_package)
            successful_uploads, failed_uploads, import_failed = importer.import_data_package()
            parent_id = self.data_package.get('DatasetID', self.data_package.get('ScreenID', 'Unknown'))
            if import_failed or failed_uploads:
                log_ingestion_step(self.data_package.data, STAGE_MOVED_FAILED)
                self.logger.error(f"Import process failed for data package {self.data_package.get('UUID')} in {parent_id} due to failed uploads or importer failure.")
            else:
                log_ingestion_step(self.data_package.data, STAGE_MOVED_COMPLETED)
                self.logger.info(f"Data package {self.data_package.get('UUID')} in {parent_id} processed successfully with {len(successful_uploads)} successful uploads.")
            return self.data_package.get('UUID')
        except Exception as e:
            self.logger.error(f"Error during import_data_package: {e}", exc_info=True)
            log_ingestion_step(self.data_package.data, STAGE_MOVED_FAILED)
            return self.data_package.get('UUID')

# --------------------------------------------------
# DatabasePoller: Polls the DB for new upload orders
# --------------------------------------------------
class DatabasePoller:
    """
    Polls the database for new upload orders.
    New orders are identified by the stage STAGE_NEW_ORDER ("Upload Order Received").
    Once detected, a new ingestion process is triggered.
    """
    def __init__(self, config, executor, poll_interval=5):
        self.config = config
        self.executor = executor
        self.poll_interval = poll_interval
        self.shutdown_event = Event()
        self.logger = logging.getLogger(f"{__name__}.db_poller")

        from utils.ingest_tracker import _ingest_tracker, IngestionTracking
        self.ingest_tracker = _ingest_tracker  # global instance
        self.IngestionTracking = IngestionTracking

    def start(self):
        """Start the polling thread."""
        self.poller_thread = Thread(target=self.poll_database)
        self.poller_thread.start()

    def stop(self):
        """Stop the polling thread."""
        self.shutdown_event.set()
        self.poller_thread.join()

    def poll_database(self):
        """
        Continuously poll the database for new upload orders.
        A new order is identified by having stage == STAGE_NEW_ORDER and (for simplicity) not having been processed further.
        
        To improve efficiency, we track the highest processed ID (last_max_id) and only query for new orders.
        """
        from sqlalchemy.orm import sessionmaker
        Session = self.ingest_tracker.Session
        last_max_id = 0  # Initialize the highest processed ID
        while not self.shutdown_event.is_set():
            session = Session()
            try:
                # Retrieve new orders with stage STAGE_NEW_ORDER and id greater than last_max_id
                new_orders = session.query(self.IngestionTracking).filter(
                    self.IngestionTracking.stage == STAGE_NEW_ORDER,
                    self.IngestionTracking.id > last_max_id
                ).order_by(self.IngestionTracking.id.asc()).all()
                for order in new_orders:
                    if order.id > last_max_id:
                        last_max_id = order.id
                    self.logger.info(f"Detected new upload order with UUID {order.uuid}")
                    # Log detection as a new ingestion step.
                    log_ingestion_step(order.__dict__, STAGE_DETECTED)
                    # Create a DataPackage using the order record.
                    data_package = DataPackage(order.__dict__, order_identifier=order.uuid)
                    # Instantiate the UploadOrderManager from the order record.
                    order_manager = UploadOrderManager.from_order_record(order.__dict__, self.config)
                    ingestion_process = IngestionProcess(data_package, self.config, order_manager)
                    # Submit the ingestion process to the executor.
                    future = self.executor.submit(ingestion_process.import_data_package)
                    future.add_done_callback(
                        lambda f, uuid=order.uuid: self.logger.info(f"Order {uuid} processing complete.")
                    )
            except Exception as e:
                self.logger.error(f"Error polling database: {e}", exc_info=True)
            finally:
                session.close()
            time.sleep(self.poll_interval)

# --------------------------------------------------
# Application run loop
# --------------------------------------------------
def run_application(config, groups_info, executor):
    logger = logging.getLogger(__name__)
    initialize_system(config)
    # Initialize the global ingest tracker (must be called before polling)
    from utils.ingest_tracker import initialize_ingest_tracker
    initialize_ingest_tracker(config)

    shutdown_event = Event()
    shutdown_timeout = config.get('shutdown_timeout', 30)

    def graceful_exit(signum, frame):
        logger.info("Graceful shutdown initiated.")
        shutdown_event.set()

    # Register signal handlers for graceful shutdown
    signal.signal(signal.SIGINT, graceful_exit)
    signal.signal(signal.SIGTERM, graceful_exit)

    # Start the DatabasePoller instead of the DirectoryPoller.
    db_poller = DatabasePoller(config, executor)
    db_poller.start()
    log_flag(logger, 'start')
    start_time = datetime.datetime.now()

    try:
        while not shutdown_event.is_set():
            time.sleep(1)
    finally:
        logger.info("Initiating shutdown sequence...")
        log_flag(logger, 'end')
        db_poller.stop()
        executor.shutdown(wait=True, timeout=shutdown_timeout)
        end_time = datetime.datetime.now()
        logger.info(f"Program completed. Total runtime: {end_time - start_time}")

def main():
    try:
        # Load configuration first
        config = load_config()
        # Groups are now obtained directly from OMERO via the database; no separate groups file is needed.
        groups_info = None  # (Placeholder; not used by ADI anymore.)
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

        logger.info("Creating process executor...")
        executor = create_executor(config)

        run_application(config, groups_info, executor)
    except Exception as e:
        print(f"Fatal error in main: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()

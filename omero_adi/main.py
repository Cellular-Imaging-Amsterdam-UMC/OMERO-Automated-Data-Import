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
from utils.ingest_tracker import log_ingestion_step, STAGE_NEW_ORDER

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
# ProcessPoolExecutor creation
# --------------------------------------------------
def create_executor(config):
    """Create a ProcessPoolExecutor with logging initialization."""
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
# DataPackage and IngestionProcess
# --------------------------------------------------
#TODO: Review this example one more time to have may be nicer names and example files.
class DataPackage(UserDict):
    #TODO: Change DataPackage attribute to Destination ID or something like that.
    # Data package attribute within datapackage class is just sloppy naming.
    """
    A dictionary-like object that represents a data package for OMERO upload orders.
    
    This class extends UserDict to provide dictionary functionality for handling OMERO 
    upload order information. Each DataPackage instance corresponds to a specific upload 
    request tracked in the database, containing essential information such as:
    - Target OMERO Dataset/Screen ID (stored in 'DataPackage' key)
    - Unique order identifier (UUID)
    - Group and user information
    - File paths and names to be uploaded

    Example order data:
        {
            "Group": "Test Group",          # OMERO group name
            "GroupID": "3",                 # OMERO group identifier
            "Username": "ttest",            # OMERO username
            "DataPackage": "1",             # Target Dataset/Screen ID in OMERO
            "UUID": "550e8400-e29b-41d4-a716-446655440000",  # Unique order ID
            "Files": [                      # Full paths to files
                "/auto-importer/tests/Barbie1.tif",
                "/auto-importer/tests/Barbie2.tif",
                "/auto-importer/tests/Barbie3.tif"
            ],
            "FileNames": [                  # Original file names
                "Barbie1.tif",
                "Barbie2.tif",
                "Barbie3.tif"
            ]
        }
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
        :param data_package: DataPackage object.
        :param config: Configuration dictionary gathered from load_config function.
        :param order_manager: UploadOrderManager object
        """
        self.data_package = data_package
        self.config = config
        self.order_manager = order_manager
        self.logger = logging.getLogger(__name__)
        
    def import_data_package(self):
        """
        Import the data package and return the UUID for tracking.
        
        Returns:
            str: The UUID of the data package that was processed
        """
        try:
            importer = DataPackageImporter(self.config, self.data_package)
            successful_uploads, failed_uploads, import_failed = importer.import_data_package()
            target_id = self.data_package.get('DataPackage')
            package_uuid = self.data_package.get('UUID')

            if import_failed or failed_uploads:
                self.logger.error(
                    f"Import failed for data package {package_uuid} targeting OMERO {target_id}. "
                    f"Failed files: {[f[0] for f in failed_uploads]}"
                )
            else:
                self.logger.info(
                    f"Successfully imported data package {package_uuid} to OMERO {target_id}. "
                    f"Imported {len(successful_uploads)} files: {[f[2] for f in successful_uploads]}"
                )
            
            if successful_uploads:
                self.logger.debug(
                    f"Successful uploads details for {package_uuid}:\n" + 
                    "\n".join([f"- {f[2]} -> ID: {f[3]}" for f in successful_uploads])
                )
            
            return package_uuid
            
        except Exception as e:
            self.logger.error(f"Error during import_data_package: {e}", exc_info=True)
            return self.data_package.get('UUID')

# --------------------------------------------------
# DatabasePoller: Polls the DB for new upload orders
# --------------------------------------------------
class DatabasePoller:
    """
    Polls the database for new upload orders.
    New orders are identified by the Stage STAGE_NEW_ORDER ("Upload Order Received").
    Once detected, a new ingestion process is triggered.
    """
    def __init__(self, config, executor, poll_interval=5):
        self.config = config
        self.executor = executor
        self.poll_interval = poll_interval
        self.shutdown_event = Event()
        self.logger = logging.getLogger(f"{__name__}.db_poller")

        from utils.ingest_tracker import _ingest_tracker, IngestionTracking, Base
        self.ingest_tracker = _ingest_tracker  # global instance
        self.IngestionTracking = IngestionTracking
        
        # Ensure tables exist
        Base.metadata.create_all(self.ingest_tracker.engine)

    def start(self):
        """Start the polling thread."""
        self.poller_thread = Thread(target=self.poll_database)
        self.poller_thread.start()

    def stop(self):
        """Stop the polling thread."""
        self.shutdown_event.set()
        self.poller_thread.join()

    def poll_database(self) -> None:
        """
        Continuously polls the database for new upload orders using a context manager
        and processes each new order.
        """
        Session = self.ingest_tracker.Session
        last_max_id = 0  # Initialize the highest processed ID
        while not self.shutdown_event.is_set():
            with Session() as session:
                try:
                    new_orders = session.query(self.IngestionTracking).filter(
                        self.IngestionTracking.Stage == STAGE_NEW_ORDER,
                        self.IngestionTracking.id > last_max_id
                    ).order_by(self.IngestionTracking.id.asc()).all()
                    for order in new_orders:
                        last_max_id = max(last_max_id, order.id)
                        self.process_order(order)
                except Exception as e:
                    self.logger.error(f"Error polling database: {e}", exc_info=True)
            time.sleep(self.poll_interval)

    def process_order(self, order) -> None:
        """
        Process a single new order by logging, creating a DataPackage, and submitting an ingestion process.
        Uses SQLAlchemy model's attribute access directly.
        """
        self.logger.info(f"Detected new upload order with UUID {order.UUID}")
        
        log_ingestion_step(order.__dict__, STAGE_DETECTED) #TODO: Remove this, we are no longer using detected...
        data_package = DataPackage(order.__dict__, order_identifier=order.UUID)
        order_manager = UploadOrderManager.from_order_record(order.__dict__, self.config)
        ingestion_process = IngestionProcess(data_package, self.config, order_manager)
        future = self.executor.submit(ingestion_process.import_data_package)
        future.add_done_callback(self.create_order_callback(order.UUID))

    def create_order_callback(self, uuid: str):
        """
        Returns a callback function that logs when the order processing is complete.
        """
        def callback(future) -> None:
            self.logger.info(f"Order {uuid} processing complete.")
        return callback

# --------------------------------------------------
# Application run loop
# --------------------------------------------------
def run_application(config: dict, groups_info, executor) -> None:
    """
    Runs the main application loop.
    Assumes system initialization has already been done.
    """
    logger = logging.getLogger(__name__)
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
        executor.shutdown(wait=True)
        end_time = datetime.datetime.now()
        logger.info(f"Program completed. Total runtime: {end_time - start_time}")

def main():
    logger = logging.getLogger(__name__)
    try:
        config = load_config()
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

        logger.info("Starting application...")

        # Test OMERO connectivity
        from utils.initialize import test_omero_connection
        test_omero_connection()

        initialize_system(config)
        logger.info("Creating process executor...")
        executor = create_executor(config)
        groups_info = None
        run_application(config, groups_info, executor)
    except Exception as e:
        logger.error("Fatal error in main: %s", e, exc_info=True)
        while True:
            time.sleep(60)

if __name__ == "__main__":
    main()

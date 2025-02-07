import logging
import enum
from sqlalchemy import create_engine, Column, String, Integer, DateTime, Text, Enum, Index
from sqlalchemy.orm import sessionmaker, declarative_base
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.sql import func
from datetime import datetime
import json
from sqlalchemy.sql import text

# Stage constants
STAGE_IMPORTED = "Data Imported"
STAGE_MOVED_COMPLETED = "Order Moved to Completed"
STAGE_MOVED_FAILED = "Order Moved to Failed"
STAGE_DETECTED = "Data Package Detected"
STAGE_PREPROCESSING = "Preprocessing Data"
STAGE_NEW_ORDER = "Upload Order Received"

Base = declarative_base()

class IngestionTracking(Base):
    """Database model for tracking ingestion steps."""
    __tablename__ = 'ingestion_tracking'

    id = Column(Integer, primary_key=True)
    group_name = Column(String, nullable=False)
    group_id = Column(String, nullable=True)  # New field for group ID (if provided by user)
    user_name = Column(String, nullable=False, index=True)
    data_package = Column(String, nullable=False)
    stage = Column(String, nullable=False)
    uuid = Column(String(36), nullable=False, index=True)
    timestamp = Column(DateTime(timezone=True), default=func.now())
    _files = Column("files", Text, nullable=False)
    _file_names = Column("file_names", Text, nullable=True)

    @property
    def files(self):
        return json.loads(self._files)

    @files.setter
    def files(self, files):
        self._files = json.dumps(files)

    @property
    def file_names(self):
        return json.loads(self._file_names) if self._file_names else []

    @file_names.setter
    def file_names(self, file_names):
        self._file_names = json.dumps(file_names)

# Define the index outside of the class
Index('ix_uuid_timestamp', IngestionTracking.uuid, IngestionTracking.timestamp)

class IngestTracker:
    """Handles tracking of ingestion steps in the database."""
    def __init__(self, config):
        """Initialize IngestTracker with logging and database connection."""
        if not config or 'ingest_tracking_db' not in config:
            raise ValueError("Configuration must include 'ingest_tracking_db'")
            
        self.logger = logging.getLogger(__name__)
        self.logger.info("Initializing IngestTracker")

        try:
            self.database_url = config['ingest_tracking_db']
            self.logger.debug(f"Using database URL: {self.database_url}")

            # Configure connection based on database type
            connect_args = {}
            if self.database_url.startswith('sqlite'):
                connect_args['timeout'] = 5
            else:  # PostgreSQL or other databases
                connect_args['connect_timeout'] = 5

            self.engine = create_engine(
                self.database_url,
                connect_args=connect_args
            )
            self.Session = sessionmaker(bind=self.engine)

            # Verify connection
            with self.engine.connect() as conn:
                conn.execute(text("SELECT 1"))

            # Create tables
            self.logger.debug("Verifying database schema...")
            Base.metadata.create_all(self.engine)
            self.logger.info("Database initialization successful")

        except SQLAlchemyError as e:
            self.logger.error(f"Database initialization error: {e}", exc_info=True)
            raise
        except Exception as e:
                    self.logger.error(f"Unexpected error during initialization: {e}", exc_info=True)
                    raise

    def dispose(self):
        """Safely dispose of database resources."""
        try:
            if hasattr(self, 'engine'):
                self.engine.dispose()
                self.logger.debug("Database resources disposed")
        except Exception as e:
            self.logger.error(f"Error disposing database resources: {e}")

    def db_log_ingestion_event(self, order_info: dict, stage: str) -> int:
        """
        Log an ingestion step to the database using a context manager for the session.
        Returns the new entry ID or None if logging failed.
        """
        required_fields = ['Group', 'Username', 'UUID']
        if not all(field in order_info for field in required_fields):
            self.logger.error(f"Missing required fields in order_info: {required_fields}")
            return None
    
        if not ('DatasetID' in order_info or 'ScreenID' in order_info):
            self.logger.error("Either DatasetID or ScreenID must be provided")
            return None
    
        self.logger.debug(f"Logging ingestion event - Stage: {stage}, UUID: {order_info.get('UUID')}")
        try:
            with self.Session() as session:
                new_entry = IngestionTracking(
                    group_name=order_info.get('Group'),
                    group_id=order_info.get('GroupID'),
                    user_name=order_info.get('Username', 'Unknown'),
                    data_package=str(order_info.get('DatasetID', order_info.get('ScreenID'))),
                    stage=stage,
                    uuid=str(order_info.get('UUID', 'Unknown')),
                    files=order_info.get('Files', []),
                    file_names=order_info.get('file_names', [])
                )
                session.add(new_entry)
                session.commit()
    
                self.logger.info(
                    f"Ingestion event logged: {stage} | "
                    f"UUID: {new_entry.uuid} | "
                    f"Group: {new_entry.group_name} (ID: {new_entry.group_id}) | "
                    f"User: {new_entry.user_name} | "
                    f"Dataset: {new_entry.data_package}"
                )
                self.logger.debug(f"Created database entry: {new_entry.id}")
                return new_entry.id
        except SQLAlchemyError as e:
            self.logger.error(f"Database error logging ingestion step: {e}", exc_info=True)
            return None
        except Exception as e:
            self.logger.error(f"Unexpected error logging ingestion step: {e}", exc_info=True)
            return None

# Global instance management
_ingest_tracker = None

def get_ingest_tracker():
    """Return the current global IngestTracker instance."""
    return _ingest_tracker

def initialize_ingest_tracker(config):
    """Initialize the global IngestTracker instance with proper error handling."""
    global _ingest_tracker
    try:
        _ingest_tracker = IngestTracker(config)
        if not hasattr(_ingest_tracker, 'engine'):
            raise Exception("IngestTracker failed to initialize properly")
        return True
    except Exception as e:
        logger = logging.getLogger(__name__)
        logger.error(f"Failed to initialize IngestTracker: {e}", exc_info=True)
        _ingest_tracker = None
        return False

def log_ingestion_step(order_info, stage):
    """Thread-safe function to log ingestion steps."""
    tracker = get_ingest_tracker()
    if tracker is not None:
        return tracker.db_log_ingestion_event(order_info, stage)
    else:
        logger = logging.getLogger(__name__)
        logger.error("IngestTracker not initialized. Call initialize_ingest_tracker first.")
        return None
import logging
import enum
from sqlalchemy import create_engine, Column, String, Integer, DateTime, Text, Enum, Index
from sqlalchemy.orm import sessionmaker, declarative_base
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.sql import func
from datetime import datetime
import json

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
        self.logger = logging.getLogger(__name__)
        self.logger.info("Initializing IngestTracker")

        try:
            # Use a default for testing if not provided.
            self.database_url = config.get('ingest_tracking_db', "sqlite:///:memory:")
            self.logger.debug(f"Using database URL: {self.database_url}")

            self.engine = create_engine(self.database_url)
            self.Session = sessionmaker(bind=self.engine)

            # Log table creation/verification
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

    def db_log_ingestion_event(self, order_info, stage):
        """Log an ingestion step to the database with proper error handling."""
        session = self.Session()
        self.logger.debug(f"Logging ingestion event - Stage: {stage}, UUID: {order_info.get('UUID')}")
        try:
            new_entry = IngestionTracking(
                group_name=order_info.get('Group'),       # Trust the group name provided by the user
                group_id=order_info.get('GroupID'),         # Capture group ID if provided
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
            session.rollback()
            self.logger.error(f"Database error logging ingestion step: {e}", exc_info=True)
            return None
        except Exception as e:
            session.rollback()
            self.logger.error(f"Unexpected error logging ingestion step: {e}", exc_info=True)
            return None
        finally:
            session.close()
            self.logger.debug("Database session closed")

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
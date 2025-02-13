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
Stage_IMPORTED = "Data Imported"
Stage_MOVED_COMPLETED = "Order Moved to Completed"
Stage_MOVED_FAILED = "Order Moved to Failed"
Stage_DETECTED = "Data Package Detected"
Stage_PREPROCESSING = "Preprocessing Data"
Stage_NEW_ORDER = "Upload Order Received"

Base = declarative_base()

class IngestionTracking(Base):
    """Database model for tracking ingestion steps."""
    __tablename__ = 'ingestion_tracking'

    id = Column(Integer, primary_key=True)
    Group = Column(String, nullable=False)
    GroupID = Column(String, nullable=True)
    Username = Column(String, nullable=False, index=True)
    DataPackage = Column(String, nullable=False)  # Single identifier for both Dataset and Screen
    Stage = Column(String, nullable=False)
    UUID = Column(String(36), nullable=False, index=True)
    Timestamp = Column(DateTime(timezone=True), default=func.now())
    _Files = Column("Files", Text, nullable=False)
    _FileNames = Column("FileNames", Text, nullable=True)

    @property
    def Files(self):
        return json.loads(self._Files)

    @Files.setter
    def Files(self, files):
        self._Files = json.dumps(files)

    @property
    def FileNames(self):
        return json.loads(self._FileNames) if self._FileNames else []

    @FileNames.setter
    def FileNames(self, file_names):
        self._FileNames = json.dumps(file_names)

# Define the index outside of the class
Index('ix_uuid_timestamp', IngestionTracking.UUID, IngestionTracking.Timestamp)

class IngestTracker:
    def __init__(self, config):
        if not config or 'ingest_tracking_db' not in config:
            raise ValueError("Configuration must include 'ingest_tracking_db'")
            
        self.logger = logging.getLogger(__name__)
        self.logger.info("Initializing IngestTracker")
        try:
            self.database_url = config['ingest_tracking_db']
            self.logger.debug(f"Using database URL: {self.database_url}")

            connect_args = {}
            if self.database_url.startswith('sqlite'):
                connect_args['timeout'] = 5
            else:
                connect_args['connect_timeout'] = 5

            self.engine = create_engine(
                self.database_url,
                connect_args=connect_args
            )
            self.Session = sessionmaker(bind=self.engine)

            # Verify connection
            with self.engine.connect() as conn:
                conn.execute(text("SELECT 1"))
            
            # Create tables now
            self.logger.debug("Creating tables if they do not exist...")
            Base.metadata.create_all(self.engine)
            self.logger.info("Database initialization successful")
        except Exception as e:
            self.logger.error(f"Unexpected error during IngestTracker initialization: {e}", exc_info=True)
            raise


    def dispose(self):
        """Safely dispose of database resources."""
        try:
            if hasattr(self, 'engine'):
                self.engine.dispose()
                self.logger.debug("Database resources disposed")
        except Exception as e:
            self.logger.error(f"Error disposing database resources: {e}")

    def db_log_ingestion_event(self, order_info: dict, Stage: str) -> int:
        """
        Log an ingestion step to the database using a context manager for the session.
        Returns the new entry ID or None if logging failed.
        """
        required_fields = ['Group', 'Username', 'UUID']
        if not all(field in order_info for field in required_fields):
            self.logger.error(f"Missing required fields in order_info: {required_fields}")
            return None
    
        if 'DataPackage' not in order_info:
            self.logger.error("DataPackage must be provided")
            return None
    
        self.logger.debug(f"Logging ingestion event - Stage: {Stage}, UUID: {order_info.get('UUID')}")
        try:
            with self.Session() as session:
                new_entry = IngestionTracking(
                    Group=order_info.get('Group'),
                    GroupID=order_info.get('GroupID'),
                    Username=order_info.get('Username', 'Unknown'),
                    DataPackage=str(order_info.get('DataPackage')),  # Single identifier for both Dataset and Screen
                    Stage=Stage,
                    UUID=str(order_info.get('UUID', 'Unknown')),
                    Files=order_info.get('Files', []),
                    FileNames=order_info.get('FileNames', [])
                )
                session.add(new_entry)
                session.commit()
    
                self.logger.info(
                    f"Ingestion event logged: {Stage} | "
                    f"UUID: {new_entry.UUID} | "
                    f"Group: {new_entry.Group} (ID: {new_entry.GroupID}) | "
                    f"User: {new_entry.Username} | "
                    f"DataPackage: {new_entry.DataPackage}"
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

def log_ingestion_step(order_info, Stage):
    """Thread-safe function to log ingestion steps."""
    tracker = get_ingest_tracker()
    if tracker is not None:
        return tracker.db_log_ingestion_event(order_info, Stage)
    else:
        logger = logging.getLogger(__name__)
        logger.error("IngestTracker not initialized. Call initialize_ingest_tracker first.")
        return None
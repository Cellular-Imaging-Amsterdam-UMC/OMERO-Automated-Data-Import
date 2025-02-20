#!/usr/bin/env python
# ingest_tracker.py

import logging
from sqlalchemy import create_engine, Column, String, Integer, DateTime, Text, Index, ForeignKey, JSON
from sqlalchemy.orm import sessionmaker, declarative_base, relationship
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.sql import func, text
from datetime import datetime
import json

# --------------------------------------------------
# Stage constants
# --------------------------------------------------
STAGE_IMPORTED = "Import Completed"
STAGE_PREPROCESSING = "Import Preprocessing"
STAGE_NEW_ORDER = "Import Pending"
STAGE_INGEST_STARTED = "Import Started"
STAGE_INGEST_FAILED = "Import Failed"

Base = declarative_base()

class Preprocessing(Base):
    """Database model for storing preprocessing parameters."""
    __tablename__ = 'imports_preprocessing'

    id = Column(Integer, primary_key=True)
    container = Column(String, nullable=False)
    input_file = Column(String, nullable=False)
    output_folder = Column(String, nullable=False)
    alt_output_folder = Column(String, nullable=True)
    extra_params = Column(JSON, nullable=True) # Storing dynamic kwargs here


class IngestionTracking(Base):
    """Database model for tracking ingestion steps."""
    __tablename__ = 'imports'

    id = Column(Integer, primary_key=True)
    group_name = Column(String, nullable=False)
    user_name = Column(String, nullable=False, index=True)
    destination_id = Column(String, nullable=False)
    destination_type = Column(String, nullable=False)
    stage = Column(String, nullable=False)
    uuid = Column(String(36), nullable=False, index=True)
    timestamp = Column(DateTime(timezone=True), default=func.now())
    _files = Column("files", Text, nullable=False)
    _file_names = Column("file_names", Text, nullable=True)
    
    preprocessing_id = Column(Integer, ForeignKey('imports_preprocessing.id'), nullable=True)
    preprocessing = relationship("Preprocessing", backref="imports")

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
Index(f"{IngestionTracking.__tablename__}_ix_uuid_timestamp", IngestionTracking.uuid, IngestionTracking.timestamp)

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
            
            # Create tables if they do not exist
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

    def db_log_ingestion_event(self, order_info: dict, stage: str) -> int:
        """
        Log an ingestion step to the database.
        Returns the new entry ID or None if logging failed.
        """
        required_fields = ['Group', 'Username', 'UUID', 'DestinationID']
        if not all(field in order_info for field in required_fields):
            self.logger.error(f"Missing required fields in order_info: {required_fields}")
            return None
    
        self.logger.debug(f"Logging ingestion event - Stage: {stage}, UUID: {order_info.get('UUID')}, Order: {order_info}")
        try:
            with self.Session() as session:
                new_entry = IngestionTracking(
                        group_name=order_info.get('Group'),
                        user_name=order_info.get('Username', 'Unknown'),
                        destination_id=str(order_info.get('DestinationID', '')),
                        destination_type=str(order_info.get('DestinationType', '')),
                        stage=stage,
                        uuid=str(order_info.get('UUID', 'Unknown')),
                        files=order_info.get('Files', []),
                        file_names=order_info.get('FileNames', [])
                    )

                session.add(new_entry)
                
                # Check for _preprocessing_id in order_info
                preprocessing_id = order_info.get('_preprocessing_id')
                if preprocessing_id:
                    # Directly set the preprocessing_id without querying for the object
                    new_entry.preprocessing_id = preprocessing_id
                elif "preprocessing_container" in order_info:
                    # Known hardcoded preprocessing fields
                    hardcoded_fields = {
                        "container": order_info.get("preprocessing_container"),
                        "input_file": order_info.get("preprocessing_inputfile"),
                        "output_folder": order_info.get("preprocessing_outputfolder"),
                        "alt_output_folder": order_info.get("preprocessing_altoutputfolder")
                    }

                    # Extract any extra params that aren't part of the hardcoded fields
                    extra_params = {
                        key.replace("preprocessing_", ""): value
                        for key, value in order_info.items()
                        if key.startswith("preprocessing_") and key not in {
                            "preprocessing_container",
                            "preprocessing_inputfile",
                            "preprocessing_outputfolder",
                            "preprocessing_altoutputfolder"
                        }
                    }

                    new_preprocessing = Preprocessing(
                        **hardcoded_fields,
                        extra_params=extra_params
                    )
                    session.add(new_preprocessing)
                    new_entry.preprocessing = new_preprocessing  # Link the new preprocessing row to the ingestion entry
                    
                session.commit()

                self.logger.info(
                    f"Ingestion event logged: {stage} | "
                    f"UUID: {new_entry.uuid} | "
                    f"Group: {new_entry.group_name} | "
                    f"User: {new_entry.user_name} | "
                    f"DestinationID: {new_entry.destination_id} | "
                    f"DestinationType: {new_entry.destination_type} | "
                    f"Preprocessing: {new_entry.preprocessing}"
                )
                self.logger.debug(f"Created database entry: {new_entry.id}")
                return new_entry.id
        except SQLAlchemyError as e:
            self.logger.error(f"Database error logging ingestion step: {e}", exc_info=True)
            return None
        except Exception as e:
            self.logger.error(f"Unexpected error logging ingestion step: {e}", exc_info=True)
            return None

# --------------------------------------------------
# Global instance management
# --------------------------------------------------
_ingest_tracker = None

def get_ingest_tracker():
    """Return the current global IngestTracker instance."""
    return _ingest_tracker

def initialize_ingest_tracker(config):
    """Initialize the global IngestTracker instance."""
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

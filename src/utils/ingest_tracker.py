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

# ingest_tracker.py

from .logger import LoggerManager
from sqlalchemy import create_engine, Column, String, Integer, DateTime, Text
from sqlalchemy.orm import sessionmaker, declarative_base
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.sql import func
from datetime import datetime
import json

STAGE_IMPORTED = "Data Imported"
STAGE_MOVED_COMPLETED = "Order Moved to Completed"
STAGE_MOVED_FAILED = "Order Moved to Failed"
STAGE_DETECTED = "Data Package Detected"

Base = declarative_base()


class IngestionTracking(Base):
    """Database model for tracking ingestion steps."""
    __tablename__ = 'ingestion_tracking'

    id = Column(Integer, primary_key=True)
    group_name = Column(String, nullable=False)
    user_name = Column(String, nullable=False)
    data_package = Column(String, nullable=False)
    stage = Column(String, nullable=False)
    uuid = Column(String, nullable=False)
    timestamp = Column(DateTime(timezone=True), server_default=func.now())
    _files = Column("files", Text, nullable=False)  # Underlying storage
    _file_names = Column("file_names", Text, nullable=True)  # New column for file names
    
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


class IngestTracker:
    """Handles tracking of ingestion steps in the database."""
    def __init__(self, config):
        """Initialize IngestTracker with logging and database connection."""
        if not LoggerManager.is_initialized():
            raise RuntimeError("LoggerManager must be initialized before creating IngestTracker")
        
        self.logger = LoggerManager.get_module_logger(__name__)
        self.logger.info("Initializing IngestTracker")
        
        try:
            self.database_url = config['ingest_tracking_db']
            self.engine = create_engine(self.database_url)
            self.Session = sessionmaker(bind=self.engine)
            Base.metadata.create_all(self.engine)
            self.logger.info("Database initialization successful")
        except Exception as e:
            self.logger.error(f"Failed to initialize database: {e}")
            raise

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.dispose()
        if exc_type is not None:
            self.logger.error(f"Error during IngestTracker cleanup: {exc_value}")
            return False
        return True

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
        try:
            # Convert DataPackage to dict if necessary
            if not isinstance(order_info, dict):
                order_info = order_info.__dict__

            new_entry = IngestionTracking(
                group_name=order_info.get('Group', 'Unknown'),
                user_name=order_info.get('Username', 'Unknown'),
                data_package=str(order_info.get('DatasetID', str(order_info.get('ScreenID','Unknown')))),
                stage=stage,
                uuid=str(order_info.get('UUID', 'Unknown')),
                files=order_info.get('Files', ['Unknown']),
                file_names=order_info.get('file_names', [])
            )
            
            session.add(new_entry)
            session.commit()
            
            self.logger.info(
                f"Ingestion event logged: {stage} | "
                f"UUID: {new_entry.uuid} | "
                f"Group: {new_entry.group_name} | "
                f"User: {new_entry.user_name} | "
                f"Dataset: {new_entry.data_package}"
            )
            return new_entry.id

        except SQLAlchemyError as e:
            session.rollback()
            self.logger.error(f"Database error logging ingestion step: {e}")
            return None
        except Exception as e:
            session.rollback()
            self.logger.error(f"Unexpected error logging ingestion step: {e}")
            return None
        finally:
            session.close()

# Global instance management
_ingest_tracker = None

def initialize_ingest_tracker(config):
    """Initialize the global IngestTracker instance with proper error handling."""
    global _ingest_tracker
    try:
        _ingest_tracker = IngestTracker(config)
        return True
    except Exception as e:
        logger = LoggerManager.get_module_logger(__name__)
        logger.error(f"Failed to initialize IngestTracker: {e}")
        return False

def log_ingestion_step(order_info, stage):
    """Thread-safe function to log ingestion steps."""
    if _ingest_tracker is not None:
        return _ingest_tracker.db_log_ingestion_event(order_info, stage)
    else:
        logger = LoggerManager.get_module_logger(__name__)
        logger.error("IngestTracker not initialized. Call initialize_ingest_tracker first.")
        return None

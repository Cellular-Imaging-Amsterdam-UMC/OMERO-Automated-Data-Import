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

from .logger import setup_logger
from sqlalchemy import create_engine, Column, String, Integer, DateTime, Text
from sqlalchemy.orm import sessionmaker, declarative_base
from sqlalchemy.ext.hybrid import hybrid_property
from sqlalchemy.sql import func
from datetime import datetime, timezone
import json

STAGE_IMPORTED = "Data Imported"
STAGE_MOVED_COMPLETED = "Order Moved to Completed"
STAGE_MOVED_FAILED = "Order Moved to Failed"
STAGE_DETECTED = "Data Package Detected"

Base = declarative_base()


class IngestionTracking(Base):
    __tablename__ = 'ingestion_tracking'

    id = Column(Integer, primary_key=True)
    group_name = Column(String, nullable=False)
    user_name = Column(String, nullable=False)
    data_package = Column(String, nullable=False)
    stage = Column(String, nullable=False)
    uuid = Column(String, nullable=False)
    timestamp = Column(DateTime(timezone=True), default=func.now())
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
    def __init__(self, config):
        self.database_url = config['ingest_tracking_db']
        self.logger = setup_logger(__name__, config['log_file_path'])
        self.engine = create_engine(self.database_url)
        self.Session = sessionmaker(bind=self.engine)
        # Create tables if they don't exist
        Base.metadata.create_all(self.engine)
        
    def __exit__(self, exc_type, exc_value, traceback):
        """Clean up resources when the class instance is destroyed or goes out of scope."""
        self.dispose()

    def dispose(self):
        """Dispose of the database session and logger resources."""
        # Close the logger handlers
        for handler in self.logger.handlers:
            handler.close()
            self.logger.removeHandler(handler)

        # Dispose of the database engine
        self.engine.dispose()

    def db_log_ingestion_event(self, order_info, stage):
        """Log an ingestion step to the database."""
        with self.Session() as session:
            try:
                new_entry = IngestionTracking(
                    group_name=order_info.get('Group', 'Unknown'),
                    user_name=order_info.get('Username', 'Unknown'),
                    data_package=order_info.get('Dataset', 'Unknown'),
                    stage=stage,
                    uuid=str(order_info.get('UUID', 'Unknown')),
                    files=order_info.get('Files', ['Unknown']),
                    file_names=order_info.get('file_names', [])  # Add this line
                )
                session.add(new_entry)
                session.commit()
                self.logger.info(f"Ingestion event logged: {stage} | UUID: {new_entry.uuid} | Group: {new_entry.group_name} | User: {new_entry.user_name} | Dataset: {new_entry.data_package}")
                return new_entry.id
            except Exception as e:
                session.rollback()
                self.logger.error(f"Error logging ingestion step: {e}")
                return None
        

# Global instance of IngestTracker
ingest_tracker = None


def initialize_ingest_tracker(config):
    """Initialize the global IngestTracker instance."""
    global ingest_tracker
    ingest_tracker = IngestTracker(config)


def log_ingestion_step(order_info, stage):
    """Global function to log ingestion steps using the IngestTracker instance."""
    if ingest_tracker is not None:
        return ingest_tracker.db_log_ingestion_event(order_info, stage)
    else:
        print("Error: IngestTracker not initialized. Call initialize_ingest_tracker first.")

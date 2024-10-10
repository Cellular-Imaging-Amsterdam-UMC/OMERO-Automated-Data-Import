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
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from .initialize import IngestionTracking

class IngestTracker:
    def __init__(self, config):
        self.database_path = config['ingest_tracking_db']
        self.logger = setup_logger(__name__, config['log_file_path'])
        self.engine = create_engine(f'sqlite:///{self.database_path}')
        self.Session = sessionmaker(bind=self.engine)

    def log_ingestion_step(self, group, user, dataset, stage, uuid):
        """Log an ingestion step to the database."""
        with self.Session() as session:
            try:
                new_entry = IngestionTracking(
                    group_name=group,
                    user_name=user,
                    data_package=dataset,
                    stage=stage,
                    uuid=str(uuid)
                )
                session.add(new_entry)
                session.commit()
                self.logger.info(f"Logged ingestion step: {group}, {user}, {dataset}, {stage}, {uuid}")
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

def log_ingestion_step(group, user, dataset, stage, uuid):
    """Global function to log ingestion steps using the IngestTracker instance."""
    if ingest_tracker is not None:
        return ingest_tracker.log_ingestion_step(group, user, dataset, stage, uuid)
    else:
        print("Error: IngestTracker not initialized. Call initialize_ingest_tracker first.")
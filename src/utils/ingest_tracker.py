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

import sqlite3
from sqlite3 import Error
from .logger import setup_logger

class IngestTracker:
    def __init__(self, config):
        self.database_path = config['ingest_tracking_db']
        self.logger = setup_logger(__name__, config['log_file_path'])

    def create_connection(self):
        """Create a database connection to the SQLite database."""
        try:
            return sqlite3.connect(self.database_path)
        except Error as e:
            self.logger.error(f"Error connecting to database: {e}")
        return None

    def log_ingestion_step(self, group, user, dataset, stage, uuid):
        """Log an ingestion step to the database."""
        conn = self.create_connection()
        if conn is not None:
            try:
                sql = ''' INSERT INTO ingestion_tracking(group_name, user_name, data_package, stage, uuid)
                          VALUES(?,?,?,?,?) '''
                cur = conn.cursor()
                cur.execute(sql, (group, user, dataset, stage, str(uuid)))
                conn.commit()
                self.logger.info(f"Logged ingestion step: {group}, {user}, {dataset}, {stage}, {uuid}")
                return cur.lastrowid
            except Error as e:
                self.logger.error(f"Error logging ingestion step: {e}")
            finally:
                conn.close()
        else:
            self.logger.error("Error! Cannot create the database connection for logging ingestion step.")

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
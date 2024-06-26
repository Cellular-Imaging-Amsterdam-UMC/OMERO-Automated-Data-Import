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

#ingest_tracker.py

import sqlite3
from sqlite3 import Error

DATABASE_PATH = '/OMERO/ingestion_tracking.db'

def create_connection(db_file):
    """Create a database connection to a SQLite database."""
    conn = None
    try:
        conn = sqlite3.connect(db_file)
        return conn
    except Error as e:
        print(e)
    return conn

def create_table(conn, create_table_sql):
    """Create a table from the create_table_sql statement."""
    try:
        c = conn.cursor()
        c.execute(create_table_sql)
    except Error as e:
        print(e)

def initialize_database():
    """Initialize the database with the required tables."""
    database = DATABASE_PATH

    sql_create_ingestion_table = """ CREATE TABLE IF NOT EXISTS ingestion_tracking (
                                        id integer PRIMARY KEY,
                                        group_name text NOT NULL,
                                        user_name text NOT NULL,
                                        data_package text NOT NULL,
                                        stage text NOT NULL,
                                        uuid text NOT NULL,
                                        timestamp DATETIME DEFAULT CURRENT_TIMESTAMP
                                    ); """

    conn = create_connection(database)
    if conn is not None:
        create_table(conn, sql_create_ingestion_table)
    else:
        print("Error! Cannot create the database connection.")

def log_ingestion_step(group, user, dataset, stage, uuid):
    conn = create_connection(DATABASE_PATH)
    with conn:
        sql = ''' INSERT INTO ingestion_tracking(group_name, user_name, data_package, stage, uuid)
                  VALUES(?,?,?,?,?) '''
        cur = conn.cursor()
        cur.execute(sql, (group, user, dataset, stage, str(uuid)))
        conn.commit()
        return cur.lastrowid
import sqlite3
from sqlite3 import Error

DATABASE_PATH = 'ingestion_tracking.db'

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
                                        ingestion_id text NOT NULL,
                                        timestamp DATETIME DEFAULT CURRENT_TIMESTAMP
                                    ); """

    conn = create_connection(database)
    if conn is not None:
        create_table(conn, sql_create_ingestion_table)
    else:
        print("Error! Cannot create the database connection.")

def log_ingestion_step(group, user, dataset, stage, ingestion_id):
    """Log an ingestion step for a DataPackage with an ingestion ID."""
    conn = create_connection(DATABASE_PATH)
    with conn:
        sql = ''' INSERT INTO ingestion_tracking(group_name, user_name, data_package, stage, ingestion_id)
                  VALUES(?,?,?,?,?) '''
        cur = conn.cursor()
        cur.execute(sql, (group, user, dataset, stage, str(ingestion_id)))
        conn.commit()
        return cur.lastrowid
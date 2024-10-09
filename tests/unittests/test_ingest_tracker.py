import os
import pytest
import sqlite3
from utils.ingest_tracker import initialize_ingest_tracker, log_ingestion_step
from utils.initialize import initialize_database
from unittest.mock import patch
import tempfile

TEST_LOG_PATH = 'test_logfile.log'  # Static log file path

# Create a temporary database file
@pytest.fixture(scope='module', autouse=True)
def setup_database():
    """Set up a temporary SQLite database before tests run and clean up afterward."""
    # Create a temporary file
    with tempfile.NamedTemporaryFile(delete=False, suffix='.db') as temp_db:
        TEST_DB_PATH = temp_db.name

    config = {
        'ingest_tracking_db': TEST_DB_PATH,
        'log_file_path': TEST_LOG_PATH
    }

    # Use patch to mock the logger
    with patch('utils.logger.setup_logger') as mock_logger:
        # Initialize the ingest tracking database
        initialize_database(config['ingest_tracking_db'], mock_logger)
        
        # Check that the logger was called
        mock_logger.info.assert_called_with("Ingest tracking database initialized successfully.")
        mock_logger.error.assert_not_called()  # Ensure no error was logged

    initialize_ingest_tracker(config)

    yield TEST_DB_PATH  # Yield the path for use in tests

    # Clean up the database file after tests
    os.remove(TEST_DB_PATH)


def test_log_ingestion_step(setup_database):
    """Test logging an ingestion step."""
    TEST_DB_PATH = setup_database  # Get the temp database path

    group = 'test_group'
    user = 'test_user'
    dataset = 'test_dataset'
    stage = 'test_stage'
    uuid = '123e4567-e89b-12d3-a456-426614174000'
    
    # Call the logging function
    ingestion_id = log_ingestion_step(group, user, dataset, stage, uuid)

    # Check if the returned ID is not None
    assert ingestion_id is not None

    # Verify that the ingestion step was logged correctly
    conn = sqlite3.connect(TEST_DB_PATH)
    cursor = conn.cursor()
    
    cursor.execute('SELECT * FROM ingestion_tracking WHERE uuid = ?', (uuid,))
    record = cursor.fetchone()
    assert record is not None
    assert record[1] == group  # group_name
    assert record[2] == user   # user_name
    assert record[3] == dataset # data_package
    assert record[4] == stage   # stage
    assert record[5] == uuid    # uuid
    
    conn.close()

import json
import os
import pytest
from omero_adi.utils.ingest_tracker import initialize_ingest_tracker, log_ingestion_step, STAGE_IMPORTED
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker
import tempfile

TEST_LOG_PATH = 'test_logfile.log'  # Static log file path

# Create a temporary database file
@pytest.fixture(scope='module', autouse=True)
def setup_database():
    """Set up a temporary SQLite database before tests run and clean up afterward."""
    # Create a temporary SQLite database file
    with tempfile.NamedTemporaryFile(delete=False, suffix='.db') as temp_db:
        TEST_DB_PATH = temp_db.name

    config = {
        'ingest_tracking_db': f"sqlite:///{TEST_DB_PATH}",  # Correct URI format for SQLite
        'log_file_path': TEST_LOG_PATH
    }

    # Initialize the ingest tracker with the SQLite database
    initialize_ingest_tracker(config)

    yield config  # Yield the config for use in tests

    # Dispose the global ingest_tracker
    from omero_adi.utils.ingest_tracker import ingest_tracker
    if ingest_tracker:
        ingest_tracker.dispose()
        
    # Clean up the database file after tests
    if os.path.exists(TEST_DB_PATH):
        os.remove(TEST_DB_PATH)
    # Clean up the log file
    if os.path.exists(TEST_LOG_PATH):
        os.remove(TEST_LOG_PATH)


def test_log_ingestion_step(setup_database):
    """Test logging an ingestion step."""
    config = setup_database  # Get the configuration, including the temp database path

    # Create a mock order_info dictionary
    order_info = {
        'Group': 'test_group',
        'Username': 'test_user',
        'Dataset': 'test_dataset',
        'UUID': '123e4567-e89b-12d3-a456-426614174000',
        'Files': ["/path/to/test_file.tif", "/path/to/second/file.qptiff"],
        'file_names': ["test_file.tif", "file.qptiff"]
    }
    
    stage = STAGE_IMPORTED

    # Call the logging function with the updated signature
    ingestion_id = log_ingestion_step(order_info, stage)

    # Check if the returned ID is not None
    assert ingestion_id is not None

    # Verify that the ingestion step was logged correctly using SQLAlchemy
    engine = create_engine(config['ingest_tracking_db'])
    Session = sessionmaker(bind=engine)

    # Use 'with session' for session management
    with Session() as session:
        result = session.execute(text('SELECT * FROM ingestion_tracking WHERE uuid = :uuid'), {'uuid': order_info['UUID']}).fetchone()

        assert result is not None
        assert result.group_name == order_info['Group']
        assert result.user_name == order_info['Username']
        assert result.data_package == order_info['Dataset']
        assert result.stage == stage
        assert result.uuid == order_info['UUID']
        assert json.loads(result.files) == order_info['Files']
        
    # Close the engine explicitly to release resources
    engine.dispose()

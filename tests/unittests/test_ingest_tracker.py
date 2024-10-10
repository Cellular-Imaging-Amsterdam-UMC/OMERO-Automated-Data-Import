import os
import pytest
from utils.ingest_tracker import initialize_ingest_tracker, log_ingestion_step
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
    from utils.ingest_tracker import ingest_tracker
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

    group = 'test_group'
    user = 'test_user'
    dataset = 'test_dataset'
    stage = 'test_stage'
    uuid = '123e4567-e89b-12d3-a456-426614174000'
    
    # Call the logging function
    ingestion_id = log_ingestion_step(group, user, dataset, stage, uuid)

    # Check if the returned ID is not None
    assert ingestion_id is not None

    # Verify that the ingestion step was logged correctly using SQLAlchemy
    engine = create_engine(config['ingest_tracking_db'])
    Session = sessionmaker(bind=engine)

    # Use 'with session' for session management
    with Session() as session:
        # Use the text() function to wrap raw SQL queries
        result = session.execute(text('SELECT * FROM ingestion_tracking WHERE uuid = :uuid'), {'uuid': uuid}).fetchone()

        assert result is not None
        assert result.group_name == group  # group_name
        assert result.user_name == user    # user_name
        assert result.data_package == dataset  # data_package
        assert result.stage == stage   # stage
        assert result.uuid == uuid     # uuid
        
    # Close the engine explicitly to release resources
    engine.dispose()

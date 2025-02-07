import os
import tempfile
import yaml
import json
import logging
import pytest
from pathlib import Path

# Update these imports to use the new package structure
from omero_adi import (
    run_application,
    DatabasePoller,
    load_settings,
    initialize_system,
    get_ingest_tracker,
    IngestTracker,
    IngestionTracking
)

# Dummy configuration for testing
TEST_CONFIG = {
    'log_level': 'DEBUG',
    'log_file_path': 'test_app.log',
    'shutdown_timeout': 5,
    'max_workers': 2,
    'ingest_tracking_db': 'sqlite:///:memory:'
}

# A dummy order info dictionary for testing ingestion logging
DUMMY_ORDER_INFO = {
    'Group': 'TestGroup',
    'GroupID': '123',
    'Username': 'TestUser',
    'UUID': '0000-1111-2222-3333',
    'DatasetID': 'Dataset123',
    'Files': ['file1.txt', 'file2.txt'],
    'file_names': ['file1.txt', 'file2.txt']
}


@pytest.fixture(scope="function")
def temp_yaml_file(tmp_path):
    # Create a temporary YAML settings file
    data = {
        'log_level': 'DEBUG',
        'log_file_path': 'test_app.log',
        'shutdown_timeout': 5,
        'max_workers': 2,
        'ingest_tracking_db': 'sqlite:///:memory:'
    }
    file_path = tmp_path / "settings.yml"
    with file_path.open("w") as f:
        yaml.dump(data, f)
    return str(file_path)


def test_load_settings(temp_yaml_file):
    settings = load_settings(temp_yaml_file)
    assert isinstance(settings, dict)
    assert settings['ingest_tracking_db'] == 'sqlite:///:memory:'


def test_initialize_system_and_ingest_tracker():
    # Initialize the system and verify that the global ingest tracker is set
    initialize_system(TEST_CONFIG)
    tracker = get_ingest_tracker()
    assert tracker is not None
    assert hasattr(tracker, 'engine')
    
    # Test logging an ingestion event
    entry_id = tracker.db_log_ingestion_event(DUMMY_ORDER_INFO, "Test Stage")
    assert entry_id is not None

    # Verify that the entry was added to the in-memory DB
    with tracker.engine.connect() as conn:
        result = conn.execute("SELECT COUNT(*) FROM ingestion_tracking")
        count = result.scalar()
        assert count >= 1  # At least one entry should exist


def test_database_poller(monkeypatch):
    """
    Test DatabasePoller.poll_database by simulating an empty result set.
    We create a dummy ingest_tracker with an in-memory SQLite DB.
    """
    # Set up a dummy ingest tracker
    tracker = IngestTracker(TEST_CONFIG)
    # Create a dummy DatabasePoller instance with a simple executor
    from concurrent.futures import ThreadPoolExecutor
    executor = ThreadPoolExecutor(max_workers=1)
    
    # Instantiate DatabasePoller with our dummy tracker
    poller = DatabasePoller(TEST_CONFIG, executor, poll_interval=1)
    poller.ingest_tracker = tracker  # Inject our tracker
    poller.IngestionTracking = IngestionTracking

    # Monkey-patch the poll_database method to run only one iteration
    original_sleep = __import__("time").sleep
    call_count = 0
    def fake_sleep(seconds):
        nonlocal call_count
        call_count += 1
        if call_count >= 2:
            poller.shutdown_event.set()
        original_sleep(0.1)
    monkeypatch.setattr("time.sleep", fake_sleep)

    # Run poll_database in a separate thread
    from threading import Thread
    t = Thread(target=poller.poll_database)
    t.start()
    t.join()
    
    # If no exceptions occurred and poller.shutdown_event was set, the test passes.
    assert poller.shutdown_event.is_set()

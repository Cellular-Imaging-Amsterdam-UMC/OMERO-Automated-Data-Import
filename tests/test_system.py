# tests/test_system.py

import os
import tempfile
import yaml
import pytest
import logging
from sqlalchemy import inspect

# Import functions and classes from your modules
from utils.initialize import load_settings, initialize_system
from utils.ingest_tracker import initialize_ingest_tracker, log_ingestion_step, IngestTracker, _ingest_tracker

# Fixture: Create a temporary configuration with an in-memory SQLite DB.
@pytest.fixture(scope="function")
def test_config(tmp_path):
    config_data = {
        "log_file_path": "logs/test_app.log",
        "ingest_tracking_db": "sqlite:///:memory:",
        "max_workers": 1
    }
    config_file = tmp_path / "test_settings.yml"
    config_file.write_text(yaml.dump(config_data))
    # Return both the config data and path, if needed
    return config_data

def test_load_settings(tmp_path):
    # Create a temporary YAML settings file.
    config_data = {
        "log_file_path": "logs/test_app.log",
        "ingest_tracking_db": "sqlite:///:memory:",
        "max_workers": 1
    }
    config_file = tmp_path / "config.yml"
    config_file.write_text(yaml.dump(config_data))
    loaded = load_settings(str(config_file))
    assert loaded["log_file_path"] == "logs/test_app.log"
    assert loaded["ingest_tracking_db"] == "sqlite:///:memory:"
    assert loaded["max_workers"] == 1

def test_initialize_ingest_tracker(test_config):
    # Test that the ingest tracker initializes correctly.
    success = initialize_ingest_tracker(test_config)
    assert success is True
    # Verify that the table "ingestion_tracking" exists.
    engine = _ingest_tracker.engine
    inspector = inspect(engine)
    table_names = inspector.get_table_names()
    assert "ingestion_tracking" in table_names

def test_log_ingestion_step(test_config):
    # Ensure that logging an ingestion event works.
    initialize_ingest_tracker(test_config)
    dummy_order = {
        "Group": "Test Group",
        "GroupID": "123",
        "Username": "test_user",
        "DatasetID": "456",
        "UUID": "dummy-uuid",
        "Files": ["file1.txt", "file2.txt"],
        "file_names": ["file1.txt", "file2.txt"]
    }
    event_id = log_ingestion_step(dummy_order, "Test Stage")
    assert event_id is not None
    # Optionally, verify the inserted record by querying the in-memory DB.
    session = _ingest_tracker.Session()
    try:
        record = session.query(IngestTracker).filter_by(uuid="dummy-uuid").first()
        assert record is not None
        assert record.group_name == "Test Group"
        assert record.group_id == "123"
    finally:
        session.close()

# Additional tests for the initialize_system function:
def test_initialize_system_logs(test_config, caplog):
    caplog.set_level(logging.DEBUG)
    # This test checks that initialize_system runs without raising an exception.
    try:
        initialize_system(test_config)
    except Exception:
        pytest.fail("initialize_system() raised an exception unexpectedly!")
    assert "Starting system initialization" in caplog.text

# You can add more tests as needed to simulate calls in main.py
# For instance, testing create_executor can be done by checking that it returns a ProcessPoolExecutor.

def test_create_executor(test_config):
    from utils.ingest_tracker import _ingest_tracker  # ensure the logging is not interfering
    from concurrent.futures import ProcessPoolExecutor
    # Dummy config must have max_workers
    test_config["max_workers"] = 2
    executor = None
    try:
        from main import create_executor
        executor = create_executor(test_config)
        assert isinstance(executor, ProcessPoolExecutor)
    finally:
        if executor:
            executor.shutdown()

import os
import tempfile
import yaml
import json
import logging
import pytest
from pathlib import Path
from sqlalchemy.sql import text

# Updated imports based on your new package structure.
from omero_adi import (
    run_application,
    DatabasePoller,
    load_settings,
    initialize_system,
    get_ingest_tracker,
    IngestTracker,
    IngestionTracking
)
from omero_adi.main import load_config, create_executor, DataPackage, IngestionProcess

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


def test_load_settings_json(tmp_path):
    # Test that a JSON file is loaded correctly.
    data = {
        'log_level': 'DEBUG',
        'log_file_path': 'test_app.log',
        'shutdown_timeout': 5,
        'max_workers': 2,
        'ingest_tracking_db': 'sqlite:///:memory:'
    }
    file_path = tmp_path / "settings.json"
    file_path.write_text(json.dumps(data))
    settings = load_settings(str(file_path))
    assert isinstance(settings, dict)
    assert settings['ingest_tracking_db'] == 'sqlite:///:memory:'


def test_load_settings_unsupported_extension(tmp_path):
    # Ensure that a file with an unsupported extension causes a ValueError.
    file_path = tmp_path / "settings.txt"
    file_path.write_text("unsupported content")
    with pytest.raises(ValueError):
        load_settings(str(file_path))


def test_load_settings_file_not_found(tmp_path):
    # Ensure that a FileNotFoundError is raised for a non-existent file.
    non_existent_file = tmp_path / "nonexistent.yml"
    with pytest.raises(FileNotFoundError):
        load_settings(str(non_existent_file))


def test_initialize_system_and_ingest_tracker():
    # Initialize the system and verify that the global ingest tracker is set.
    initialize_system(TEST_CONFIG)
    tracker = get_ingest_tracker()
    assert tracker is not None
    assert hasattr(tracker, 'engine')
    
    # Test logging an ingestion event.
    entry_id = tracker.db_log_ingestion_event(DUMMY_ORDER_INFO, "Test Stage")
    assert entry_id is not None

    # Verify that the entry was added to the in-memory DB.
    with tracker.engine.connect() as conn:
        result = conn.execute(text("SELECT COUNT(*) FROM ingestion_tracking"))
        count = result.scalar()
        assert count >= 1  # At least one entry should exist.


def test_initialize_system_output(capsys):
    # Test that initialize_system prints expected debug output.
    initialize_system(TEST_CONFIG)
    captured = capsys.readouterr().out
    assert "THIS IS A TEST CHANGE" in captured
    assert "Initialization success:" in captured


def test_initialize_system_with_invalid_config(capsys):
    # Test initialize_system with missing required configuration (e.g. missing ingest_tracking_db).
    incomplete_config = TEST_CONFIG.copy()
    del incomplete_config['ingest_tracking_db']
    initialize_system(incomplete_config)
    tracker = get_ingest_tracker()
    # Expect the global tracker to be None since initialization failed.
    assert tracker is None


def test_db_log_ingestion_event_missing_fields():
    # Test that db_log_ingestion_event returns None when required fields are missing.
    tracker = IngestTracker(TEST_CONFIG)
    incomplete_order = DUMMY_ORDER_INFO.copy()
    del incomplete_order['Group']  # Remove a required field.
    result = tracker.db_log_ingestion_event(incomplete_order, "Test Stage")
    assert result is None


def test_ingest_tracker_dispose():
    # Test that calling dispose does not raise exceptions.
    tracker = IngestTracker(TEST_CONFIG)
    tracker.dispose()


def test_load_config(tmp_path):
    # Test the load_config function from main.py.
    config_data = {
        'log_level': 'DEBUG',
        'log_file_path': 'test_app.log',
        'shutdown_timeout': 5,
        'max_workers': 2,
        'ingest_tracking_db': 'sqlite:///:memory:'
    }
    file_path = tmp_path / "settings.yml"
    with file_path.open("w") as f:
        yaml.dump(config_data, f)
    loaded_config = load_config(str(file_path))
    assert loaded_config == config_data


def test_create_executor():
    # Test that create_executor returns a ProcessPoolExecutor with the correct number of max workers.
    executor = create_executor(TEST_CONFIG)
    # Check the _max_workers attribute (this is implementation-dependent).
    assert executor._max_workers == TEST_CONFIG['max_workers']
    executor.shutdown(wait=True)


def test_ingestion_process(monkeypatch):
    # Test IngestionProcess.import_data_package by monkeypatching DataPackageImporter.
    class DummyImporter:
        def __init__(self, config, data_package):
            pass
        def import_data_package(self):
            # Simulate success: one successful upload, no failures.
            return (['upload1'], [], False)
    
    # Patch the DataPackageImporter in the omero_adi.main module.
    monkeypatch.setattr("omero_adi.main.DataPackageImporter", DummyImporter)
    data_package = DataPackage(DUMMY_ORDER_INFO, order_identifier=DUMMY_ORDER_INFO['UUID'])
    # Create a dummy order manager (details not used in this test).
    dummy_order_manager = type("DummyOrderManager", (), {})()
    process = IngestionProcess(data_package, TEST_CONFIG, dummy_order_manager)
    result_uuid = process.import_data_package()
    assert result_uuid == DUMMY_ORDER_INFO['UUID']


def test_main_integration(monkeypatch, tmp_path, caplog):
    """
    Integration test to check interplay between main.py and its utilities.
    This test simulates a run of main() by:
      - Providing a temporary configuration.
      - Monkeypatching signal.signal to trigger graceful shutdown immediately.
      - Overriding time.sleep to prevent long blocking.
      - Capturing log output to verify key messages.
    """
    # Set up logging to capture messages
    caplog.set_level(logging.INFO)
    
    # Create a temporary config file.
    config_data = {
        'log_level': 'DEBUG',
        'log_file_path': str(tmp_path / "test_app.log"),
        'shutdown_timeout': 1,
        'max_workers': 2,
        'ingest_tracking_db': 'sqlite:///:memory:'
    }
    config_path = tmp_path / "settings.yml"
    with config_path.open("w") as f:
        yaml.dump(config_data, f)

    # Initialize the database before running main
    from omero_adi.utils.ingest_tracker import Base, initialize_ingest_tracker
    initialize_ingest_tracker(config_data)

    # Monkeypatch load_config in main to return our temporary config.
    monkeypatch.setattr("omero_adi.main.load_config", lambda path="config/settings.yml": config_data)

    # Monkeypatch signal.signal to immediately call the handler.
    def fake_signal(signum, handler):
        handler(signum, None)
    monkeypatch.setattr("signal.signal", fake_signal)

    # Override time.sleep to return immediately.
    monkeypatch.setattr("time.sleep", lambda s: None)

    # Run main and capture potential SystemExit.
    try:
        from omero_adi.main import main as main_func
        main_func()
    except SystemExit as e:
        # Expect a normal exit code.
        assert e.code == 0

    # Check that log messages from the integrated run are present.
    assert any("Starting application..." in record.message for record in caplog.records)
    assert any("Program completed." in record.message for record in caplog.records)

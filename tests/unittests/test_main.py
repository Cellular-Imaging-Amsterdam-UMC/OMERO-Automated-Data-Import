import pytest
from unittest.mock import patch, MagicMock
from pathlib import Path
import os
import json

from utils.config_manager import load_settings
from utils.logger import setup_logger
from utils.initialize import initialize_system
from utils.upload_order_manager import UploadOrderManager
from utils.importer import DataPackageImporter
from utils.ingest_tracker import STAGE_DETECTED, STAGE_MOVED_COMPLETED, STAGE_MOVED_FAILED, log_ingestion_step

# Get the absolute path to the test_config directory
TEST_CONFIG_DIR = Path(__file__).parent / 'test_config'

# Patch the config before importing main
with patch('utils.config_manager.load_settings') as mock_load_settings, \
     patch('utils.config_manager.load_json') as mock_load_json:
    
    mock_config = {
        'base_dir': str(TEST_CONFIG_DIR),
        'group_list': str(TEST_CONFIG_DIR / 'sample_groups_list.json'),
        'upload_orders_dir_name': 'upload_orders',
        'log_file_path': str(TEST_CONFIG_DIR / 'test.log'),
        'max_workers': 4
    }
    mock_load_settings.return_value = mock_config
    
    with open(mock_config['group_list'], 'r') as f:
        groups = json.load(f)
    mock_load_json.return_value = groups
    
    from main import DataPackage, IngestionProcess, DirectoryPoller, main

@pytest.fixture
def mock_config():
    return {
        'base_dir': str(TEST_CONFIG_DIR),
        'group_list': str(TEST_CONFIG_DIR / 'test_groups.json'),
        'upload_orders_dir_name': 'upload_orders',
        'log_file_path': str(TEST_CONFIG_DIR / 'test.log'),
        'max_workers': 4
    }

@pytest.fixture
def sample_order_file():
    return TEST_CONFIG_DIR / 'sample_upload_order.txt'

@pytest.fixture
def mock_order_info(sample_order_file):
    # Use UploadOrderManager to parse the sample file
    config = load_settings(str(TEST_CONFIG_DIR / 'settings.yml'))
    order_manager = UploadOrderManager(str(sample_order_file), config)
    return order_manager.get_order_info()

def test_data_package_initialization(mock_order_info):
    data_package = DataPackage(mock_order_info, str(TEST_CONFIG_DIR))
    assert data_package.Group == mock_order_info['Group']
    assert data_package.Username == mock_order_info['Username']
    assert data_package.Dataset == mock_order_info['Dataset']
    assert data_package.UUID == mock_order_info['UUID']
    assert data_package.Files == mock_order_info['Files']
    assert data_package.file_names == mock_order_info['file_names']

def test_ingestion_process(mock_config, mock_order_info):
    with patch('main.DataPackageImporter') as mock_importer, \
         patch('main.log_ingestion_step') as mock_log_ingestion_step:
        mock_order_manager = MagicMock()
        data_package = DataPackage(mock_order_info, str(TEST_CONFIG_DIR))
        ingestion_process = IngestionProcess(data_package, mock_config, mock_order_manager)

        mock_importer_instance = mock_importer.return_value
        mock_importer_instance.import_data_package.return_value = ([], [], False)

        ingestion_process.import_data_package()

        mock_order_manager.move_upload_order.assert_called_once_with('completed')
        mock_log_ingestion_step.assert_called_once_with(data_package.__dict__, STAGE_MOVED_COMPLETED)

def test_directory_poller_process_event(mock_config, sample_order_file):
    with patch('main.UploadOrderManager') as mock_upload_order_manager, \
         patch('main.log_ingestion_step') as mock_log_ingestion_step:
        mock_executor = MagicMock()
        mock_logger = MagicMock()
        poller = DirectoryPoller(mock_config, mock_executor, mock_logger)

        mock_order_manager = mock_upload_order_manager.return_value
        mock_order_manager.get_order_info.return_value = mock_order_info(sample_order_file)

        poller.process_event(sample_order_file)

        mock_log_ingestion_step.assert_called_once()
        mock_executor.submit.assert_called_once()

def test_main(mock_config):
    with patch('main.initialize_system') as mock_initialize_system, \
         patch('main.DirectoryPoller') as mock_directory_poller, \
         patch('main.ProcessPoolExecutor') as mock_process_pool_executor, \
         patch('main.setup_logger') as mock_setup_logger, \
         patch('main.shutdown_event', MagicMock()) as mock_shutdown_event:
        
        mock_shutdown_event.is_set.side_effect = [False, True]  # Run loop once then exit
        main()

        mock_initialize_system.assert_called_once()
        mock_directory_poller.assert_called_once()
        mock_process_pool_executor.assert_called_once()

if __name__ == '__main__':
    pytest.main()

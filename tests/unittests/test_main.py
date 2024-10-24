import pytest
import json
from pathlib import Path
from unittest.mock import patch, MagicMock

# Get the absolute path to the test_config directory
TEST_CONFIG_DIR = Path(__file__).parent / 'test_config'

def mock_load_settings(file_path):
    if file_path == "config/settings.yml":
        return {
            'base_dir': str(TEST_CONFIG_DIR),
            'group_list': str(TEST_CONFIG_DIR / 'sample_groups_list.json'),
            'upload_orders_dir_name': 'upload_orders',
            'completed_orders_dir_name': 'completed_orders',
            'failed_uploads_directory_name': 'failed_uploads',
            'log_file_path': str(TEST_CONFIG_DIR / 'test.log'),
            'max_workers': 4
        }
    elif file_path.endswith('sample_groups_list.json'):
        with open(TEST_CONFIG_DIR / 'sample_groups_list.json', 'r') as f:
            return json.load(f)
    else:
        raise ValueError(f"Unexpected file path in mock: {file_path}")

@pytest.fixture
def mock_config():
    return mock_load_settings("config/settings.yml")

@pytest.fixture
def mock_groups_info():
    return mock_load_settings("sample_groups_list.json")

@pytest.fixture(autouse=True)
def mock_main_imports():
    with patch('main.load_settings', side_effect=mock_load_settings), \
         patch('main.ProcessPoolExecutor'), \
         patch('main.setup_logger'):
        yield

@pytest.fixture
def mock_order_info():
    return {
        'Group': 'Private',
        'Username': 'TestUser',
        'Dataset': 'TestDataset',
        'UUID': 'TestUUID',
        'Files': ['file1.txt', 'file2.txt'],
        'file_names': ['file1.txt', 'file2.txt'],
        'UserID': 1,
        'GroupID': 1,
        'ProjectID': 1,
        'DatasetID': 1,
        'ScreenID': None
    }

def test_data_package_initialization(mock_order_info):
    from main import DataPackage
    data_package = DataPackage(mock_order_info, str(TEST_CONFIG_DIR))
    assert data_package.Group == mock_order_info['Group']
    assert data_package.Username == mock_order_info['Username']
    assert data_package.Dataset == mock_order_info['Dataset']
    assert data_package.UUID == mock_order_info['UUID']
    assert data_package.Files == mock_order_info['Files']
    assert data_package.file_names == mock_order_info['file_names']
    assert data_package.UserID == mock_order_info['UserID']
    assert data_package.GroupID == mock_order_info['GroupID']
    assert data_package.ProjectID == mock_order_info['ProjectID']
    assert data_package.DatasetID == mock_order_info['DatasetID']
    assert data_package.ScreenID == mock_order_info['ScreenID']

def test_data_package_str_representation(mock_order_info):
    from main import DataPackage
    data_package = DataPackage(mock_order_info, str(TEST_CONFIG_DIR))
    str_repr = str(data_package)
    assert 'DataPackage(' in str_repr
    assert 'Group: Private' in str_repr
    assert 'Username: TestUser' in str_repr
    assert 'Dataset: TestDataset' in str_repr
    assert 'UUID: TestUUID' in str_repr
    assert 'Files: 2 files' in str_repr

def test_data_package_get_method(mock_order_info):
    from main import DataPackage
    data_package = DataPackage(mock_order_info, str(TEST_CONFIG_DIR))
    assert data_package.get('Group') == 'Private'
    assert data_package.get('NonExistentKey', 'DefaultValue') == 'DefaultValue'

def test_load_config():
    with patch('main.load_settings', side_effect=mock_load_settings):
        from main import load_config
        config, groups_info = load_config()
        assert config == mock_load_settings("config/settings.yml")
        assert groups_info == mock_load_settings("sample_groups_list.json")

def test_create_executor(mock_config):
    with patch('main.ProcessPoolExecutor') as mock_executor:
        from main import create_executor
        executor = create_executor(mock_config)
        mock_executor.assert_called_once_with(max_workers=mock_config['max_workers'])

def test_setup_logging(mock_config):
    with patch('main.setup_logger') as mock_setup_logger:
        from main import setup_logging
        logger = setup_logging(mock_config)
        mock_setup_logger.assert_called_once_with('main', mock_config['log_file_path'])

@pytest.fixture
def mock_directory_poller():
    with patch('main.DirectoryPoller') as mock_dp:
        yield mock_dp.return_value

def test_run_application(mock_config, mock_groups_info, mock_directory_poller):
    mock_executor = MagicMock()
    mock_logger = MagicMock()
    
    with patch('main.initialize_system') as mock_init_system, \
         patch('main.signal.signal') as mock_signal, \
         patch('main.time.sleep', side_effect=[None, Exception("Stop loop")]):  # Force loop to exit after one iteration
        
        from main import run_application
        
        try:
            run_application(mock_config, mock_groups_info, mock_executor, mock_logger)
        except Exception as e:
            assert str(e) == "Stop loop"  # Ensure we exited due to our forced exception
        
        mock_init_system.assert_called_once_with(mock_config)
        assert mock_signal.call_count == 2  # Should be called twice for SIGINT and SIGTERM
        mock_directory_poller.start.assert_called_once()
        mock_directory_poller.stop.assert_called_once()
        mock_executor.shutdown.assert_called_once_with(wait=True)

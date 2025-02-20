import unittest
import os
import json
from unittest.mock import patch, mock_open
from pathlib import Path
import yaml
import pytest

from omero_adi.utils.upload_order_manager import UploadOrderManager
from omero_adi.utils.initialize import load_settings

class TestUploadOrderManager(unittest.TestCase):

    @pytest.fixture(autouse=True)
    def setup(self, tmp_path):
        self.test_config_dir = tmp_path / "test_config"
        self.test_config_dir.mkdir()
        self.settings_file = self.test_config_dir / "settings.yml"
        self.settings_file.write_text("test: value")
        
        with patch('utils.initialize.load_settings') as mock_load:
            mock_load.return_value = {
                'group_list': str(self.test_config_dir / "groups.json"),
                'log_file_path': str(self.test_config_dir / "test.log"),
            }
            self.config = mock_load(str(self.settings_file))

    def setUp(self):
        # Path to the test config directory
        self.test_config_dir = Path(__file__).parent / 'test_config'
        
        # Load settings from YAML file
        self.config = load_settings(self.test_config_dir / 'settings.yml')
        
        # Path to the existing sample upload order file
        self.order_file_path = self.test_config_dir / 'sample_upload_order.txt'

        # Ensure the sample upload order file exists
        if not self.order_file_path.exists():
            raise FileNotFoundError(f"Sample upload order file not found at {self.order_file_path}")

        # Use the existing sample_groups_list.json file
        self.groups_file_path = self.test_config_dir / 'sample_groups_list.json'

        # Update config with test-specific paths
        self.config['group_list'] = str(self.groups_file_path)
        self.config['log_file_path'] = str(self.test_config_dir / 'test.log')

    @patch('utils.upload_order_manager.setup_logger')
    def test_parse_order_file(self, mock_logger):
        manager = UploadOrderManager(str(self.order_file_path), self.config)
        order_info = manager.get_order_info()

        # Check if all attributes are correctly parsed
        self.assertEqual(order_info['Version'], 2.0)
        self.assertEqual(order_info['UUID'], 'afe38fe0-ea2b-43e6-949d-364827c66230')
        self.assertEqual(order_info['Username'], 'rrosas')
        self.assertEqual(order_info['Group'], 'Private')
        self.assertEqual(order_info['UserID'], 52)
        self.assertEqual(order_info['GroupID'], 103)
        self.assertEqual(order_info['ProjectID'], 51)
        self.assertEqual(order_info['DatasetID'], 101)
        self.assertEqual(order_info['Files'], [
            '/divg/coreReits/.omerodata2/2024/09/09/14-04-57/sample_image1_coreReits.tif',
            '/divg/coreReits/.omerodata2/2024/09/09/14-04-57/sample_image2_coreReits.tif',
            '/divg/coreReits/.omerodata2/2024/09/09/14-04-57/sample_image3_coreReits.tif'
        ])

        # Check if file_names list is correctly created
        expected_file_names = [
            'sample_image1_coreReits.tif',
            'sample_image2_coreReits.tif',
            'sample_image3_coreReits.tif'
        ]
        self.assertEqual(order_info['file_names'], expected_file_names)

    @patch('utils.upload_order_manager.setup_logger')
    def test_switch_path_prefix(self, mock_logger):
        manager = UploadOrderManager(str(self.order_file_path), self.config)
        manager.switch_path_prefix()
        order_info = manager.get_order_info()

        # Check if the path prefix has been switched from '/divg' to '/data'
        expected_files = [
            '/data/coreReits/.omerodata2/2024/09/09/14-04-57/sample_image1_coreReits.tif',
            '/data/coreReits/.omerodata2/2024/09/09/14-04-57/sample_image2_coreReits.tif',
            '/data/coreReits/.omerodata2/2024/09/09/14-04-57/sample_image3_coreReits.tif'
        ]
        self.assertEqual(order_info['Files'], expected_files)

        # Check if file_names remain unchanged after switch_path_prefix
        expected_file_names = [
            'sample_image1_coreReits.tif',
            'sample_image2_coreReits.tif',
            'sample_image3_coreReits.tif'
        ]
        self.assertEqual(order_info['file_names'], expected_file_names)

    @patch('utils.upload_order_manager.setup_logger')
    def test_validate_order_attributes_success(self, mock_logger):
        # All required attributes are present
        self.config['upload_order_attributes'] = ['Version', 'UUID', 'Username', 'Group', 'Files']
        manager = UploadOrderManager(str(self.order_file_path), self.config)
        # The validation happens in the constructor, so if no exception is raised, it's successful

if __name__ == '__main__':
    unittest.main()

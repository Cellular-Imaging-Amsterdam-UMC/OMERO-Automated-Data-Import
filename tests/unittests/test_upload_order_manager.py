import unittest
import os
import sys
import json
from unittest.mock import patch
from pathlib import Path

# Add the src directory to the Python path
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))
sys.path.insert(0, os.path.join(project_root, 'src'))

# Mock the problematic imports
sys.modules['ezomero'] = unittest.mock.MagicMock()
sys.modules['utils.importer'] = unittest.mock.MagicMock()
sys.modules['utils.ingest_tracker'] = unittest.mock.MagicMock()

from utils.upload_order_manager import UploadOrderManager

class TestUploadOrderManager(unittest.TestCase):

    def setUp(self):
        self.config = {
            'log_file_path': 'test_log.log',
            'base_dir': '/tmp',
            'group_list': 'test_groups.json'
        }
        
        # Path to the existing sample upload order file
        self.order_file_path = os.path.join(project_root, 'tests', 'unittests', 'test_config', 'sample_upload_order.txt')

        # Ensure the sample upload order file exists
        if not os.path.exists(self.order_file_path):
            raise FileNotFoundError(f"Sample upload order file not found at {self.order_file_path}")

        # Create a mock groups.json file
        self.groups_file_path = os.path.join(os.path.dirname(self.order_file_path), 'test_groups.json')
        with open(self.groups_file_path, 'w') as f:
            json.dump([{"omero_grp_name": "Reits", "core_grp_name": "coreReits"}], f)

        self.config['group_list'] = self.groups_file_path

    def tearDown(self):
        # Clean up temporary files
        if os.path.exists(self.groups_file_path):
            os.remove(self.groups_file_path)

    @patch('utils.upload_order_manager.setup_logger')
    def test_parse_order_file(self, mock_logger):
        manager = UploadOrderManager(self.order_file_path, self.config)
        order_info = manager.get_order_info()

        # Check if all attributes are correctly parsed
        self.assertEqual(order_info['Version'], 2.0)
        self.assertEqual(order_info['UUID'], 'afe38fe0-ea2b-43e6-949d-364827c66230')
        self.assertEqual(order_info['Username'], 'rrosas')
        self.assertEqual(order_info['Group'], 'Reits')
        self.assertEqual(order_info['UserID'], 102)
        self.assertEqual(order_info['GroupID'], 3)
        self.assertEqual(order_info['ProjectID'], 455)
        self.assertEqual(order_info['DatasetID'], 807)
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
        manager = UploadOrderManager(self.order_file_path, self.config)
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
    def test_get_core_grp_name_from_omero_name(self, mock_logger):
        manager = UploadOrderManager(self.order_file_path, self.config)
        
        # Test with existing group
        core_name = manager.get_core_grp_name_from_omero_name('Reits')
        self.assertEqual(core_name, 'coreReits')
        
        # Test with non-existing group
        core_name = manager.get_core_grp_name_from_omero_name('NonExistingGroup')
        self.assertIsNone(core_name)

if __name__ == '__main__':
    unittest.main()

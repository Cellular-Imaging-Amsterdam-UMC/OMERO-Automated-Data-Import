import os
import pytest
import yaml
import json
from pathlib import Path

# Load settings and group list for testing
def load_settings():
    with open("config/settings.yml", "r") as file:
        return yaml.safe_load(file)

def load_group_list():
    with open("config/groups_list.json", "r") as file:
        return json.load(file)

settings = load_settings()
group_list = load_group_list()

def test_path_format():
    """ Test if paths in settings are correctly formatted """
    for key, value in settings.items():
        if 'path' in key or 'dir' in key:
            assert '/' in value, f"Path format error in setting: {key}"

def test_file_and_directory_existence():
    """ Test if files and directories specified in settings exist """
    base_dir = Path(settings['base_dir'])
    assert base_dir.exists(), "Base directory does not exist"

    # Check if directories related to group settings exist
    for group in group_list:
        group_base_path = base_dir / group['core_grp_name']
        assert group_base_path.exists(), f"Group base directory does not exist for {group['core_grp_name']}"
        for dir_name in ['upload_orders_dir_name', 'completed_orders_dir_name', 'failed_uploads_directory_name']:
            dir_path = group_base_path / settings[dir_name]
            assert dir_path.exists(), f"Directory {dir_name} does not exist in group {group['core_grp_name']}"

def test_directory_access():
    """ Test if the application has necessary permissions for directories """
    base_dir = Path(settings['base_dir'])
    for group in group_list:
        group_base_path = base_dir / group['core_grp_name']
        for dir_name in ['upload_orders_dir_name', 'completed_orders_dir_name', 'failed_uploads_directory_name']:
            dir_path = group_base_path / settings[dir_name]
            # Check read, write, and delete permissions
            try:
                # Test write access
                with open(dir_path / 'test.tmp', 'w') as test_file:
                    test_file.write('Access test.')
                
                # Test read access
                with open(dir_path / 'test.tmp', 'r') as test_file:
                    assert test_file.read() == 'Access test.', "Failed to read the test file correctly."
                
                # Test delete access
                os.remove(dir_path / 'test.tmp')
            except Exception as e:
                pytest.fail(f"Access check failed for {dir_path}: {e}")
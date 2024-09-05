# Copyright 2023 Rodrigo Rosas-Bertolini
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# upload_order_manager.py

import shutil
import json
from pathlib import Path
from .logger import setup_logger
from .ingest_tracker import log_ingestion_step

class UploadOrderManager:
    def __init__(self, order_file_path, settings):
        """
        Initialize the UploadOrderManager with the given order file and settings.
        
        :param order_file_path: Path to the upload order file
        :param settings: Dictionary containing application settings
        """
        self.settings = settings
        self.logger = setup_logger(__name__, self.settings.get('log_file_path', 'upload_order_manager.log'))
        self.order_file_path = Path(order_file_path)
        self.order_info = self._parse_order_file()
        self.switch_path_prefix()
        self.groups_info = self.load_groups_info()

    def load_groups_info(self):
        """Load group information from the JSON configuration file."""
        with open('config/groups_list.json') as f:
            return json.load(f)

    def get_core_grp_name_from_omero_name(self, omero_grp_name):
        """
        Get the core group name corresponding to the given OMERO group name.
        
        :param omero_grp_name: OMERO group name
        :return: Corresponding core group name, or None if not found
        """
        for group in self.groups_info:
            if group['omero_grp_name'] == omero_grp_name:
                return group['core_grp_name']
        self.logger.error(f"Core group name not found for OMERO group: {omero_grp_name}")
        return None

    def _parse_order_file(self):
        """Parse the upload order file and return its content as a dictionary."""
        with open(self.order_file_path, 'r') as file:
            content = file.read()
            return json.loads(self._text_to_json(content))

    def _text_to_json(self, text):
        """
        Convert the text content of the upload order file to JSON format.
        
        :param text: Raw text content of the upload order file
        :return: JSON string representation of the upload order
        """
        lines = text.strip().split('\n')
        data = {}
        for line in lines:
            key, value = line.split(': ', 1)
            if key == 'Files':
                files_list = value.strip('[]').split(',')
                data[key] = [file.strip().strip('"') for file in files_list]
            else:
                data[key] = value.strip().strip('"')
        return json.dumps(data)

    def switch_path_prefix(self):
        """
        Switch the '/divg' prefix with '/data' for each file path in the 'Files' list.
        This method updates the file paths in the order_info dictionary.
        """
        if 'Files' in self.order_info:
            updated_files = []
            for file_path in self.order_info['Files']:
                parts = Path(file_path).parts
                if parts[1].lower() == 'divg':
                    new_path = Path('/data', *parts[2:])
                    updated_files.append(str(new_path))
                    self.logger.debug(f"Switched 'divg' to 'data' in path: {file_path} -> {new_path}")
                else:
                    updated_files.append(file_path)
            self.order_info['Files'] = updated_files
            self.logger.debug("Updated file paths after switching 'divg' to 'data'.")

    def log_upload_order_info(self):
        """Log the upload order information for debugging purposes."""
        info_lines = [f"{key}: {value}" for key, value in self.order_info.items()]
        log_message = "Upload Order Information:\n" + "\n".join(info_lines)
        self.logger.info(log_message)
        
    def log_order_movement(self, outcome):
        """
        Log the movement of the upload order file to the database.
        
        :param outcome: A string indicating the outcome, either 'failed' or 'completed'.
        """
        stage = "Order Moved to Completed" if outcome == 'completed' else "Order Moved to Failed"
        log_ingestion_step(
            self.order_info.get('Group', 'Unknown'),
            self.order_info.get('Username', 'Unknown'),
            self.order_info.get('Dataset', 'Unknown'),
            stage,
            self.order_info.get('UUID', 'Unknown')
        )

    def move_upload_order(self, outcome):
        """
        Move the upload order file to the appropriate directory based on the outcome.
        
        :param outcome: A string indicating the outcome, either 'failed' or 'completed'.
        """
        source_file_path = self.order_file_path
        omero_grp_name = self.order_info.get('Group', 'Unknown')
        core_grp_name = self.get_core_grp_name_from_omero_name(omero_grp_name)

        if core_grp_name is None:
            self.logger.error("Failed to retrieve core group name for moving upload order.")
            return

        username = self.order_info.get('Username', 'Unknown')

        if outcome == 'failed':
            destination_dir_name = self.settings['failed_uploads_directory_name']
        elif outcome == 'completed':
            destination_dir_name = self.settings['completed_orders_dir_name']
        else:
            self.logger.error(f"Invalid outcome specified: {outcome}")
            return

        destination_directory = Path(self.settings['base_dir']) / core_grp_name / destination_dir_name / username
        destination_file = destination_directory / source_file_path.name

        try:
            destination_directory.mkdir(parents=True, exist_ok=True)
            shutil.move(str(source_file_path), str(destination_file))
            self.logger.info(f"Moved upload order file {source_file_path} to {destination_file}")
            self.log_order_movement(outcome)
        except Exception as e:
            self.logger.error(f"Error moving upload order file {source_file_path} to {destination_file}: {e}")

    def get_order_info(self):
        """Return the parsed order information."""
        return self.order_info
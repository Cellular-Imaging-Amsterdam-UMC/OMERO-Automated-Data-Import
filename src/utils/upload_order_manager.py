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

import ast
import shutil
import json
import logging
from pathlib import Path
from .ingest_tracker import STAGE_MOVED_COMPLETED, STAGE_MOVED_FAILED, log_ingestion_step

class UploadOrderManager:
    def __init__(self, order_file_path, settings):
        """
        Initialize the UploadOrderManager with the given order file and settings.
        
        :param order_file_path: Path to the upload order file
        :param settings: Dictionary containing application settings
        """
        self.settings = settings
        self.logger = logging.getLogger(__name__)  # Use the existing logger
        self.order_file_path = Path(order_file_path)
        self.order_info = self._parse_order_file()
        self.groups_info = self.load_groups_info(self.settings.get('group_list', 'config/groups_list.json'))
        self._create_file_names_list()
        self.validate_order_attributes()

    def load_groups_info(self, group_list_path):
        """Load group information from the JSON configuration file."""
        with open(group_list_path) as f:
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
        """
        Parse the upload order file and return its content as a dictionary.
        
        :return: Dictionary representation of the upload order
        """
        try:
            with open(self.order_file_path, 'r') as file_obj:
                lines = file_obj.read().strip().split('\n')
        except (FileNotFoundError, IOError) as e:
            raise Exception(f"Unable to read the file at {self.order_file_path}: {e}")

        order_data = {}
        for line in lines:
            # Skip empty lines or lines without the expected separator
            if not line.strip() or ': ' not in line:
                continue  # Or handle accordingly

            key, value = line.split(': ', 1)
            key = key.strip()
            value = value.strip().strip('"')  # Remove surrounding whitespace and quotes

            # Attempt to parse the value into an appropriate data type
            parsed_value = value  # Default to the original string
            for conversion_func in (int, float, ast.literal_eval):
                try:
                    parsed_value = conversion_func(value)
                    break  # Exit loop if conversion is successful
                except (ValueError, SyntaxError):
                    continue  # Try the next conversion function

            order_data[key] = parsed_value

        return order_data  # Dictionary representation of the upload order


    def validate_order_attributes(self):
        """
        Validate the attributes of the upload order against those specified in settings.yml.
        Raises a ValueError if any required attribute is missing.
        """
        required_attributes = self.settings.get('upload_order_attributes', [])
        missing_attributes = [attr for attr in required_attributes if attr not in self.order_info]
        
        if missing_attributes:
            error_message = f"Missing required attributes in upload order: {', '.join(missing_attributes)}"
            self.logger.error(error_message)
            raise ValueError(error_message)
        
        self.logger.info("All required attributes are present in the upload order.")


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
            self.logger.debug(f"Updated {len(updated_files)} file paths after switching 'divg' to 'data'.")

    def _create_file_names_list(self):
        """
        Create a list of file names from the 'Files' attribute and add it as 'file_names' to order_info.
        """
        if 'Files' in self.order_info:
            self.order_info['file_names'] = [Path(file_path).name for file_path in self.order_info['Files']]
            self.logger.debug(f"Created file_names list with {len(self.order_info['file_names'])} entries")
        else:
            self.order_info['file_names'] = []
            self.logger.warning("No 'Files' attribute found in order_info. Created empty file_names list.")
        
    def log_order_movement(self, outcome):
        """
        Log the movement of the upload order file to the database using the ingest tracker.
    
        This method uses the global log_ingestion_step function from the ingest_tracker module
        to record the movement event in the database. It determines the appropriate stage
        based on the outcome and passes the order information along with the stage to the
        logging function.

        :param outcome: A string indicating the outcome, either 'failed' or 'completed'.
        """
        stage = STAGE_MOVED_COMPLETED if outcome == 'completed' else STAGE_MOVED_FAILED
        log_ingestion_step(self.order_info, stage)

    def move_upload_order(self, outcome):
        """
        Move the upload order file to the appropriate directory based on the outcome
        and log the movement using the ingest tracker.

        This method performs the following actions:
        1. Determines the destination directory based on the outcome ('failed' or 'completed').
        2. Moves the upload order file to the appropriate directory.
        3. Logs the movement event using the ingest tracker, which records the action in the database.

        :param outcome: A string indicating the outcome, either 'failed' or 'completed'.
        """
        source_file_path = self.order_file_path
        omero_grp_name = self.order_info.get('Group', 'Unknown')
        core_grp_name = self.get_core_grp_name_from_omero_name(omero_grp_name)

        if core_grp_name is None:
            self.logger.error("Failed to retrieve core group name for moving upload order.")
            return

        username = self.order_info.get('Username', 'Unknown')

        outcome_dirs = {
            'failed': self.settings['failed_uploads_directory_name'],
            'completed': self.settings['completed_orders_dir_name']
        }

        destination_dir_name = outcome_dirs.get(outcome)
        if destination_dir_name is None:
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

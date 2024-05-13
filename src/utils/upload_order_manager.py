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
from pathlib import Path
from .logger import setup_logger
from .ingest_tracker import log_ingestion_step

class UploadOrderManager:
    def __init__(self, order_file_path, settings):
        self.settings = settings
        self.logger = setup_logger(__name__, self.settings.get('log_file_path', 'upload_order_manager.log'))
        self.order_file_path = Path(order_file_path)  # Store the path to the upload order file
        self.order_info = self._parse_order_file(order_file_path)
        self.validate_order_info()

    def _parse_order_file(self, order_file_path):
        order_info = {}
        with open(order_file_path, 'r') as file:
            for line in file:
                key, value = line.strip().split(': ', 1)
                if key == 'Files':
                    files_list = value.strip('[]').split(',')
                    order_info[key] = [file_name.strip() for file_name in files_list]
                else:
                    order_info[key] = value.strip()
        return order_info

    def validate_order_info(self):
        required_keys = ['UUID', 'Group', 'Username', 'Dataset', 'Path', 'Files']
        missing_keys = [key for key in required_keys if key not in self.order_info]
        empty_keys = [key for key, value in self.order_info.items() if not value]

        if missing_keys:
            self.logger.error(f"Missing required keys in order info: {', '.join(missing_keys)}")
        if empty_keys:
            self.logger.error(f"Empty values found for keys in order info: {', '.join(empty_keys)}")

        if not missing_keys and not empty_keys:
            self.logger.info("Order info validation passed: All required keys are present and non-empty.")
            # Log the validation step in the database
            log_ingestion_step(
                self.order_info['Group'],
                self.order_info['Username'],
                self.order_info['Dataset'],
                "New Order Validated",
                self.order_info['UUID']
            )

    def log_upload_order_info(self):
        info_lines = [f"{key}: {value}" for key, value in self.order_info.items()]
        log_message = "Upload Order Information:\n" + "\n".join(info_lines)
        self.logger.info(log_message)

    def get_order_info(self):
        required_keys = ['UUID', 'Group', 'Username', 'Dataset', 'Files']
        missing_keys = [key for key in required_keys if key not in self.order_info]
        if missing_keys:
            raise KeyError(f"Missing required keys in order info: {', '.join(missing_keys)}")
        return (
            self.order_info['UUID'],
            self.order_info['Group'],
            self.order_info['Username'],
            self.order_info['Dataset'],
            self.order_info['Files']
        )
        
    def log_order_movement(self, outcome):
        """
        Logs the movement of the upload order file to the database.
        
        Parameters:
        - outcome: A string indicating the outcome, either 'failed' or 'completed'.
        """
        # Define the stage based on the outcome
        stage = "Order Moved to Completed" if outcome == 'completed' else "Order Moved to Failed"
        
        # Log the step in the database
        log_ingestion_step(
            self.order_info['Group'],
            self.order_info['Username'],
            self.order_info['Dataset'],
            stage,
            self.order_info['UUID']
        )

    def move_upload_order(self, outcome):
        """
        Moves the upload order file based on the outcome of the upload process.
        
        Parameters:
        - outcome: A string indicating the outcome, either 'failed' or 'completed'.
        """
        # Use self.order_file_path directly
        source_file_path = self.order_file_path
        group = self.order_info['Group']
        username = self.order_info['Username']

        if outcome == 'failed':
            destination_dir_name = self.settings['failed_uploads_directory_name']
        elif outcome == 'completed':
            destination_dir_name = self.settings['completed_orders_dir_name']
        else:
            self.logger.error(f"Invalid outcome specified: {outcome}")
            return

        destination_directory = Path(self.settings['base_dir']) / group / destination_dir_name / username
        destination_file = destination_directory / source_file_path.name

        try:
            destination_directory.mkdir(parents=True, exist_ok=True)
            shutil.move(str(source_file_path), str(destination_file))
            self.logger.info(f"Moved upload order file {source_file_path} to {destination_file}")
            self.log_order_movement(outcome)
        except Exception as e:
            self.logger.error(f"Error moving upload order file {source_file_path} to {destination_file}: {e}")
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

#importer.py

import os
import subprocess
import ezomero
from omero.gateway import BlitzGateway
from dotenv import load_dotenv
import json
#from pathlib import Path

from .logger import setup_logger
from utils.ingest_tracker import log_ingestion_step

# Load environment variables from .env file
load_dotenv('.env') #TODO check if I can get rid of this

class DataPackageImporter:
    def __init__(self, config):
        self.config = config
        self.logger = setup_logger(__name__, self.config['log_file_path'])
        
        # Set OMERO server details as instance attributes
        self.host = os.getenv('OMERO_HOST')
        self.password = os.getenv('OMERO_PASSWORD')
        self.user = os.getenv('OMERO_USER')
        self.port = os.getenv('OMERO_PORT')
        self.groups_info = self.load_groups_info()

    def load_groups_info(self):
        with open('config/groups_list.json') as f:
            return json.load(f)
    
    def create_dataset(self, conn, dataset_name, uuid, project_id=None):
        description = f"uploaded through datapackage uuid: {uuid}"
        try:
            dataset_id = ezomero.post_dataset(conn, dataset_name, project_id, description)
            self.logger.info(f"Created dataset: {dataset_name} with ID: {dataset_id}")
            return dataset_id
        except Exception as e:
            self.logger.error(f"Error creating dataset: {e}")
            return None

    def upload_files(self, conn, file_paths, dataset_id, dataset_name):
        successful_uploads = []
        failed_uploads = []
        for file_path in file_paths:
            try:
                # ln_s defines in-place imports. Change to False for normal https transfer
                file_id = ezomero.ezimport(conn, str(file_path), dataset=dataset_id, ln_s=True)
                if file_id is not None:
                    self.logger.info(f"Uploaded file: {file_path} to dataset ID: {dataset_id} with File ID: {file_id}")
                    successful_uploads.append((file_path, dataset_name, os.path.basename(file_path), file_id))
                else:
                    self.logger.error(f"Upload rejected by OMERO for file {file_path} to dataset ID: {dataset_id}. No ID returned.")
                    failed_uploads.append((file_path, dataset_name, os.path.basename(file_path), None))
            except Exception as e:
                self.logger.error(f"Error uploading file {file_path} to dataset ID: {dataset_id}: {e}")
                failed_uploads.append((file_path, dataset_name, os.path.basename(file_path), None))
        return successful_uploads, failed_uploads

    def import_data_package(self, data_package):
        self.logger.info(f"Starting import for data package: {data_package.dataset}")
    
        # Log the connection parameters as a debug message
        self.logger.debug(f"Attempting to connect to OMERO with User: {self.user}, Host: {self.host}, Port: {self.port}, Group: {data_package.group}")
        
        with BlitzGateway(self.user, self.password, group=data_package.group, host=self.host, port=self.port, secure=True) as conn:
            if not conn.connect():
                self.logger.error("Failed to connect to OMERO.")
                return [], [], True
    
            # Initialize the lists to store upload results
            all_successful_uploads = []
            all_failed_uploads = []
    
            try:
                dataset_id = self.create_dataset(conn, data_package.dataset, data_package.uuid)
                if dataset_id is None:
                    raise Exception("Failed to create dataset.")
    
                # Use the full paths directly from data_package.files
                file_paths = data_package.files
                successful_uploads, failed_uploads = self.upload_files(conn, file_paths, dataset_id, data_package.dataset)
                all_successful_uploads.extend(successful_uploads)
                all_failed_uploads.extend(failed_uploads)
    
                # Log the "Data Imported" step here, after successful uploads
                if successful_uploads:
                    log_ingestion_step(data_package.group, data_package.username, data_package.dataset, "Data Imported", str(data_package.uuid))
    
                # Change dataset ownership after creation and file upload
                self.change_dataset_ownership(conn, dataset_id, data_package.username)
    
            except Exception as e:
                self.logger.error(f"Exception during import: {e}")
                return [], [], True  # Indicate any exception as an import failure
    
        return all_successful_uploads, all_failed_uploads, False  # False indicates
    
    def change_dataset_ownership(self, conn, dataset_id, new_owner_username):
        new_owner_id = ezomero.get_user_id(conn, new_owner_username)
        if new_owner_id is None:
            self.logger.error(f"Failed to find user ID for username: {new_owner_username}")
            return
    
        login_command = f"omero login {self.user}@{self.host}:{self.port} -w {self.password}"
        # Updated to target Dataset instead of Project
        chown_command = f"omero chown {new_owner_id} Dataset:{dataset_id}"
        omero_cli_command = f"{login_command} && {chown_command}"
    
        try:
            self.logger.debug(f"Executing command: {omero_cli_command}")
            result = subprocess.run(omero_cli_command, shell=True, check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, executable='/bin/bash')
            self.logger.debug(f"Ownership change successful. Output: {result.stdout}")
        except subprocess.CalledProcessError as e:
            self.logger.error(f"Failed to change ownership. Error: {e.stderr}")
        except Exception as e:
            self.logger.error(f"Unexpected error during ownership change: {e}")
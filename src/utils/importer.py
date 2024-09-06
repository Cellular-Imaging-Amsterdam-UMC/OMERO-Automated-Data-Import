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

# importer.py

import os
import subprocess
import ezomero
from omero.gateway import BlitzGateway
from .logger import setup_logger
from utils.ingest_tracker import log_ingestion_step

class DataPackageImporter:
    """
    Handles the import of data packages into OMERO.
    """
    def __init__(self, config):
        """
        Initialize the DataPackageImporter with configuration settings.

        :param config: Configuration dictionary containing settings
        """
        self.config = config
        self.logger = setup_logger(__name__, self.config['log_file_path'])
        
        # Set OMERO server details as instance attributes
        self.host = os.getenv('OMERO_HOST')
        self.password = os.getenv('OMERO_PASSWORD')
        self.user = os.getenv('OMERO_USER')
        self.port = os.getenv('OMERO_PORT')

    def upload_files(self, conn, file_paths, dataset_id):
        """
        Upload files to a specified dataset in OMERO.

        :param conn: OMERO connection object
        :param file_paths: List of file paths to upload
        :param dataset_id: ID of the dataset to upload files to
        :return: Tuple of successful and failed uploads
        """
        successful_uploads = []
        failed_uploads = []
        for file_path in file_paths:
            self.logger.debug(f"Uploading file: {file_path}")
            try:
                # ln_s defines in-place imports. Change to False for normal https transfer
                image_id = ezomero.ezimport(conn=conn, target=str(file_path), dataset=dataset_id, transfer="ln_s")
                if image_id is not None:
                    # Ensure image_id is a single integer, not a list
                    if isinstance(image_id, list):
                        if len(image_id) == 1:
                            image_id = image_id[0]
                        else:
                            raise ValueError(f"Unexpected multiple image IDs returned for file {file_path}: {image_id}")
                    self.logger.info(f"Uploaded file: {file_path} to dataset ID: {dataset_id} with Image ID: {image_id}")
                    successful_uploads.append((file_path, dataset_id, os.path.basename(file_path), image_id))
                else:
                    self.logger.error(f"Upload rejected by OMERO for file {file_path} to dataset ID: {dataset_id}. No ID returned.")
                    failed_uploads.append((file_path, dataset_id, os.path.basename(file_path), None))
            except Exception as e:
                self.logger.error(f"Error uploading file {file_path} to dataset ID: {dataset_id}: {e}")
                failed_uploads.append((file_path, dataset_id, os.path.basename(file_path), None))
        return successful_uploads, failed_uploads

    def import_data_package(self, data_package):
        """
        Import a data package into OMERO.

        :param data_package: DataPackage object containing import information
        :return: Tuple of (successful_uploads, failed_uploads, import_status)
        """
        self.logger.info(f"Starting import for data package: {data_package.get('UUID', 'Unknown')}")
        self.logger.debug(f"Data package contents: {data_package}")
    
        self.logger.debug(f"Attempting to connect to OMERO with User: {self.user}, Host: {self.host}, Port: {self.port}, Group: {data_package.get('Group')}")

        with BlitzGateway(self.user, self.password, group=data_package.get('Group'), host=self.host, port=self.port, secure=True) as conn:
            if not conn.connect():
                self.logger.error("Failed to connect to OMERO.")
                return [], [], True
    
            all_successful_uploads = []
            all_failed_uploads = []
    
            try:
                dataset_id = data_package.get('DatasetID')
                if dataset_id is None:
                    raise ValueError("Dataset ID not provided in data package.")
    
                file_paths = data_package.get('Files', [])
                self.logger.debug(f"File paths to be uploaded: {file_paths}")
                successful_uploads, failed_uploads = self.upload_files(conn, file_paths, dataset_id)
                all_successful_uploads.extend(successful_uploads)
                all_failed_uploads.extend(failed_uploads)
    
                # Log the "Data Imported" step here, after successful uploads
                if successful_uploads:
                    log_ingestion_step(
                        data_package.get('Group', 'Unknown'),
                        data_package.get('Username', 'Unknown'),
                        data_package.get('DatasetID', 'Unknown'),  # Ensure DatasetID is used
                        "Data Imported",
                        str(data_package.get('UUID', 'Unknown'))
                    )
    
                # Change image ownership after upload
                self.change_image_ownership(conn, successful_uploads, data_package.get('UserID'))
    
            except Exception as e:
                self.logger.error(f"Exception during import: {e}")
                return [], [], True
    
        return all_successful_uploads, all_failed_uploads, False
    
    def change_image_ownership(self, conn, successful_uploads, new_owner_id):
        """
        Change the ownership of uploaded images to the specified user.

        :param conn: OMERO connection object
        :param successful_uploads: List of successfully uploaded files
        :param new_owner_id: ID of the new owner for the images
        """
        omero_user_id = ezomero.get_user_id(conn, self.user)
        if new_owner_id == omero_user_id:
            self.logger.info(f"{self.user} already owns the uploaded images.")
            return
            
        login_command = f"omero login {self.user}@{self.host}:{self.port} -w {self.password}"
        
        for _, _, _, image_id in successful_uploads:
            chown_command = f"omero chown {new_owner_id} Image:{image_id}"
            omero_cli_command = f"{login_command} && {chown_command}"
    
            try:
                self.logger.debug(f"Executing command: {omero_cli_command}")
                result = subprocess.run(omero_cli_command, shell=True, check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, executable='/bin/bash')
                self.logger.debug(f"Ownership change successful for Image:{image_id}. Output: {result.stdout}")
            except subprocess.CalledProcessError as e:
                self.logger.error(f"Failed to change ownership for Image:{image_id}. Error: {e.stderr}")
            except Exception as e:
                self.logger.error(f"Unexpected error during ownership change for Image:{image_id}: {str(e)}")
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
import ezomero
from omero.gateway import BlitzGateway
from .logger import setup_logger
from utils.ingest_tracker import STAGE_IMPORTED, log_ingestion_step
import Ice

MAX_RETRIES = 5  # Maximum number of retries
RETRY_DELAY = 5  # Delay between retries (in seconds)

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

    def upload_files(self, conn, dataset_id, file_paths, uuid):
        """
        Upload files to a specified dataset in OMERO.

        :param conn: OMERO connection object
        :param dataset_id: ID of the dataset to upload files to
        :param file_paths: List of file paths to upload
        :param uuid: UUID of the data package
        :return: Tuple of successful and failed uploads
        """
        successful_uploads = []
        failed_uploads = []
        for file_path in file_paths:
            self.logger.debug(f"Uploading file: {file_path}")
            try:
                # ln_s defines in-place imports. Change to False for normal https transfer
                image_ids = ezomero.ezimport(conn=conn, target=str(file_path), dataset=dataset_id, transfer="ln_s")
                if image_ids:
                    # Ensure we're working with a single integer ID
                    image_id = image_ids[0] if isinstance(image_ids, list) else image_ids
                    try:
                        self.add_image_annotations(conn, image_id, uuid, file_path)
                        self.logger.info(f"Uploaded file: {file_path} to dataset ID: {dataset_id} with Image ID: {image_id}")
                        successful_uploads.append((file_path, dataset_id, os.path.basename(file_path), image_id))
                    except Exception as annotation_error:
                        self.logger.error(f"File uploaded but annotation failed for {file_path}: {annotation_error}")
                        # Still consider it a successful upload even if annotation fails
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
        Import a data package into OMERO as the intended user.

        :param data_package: DataPackage object containing import information
        :return: Tuple of (successful_uploads, failed_uploads, import_status)
        """
        self.logger.info(f"Starting import for data package: {data_package.get('UUID', 'Unknown')}")
        self.logger.debug(f"Data package contents: {data_package}")

        intended_username = data_package.get('Username')
        group_id = data_package.get('GroupID')
        group_name = data_package.get('Group')

        if not intended_username or not group_id or not group_name:
            self.logger.error("Required user or group information not provided in data package.")
            return [], [], True

        # Connect as root
        with BlitzGateway(self.user, self.password, host=self.host, port=self.port, secure=True) as root_conn:
            # Retry mechanism for the connection
            retry_count = 0
            while retry_count < MAX_RETRIES:
                try:
                    if not root_conn.connect():
                        self.logger.error("Failed to connect to OMERO as root.")
                        return [], [], True
                    else:
                        self.logger.info("Connected to OMERO as root.")
                        break
                except Ice.ConnectionRefusedException as e:
                    retry_count += 1
                    self.logger.error(f"Connection refused (attempt {retry_count}/{MAX_RETRIES}): {e}")
                    if retry_count < MAX_RETRIES:
                        self.logger.info(f"Retrying in {RETRY_DELAY} seconds...")
                        time.sleep(RETRY_DELAY)
                    else:
                        self.logger.error("Max retries reached. Aborting import.")
                        self.logger.error("Failed to connect to OMERO as root.")
                        return [], [], True  # Fail after max retries

            try:
                # Create a new connection as the intended user
                user_conn = root_conn.suConn(intended_username)
                if not user_conn:
                    self.logger.error(f"Failed to create connection as user {intended_username}")
                    return [], [], True

                # Set the correct group for the session
                user_conn.setGroupForSession(group_id)

                self.logger.info(f"Connected as user {intended_username} in group {group_name}")

                all_successful_uploads = []
                all_failed_uploads = []

                dataset_id = data_package.get('DatasetID')
                if dataset_id is None:
                    raise ValueError("Dataset ID not provided in data package.")

                file_paths = data_package.get('Files', [])
                self.logger.debug(f"File paths to be uploaded: {file_paths}")
                successful_uploads, failed_uploads = self.upload_files(user_conn, dataset_id, file_paths, data_package.get('UUID'))
                all_successful_uploads.extend(successful_uploads)
                all_failed_uploads.extend(failed_uploads)

                if successful_uploads: 
                    log_ingestion_step(
                        data_package.get('Group', 'Unknown'),
                        data_package.get('Username', 'Unknown'),
                        data_package.get('DatasetID', 'Unknown'),
                        STAGE_IMPORTED,
                        str(data_package.get('UUID', 'Unknown')),
                        data_package.get('Files', ['Unknown'])
                    )

            except Exception as e:
                self.logger.error(f"Exception during import: {e}, {type(e)}")
                return [], [], True
            finally:
                if 'user_conn' in locals() and user_conn:
                    user_conn.close()

        return all_successful_uploads, all_failed_uploads, False
    
    def add_image_annotations(self, conn, image_id, uuid, file_path):
        """Add UUID and filepath as annotations to the image."""
        try:
            annotation_dict = {'UUID': str(uuid), 'Filepath': str(file_path)}
            ns = "custom.namespace.for.image.annotations"  # Define your custom namespace here
            
            self.logger.debug(f"Attempting to add annotations to Image:{image_id}")
            self.logger.debug(f"Annotation dict: {annotation_dict}")
            
            map_ann_id = ezomero.post_map_annotation(
                conn=conn, 
                object_type="Image", 
                object_id=image_id, 
                kv_dict=annotation_dict, 
                ns=ns,
                across_groups=False  # Set to False if you don't want cross-group behavior
            )
            
            if map_ann_id:
                self.logger.info(f"Successfully added annotations to Image:{image_id}. MapAnnotation ID: {map_ann_id}")
            else:
                self.logger.warning(f"MapAnnotation created for Image:{image_id}, but no ID was returned.")
            
        except Exception as e:
            self.logger.error(f"Failed to add annotations to Image:{image_id}. Error: {str(e)}")

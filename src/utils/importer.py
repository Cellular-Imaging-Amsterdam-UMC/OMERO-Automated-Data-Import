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
from utils.ingest_tracker import log_ingestion_step
from omero.cmd import Chown2, DoAll
from omero.callbacks import CmdCallbackI

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
        :param file_paths: List of file paths to upload
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
                        # Decide if you want to consider this a failed upload or not
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
                successful_uploads, failed_uploads = self.upload_files(conn, dataset_id, file_paths, data_package.get('UUID'))
                all_successful_uploads.extend(successful_uploads)
                all_failed_uploads.extend(failed_uploads)
    
                # Log the "Data Imported" step here, after successful uploads
                if successful_uploads:
                    log_ingestion_step(
                        data_package.get('Group', 'Unknown'),
                        data_package.get('Username', 'Unknown'),
                        data_package.get('DatasetID', 'Unknown'),
                        "Data Imported",
                        str(data_package.get('UUID', 'Unknown'))
                    )
    
                # Change image ownership after all uploads
                self.change_image_ownership(conn, successful_uploads, data_package.get('UserID'))
    
            except Exception as e:
                self.logger.error(f"Exception during import: {e}")
                return [], [], True
    
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
    
    def change_image_ownership(self, conn, successful_uploads, new_owner_id):
        """
        Change the ownership of uploaded images to the specified user.

        :param conn: OMERO connection object
        :param successful_uploads: List of successfully uploaded files
        :param new_owner_id: UserID of the new owner for the images
        """
        try:
            # Get the current user's ID
            current_user = conn.getUser()
            current_user_id = current_user.getId()

            if new_owner_id == current_user_id:
                self.logger.info(f"Current user (ID: {current_user_id}) already owns the uploaded images.")
                return

            self.logger.info(f"Attempting to change ownership of images to user ID: {new_owner_id}")

            for _, _, _, image_id in successful_uploads:
                try:
                    # Get the image object
                    image = conn.getObject("Image", image_id)
                    if not image:
                        self.logger.warning(f"Image with ID {image_id} not found. Skipping ownership change.")
                        continue

                    # Prepare the Chown2 command
                    chown = Chown2(targetObjects={'Image': [image_id]}, userId=new_owner_id)
                    
                    # Execute the command
                    handle = conn.c.submit(DoAll([chown]))
                    cb = CmdCallbackI(conn.c, handle)
                    
                    # Wait for the command to complete
                    cb.loop(10, 500)  # Wait for a maximum of 5 seconds
                    
                    # Get the response and check for errors
                    rsp = cb.getResponse()
                    if isinstance(rsp, dict) and rsp.get('Category') == 'Error':
                        self.logger.error(f"Error changing ownership for Image:{image_id}. Response: {rsp}")
                    else:
                        self.logger.info(f"Ownership change command executed for Image:{image_id}")

                    # Verify the ownership change
                    updated_image = conn.getObject("Image", image_id)
                    if updated_image.getOwner().getId() == new_owner_id:
                        self.logger.info(f"Verified: Image:{image_id} ownership changed to user ID: {new_owner_id}")
                    else:
                        self.logger.warning(f"Ownership change failed for Image:{image_id}. Current owner: {updated_image.getOwner().getId()}")

                except Exception as e:
                    self.logger.error(f"Failed to change or verify ownership for Image:{image_id}. Error: {str(e)}")

        except Exception as e:
            self.logger.error(f"Error in change_image_ownership: {str(e)}")
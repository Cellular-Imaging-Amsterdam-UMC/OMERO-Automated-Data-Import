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
import functools
from omero.gateway import BlitzGateway
from omero.sys import Parameters
from .logger import LoggerManager
from utils.ingest_tracker import STAGE_IMPORTED, log_ingestion_step
import Ice
import time
from omero.cli import CLI
from omero.rtypes import rstring, rlong
from omero.plugins.sessions import SessionsControl
from importlib import import_module
ImportControl = import_module("omero.plugins.import").ImportControl

MAX_RETRIES = 5  # Maximum number of retries
RETRY_DELAY = 5  # Delay between retries (in seconds)

def connection(func):
    @functools.wraps(func)
    def wrapper_connection(self, *args, **kwargs):
        # Step 1: Create a connection as the root user
        with BlitzGateway(self.user, self.password, host=self.host, port=self.port, secure=True) as root_conn:
            self.logger.debug("Connected (again) as root")
            if root_conn.connect():
                uuid = self.data_package.get('UUID')
                intended_username = self.data_package.get('Username')
                group_id = self.data_package.get('GroupID')
                group_name = self.data_package.get('Group')
                
                # Step 2: Switch to the intended user's session
                with root_conn.suConn(intended_username, ttl=self.ttl_for_user_conn) as user_conn:
                    user_conn.keepAlive()  # Keep the session alive
                    self.logger.debug(f"Connected (again) as user {intended_username}")
                    # Set the correct group for the session
                    user_conn.setGroupForSession(group_id)
                    self.logger.debug(f"Session group set to {group_name}")

                    # Step 3: Execute the original function with the user_conn
                    result = func(self, user_conn, *args, **kwargs)
                    return result
            else:
                raise ConnectionError("Could not connect to the OMERO server")
                
    return wrapper_connection

class DataPackageImporter:
    """
    Handles the import of data packages into OMERO.
    """
    def __init__(self, config, data_package, ttl_for_user_conn = 6000000):
        """
        Initialize the DataPackageImporter with configuration settings.

        :param config: Configuration dictionary containing settings
        :param data_package: DataPackage object containing import information
        :param ttl_for_user_conn: Connection timeout in ms (60000 per minute, default is 1 min)
        """
        self.config = config
        # Update logger initialization
        if not LoggerManager.is_initialized():
            raise RuntimeError("LoggerManager must be initialized before creating DataPackageImporter")
        self.logger = LoggerManager.get_module_logger(__name__)
        
        self.data_package = data_package
        
        self.ttl_for_user_conn = ttl_for_user_conn
        
        # Set OMERO server details as instance attributes
        self.host = os.getenv('OMERO_HOST')
        self.password = os.getenv('OMERO_PASSWORD')
        self.user = os.getenv('OMERO_USER')
        self.port = os.getenv('OMERO_PORT')
       
    @connection 
    def import_to_omero(self, conn, file_path, target_id, target_type, uuid, transfer_type="ln_s", depth=None):
        """
        Import a file to OMERO using CLI, either to a dataset or screen.

        :param conn: OMERO connection object
        :param file_path: Path to the file to import
        :param target_id: ID of the target dataset or screen
        :param target_type: Type of the target ('dataset' or 'screen')
        :param uuid: Unique identifier (UUID) used for log and error files
        :param transfer_type: File transfer method, default is 'ln_s'
        :param depth: Optional depth argument for import
        :return: True if the import was successful, False otherwise
        """
        cli = CLI()
        cli.register('import', ImportControl, '_')
        cli.register('sessions', SessionsControl, '_')
        
        # Common CLI arguments for importing
        arguments = ['import',
                    '-k', conn.getSession().getUuid().val,
                    '-s', conn.host,
                    '-p', str(conn.port),
                    f'--transfer={transfer_type}',
                    '--no-upgrade',
                    '--file', f"logs/cli.{uuid}.logs",
                    '--errs', f"logs/cli.{uuid}.errs"]
        
        # Add depth argument if provided
        if depth:
            arguments.append(f'--depth={depth}')
        
        # Add target-specific argument
        if target_type == 'screen':
            arguments.extend(['-r', str(target_id)])
        elif target_type == 'dataset':
            arguments.extend(['-d', str(target_id)])
        else:
            raise ValueError("Invalid target_type. Must be 'dataset' or 'screen'.")
        
        # Add the file path to the arguments
        arguments.append(str(file_path))
        
        # Invoke the CLI with the constructed arguments
        cli.invoke(arguments)
        
        if cli.rv == 0:
            self.imported = True
            self.logger.info(f'Imported {str(file_path)}')
            return True
        else:
            self.imported = False
            self.logger.error(f'Import of {str(file_path)} has failed!')
            return False

    @connection
    def get_plate_ids(self, conn, file_path, screen_id):
        """Get the Ids of imported plates.
        Note that this will not find plates if they have not been imported.
        Also, while plate_ids are returned, this method also sets
        ``self.plate_ids``.
        Returns
        -------
        plate_ids : list of ints
            Ids of plates imported from the specified client path, which
            itself is derived from ``self.file_path`` and ``self.filename``.
        """
        if self.imported is not True:
            self.logger.error(f'File {file_path} has not been imported')
            return None
        else:
            self.logger.debug("time to get some IDs")
            q = conn.getQueryService()
            self.logger.debug(q)
            params = Parameters()
            path_query = f"{str(file_path).strip('/')}%"
            self.logger.debug(f"path query: {path_query}. Screen_id: {screen_id}")
            params.map = {
                "cpath": rstring(path_query),
                "screen_id": rlong(screen_id),
            }
            self.logger.debug(params)
            # If this is costly, just select the highest ID, since that is the newest
            results = q.projection(
                "SELECT DISTINCT p.id, p.details.creationEvent.time FROM Plate p "
                "JOIN p.wells w "
                "JOIN w.wellSamples ws "
                "JOIN ws.image i "
                "JOIN i.fileset fs "
                "JOIN fs.usedFiles u "
                "JOIN p.screenLinks spl "
                "WHERE u.clientPath LIKE :cpath AND spl.parent.id = :screen_id "
                "ORDER BY p.details.creationEvent.time DESC",
                params,
                conn.SERVICE_OPTS
            )
            self.logger.debug(results)
            plate_ids = [r[0].val for r in results]
            return plate_ids

    @connection
    def import_dataset(self, conn, target, dataset, transfer="ln_s"):
        return ezomero.ezimport(conn, target, dataset, transfer)

    def upload_files(self, conn, file_paths, dataset_id=None, screen_id=None):
        """
        Upload files to a specified dataset or screen in OMERO.

        :param conn: OMERO connection object
        :param file_paths: List of file paths to upload
        :param data_packge: the data package
        :param dataset_id: (Optional) ID of the dataset to upload files to
        :param screen_id: (Optional) ID of the screen to upload files to
        :return: Tuple of successful and failed uploads
        """
        uuid = self.data_package.get('UUID')
        intended_username = self.data_package.get('Username')
        group_id = self.data_package.get('GroupID')
        group_name = self.data_package.get('Group')
        
        if dataset_id and screen_id:
            raise ValueError("Cannot specify both dataset_id and screen_id. Please provide only one.")
        if not dataset_id and not screen_id:
            raise ValueError("Either dataset_id or screen_id must be specified.")

        successful_uploads = []
        failed_uploads = []

        for file_path in file_paths:
            self.logger.debug(f"Uploading file: {file_path}")
            try:
                if screen_id:
                    self.logger.debug(f"Uploading to screen: {screen_id}")
                    imported = self.import_to_omero( 
                            file_path=str(file_path),
                            target_id=screen_id, 
                            target_type='screen',
                            uuid=uuid,
                            depth=10
                            )
                    self.logger.debug("Upload done. Retrieving plate id.")
                    image_ids = self.get_plate_ids(str(file_path), screen_id)
                    # # import_screen(conn=conn, file_path=str(file_path), screen_id=screen_id)
                    # image_ids = ezomero.ezimport(conn=conn, target=str(file_path), screen=screen_id, transfer="ln_s", errs='logs/cli.errs')
                else:  # Only dataset_id can be here
                    self.logger.debug(f"Uploading to dataset: {dataset_id}")
                    image_ids = self.import_dataset(target=str(file_path), dataset=dataset_id, transfer="ln_s")

                if image_ids:
                    # Ensure we're working with a single integer ID
                    image_or_plate_id = image_ids[0] if isinstance(image_ids, list) else image_ids
                    
                    try:
                        self.add_image_annotations(
                            image_or_plate_id, 
                            uuid, 
                            file_path, 
                            is_screen=bool(screen_id)  # True if screen_id is provided, False otherwise
                        )  
                        
                        self.logger.info(f"Uploaded file: {file_path} to dataset/screen ID: {dataset_id or screen_id} with ID: {image_or_plate_id}")       
                    except Exception as annotation_error:
                        self.logger.error(f"File uploaded but annotation failed for {file_path}: {annotation_error}")
                    # Still consider it a successful upload even if annotation fails
                    successful_uploads.append((file_path, dataset_id or screen_id, os.path.basename(file_path), image_or_plate_id))
                else:
                    self.logger.error(f"Upload rejected by OMERO for file {file_path} to dataset/screen ID: {dataset_id or screen_id}. No ID returned.")
                    failed_uploads.append((file_path, dataset_id or screen_id, os.path.basename(file_path), None))
            except Exception as e:
                self.logger.error(f"Error uploading file {file_path} to dataset/screen ID: {dataset_id or screen_id}: {e}")
                failed_uploads.append((file_path, dataset_id or screen_id, os.path.basename(file_path), None))
        return successful_uploads, failed_uploads

    def import_data_package(self):
        """
        Import a data package into OMERO as the intended user.

        :param data_package: DataPackage object containing import information
        :return: Tuple of (successful_uploads, failed_uploads, import_status)
        """
        self.logger.info(f"Starting import for data package: {self.data_package.get('UUID', 'Unknown')}")
        self.logger.debug(f"Data package contents: {self.data_package}")

        intended_username = self.data_package.get('Username')
        group_id = self.data_package.get('GroupID')
        group_name = self.data_package.get('Group')

        if not intended_username or not group_id or not group_name:
            self.logger.error("Required user or group information not provided in data package.")
            return [], [], True

        # Retry mechanism for the connection
        retry_count = 0
        while retry_count < MAX_RETRIES:
            try:
                # Connect as root
                with BlitzGateway(self.user, self.password, host=self.host, port=self.port, secure=True) as root_conn:           
                    if not root_conn.connect():
                        self.logger.error("Failed to connect to OMERO as root.")
                        return [], [], True
                    else:
                        self.logger.info("Connected to OMERO as root.")
                        
                    root_conn.keepAlive() 
                            
                    # Create a new connection as the intended user
                    with root_conn.suConn(intended_username, ttl=self.ttl_for_user_conn) as user_conn:
                        if not user_conn:
                            self.logger.error(f"Failed to create connection as user {intended_username}")
                            return [], [], True

                        user_conn.keepAlive() 
                        # Set the correct group for the session
                        user_conn.setGroupForSession(group_id)

                        self.logger.info(f"Connected as user {intended_username} in group {group_name}")

                        all_successful_uploads = []
                        all_failed_uploads = []

                        dataset_id = self.data_package.get('DatasetID')
                        screen_id = self.data_package.get('ScreenID')

                        # Validation for dataset_id and screen_id
                        if dataset_id and screen_id:
                            raise ValueError("Cannot specify both DatasetID and ScreenID in the data package. Please provide only one.")
                        if not dataset_id and not screen_id:
                            raise ValueError("Either DatasetID or ScreenID must be provided in the data package.")

                        file_paths = self.data_package.get('Files', [])
                        self.logger.debug(f"File paths to be uploaded: {file_paths}")

                        # Call upload_files with the appropriate ID
                        successful_uploads, failed_uploads = self.upload_files(
                            user_conn,
                            file_paths,
                            dataset_id=dataset_id,
                            screen_id=screen_id
                        )
                        self.logger.debug(f"Successful uploads: {successful_uploads}")
                        self.logger.debug(f"Failed uploads: {failed_uploads}")
                        
                        all_successful_uploads.extend(successful_uploads)
                        all_failed_uploads.extend(failed_uploads)

                        if successful_uploads: 
                            log_ingestion_step(self.data_package, STAGE_IMPORTED)
                            
                    return all_successful_uploads, all_failed_uploads, False    
            except Exception as e:
                if isinstance(e, Ice.ConnectionRefusedException) or "connect" in f"{e}".lower():
                    retry_count += 1
                    self.logger.error(f"Connection refused (attempt {retry_count}/{MAX_RETRIES}): {e}")
                    if retry_count < MAX_RETRIES:
                        self.logger.info(f"Retrying in {RETRY_DELAY} seconds...")
                        time.sleep(RETRY_DELAY)
                    else:
                        self.logger.error("Max retries reached. Aborting import.")
                        return [], [], True  # Fail after max retries
                else:
                    self.logger.error(f"Exception during import: {e}, {type(e)}")
                    return [], [], True

        # return all_successful_uploads, all_failed_uploads, False
        return [], [], True
    
    @connection
    def add_image_annotations(self, conn, object_id, uuid, file_path, is_screen=False):
        """Add UUID and filepath as annotations to the image or plate."""
        try:
            annotation_dict = {'UUID': str(uuid), 'Filepath': str(file_path)}
            ns = "omeroadi.import"

            if is_screen:
                self.logger.debug(f"Attempting to add annotations to Plate ID: {object_id}")
                object_type = "Plate"  # Set to Plate when it's a screen
            else:
                self.logger.debug(f"Attempting to add annotations to Image ID: {object_id}")
                object_type = "Image"  # Set to Image when it's a dataset

            self.logger.debug(f"Annotation dict: {annotation_dict}")

            map_ann_id = ezomero.post_map_annotation(
                conn=conn,
                object_type=object_type,
                object_id=object_id,
                kv_dict=annotation_dict,
                ns=ns,
                across_groups=False  # Set to False if you don't want cross-group behavior
            )

            if map_ann_id:
                self.logger.info(f"Successfully added annotations to {object_type} ID: {object_id}. MapAnnotation ID: {map_ann_id}")
            else:
                self.logger.warning(f"MapAnnotation created for {object_type} ID: {object_id}, but no ID was returned.")

        except Exception as e:
            self.logger.error(f"Failed to add annotations to {object_type} ID: {object_id}. Error: {str(e)}")


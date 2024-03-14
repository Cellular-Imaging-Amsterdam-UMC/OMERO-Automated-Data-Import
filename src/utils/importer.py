import os
import subprocess
import ezomero
from omero.gateway import BlitzGateway
from dotenv import load_dotenv
import json
from pathlib import Path

from .logger import setup_logger
from utils.ingest_tracker import log_ingestion_step

# Load environment variables from .env file
load_dotenv('.env')

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

    def get_omero_group_name(self, core_group_name):
        for group in self.groups_info:
            if group.get('core_grp_name') == core_group_name or group.get('core_group_name') == core_group_name:
                return group.get('omero_grp_name')
        self.logger.error(f"OMERO group name not found for core group: {core_group_name}")
        return None

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
        self.logger.info(f"data_package.path: {data_package.path}")  # Debug print
    
        omero_group_name = self.get_omero_group_name(data_package.group)
        if not omero_group_name:
            self.logger.error("OMERO group name could not be resolved.")
            return [], [], True

        conn = BlitzGateway(self.user, self.password, group=omero_group_name, host=self.host, port=self.port, secure=True)
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

            file_paths = [Path(data_package.path) / file_name for file_name in data_package.files]
            successful_uploads, failed_uploads = self.upload_files(conn, file_paths, dataset_id, data_package.dataset)
            all_successful_uploads.extend(successful_uploads)
            all_failed_uploads.extend(failed_uploads)

            # Log the "Data Imported" step here, after successful uploads
            #TODO extend to support partial imports.
            if successful_uploads:
                log_ingestion_step(data_package.group, data_package.username, data_package.dataset, "Data Imported", str(data_package.uuid))

        except Exception as e:
            self.logger.error(f"Exception during import: {e}")
            return [], [], True  # Indicate any exception as an import failure
        finally:
            conn.close()

        return all_successful_uploads, all_failed_uploads, False  # False indicates no import failure
    
    # TODO fix this its not working add more logs too.
    def change_project_ownership(self, conn, project_id, new_owner_username):
        new_owner_id = ezomero.get_user_id(conn, new_owner_username)
        if new_owner_id is None:
            self.logger.error(f"Failed to find user ID for username: {new_owner_username}")
            return
    
        login_command = f"omero login {self.user}@{self.host}:{self.port} -w {self.password}"
        chown_command = f"omero chown {new_owner_id} Project:{project_id}"
        omero_cli_command = f"{login_command} && {chown_command}"
    
        try:
            self.logger.info(f"Changing ownership of project ID {project_id} to user ID {new_owner_id}")
            result = subprocess.run(omero_cli_command, shell=True, check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, executable='/bin/bash')
            self.logger.info(f"Ownership change successful. Output: {result.stdout}")
        except subprocess.CalledProcessError as e:
            self.logger.error(f"Failed to change ownership. Error: {e.stderr}")

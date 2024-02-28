import os
import subprocess
import logging
import ezomero
from omero.gateway import BlitzGateway
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv('.env')

class DataPackageImporter:
    def __init__(self, config):
        self.config = config
        logging.basicConfig(level=logging.INFO, filename=self.config['log_file_path'], filemode='a',
                            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        self.logger = logging.getLogger(__name__)
        
        # Set OMERO server details as instance attributes
        self.host = os.getenv('OMERO_HOST')
        self.password = os.getenv('OMERO_PASSWORD')
        self.user = os.getenv('OMERO_USER')
        self.port = os.getenv('OMERO_PORT')

    def create_dataset(self, conn, dataset_name, description, project_id=None):
        try:
            dataset_id = ezomero.post_dataset(conn, dataset_name, project_id, description)
            self.logger.info(f"Created dataset: {dataset_name} with ID: {dataset_id}")
            return dataset_id
        except Exception as e:
            self.logger.error(f"Error creating dataset: {e}")
            return None

    def create_project(self, conn, project_name, description):
        try:
            project_id = ezomero.post_project(conn, project_name, description)
            self.logger.info(f"Created project: {project_name} with ID: {project_id}")
            return project_id
        except Exception as e:
            self.logger.error(f"Error creating project: {e}")
            return None

    def upload_files(self, conn, file_paths, dataset_id, project_name, dataset_name):
        successful_uploads = []
        failed_uploads = []
        for file_path in file_paths:
            try:
                file_id = ezomero.ezimport(conn, str(file_path), dataset=dataset_id)
                if file_id is not None:
                    self.logger.info(f"Uploaded file: {file_path} to dataset ID: {dataset_id} with File ID: {file_id}")
                    successful_uploads.append((file_path, project_name, dataset_name, os.path.basename(file_path), file_id))
                else:
                    self.logger.error(f"Upload rejected by OMERO for file {file_path} to dataset ID: {dataset_id}. No ID returned.")
                    failed_uploads.append((file_path, project_name, dataset_name, os.path.basename(file_path), None))
            except Exception as e:
                self.logger.error(f"Error uploading file {file_path} to dataset ID: {dataset_id}: {e}")
                failed_uploads.append((file_path, project_name, dataset_name, os.path.basename(file_path), None))
        return successful_uploads, failed_uploads

    def import_data_package(self, data_package):
        self.logger.info(f"DataPackage Details:\n"
                         f"Group: {data_package.group}\n"
                         f"User: {data_package.user}\n"
                         f"Project: {data_package.project}\n"
                         f"Original Path: {data_package.landing_dir_base_path}\n"
                         f"Staging Path: {data_package.staging_dir_base_path}\n"
                         f"Datasets: {data_package.datasets}")
        
        conn = BlitzGateway(self.user, self.password, group=data_package.group.replace('core', ''), host=self.host, port=self.port, secure=True)
        if not conn.connect():
            self.logger.error("Failed to connect to OMERO.")
            return

        new_owner_id = ezomero.get_user_id(conn, data_package.user)
        if new_owner_id is None:
            self.logger.error(f"Failed to find user ID for user name: {data_package.user}")
            conn.close()
            return

        project_description = 'This is a test project description'
        project_id = self.create_project(conn, data_package.project, project_description)
        if project_id is None:
            conn.close()
            return

        all_successful_uploads = []
        all_failed_uploads = []
        for dataset_name, file_paths in data_package.datasets.items():
            dataset_id = self.create_dataset(conn, dataset_name, 'This is a test description', project_id)
            if dataset_id is None:
                continue
            successful_uploads, failed_uploads = self.upload_files(conn, file_paths, dataset_id, data_package.project, dataset_name)
            all_successful_uploads.extend(successful_uploads)
            all_failed_uploads.extend(failed_uploads)
        
        conn.close()         

        # Change the ownership of the project using the CLI command
        login_command = f"omero login {self.user}@{self.host}:{self.port} -w {self.password}"
        chown_command = f"omero chown {new_owner_id} Project:{project_id}"
        omero_cli_command = f"{login_command} && {chown_command}"

        try:
            self.logger.info(f"Changing ownership of project ID {project_id} to user ID {new_owner_id}")
            result = subprocess.run(omero_cli_command, shell=True, check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, executable='/bin/bash')
            self.logger.info(f"Ownership change successful. Output: {result.stdout}")
        except subprocess.CalledProcessError as e:
            self.logger.error(f"Failed to change ownership. Error: {e.stderr}")
        
        return all_successful_uploads, all_failed_uploads
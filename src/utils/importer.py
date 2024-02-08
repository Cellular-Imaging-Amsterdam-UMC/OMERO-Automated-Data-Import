# importer.py

import os
from pathlib import Path
import ezomero
import logging
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

    def upload_files(self, conn, file_paths, dataset_id):
        try:
            # Adjust the base path for file_paths to the correct directory
            corrected_file_paths = [Path(str(fp).replace('test_mnt\\test_L_Drive', 'test_mnt\\test_OMERO_Dir')) for fp in file_paths] #TODO revisit the path handling and make it all through pathlib
            for file_path in corrected_file_paths:
                # Import function of ezomero
                ezomero.ezimport(conn, str(file_path), dataset=dataset_id) #TODO change this to just project and dataset to see it that is the issue
            self.logger.info(f"Uploaded files: {corrected_file_paths}")
        except Exception as e:
            self.logger.error(f"Error uploading files {corrected_file_paths}: {e}")

    def import_data_package(self, data_package):
        # Log a detailed representation of the data_package
        detailed_repr = (f"DataPackage Details:\n"
                         f"Group: {data_package.group}\n"
                         f"User: {data_package.user}\n"
                         f"Project: {data_package.project}\n"
                         f"Original Path: {data_package.original_path}\n"
                         f"Hidden Path: {data_package.hidden_path}\n"
                         f"Datasets: {data_package.datasets}")
        self.logger.info(detailed_repr)
        # Correct the base path for path_to_use if necessary
        path_to_use = str(data_package.hidden_path if data_package.hidden_path else data_package.original_path).replace('test_mnt\\test_L_Drive', 'test_mnt\\test_OMERO_Dir')
        self.logger.info(f"Importing data from path: {path_to_use}")

        # Use environment variables directly
        # HOST = os.getenv('HOST')
        # USER = os.getenv('USER')
        # PASSWORD = os.getenv('PASSWORD')
        HOST = 'omero-acc.amc.nl'
        USER = 'rrosas'
        PASSWORD = 'omero'
        PORT = int(os.getenv('PORT'))
        GROUP = data_package.group.replace('core', '')

        self.logger.info(f"Attempting to connect to OMERO with host: {HOST}, username: {USER}, port: {PORT}, group: {GROUP}")

        conn = BlitzGateway(USER, PASSWORD, group=GROUP, host=HOST, port=PORT, secure=True)
        if not conn.connect():
            self.logger.error("Failed to connect to OMERO.")
            return

        project_description = 'This is a test project description'
        project_id = self.create_project(conn, data_package.project, project_description)
        if project_id is None:
            conn.close()
            return

        for dataset_name, file_paths in data_package.datasets.items():
            dataset_id = self.create_dataset(conn, dataset_name, 'This is a test description', project_id)
            if dataset_id is None:
                continue
            self.upload_files(conn, file_paths, dataset_id)

        conn.close()
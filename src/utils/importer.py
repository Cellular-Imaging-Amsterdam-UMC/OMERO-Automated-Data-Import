# importer.py

import os
from pathlib import Path
import ezomero
import logging
from omero.gateway import BlitzGateway
from dotenv import load_dotenv

# Load environment variables from .env file and logger
load_dotenv()
logger = logging.getLogger(__name__)

# Function to create a new dataset in OMERO
def create_dataset(conn, dataset_name, description, project_id=None):
    try:
        dataset_id = ezomero.post_dataset(conn, dataset_name, project_id, description)
        logger.info(f"Created dataset: {dataset_name}")
        return dataset_id
    except Exception as e:
        logger.error(f"Error creating dataset: {e}")
        return None
    
def create_project(conn, project_name, description):
    try:
        project_id = ezomero.post_project(conn, project_name, description)
        logger.info(f"Created project: {project_name}")
        return project_id
    except Exception as e:
        logger.error(f"Error creating project: {e}")
        return None

# Function to upload files to the dataset
def upload_files(conn, file_paths, project, dataset):
    try:
        # Convert file_paths to Path objects
        file_paths = [Path(fp) for fp in file_paths]

        # Import the files into OMERO
        for file_path in file_paths:
            ezomero.ezimport(conn, str(file_path), project, dataset)

        logger.info(f"Uploaded files: {file_paths}")
    except Exception as e:
        logger.error(f"Error uploading files {file_paths}: {e}")

# Main function to orchestrate the upload process
def import_data_package(data_package, config):
    # Initialize the logger
    logging.basicConfig(level=logging.INFO, filename=config['log_file_path'], filemode='a',
                        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')

    # Connection parameters (these should be replaced with your own server details)
    HOST = os.getenv('HOST')
    USER = os.getenv('USER')
    PASSWORD = os.getenv('PASSWORD')
    PORT = int(os.getenv('PORT'))
    GROUP = data_package.group.replace('core', '')

    # Log the connection parameters
    logger.info(f"Attempting to connect to OMERO with host: {HOST}, username: {USER}, port: {PORT}, group: {GROUP}")

    # Connect to OMERO
    #conn = ezomero.connect(user=USER, password=PASSWORD, host=HOST, group=GROUP, port=PORT, secure=False)
    conn = BlitzGateway(USER, PASSWORD, group = GROUP, host = HOST, port=4064, secure=True)
    conn.connect()
    if conn is None:
        return

    # Create a new project
    project_description = 'This is a test project description'
    project_id = create_project(conn, data_package.project, project_description)
    if project_id is None:
        logger.error(f"Error creating project: {data_package.project}")
        return

    # Loop through each dataset in the data package
    for dataset_name, file_paths in data_package.datasets.items():
        # Create a new dataset
        description = 'This is a test description'
        dataset_id = create_dataset(conn, dataset_name, description, project_id)
        if dataset_id is None:
            continue

        # Upload files
        upload_files(conn, file_paths, project_id, dataset_id)

    # Close the connection
    conn.close()

# Call the main function
if __name__ == '__main__':
    import_data_package()
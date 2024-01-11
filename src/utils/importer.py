# importer.py

import ezomero
from getpass import getpass
import logging

logger = logging.getLogger(__name__)

# Function to connect to the OMERO server
def connect_to_omero(host, username, password, port, group):
    try:
        conn = ezomero.connect(host, username, password, port, group)
        logger.info(f"Connected to OMERO server: {host}")
        return conn
    except Exception as e:
        logger.error(f"Failed to connect to OMERO server: {e}")
        return None

# Function to create a new dataset in OMERO
def create_dataset(conn, dataset_name, description):
    try:
        dataset_id = ezomero.post_dataset(conn, dataset_name, description)
        logger.info(f"Created dataset: {dataset_name}")
        return dataset_id
    except Exception as e:
        logger.error(f"Error creating dataset: {e}")
        return None

# Function to upload files to the dataset
def upload_files(conn, dataset_id, file_paths):
    for file_path in file_paths:
        try:
            with open(file_path, 'rb') as file:
                file_name = file_path.split('/')[-1]
                ezomero.post_file(conn, file, dataset_id, file_name, file_path, format='image/jpeg')
                logger.info(f"Uploaded file: {file_name}")
        except Exception as e:
            logger.error(f"Error uploading file {file_path}: {e}")

# Main function to orchestrate the upload process
def import_data_package(data_package, config):
    # Initialize the logger
    logging.basicConfig(level=logging.INFO, filename=config['log_file_path'], filemode='a',
                        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')

    # Connection parameters (these should be replaced with your own server details)
    HOST = 'localhost'
    USER = 'root'
    PASSWORD = 'omero'
    PORT = 4064  # Default OMERO port
    GRPOUP = data_package.group.replace('core', '')

    # Log the connection parameters
    logger.info(f"Attempting to connect to OMERO with host: {HOST}, username: {USER}, port: {PORT}, group: {GRPOUP}")

    # Connect to OMERO
    conn = ezomero.connect(user=USER, password=PASSWORD, host="localhost", group=GRPOUP, port=PORT, secure=False)
    conn.connect()
    if conn is None:
        return

    # Loop through each dataset in the data package
    for dataset_name, file_paths in data_package.datasets.items():
        # Create a new dataset
        description = 'This is a test description'
        dataset_id = create_dataset(conn, dataset_name, description)
        if dataset_id is None:
            continue

        # Upload files
        upload_files(conn, dataset_id, file_paths)

    # Close the connection
    conn.close()

# Call the main function
if __name__ == '__main__':
    import_data_package()
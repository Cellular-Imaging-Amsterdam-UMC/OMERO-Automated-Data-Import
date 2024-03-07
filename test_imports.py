import os
from omero.gateway import BlitzGateway
import ezomero
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv('.env')

# Connection details
HOST = 'omero-acc.amc.nl'
USER = 'root'
PASSWORD = 'omero'
PORT = '4064'
GROUP = 'Rosas'

# Connect to OMERO
conn = BlitzGateway(USER, PASSWORD, group=GROUP, host=HOST, port=PORT, secure=True)
if not conn.connect():
    print("Failed to connect to OMERO.")
else:
    print("Connected to OMERO.")

    # Directory to upload
    directory_path = "/mnt/L-Drive/basic/divg/coreKrawczyk/SampleProject1/rrosas/.uploads/NewDatasetFile/"
    dataset_name = os.path.basename(directory_path)  # Use directory name as dataset name

    # Create a project (optional, could be predefined or based on some logic)
    project_name = "Proj1"
    project_description = "This is a test project"
    project_id = ezomero.post_project(conn, project_name, project_description)
    print(f">>> Created project: {project_name} with ID: {project_id}")

    # Create a dataset
    dataset_description = "This dataset is created from directory: " + dataset_name
    dataset_id = ezomero.post_dataset(conn, dataset_name, project_id, dataset_description)
    print(f">>> Created dataset: {dataset_name} with ID: {dataset_id}")

    # Iterate over files in the directory and upload each
    for file_name in os.listdir(directory_path):
        file_path = os.path.join(directory_path, file_name)
        if os.path.isfile(file_path):  # Ensure it's a file
            try:
                file_id = ezomero.ezimport(conn, str(file_path), dataset=dataset_id, ln_s=True)
                print(f">>> Uploaded file: {file_path}, ID: {file_id}")
            except Exception as e:
                print(f">>> Failed to upload file: {file_path}, Error: {str(e)}")

    # Close the connection
    conn.close()
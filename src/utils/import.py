import ezomero
from getpass import getpass

# Function to connect to the OMERO server
def connect_to_omero(host, username, password, port, group):
    try:
        conn = ezomero.connect(host, username, password, port=port, group=group)
        return conn
    except Exception as e:
        print(f"Failed to connect to OMERO server: {e}")
        return None

# Function to create a new dataset in OMERO
def create_dataset(conn, dataset_name, description):
    try:
        dataset_id = ezomero.post_dataset(conn, dataset_name, description)
        return dataset_id
    except Exception as e:
        print(f"Error creating dataset: {e}")
        return None

# Function to upload files to the dataset
def upload_files(conn, dataset_id, file_paths):
    for file_path in file_paths:
        try:
            with open(file_path, 'rb') as file:
                file_name = file_path.split('/')[-1]
                ezomero.post_file(conn, file, dataset_id, file_name, file_path, format='image/jpeg')
                print(f"Uploaded file: {file_name}")
        except Exception as e:
            print(f"Error uploading file {file_path}: {e}")

# Main function to orchestrate the upload process
def main(data_package):
    # Connection parameters (these should be replaced with your own server details)
    host = 'localhost'
    username = 'root'
    password = 'omero'
    port = 4064  # Default OMERO port
    group = data_package.group

    # Connect to OMERO
    conn = connect_to_omero(host, username, password, port, group)
    if conn is None:
        return

    # Create a new dataset
    dataset_name = 'New Dataset'
    description = 'This is a test description'
    dataset_id = create_dataset(conn, dataset_name, description)
    if dataset_id is None:
        conn.close()
        return

    # List of file paths to upload
    file_paths = ['path/to/file1.jpg', 'path/to/file2.jpg']

    # Upload files
    upload_files(conn, dataset_id, file_paths)

    # Close the connection
    conn.close()

# Call the main function
if __name__ == '__main__':
    main()

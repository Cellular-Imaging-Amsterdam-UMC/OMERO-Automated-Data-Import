import os
import shutil
import uuid
import datetime
import sys
from pathlib import Path

# Change the working directory to the project's root directory
os.chdir(Path(__file__).resolve().parent.parent)

# Add the src directory to the PYTHONPATH
sys.path.append(str(Path(__file__).resolve().parent.parent / 'src'))

from utils.config_manager import load_settings, load_json

# Load configuration
config = load_settings("config/settings.yml")
groups_info = load_json("config/groups_list.json")

# Sample image and base directory
sample_image = Path(config.get('sample_image', '/auto-importer/tests/Barbie.tif'))
base_dir = Path(config['base_dir'])

# Function to create upload order file
def create_upload_order(group, core_group_name, username, dataset, files):
    order_content = {
        "Version": "2.0",
        "UUID": str(uuid.uuid4()),
        "Username": "rrosas",  # Remember to also change the UserID
        "Group": group,
        "UserID": 34,
        "GroupID": 134,
        "ProjectID": 1001,
        "Files": files
    }

    # Use sample_dataset_id if available, else default to 1701
    sample_dataset_id = config.get('sample_dataset_id', 1701)
    sample_screen_id = config.get('sample_screen_id')

    if sample_screen_id:  # If screen_id exists, replace DatasetID with ScreenID
        order_content["ScreenID"] = sample_screen_id
    else:
        order_content["DatasetID"] = sample_dataset_id
        
    upload_order_dir = base_dir / core_group_name / config['upload_orders_dir_name']
    upload_order_dir.mkdir(parents=True, exist_ok=True)
    upload_order_path = upload_order_dir / f"{dataset}.txt"
    
    with open(upload_order_path, 'w') as file:
        for key, value in order_content.items():
            if key in ["Username", "Group"]:
                file.write(f'{key}: "{value}"\n')
            elif key == "Files":
                file.write(f"{key}: {value}\n")
            else:
                file.write(f"{key}: {value}\n")
    
    print(f"Created upload order for group '{group}' at '{upload_order_path}'")
    
    # Print the contents of the created file
    with open(upload_order_path, 'r') as file:
        print(f"Contents of upload order file:\n{file.read()}")
    
    return upload_order_path

# Function to copy sample image to the target directory
def copy_sample_image(core_group_name, dataset):
    # Shortcut: If sample_image is already within base_dir, just return the path
    if sample_image.is_relative_to(base_dir / core_group_name):
        print(f"Sample image is already in base directory, returning path: {sample_image}")
        return sample_image
    
    target_dir = base_dir / core_group_name / ".omerodata2" / dataset.replace('_', '/')
    target_dir.mkdir(parents=True, exist_ok=True) 
    # Determine target path
    if sample_image.suffix == '.zarr':  # Check if it's a Zarr directory
        target_path = target_dir / f"sample_image_{core_group_name}.zarr"
        shutil.copytree(sample_image, target_path)
        print(f"Copied Zarr directory to '{target_path}'")
    else:
        target_path = target_dir / f"sample_image_{core_group_name}.tif"
        shutil.copy(sample_image, target_path)
        print(f"Copied sample image to '{target_path}'")
    return target_path

# Ensure OMERO_USER environment variable is set
username = os.getenv('OMERO_USER')
if not username:
    print("Error: OMERO_USER environment variable is not set.")
    sys.exit(1)

# Create upload orders and copy sample image for each group
for group_info in groups_info:
    group = group_info['omero_grp_name']
    core_group_name = group_info['core_grp_name']
    dataset = datetime.datetime.now().strftime("%Y_%m_%d_%H-%M-%S")
    # files = [f"/divg/{core_group_name}/.omerodata2/{dataset.replace('_', '/')}/sample_image_{core_group_name}.tif"]
    
    # Copy the sample image
    file = copy_sample_image(core_group_name, dataset)
    files = [file.replace(base_dir, "/divg")]
    
    # Create the upload order
    create_upload_order(group, core_group_name, username, dataset, files)

print("Upload orders created successfully.")
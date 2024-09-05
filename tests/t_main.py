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
from utils.logger import setup_logger

# Setup logging
config = load_settings("config/settings.yml")
logger = setup_logger(__name__, config['log_file_path'])

# Load group information
groups_info = load_json("config/groups_list.json")

# Sample image and base directory
sample_image = Path("/auto-importer/tests/Barbie.tif")
base_dir = Path(config['base_dir'])

# Function to create upload order file
def create_upload_order(group, core_group_name, username, dataset, files):
    order_content = {
        "Version": "2.0",
        "UUID": str(uuid.uuid4()),
        "Username": username,
        "Group": group,
        "UserID": 34,
        "GroupID": 134,
        "ProjectID": 951,
        "DatasetID": 1651,
        "Files": files
    }
    
    upload_order_dir = base_dir / core_group_name / config['upload_orders_dir_name']
    upload_order_dir.mkdir(parents=True, exist_ok=True)
    upload_order_path = upload_order_dir / f"{dataset}.txt"
    
    with open(upload_order_path, 'w') as file:
        for key, value in order_content.items():
            if key == "Files":
                file.write(f"{key}: {value}\n")
            else:
                file.write(f"{key}: \"{value}\"\n")
    
    logger.info(f"Created upload order for group '{group}' at '{upload_order_path}'")
    
    # Print the contents of the created file
    with open(upload_order_path, 'r') as file:
        logger.info(f"Contents of upload order file:\n{file.read()}")
    
    return upload_order_path

# Function to copy sample image to the target directory
def copy_sample_image(core_group_name, dataset):
    target_dir = base_dir / core_group_name / ".omerodata2" / dataset.replace('_', '/')
    target_dir.mkdir(parents=True, exist_ok=True)
    target_path = target_dir / f"sample_image_{core_group_name}.tif"
    shutil.copy(sample_image, target_path)
    logger.info(f"Copied sample image to '{target_path}'")
    return target_path

# Ensure OMERO_USER environment variable is set
username = os.getenv('OMERO_USER')
if not username:
    logger.error("OMERO_USER environment variable is not set.")
    sys.exit("Error: OMERO_USER environment variable is not set.")

# Create upload orders and copy sample image for each group
for group_info in groups_info:
    group = group_info['omero_grp_name']
    core_group_name = group_info['core_grp_name']
    dataset = datetime.datetime.now().strftime("%Y_%m_%d_%H-%M-%S")
    files = [f"/divg/{core_group_name}/.omerodata/{dataset.replace('_', '/')}/sample_image_{core_group_name}.tif"]
    
    # Copy the sample image
    copy_sample_image(core_group_name, dataset)
    
    # Create the upload order
    create_upload_order(group, core_group_name, username, dataset, files)

logger.info("Upload orders created successfully. Check logs/app.logs to view test results.")
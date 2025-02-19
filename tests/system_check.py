#!/usr/bin/env python3
import yaml
import json
import uuid
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from utils.ingest_tracker import Base, IngestionTracking, STAGE_NEW_ORDER
from pathlib import Path
import datetime
import shutil

def load_config(path="/auto-importer/config/settings.yml"):
    with open(path, "r") as f:
        return yaml.safe_load(f)
    
# Function to copy sample image to the target directory
def copy_sample_image(core_group_name, dataset, sample_image, base_dir):
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

def main():
    # Load configuration and get the database URL
    config = load_config()
    db_url = config["ingest_tracking_db"]
    print(f"Using database URL: {db_url}")
    
    # Create the engine and sessionmaker
    engine = create_engine(db_url)
    Session = sessionmaker(bind=engine)
    
    # Create tables if they do not exist
    Base.metadata.create_all(engine)
    
    session = Session()
    try:
        files = ["/auto-importer/tests/Barbie1.tif"]
        filenames = ["Barbie1.tif"]
        
        # Sample image and base directory
        sample_image = Path(config.get('sample_image', '/auto-importer/tests/Barbie1.tif'))
        base_dir = Path(config['base_dir'])
        
        # Copy the sample image
        file = copy_sample_image("coreReits", datetime.datetime.now().strftime("%Y_%m_%d_%H-%M-%S"), sample_image, base_dir)
        files = [str(file)]
            
        # Build a sample upload order record
        order_info = {
            "Group": "Reits",
            "Username": "rrosas",
            "DestinationID": "2401",  # Updated from "DataPackage" to "DestinationID"
            "UUID": str(uuid.uuid4()),
            "Files": files,
            "FileNames": filenames
        }
        
        # Create a new IngestionTracking instance
        new_order = IngestionTracking(
            group_name=order_info["Group"],
            user_name=order_info["Username"],
            destination_id=order_info["DestinationID"],
            stage=STAGE_NEW_ORDER,
            uuid=order_info["UUID"],
            files=order_info["Files"],
            file_names=order_info["FileNames"]
        )
        
        session.add(new_order)
        session.commit()
        
        print("New upload order created with ID:", new_order.id)
    except Exception as e:
        print("Error creating upload order:", e)
        session.rollback()
    finally:
        session.close()

if __name__ == "__main__":
    main()

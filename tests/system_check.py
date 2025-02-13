#!/usr/bin/env python3
import yaml
import json
import uuid
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from utils.ingest_tracker import Base, IngestionTracking, STAGE_NEW_ORDER

def load_config(path="config/settings.yml"):
    with open(path, "r") as f:
        return yaml.safe_load(f)

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
        # Build a sample upload order record
        order_info = {
            "Group": "Test Group",
            "GroupID": "3",
            "Username": "ttest",
            "DataPackage": "1",
            "UUID": str(uuid.uuid4()),
            "Files": ["/auto-importer/tests/Barbie1.tif", "/auto-importer/tests/Barbie2.tif","/auto-importer/tests/Barbie3.tif"],
            "FileNames": ["Barbie1.tif", "Barbie2.tif","Barbie3.tif"]
        }
        
        # Create a new IngestionTracking instance
        new_order = IngestionTracking(
            Group=order_info["Group"],
            GroupID=order_info["GroupID"],
            Username=order_info["Username"],
            DataPackage=order_info["DataPackage"],
            Stage=STAGE_NEW_ORDER,
            UUID=order_info["UUID"],
            Files=order_info["Files"],
            FileNames=order_info["FileNames"]
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

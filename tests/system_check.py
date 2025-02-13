#!/usr/bin/env python3
import yaml
import json
import uuid
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from utils.ingest_tracker import Base, IngestionTracking

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
            "DatasetID": "1",
            "UUID": str(uuid.uuid4()),
            # The model expects the files as JSON text (if you follow your property setters)
            "Files": ["/auto-importer/tests/Barbie1.tif", "/auto-importer/tests/Barbie2.tif","/auto-importer/tests/Barbie3.tif"],
            "file_names": ["Barbie1.tif", "Barbie2.tif","Barbie3.tif"]
        }
        
        # Create a new IngestionTracking instance
        new_order = IngestionTracking(
            group_name=order_info["Group"],
            group_id=order_info.get("GroupID"),
            user_name=order_info["Username"],
            data_package=order_info["DatasetID"],
            stage="Upload Order Received",  # This corresponds to STAGE_NEW_ORDER in your system
            uuid=order_info["UUID"],
            _files=json.dumps(order_info["Files"]),
            _file_names=json.dumps(order_info["file_names"])
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

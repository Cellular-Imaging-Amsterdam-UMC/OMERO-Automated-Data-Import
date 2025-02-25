# initialize.py

import yaml
import json
import logging
from pathlib import Path
from .ingest_tracker import initialize_ingest_tracker
from sqlalchemy.sql import func
from datetime import datetime, timedelta
from sqlalchemy import func
from .ingest_tracker import (
    STAGE_NEW_ORDER,
    STAGE_INGEST_FAILED,
    STAGE_IMPORTED
)


def load_settings(file_path: str) -> dict:
    """
    Load settings from either a YAML or JSON file.
    """
    logger = logging.getLogger(__name__)
    try:
        file_path_obj = Path(file_path)
        if not file_path_obj.exists():
            logger.error(f"Settings file not found: {file_path_obj}")
            raise FileNotFoundError(
                f"Settings file not found: {file_path_obj}")

        logger.debug(f"Loading settings from {file_path_obj}")
        with file_path_obj.open('r') as file:
            if file_path_obj.suffix in ['.yml', '.yaml']:
                settings = yaml.safe_load(file)
                logger.debug("Successfully loaded YAML settings")
                return settings
            elif file_path_obj.suffix == '.json':
                settings = json.load(file)
                logger.debug("Successfully loaded JSON settings")
                return settings
            else:
                logger.error(f"Unsupported file format: {file_path_obj}")
                raise ValueError(f"Unsupported file format: {file_path_obj}")
    except Exception as e:
        logger.error(f"Failed to load settings from {file_path_obj}: {str(e)}")
        raise


def initialize_system(config: dict) -> None:
    """
    Performs initial system setup.
    In the updated database-driven mode, group checks are no longer performed.
    Errors are logged but do not cause the container to exit.
    """
    logger = logging.getLogger(__name__)
    try:
        logger.info("Starting system initialization (database-driven mode)...")
        logger.debug("Initializing ingest tracking database...")
        success = initialize_ingest_tracker(config)  # Capture the return value
        if not success:
            logger.error("Failed to initialize ingest tracker")
        else:
            logger.info("System initialization completed successfully.")
    except Exception as e:
        logger.error(f"System initialization failed: {str(e)}", exc_info=True)
        print(f"Exception during initialization: {str(e)}")  # Debug print


def finalize_dangling_orders_and_get_last_id(ingest_tracker, IngestionTracking, days=1):
    """
    Finalizes any dangling import orders that were left unfinished during a previous run.

    This function performs the following steps atomically:
      1. Identifies all unique UUIDs for orders logged as STAGE_NEW_ORDER (i.e. "Import Pending")
         within the last `days` days that do NOT have a corresponding completion entry 
         (either STAGE_IMPORTED or STAGE_INGEST_FAILED).
      2. For each such dangling order, retrieves a representative record and creates a new 
         IngestionTracking entry that copies the original order's data but with the stage set to 
         STAGE_INGEST_FAILED and a fresh timestamp.
      3. Commits all new entries to the database in one transaction, ensuring no modifications 
         are made to existing records.
      4. Retrieves and returns the highest ID from the IngestionTracking table after processing.

    This approach adheres to an event-sourcing model by always appending new entries rather than
    editing pre-existing ones.

    :param ingest_tracker: Global IngestTracker instance that provides a Session.
    :param IngestionTracking: The SQLAlchemy model for ingestion tracking.
    :param STAGE_NEW_ORDER: Constant representing an order in the "Import Pending" stage.
    :param STAGE_IMPORTED: Constant representing a successfully imported order.
    :param STAGE_INGEST_FAILED: Constant representing a failed import.
    :param days: Look-back period in days for considering orders as dangling (default is 1).
    :return: The highest ID in the IngestionTracking table after processing, or 0 if no entries exist.
    """
    cutoff_time = datetime.now() - timedelta(days=days)
    Session = ingest_tracker.Session
    with Session() as session:
        # Subquery: get all UUIDs that already have a completion record (imported or failed)
        processed_subq = session.query(IngestionTracking.uuid).filter(
            IngestionTracking.stage.in_([STAGE_IMPORTED, STAGE_INGEST_FAILED])
        ).distinct().subquery()

        # Identify dangling orders: those with a STAGE_NEW_ORDER entry (within the cutoff) lacking a completion record
        dangling_uuids = session.query(IngestionTracking.uuid).filter(
            IngestionTracking.stage == STAGE_NEW_ORDER,
            IngestionTracking.timestamp >= cutoff_time,
            ~IngestionTracking.uuid.in_(processed_subq)
        ).distinct().all()

        # For each dangling order, create a new failed entry without modifying existing records
        for (order_uuid,) in dangling_uuids:
            original_order = session.query(IngestionTracking).filter(
                IngestionTracking.uuid == order_uuid,
                IngestionTracking.stage == STAGE_NEW_ORDER
            ).order_by(IngestionTracking.timestamp.asc()).first()

            if original_order:
                new_failed_entry = IngestionTracking(
                    uuid=original_order.uuid,
                    stage=STAGE_INGEST_FAILED,
                    timestamp=datetime.now(),  # New timestamp for the failed entry
                    group_name=original_order.group_name,
                    user_name=original_order.user_name,
                    destination_id=original_order.destination_id,
                    destination_type=original_order.destination_type,
                    files=original_order.files,
                    file_names=original_order.file_names
                    # Include additional fields if necessary
                )
                session.add(new_failed_entry)

        # Retrieve and return the highest ID from the table
        last_max_id = session.query(func.max(IngestionTracking.id)).scalar()

        # Commit all new failed entries atomically
        session.commit()

        return last_max_id if last_max_id is not None else 0


def test_omero_connection():
    import os
    from omero.gateway import BlitzGateway
    logger = logging.getLogger(__name__)
    host = os.getenv("OMERO_HOST")
    user = os.getenv("OMERO_USER")
    password = os.getenv("OMERO_PASSWORD")
    port = os.getenv("OMERO_PORT")
    try:
        conn = BlitzGateway(user, password, host=host, port=port, secure=True)
        if conn.connect():
            logger.info(
                "Successfully connected to OMERO server at %s:%s", host, port)
            conn.close()
            return True
        else:
            logger.error(
                "Failed to connect to OMERO server at %s:%s", host, port)
            return False
    except Exception as e:
        logger.error("Exception during OMERO connection test: %s",
                     e, exc_info=True)
        return False

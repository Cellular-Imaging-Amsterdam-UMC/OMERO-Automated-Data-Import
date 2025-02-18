# initialize.py

import os
import sys
import yaml
import json
import logging
from pathlib import Path
from .ingest_tracker import initialize_ingest_tracker

def load_settings(file_path: str) -> dict:
    """
    Load settings from either a YAML or JSON file.
    """
    logger = logging.getLogger(__name__)
    try:
        file_path_obj = Path(file_path)
        if not file_path_obj.exists():
            logger.error(f"Settings file not found: {file_path_obj}")
            raise FileNotFoundError(f"Settings file not found: {file_path_obj}")

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
        print(f"Initialization success: {success}")  # Debug print
        if not success:
            logger.error("Failed to initialize ingest tracker")
        logger.info("System initialization completed successfully.")
    except Exception as e:
        logger.error(f"System initialization failed: {str(e)}", exc_info=True)
        print(f"Exception during initialization: {str(e)}")  # Debug print


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
            logger.info("Successfully connected to OMERO server at %s:%s", host, port)
            conn.close()
            return True
        else:
            logger.error("Failed to connect to OMERO server at %s:%s", host, port)
            return False
    except Exception as e:
        logger.error("Exception during OMERO connection test: %s", e, exc_info=True)
        return False

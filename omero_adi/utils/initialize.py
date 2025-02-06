# initialize.py

import os
import sys
import yaml
import json
import logging
from pathlib import Path
from utils.ingest_tracker import initialize_ingest_tracker  # Ensure this import if needed elsewhere

def load_settings(file_path):
    """
    Load settings from either a YAML or JSON file.
    """
    logger = logging.getLogger(__name__)
    try:
        file_path = Path(file_path)  # Convert to Path object
        if not file_path.exists():
            logger.error(f"Settings file not found: {file_path}")
            raise FileNotFoundError(f"Settings file not found: {file_path}")

        logger.debug(f"Loading settings from {file_path}")
        with file_path.open('r') as file:
            if file_path.suffix in ['.yml', '.yaml']:
                settings = yaml.safe_load(file)
                logger.debug("Successfully loaded YAML settings")
                return settings
            elif file_path.suffix == '.json':
                settings = json.load(file)
                logger.debug("Successfully loaded JSON settings")
                return settings
            else:
                logger.error(f"Unsupported file format: {file_path}")
                raise ValueError(f"Unsupported file format: {file_path}")
    except Exception as e:
        logger.error(f"Failed to load settings from {file_path}: {str(e)}")
        raise

def initialize_system(config):
    """
    Performs initial system setup.
    In the updated database-driven mode, group checks are no longer performed.
    Errors are logged but do not cause the container to exit.
    """
    logger = logging.getLogger(__name__)
    try:
        logger.info("Starting system initialization (database-driven mode)...")
        logger.debug("Initializing ingest tracking database...")
        initialize_ingest_tracker(config)
        logger.info("System initialization completed successfully.")
    except Exception as e:
        logger.error(f"System initialization failed: {str(e)}", exc_info=True)

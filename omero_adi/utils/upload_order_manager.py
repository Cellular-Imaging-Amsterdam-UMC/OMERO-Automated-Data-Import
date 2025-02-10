# upload_order_manager.py

import json
import logging
from pathlib import Path

from .ingest_tracker import log_ingestion_step, STAGE_MOVED_COMPLETED, STAGE_MOVED_FAILED

class UploadOrderManager:
    def __init__(self, order_record, settings):
        """
        Initialize the UploadOrderManager with the given order record and settings.
        Now relying on group information provided in the order_record.
        """
        self.logger = logging.getLogger(__name__)
        self.logger.debug(f"Initializing UploadOrderManager for order with UUID: {order_record.get('UUID')}")
        self.settings = settings
        self.order_info = order_record
        self.validate_order_attributes()


    @classmethod
    def from_order_record(cls, order_record, settings):
        """
        Class method to instantiate an UploadOrderManager from a database order record.
        """
        return cls(order_record, settings)

    def validate_order_attributes(self):
        """
        Validate the attributes of the upload order.
        Now checks for 'Group' and 'GroupID' in the order_info.
        Raises a ValueError if any required attribute is missing.
        """
        # Define required attributes based on the new system.
        required_attributes = ['Group', 'GroupID', 'Username', 'UUID']
        missing_attributes = [attr for attr in required_attributes if attr not in self.order_info]

        if missing_attributes:
            error_message = f"Missing required attributes in upload order: {', '.join(missing_attributes)}"
            self.logger.error(error_message)
            raise ValueError(error_message)

        self.logger.info("All required attributes are present in the upload order.")


    def switch_path_prefix(self):
        """
        Switch the '/divg' prefix with '/data' for each file path in the 'Files' list.
        This method updates the file paths in the order_info dictionary.
        """
        if 'Files' in self.order_info:
            updated_files = []
            for file_path in self.order_info['Files']:
                parts = Path(file_path).parts
                if len(parts) > 1 and parts[1].lower() == 'divg':
                    new_path = Path('/data', *parts[2:])
                    updated_files.append(str(new_path))
                    self.logger.debug(f"Switched 'divg' to 'data' in path: {file_path} -> {new_path}")
                else:
                    updated_files.append(file_path)
            self.order_info['Files'] = updated_files
            self.logger.debug(f"Updated {len(updated_files)} file paths after switching 'divg' to 'data'.")

    def get_order_info(self):
        """Return the order information."""
        return self.order_info


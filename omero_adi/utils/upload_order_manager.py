import logging
from pathlib import Path


class UploadOrderManager:
    def __init__(self, order_record, settings):
        """
        Initialize the UploadOrderManager with the given order record and settings.
        Relies on group information provided in the order_record.
        """
        self.logger = logging.getLogger(__name__)
        self.logger.debug(
            f"Initializing UploadOrderManager for order with UUID: {order_record.get('UUID')}")
        self.settings = settings
        self.order_info = order_record
        self._create_file_names_list()
        self.validate_order_attributes()

    @classmethod
    def from_order_record(cls, order_record, settings):
        """
        Instantiate an UploadOrderManager from a database order record.
        """
        return cls(order_record, settings)

    def validate_order_attributes(self):
        """
        Validate the attributes of the upload order.
        Now checks for 'Group', 'Username', 'UUID', 'DestinationID', and 'DestinationType'.
        Raises a ValueError if any required attribute is missing, if 'DestinationType' is invalid,
        or if 'DestinationID' is not a valid integer.
        """
        required_attributes = ['Group', 'Username',
                               'UUID', 'DestinationID', 'DestinationType']
        missing_attributes = [
            attr for attr in required_attributes if attr not in self.order_info]

        if missing_attributes:
            error_message = f"Missing required attributes in upload order: {', '.join(missing_attributes)}"
            self.logger.error(error_message)
            raise ValueError(error_message)

        # Validate 'DestinationType'
        valid_destination_types = {'Dataset', 'Screen'}
        if self.order_info.get('DestinationType') not in valid_destination_types:
            error_message = (
                f"Invalid 'DestinationType' value: {self.order_info.get('DestinationType')}. "
                f"Must be one of: {', '.join(valid_destination_types)}"
            )
            self.logger.error(error_message)
            raise ValueError(error_message)

        # Validate and force 'DestinationID' to be an int
        try:
            self.order_info['DestinationID'] = int(
                self.order_info['DestinationID'])
        except (ValueError, TypeError):
            error_message = f"'DestinationID' must be a valid integer, got: {self.order_info['DestinationID']}"
            self.logger.error(error_message)
            raise ValueError(error_message)

        self.logger.info(
            "All required attributes are present in the upload order, 'DestinationType' is valid, and 'DestinationID' is an integer.")

    def _format_file_names(self, file_names):
        """Format the list of file names to show the first and last items with ellipsis."""
        if len(file_names) > 2:
            # List with first, ellipsis, last
            return [file_names[0], '...', file_names[-1]]
        return file_names  # Return the original list for 1 or 2 items

    def _create_file_names_list(self):
        """
        Create a list of file names from the 'Files' attribute and add it as 'file_names' to order_info.
        """
        if 'Files' in self.order_info:
            file_names = [
                Path(file_path).name for file_path in self.order_info['Files']]
            self.order_info['FileNames'] = self._format_file_names(file_names)
            self.logger.debug(
                f"Created file_names list with {len(self.order_info['FileNames'])} entries")
        else:
            self.order_info['FileNames'] = []
            self.logger.warning(
                "No 'Files' attribute found in order_info. Created empty file_names list.")

    def switch_path_prefix(self):
        """
        Switch the '/divg' prefix with '/data' for each file path in the 'Files' list.
        This method updates the file paths in the order_info dictionary.
        """
        if 'Files' in self.order_info:
            updated_files = []
            for file_path in self.order_info['Files']:
                parts = Path(file_path).parts
                # TODO: remove or generalize this "divg" stuff.
                # We will move to a different folder structure ourselves and other
                # institutions def won't have divg anywhere. Does this need to be
                # in config? Can we grab it from somewhere already?
                if len(parts) > 1 and parts[1].lower() == 'divg':
                    new_path = Path('/data', *parts[2:])
                    updated_files.append(str(new_path))
                    self.logger.debug(
                        f"Switched 'divg' to 'data' in path: {file_path} -> {new_path}")
                else:
                    updated_files.append(file_path)
            self.order_info['Files'] = updated_files
            self.logger.debug(
                f"Updated {len(updated_files)} file paths after switching 'divg' to 'data'.")

    def get_order_info(self):
        """Return the order information."""
        return self.order_info

import shutil
from pathlib import Path
from utils.logger import setup_logger

class UploadFailureHandler:
    def __init__(self, config):
        self.logger = setup_logger(__name__, config['log_file_path'])
        self.failed_uploads_directory = config['failed_uploads_directory']

    def move_failed_uploads(self, failed_uploads, user):
        """
        Moves files listed in the failed_uploads object to the specified directory,
        creating a user-specific directory if it doesn't exist.
        """
        user_specific_directory = Path(self.failed_uploads_directory) / user  # Adjusted to use user name
        try:
            user_specific_directory.mkdir(parents=True, exist_ok=True)  # Ensure the directory exists
        except Exception as e:
            self.logger.error(f"Failed to create directory {user_specific_directory}: {e}")
            return  # Early exit if directory creation fails

        for failed_upload in failed_uploads:
            file_path, _, _, _, _ = failed_upload  # Assuming failed_upload structure
            destination_directory = user_specific_directory / Path(file_path).name

            try:
                shutil.move(str(file_path), str(destination_directory))
                self.logger.info(f"Moved failed upload {file_path} to {destination_directory}")
            except Exception as e:
                self.logger.error(f"Error moving file {file_path} to {destination_directory}: {e}")
                
    def move_entire_data_package(self, data_package, current_directory):
        """
        Moves the entire data package from the current directory to the failed uploads directory,
        directly under the user-specific directory without creating a project-named directory.
        
        :param data_package: The data package object containing details about the package.
        :param current_directory: The current directory where the data package resides.
        """
        # Use current_directory directly, assuming it includes the project name
        source_directory = Path(current_directory)
        # Construct the destination directory path to include only the user directory
        destination_directory = Path(self.failed_uploads_directory) / data_package.user

        try:
            destination_directory.mkdir(parents=True, exist_ok=True)  # Ensure the user directory exists
            # Move the entire source directory (which includes the project) to the user's directory
            shutil.move(str(source_directory), str(destination_directory))
            self.logger.info(f"Moved entire data package from {source_directory} to {destination_directory}")
        except Exception as e:
            self.logger.error(f"Error moving data package from {source_directory} to {destination_directory}: {e}")

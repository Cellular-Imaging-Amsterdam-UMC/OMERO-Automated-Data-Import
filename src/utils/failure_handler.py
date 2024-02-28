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
        user_specific_directory.mkdir(parents=True, exist_ok=True)  # Ensure the directory exists

        for failed_upload in failed_uploads:
            file_path, _, _, _, _ = failed_upload  # Assuming failed_upload structure
            destination_directory = user_specific_directory / Path(file_path).name

            try:
                shutil.move(str(file_path), str(destination_directory))
                self.logger.info(f"Moved failed upload {file_path} to {destination_directory}")
            except Exception as e:
                self.logger.error(f"Error moving file {file_path} to {destination_directory}: {e}")

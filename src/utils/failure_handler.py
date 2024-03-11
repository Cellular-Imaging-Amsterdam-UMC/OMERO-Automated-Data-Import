import shutil
from pathlib import Path
from utils.logger import setup_logger

class UploadFailureHandler:
    def __init__(self, config):
        self.logger = setup_logger(__name__, config['log_file_path'])
        self.base_dir = config['base_dir']  # Store base directory from config
        
    def move_failed_uploads(self, failed_uploads, user, group, config):
        """
        Adjusted to include group in the path construction.
        """
        failed_uploads_directory = Path(self.base_dir) / group / config['failed_uploads_directory_name'] / user
        try:
            failed_uploads_directory.mkdir(parents=True, exist_ok=True)
        except Exception as e:
            self.logger.error(f"Failed to create directory {failed_uploads_directory}: {e}")
            return

        for failed_upload in failed_uploads:
            file_path, _, _, _, _ = failed_upload
            destination_directory = failed_uploads_directory / Path(file_path).name
            try:
                shutil.move(str(file_path), str(destination_directory))
                self.logger.info(f"Moved failed upload {file_path} to {destination_directory}")
            except Exception as e:
                self.logger.error(f"Error moving file {file_path} to {destination_directory}: {e}")

    def move_entire_data_package(self, data_package, current_directory, config):
        """
        Adjusted to include group in the path construction.
        """
        failed_uploads_directory = Path(self.base_dir) / data_package.group / config['failed_uploads_directory_name'] / data_package.user
        try:
            failed_uploads_directory.mkdir(parents=True, exist_ok=True)
            shutil.move(str(Path(current_directory)), str(failed_uploads_directory))
            self.logger.info(f"Moved entire data package from {current_directory} to {failed_uploads_directory}")
        except Exception as e:
            self.logger.error(f"Error moving data package from {current_directory} to {failed_uploads_directory}: {e}")

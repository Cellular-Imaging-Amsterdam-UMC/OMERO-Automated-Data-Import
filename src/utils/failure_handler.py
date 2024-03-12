import shutil
from pathlib import Path
from utils.logger import setup_logger

class UploadFailureHandler:
    def __init__(self, config):
        self.logger = setup_logger(__name__, config['log_file_path'])
        self.base_dir = config['base_dir']  # Store base directory from config
        
    def move_failed_uploads(self, failed_uploads, username, group, config):
        """
        Adjusted to include group in the path construction.
        """
        failed_uploads_directory = Path(self.base_dir) / group / config['failed_uploads_directory_name'] / username
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

    def move_upload_order(self, dataset_name, username, group, config):
        """
        Moves the upload order file to the .failed_uploads directory, taking into account the .txt extension.
        """
        # Correctly append the .txt extension to the dataset name to form the filename
        source_file_name = f"{dataset_name}.txt"
        source_file = Path(config['base_dir']) / group / config['upload_orders_dir_name'] / source_file_name
        destination_directory = Path(config['base_dir']) / group / config['failed_uploads_directory_name'] / username

        # Ensure the destination directory exists
        try:
            destination_directory.mkdir(parents=True, exist_ok=True)
        except Exception as e:
            self.logger.error(f"Failed to create directory {destination_directory}: {e}")
            return

        # Move the file
        destination_file = destination_directory / source_file_name
        try:
            shutil.move(str(source_file), str(destination_file))
            self.logger.info(f"Moved upload order file {source_file} to {destination_file}")
        except Exception as e:
            self.logger.error(f"Error moving upload order file {source_file} to {destination_file}: {e}")

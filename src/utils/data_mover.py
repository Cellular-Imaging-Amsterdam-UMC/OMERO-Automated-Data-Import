#data_mover.py

from pathlib import Path
import time
import hashlib
import shutil
import datetime
from utils.logger import setup_logger

class DataPackageMover:
    def __init__(self, data_package, config):
        self.data_package = data_package
        self.config = config
        self.logger = setup_logger(__name__, self.config['log_file_path'])
    
    def _wrap_data_package(self, path):
        # Check if the path as provided exists (likely a directory)
        if path.exists():
            return path
        else:
            # Path does not exist as provided, attempt to find a matching file
            possible_files = list(path.parent.glob(path.name + ".*"))
            if possible_files:
                # Assuming only one file matches, adjust as necessary
                file_path = possible_files[0]
                folder_name = file_path.stem  # File name without the extension
                folder_path = file_path.parent / folder_name
                folder_path.mkdir(exist_ok=True)
    
                # Move the file into the newly created folder
                new_file_path = folder_path / file_path.name
                file_path.rename(new_file_path)
                self.logger.info(f"Moved file into new folder: {folder_path}")
                return folder_path
            else:
                # No matching file found, log an error or handle as needed
                self.logger.error(f"No file found matching: {path}")
                return None

    def move_data_package(self):
        self.logger.info(f"Starting move for {self.data_package.project}")
        source_path = self.data_package.landing_dir_base_path

        # Check and wrap the data package if it's a file
        source_path = self._wrap_data_package(source_path)

        if not self._verify_source_path(source_path):
            return False

        if not self._wait_until_stable(source_path):
            self.logger.warning("Data package size is not stabilizing.")
            return False

        hidden_path = self._hide_data_package(source_path)
        if not hidden_path:
            return False

        dest_path = self._define_destination_path(hidden_path)
        if not self._copy_data_package(hidden_path, dest_path):
            return False

        if not self._verify_data_integrity(hidden_path, dest_path):
            return False

        if self._delete_original_data_package(hidden_path):
            self.logger.info(f"Data package {self.data_package.project} moved successfully.")
            return True
        else:
            return False

    def _verify_source_path(self, path):
        if not path.exists():
            self.logger.error(f"Source path does not exist: {path}")
            return False
        return True

    def _wait_until_stable(self, path, interval=2):
        """Wait indefinitely for the data package size to stop growing, logging the size in GB at the first interval and then every 10 intervals."""
        last_size = 0
        interval_counter = 0  # Initialize a counter to track the intervals

        while True:
            current_size = self._calculate_size(path)
            size_in_gb = current_size / (1024**3)  # Convert bytes to gigabytes

            # Log the size at the first interval and then every 10 intervals
            if interval_counter == 0 or interval_counter % 10 == 0:
                self.logger.info(f"Current data package size: {size_in_gb:.3f} GB")

            if current_size == last_size:
                # Ensure the final size is logged if it hasn't been already
                if interval_counter % 10 != 0:
                    self.logger.info(f"Final data package size: {size_in_gb:.3f} GB")
                self.logger.info("Data package size stabilized.")
                return True

            last_size = current_size
            interval_counter += 1  # Increment the counter at each interval
            time.sleep(interval)

    def _calculate_size(self, path):
        return sum(f.stat().st_size for f in path.rglob('*') if f.is_file())

    def _hide_data_package(self, path):
        try:
            hidden_path = path.parent / ('.' + path.name)
            path.rename(hidden_path)
            self.logger.info(f"Data package hidden at: {hidden_path}")
            self.data_package.hidden_path = hidden_path  # Update the hidden_path attribute
            return hidden_path
        except Exception as e:
            self.logger.error(f"Failed to hide data package: {e}")
            return None

    def _define_destination_path(self, hidden_path):
        return Path(self.config["staging_dir_path"]) / self.data_package.group / self.data_package.user / hidden_path.name.strip('.')

    def _copy_data_package(self, source_path, dest_path):
        try:
            shutil.copytree(source_path, dest_path)
            self.logger.info(f"Data package copied to staging at: {dest_path}")
            return True
        except Exception as e:
            self.logger.error(f"Error during data package copy: {e}")
            return False

    def _verify_data_integrity(self, source_path, dest_path):
        source_hash = self._calculate_hash(source_path)
        dest_hash = self._calculate_hash(dest_path)
        if source_hash == dest_hash:
            self.logger.info("Data integrity check passed.")
            return True
        else:
            self.logger.error("Data integrity check failed.")
            return False

    def _calculate_hash(self, path):
        hash_md5 = hashlib.md5()
        for item in sorted(path.rglob('*')):
            if item.is_file():
                with open(item, "rb") as f:
                    for chunk in iter(lambda: f.read(4096), b""):
                        hash_md5.update(chunk)
        return hash_md5.hexdigest()

    def _delete_original_data_package(self, path):
        try:
            shutil.rmtree(path)
            self.logger.info(f"Successfully deleted original data package at: {path}")
            return True
        except Exception as e:
            self.logger.error(f"Failed to delete the original data package at {path}: {e}")
            return False
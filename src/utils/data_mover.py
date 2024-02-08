#data_mover.py

from pathlib import Path
import time
import hashlib
import shutil
from utils.logger import setup_logger

class DataPackageMover:
    def __init__(self, data_package, config):
        self.data_package = data_package
        self.config = config
        self.logger = setup_logger(__name__, self.config['log_file_path'])

    def move_data_package(self):
        self.logger.info(f"Starting move for {self.data_package.project}")
        source_path = self.data_package.landing_path

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

    def _wait_until_stable(self, path, attempts=10, interval=2):
        """Wait for the data package size to stabilize."""
        last_size = -1
        for _ in range(attempts):
            current_size = self._calculate_size(path)
            if current_size == last_size:
                return True
            last_size = current_size
            time.sleep(interval)
        return False

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
#data_mover.py

from pathlib import Path
import time
import hashlib
import shutil
from utils.logger import setup_logger

class MoveDataPackage:
    def __init__(self, data_package, config):
        self.data_package = data_package
        self.config = config
        self.logger = setup_logger(__name__, self.config['log_file_path'])
        self.move_result = self.move_datapackage()

    def calculate_datapackage_size(self, path):
        total_size = 0
        path = Path(path)
        for f in path.glob('**/*'):
            if f.is_file():
                total_size += f.stat().st_size
        return total_size

    def has_datapackage_stabilized(self, path):
        interval = self.config["monitor_interval"]
        stable_duration = self.config["stable_duration"]

        last_size = -1
        stable_time = 0

        while stable_time < stable_duration:
            current_size = self.calculate_datapackage_size(path)
            if current_size == last_size:
                stable_time += interval
            else:
                stable_time = 0
            last_size = current_size
            time.sleep(interval)

        return True

    def hide_datapackage(self, path):
        self.logger.info(f"Attempting to hide data package at: {path}")
        path = Path(path)
        if not path.exists():
            self.logger.error(f"Path does not exist: {path}")
            return None
        parent_dir = path.parent
        hidden_path = parent_dir / ('.' + path.name)
        try:
            path.rename(hidden_path)
            self.logger.info(f"Data package successfully hidden: {hidden_path}")
        except Exception as e:
            self.logger.error(f"Failed to hide data package from {path} to {hidden_path}: {e}")
            return None
        return str(hidden_path)

    def hash_datapackage(self, path):
        hash_algo = hashlib.md5()
        path = Path(path)

        for file_path in sorted(path.glob('**/*')):
            if file_path.is_file():
                with open(file_path, "rb") as f:
                    for chunk in iter(lambda: f.read(4096), b""):
                        hash_algo.update(chunk)

        return hash_algo.hexdigest()

    def copy_to_staging(self, src, dest):
        try:
            shutil.copytree(src, dest)  # Use dest directly without hiding it again
            self.logger.info(f"Successfully copied to staging at: {dest}")
        except Exception as e:
            self.logger.error(f"Error copying from {src} to {dest}: {str(e)}")
            return False
        return True

    def move_datapackage(self):
        self.logger.info(f"Starting move_datapackage for project: {self.data_package.project}")
        src_path = self.data_package.original_path
        self.logger.info(f"Original path: {src_path}")
        hidden_src_path = self.hide_datapackage(src_path)
        if hidden_src_path is None:
            self.logger.error("Aborting move_datapackage due to failure in hiding data package.")
            return False, None
        self.data_package.hidden_path = Path(hidden_src_path)
    
        self.logger.info(f"Triggered move_datapackage for project at: {hidden_src_path}")
    
        # Adjust the destination path to include group and user
        dest_path = Path(self.config["staging_dir_path"]) / self.data_package.group / self.data_package.user / Path(hidden_src_path).name
    
        if self.has_datapackage_stabilized(hidden_src_path):
            original_hash = self.hash_datapackage(hidden_src_path)
            if not self.copy_to_staging(Path(hidden_src_path), dest_path):
                self.logger.error(f"Failed to copy project from {hidden_src_path} to {dest_path}")
                return False, None
            copied_hash = self.hash_datapackage(dest_path)
    
            if original_hash == copied_hash:
                self.logger.info(f"Project {self.data_package.project} successfully moved to: {dest_path}")
                return True, Path(hidden_src_path)
            else:
                self.logger.error(f"Data integrity check failed for project {self.data_package.project}.")
                return False, None
        else:
            self.logger.warning(f"Project {self.data_package.project} is still changing and cannot be moved yet.")
            return False, None
    
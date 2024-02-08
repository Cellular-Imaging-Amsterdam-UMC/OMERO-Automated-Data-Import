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
    
    def delete_original_datapackage(self, path):
        try:
            shutil.rmtree(path)
            self.logger.info(f"Successfully deleted original data package at: {path}")
        except Exception as e:
            self.logger.error(f"Failed to delete original data package at {path}: {e}")
            return False
        return True

    def move_datapackage(self):
        self.logger.info(f"Starting move_datapackage for project: {self.data_package.project}")
        src_path = Path(self.data_package.original_path)
        self.logger.info(f"Original path: {src_path}")
    
        # Ensure the source path exists before proceeding
        if not src_path.exists():
            self.logger.error(f"Source path does not exist: {src_path}")
            return False, None
    
        # Step 1: Check if the data package has stabilized
        if not self.has_datapackage_stabilized(src_path):
            self.logger.warning(f"Data package at {src_path} is still changing. Cannot proceed with move.")
            return False, None
    
        # Step 2: Hide the data package
        hidden_src_path = self.hide_datapackage(src_path)
        if hidden_src_path is None:
            self.logger.error("Aborting move_datapackage due to failure in hiding data package.")
            return False, None
        hidden_src_path = Path(hidden_src_path)  # Ensure hidden_src_path is a Path object
    
        # Step 3: Move hidden data package to staging directory
        dest_path = Path(self.config["staging_dir_path"]) / self.data_package.group / self.data_package.user / hidden_src_path.name
        if not self.copy_to_staging(hidden_src_path, dest_path):
            self.logger.error(f"Failed to copy data package from {hidden_src_path} to {dest_path}")
            return False, None
    
        # Verify data integrity after copy
        original_hash = self.hash_datapackage(hidden_src_path)
        copied_hash = self.hash_datapackage(dest_path)
        if original_hash != copied_hash:
            self.logger.error("Data integrity check failed after copying to staging directory.")
            # Optional: Implement a rollback mechanism here if needed
            return False, None
    
        # Step 4: Remove the original data package
        if not self.delete_original_datapackage(hidden_src_path):
            self.logger.error(f"Failed to delete the original data package at {hidden_src_path}")
            # Optional: Decide if you need to rollback the copied data in staging in this case
            return False, None
    
        self.logger.info(f"Data package for project {self.data_package.project} successfully moved to staging: {dest_path}")
        return True, dest_path
        
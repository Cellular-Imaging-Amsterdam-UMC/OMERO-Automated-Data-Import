#data_mover.py

from pathlib import Path
import time
import hashlib
import shutil
import logging

logger = logging.getLogger(__name__)

def init_logger(log_file_path):
    logging.basicConfig(level=logging.INFO, filename=log_file_path, filemode='a',
                        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')

def calculate_datapackage_size(path):
    total_size = 0
    path = Path(path)
    for f in path.glob('**/*'):
        if f.is_file():
            total_size += f.stat().st_size
    return total_size

def has_datapackage_stabilized(path, config):
    interval = config["monitor_interval"]
    stable_duration = config["stable_duration"]

    last_size = -1
    stable_time = 0

    while stable_time < stable_duration:
        current_size = calculate_datapackage_size(path)
        if current_size == last_size:
            stable_time += interval
        else:
            stable_time = 0
        last_size = current_size
        time.sleep(interval)

    return True

def hide_datapackage(path):
    path = Path(path)
    parent_dir = path.parent
    hidden_path = parent_dir / ('.' + path.name)
    path.rename(hidden_path)
    return str(hidden_path)

def hash_datapackage(path):
    hash_algo = hashlib.md5()  # Changed from hashlib.sha256()
    path = Path(path)

    for file_path in sorted(path.glob('**/*')):
        if file_path.is_file():
            with open(file_path, "rb") as f:
                for chunk in iter(lambda: f.read(4096), b""):
                    hash_algo.update(chunk)

    return hash_algo.hexdigest()

def copy_to_staging(src, dest):
    try:
        shutil.copytree(src, dest)
    except Exception as e:
        logger.error(f"Error copying from {src} to {dest}: {str(e)}")
        return False
    return True

def verify_datapackage_integrity(src, dest):
    return hash_datapackage(src) == hash_datapackage(dest)

def move_datapackage(data_package, config):
    # Initialize the logger
    init_logger(config['log_file_path'])

    # Construct the paths for the source and destination directories using pathlib
    src_path = Path(config["landing_dir_base_path"]) / data_package.path
    dest_path = Path(config["staging_dir_path"]) / data_package.path

    logger.info(f"Triggered move_datapackage for project at: {src_path}")

    if has_datapackage_stabilized(str(src_path), config):
        hidden_path = hide_datapackage(str(src_path))
        original_hash = hash_datapackage(hidden_path)
        if not copy_to_staging(hidden_path, str(dest_path)):
            logger.error(f"Failed to copy project from {hidden_path} to {dest_path}")
            return False
        copied_hash = hash_datapackage(str(dest_path))
        
        if original_hash == copied_hash:
            logger.info(f"Project {data_package.project} successfully moved to: {dest_path}")
            return True
        else:
            logger.error(f"Data integrity check failed for project {data_package.project}.")
            return False
    else:
        logger.warning(f"Project {data_package.project} is still changing and cannot be moved yet.")
        return False
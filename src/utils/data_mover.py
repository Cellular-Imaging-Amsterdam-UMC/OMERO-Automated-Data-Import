#data_mover.py

from pathlib import Path
import time
import hashlib
import shutil
import logging

def calculate_directory_size(path):
    total_size = 0
    path = Path(path)
    for f in path.glob('**/*'):
        if f.is_file():
            total_size += f.stat().st_size
    return total_size

def has_dataset_stabilized(path, config):
    interval = config["monitor_interval"]
    stable_duration = config["stable_duration"]

    last_size = -1
    stable_time = 0

    while stable_time < stable_duration:
        current_size = calculate_directory_size(path)
        if current_size == last_size:
            stable_time += interval
        else:
            stable_time = 0
        last_size = current_size
        time.sleep(interval)

    return True

def hide_dataset(path):
    path = Path(path)
    parent_dir = path.parent
    hidden_path = parent_dir / ('.' + path.name)
    path.rename(hidden_path)
    return str(hidden_path)

def hash_directory(path):
    hash_algo = hashlib.sha256()
    path = Path(path)

    for file_path in sorted(path.glob('**/*')):
        if file_path.is_file():
            with open(file_path, "rb") as f:
                for chunk in iter(lambda: f.read(4096), b""):
                    hash_algo.update(chunk)

    return hash_algo.hexdigest()

def copy_to_staging(src, dest):
    shutil.copytree(src, dest)

def verify_data_integrity(src, dest):
    return hash_directory(src) == hash_directory(dest)

def move_dataset(data_package, config):
    # Set up logging
    logging.basicConfig(level=logging.INFO, filename=config['log_file_path'], filemode='a',
                        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    logger = logging.getLogger(__name__)

    # Construct the paths for the source and destination directories using pathlib
    src_path = Path(config["landing_dir_base_path"]) / data_package.path
    dest_path = Path(config["staging_dir_path"]) / data_package.path

    logger.info(f"Triggered move_dataset for dataset at: {src_path}")

    if has_dataset_stabilized(str(src_path), config):
        hidden_path = hide_dataset(str(src_path))
        original_hash = hash_directory(hidden_path)
        copy_to_staging(hidden_path, str(dest_path))
        copied_hash = hash_directory(str(dest_path))
        
        if original_hash == copied_hash:
            logger.info(f"Dataset {data_package.dataset} successfully moved to: {dest_path}")
            return True
        else:
            logger.error(f"Data integrity check failed for dataset {data_package.dataset}.")
            return False
    else:
        logger.warning(f"Dataset {data_package.dataset} is still changing and cannot be moved yet.")
        return False
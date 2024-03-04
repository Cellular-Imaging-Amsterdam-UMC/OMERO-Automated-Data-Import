"""
Intakes the folder where the new dataset has been dropped (group - user).
Identifies the destined user and group where to upload a dataset.
Investigates the source of the dataset (what type of files and in what structure are they found)
Based on the aforementioned information follows simple heuristics to defin:
    - What project and dataset folders should be created and in which of these each file must go
    - What preprocessing steps must be performed on the new dataset.
"""
from pathlib import Path
import glob
from utils.logger import setup_logger
from typing import Dict, List
from datetime import datetime  # Import the datetime module

class DataPackageStager:
    def __init__(self, config):
        self.config = config
        self.logger = setup_logger(__name__, self.config['log_file_path'])

    def identify_datasets(self, data_package) -> Dict[str, List[str]]:
        datasets = {}
        data_package_path = Path(data_package.staging_path)

        self.logger.info(f"Typifying data package at: {data_package_path}")

        # List all files and directories recursively
        files_and_dirs = glob.glob(f"{data_package_path}/**", recursive=True)

        # Get current date in YY-MM-DD format
        current_date = datetime.now().strftime("%y-%m-%d")

        # Include the top-level project directory if it contains files
        if any(data_package_path.glob('*.*')):
            datasets[current_date] = [str(f) for f in data_package_path.glob('*.*')]  # Use current_date instead of "."

        for item in files_and_dirs:
            item_path = Path(item)
            if item_path.is_dir():
                # Check if the directory contains files (not subdirectories)
                if any(item_path.glob('*.*')):
                    dataset_path = item_path.relative_to(data_package_path)
                    # If the dataset_path is '.', replace it with current_date
                    dataset_name = str(dataset_path) if str(dataset_path) != "." else current_date
                    datasets[dataset_name] = [str(f) for f in item_path.glob('*.*')]

        self.logger.info(f"Identified datasets: {list(datasets.keys())}")

        return datasets
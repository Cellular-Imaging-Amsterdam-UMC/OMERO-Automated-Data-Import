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

class DataPackageStager:
    def __init__(self, config):
        self.config = config
        self.logger = setup_logger(__name__, self.config['log_file_path'])

    def identify_datasets(self, data_package):
        datasets = {}
        data_package_path = data_package.hidden_path if data_package.hidden_path else data_package.original_path

        self.logger.info(f"Typifying data package at: {data_package_path}")

        files = glob.glob(f"{data_package_path}/**/*", recursive=True)

        for file in files:
            file_path = Path(file)
            if file_path.is_dir():
                continue

            dataset_path = file_path.parent.relative_to(data_package_path)
            datasets.setdefault(str(dataset_path), []).append(str(file_path))

        self.logger.info(f"Identified datasets: {datasets.keys()}")

        return datasets
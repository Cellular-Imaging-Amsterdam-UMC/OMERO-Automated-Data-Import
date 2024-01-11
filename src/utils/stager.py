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

def typify_data_package(data_package, config):
    """
    Identifies the datasets in the data package. A dataset is defined as any folder that contains at least one file.
    Returns a dictionary where the keys are the dataset names and the values are lists of relative paths to the files in the dataset.
    """
    datasets = {}
    data_package_path = Path(data_package)

    # Find all files in the data package path
    files = glob.glob(f"{data_package_path}/**/*", recursive=True)

    # Determine the datasets based on the directories of the files
    for file in files:
        file_path = Path(file)
        # Skip if it's a directory
        if file_path.is_dir():
            continue

        # Get the relative path of the dataset from the data package path
        dataset_path = file_path.parent.relative_to(data_package_path)

        # Add the relative file path to the dataset in the dictionary
        datasets.setdefault(str(dataset_path), []).append(str(file_path.relative_to(data_package_path)))

    return datasets
"""
Intakes the folder where the new dataset has been dropped (group - user).
Identifies the destined user and group where to upload a dataset.
Investigates the source of the dataset (what type of files and in what structure are they found)
Based on the aforementioned information follows simple heuristics to defin:
    - What project and dataset folders should be created and in which of these each file must go
    - What preprocessing steps must be performed on the new dataset.
"""
def data_package_typification(data_package, config):
    pass
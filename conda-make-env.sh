#!/bin/bash

# Make this shell script executable
# chmod +x conda-make-env.sh


# Create the conda environment
conda create -n auto-import-env python=3.10 -y

# Activate the environment
conda activate auto-import-env 

# Install omero-py from conda-forge
conda install -c conda-forge omero-py -y

# Install bftools from bioconda
conda install -c bioconda bftools -y

# Install other packages via pip in the same environment
#tested on: pip install watchdog==4.0.0 ezomero==2.1.0 pandas==2.2.0 numpy==1.26.4 openpyxl==3.0.9
pip install watchdog ezomero>=0.3.1 pandas==1.1.5 numpy==1.19.5 openpyxl==3.0.9 python-dotenv==1.0.1

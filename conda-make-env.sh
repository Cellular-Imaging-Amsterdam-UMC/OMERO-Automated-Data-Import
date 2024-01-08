#!/bin/bash

# Make this shell script executable
# chmod +x conda-make-env.sh


# Create the conda environment with the name 'abu-env'
conda create -n abu-env python=3.10 -y

# Activate the environment
conda activate abu-env

# Install omero-py from conda-forge
conda install -c conda-forge omero-py -y

# Install bftools from bioconda
conda install -c bioconda bftools -y

# Install other packages via pip in the same environment
pip install watchdog ezomero>=0.3.1 pandas==1.1.5 numpy==1.19.5 openpyxl==3.0.9

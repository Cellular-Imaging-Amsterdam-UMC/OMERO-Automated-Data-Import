# Start from the Miniconda base image to use Conda
FROM continuumio/miniconda3:4.10.3

# Set the working directory in the container
WORKDIR /app

# Create a Conda environment
# Note: `conda` commands run in Docker must use the `bash -c` prefix to ensure they execute correctly
RUN conda create -n auto-import-env python=3.10 -y

# Install omero-py and bftools
RUN conda install -n auto-import-env -c conda-forge omero-py -y && \
    conda install -n auto-import-env -c bioconda bftools -y

# Make RUN commands use the new environment:
SHELL ["conda", "run", "-n", "auto-import-env", "/bin/bash", "-c"]

# Install other required packages via pip in the same environment
RUN pip install ezomero>=2.1.0 pandas==2.2.1 numpy==1.26.4 openpyxl==3.1.2 python-dotenv==1.0.1

# Copy the application code and configuration files
COPY . /app
COPY import-system/configs/* /opt/omero/import_system/configs/

# Ensure your application's startup script is copied over and set it as the ENTRYPOINT
ENTRYPOINT ["/conda/bin/conda", "run", "--no-capture-output", "-n", "auto-import-env", "python", "/app/src/main.py"]

# Notes for creating a non-root user, omitted for brevity but follow the previous example if needed

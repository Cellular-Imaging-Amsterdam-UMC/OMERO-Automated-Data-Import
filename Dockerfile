# Start from the Miniconda base image to use Conda
FROM continuumio/miniconda3:24.1.2-0

# Set the working directory in the container
WORKDIR /auto-importer

# Create a Conda environment with Python 3.10
RUN conda create -n auto-import-env python=3.10 -y

# Install required packages using Conda
RUN conda install -n auto-import-env -c conda-forge omero-py -y && \
    conda install -n auto-import-env -c bioconda bftools -y && \
    conda install -n auto-import-env -c conda-forge psycopg2 libffi -y && \
    conda install -n auto-import-env -c conda-forge llvm-openmp -y

# Update PATH to use the new Conda environment
ENV PATH=/opt/conda/envs/auto-import-env/bin:$PATH

# Install system prerequisites
RUN apt-get update --fix-missing && apt-get install -y \
    git \
    python3-dev \
    libpq-dev \
    build-essential 

# Create a non-root user with specified UID and GID
RUN groupadd -g 1000 autoimportgroup && \
    useradd -m -r -u 1000 -g autoimportgroup autoimportuser

# Copy application code into the container
COPY . /auto-importer

# Install the Python package in editable mode
RUN pip install -e /auto-importer

# Create logs directory and set proper permissions
RUN mkdir -p /auto-importer/logs && \
    chown -R autoimportuser:autoimportgroup /auto-importer/logs

# Set proper permissions for the auto-importer directory
RUN chown -R autoimportuser:autoimportgroup /auto-importer

# Create a directory for the database and set proper permissions
RUN mkdir -p /auto-importer/db && \
    chown -R autoimportuser:autoimportgroup /auto-importer/db

# Ensure the main script is executable
RUN chmod +x /auto-importer/omero_adi/main.py

# Switch to the non-root user
USER autoimportuser

# Set the default command to run the main application and then keep the container alive.
CMD ["bash", "-c", "python -u /auto-importer/omero_adi/main.py && tail -f /dev/null"]

# Start from the Miniconda base image to use Conda
FROM continuumio/miniconda3:24.1.2-0

# Set the working directory in the container
WORKDIR /auto-importer

# Create a Conda environment
RUN conda create -n auto-import-env python=3.10 -y

# Install omero-py and bftools using Conda
RUN conda install -n auto-import-env -c conda-forge omero-py -y && \
    conda install -n auto-import-env -c bioconda bftools -y

# Activate the environment by setting the path to environment's bin directory
ENV PATH /opt/conda/envs/auto-import-env/bin:$PATH

# Copy the requirements.txt first to leverage Docker cache
COPY requirements.txt /auto-importer/
RUN pip install --no-cache-dir -r requirements.txt

# Copy the auto-importerlication code
COPY src /auto-importer/src

# Copy the configuration files
COPY config /auto-importer/config
COPY .env /auto-importer/
RUN mkdir /auto-importer/logs

# Ensure your application's startup script is executable
RUN chmod +x /auto-importer/src/main.py

# Set the default command or entrypoint to the main script
ENTRYPOINT ["/opt/conda/bin/conda", "run", "-n", "auto-import-env", "python", "src/main.py"]



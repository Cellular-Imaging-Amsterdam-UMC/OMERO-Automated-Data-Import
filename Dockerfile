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

# Copy the auto-importer application code
COPY src /auto-importer/src

# Copy the configuration files
COPY config /auto-importer/config

# Copy the tests directory
COPY tests /auto-importer/tests

# Copy the logs directory
RUN mkdir /auto-importer/logs

# Ensure your application's startup script is executable
RUN chmod +x /auto-importer/src/main.py

# Create a group and user with specified GID and UID
RUN groupadd -g 10000 autoimportgroup && \
    useradd -m -r -u 10000 -g autoimportgroup autoimportuser

# Giving access to the new user
RUN chown -R autoimportuser:autoimportgroup /auto-importer

# Switch to the new user for any RUN, CMD, or ENTRYPOINT instructions that follow
USER autoimportuser

# Set the default command or entrypoint to the main script
ENTRYPOINT ["/opt/conda/bin/conda", "run", "-n", "auto-import-env", "python", "src/main.py"]



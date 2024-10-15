# Start from the Miniconda base image to use Conda
FROM continuumio/miniconda3:24.1.2-0

# Set the working directory in the container
WORKDIR /auto-importer

# Create a Conda environment
RUN conda create -n auto-import-env python=3.10 -y

# Install omero-py, bftools, and psycopg2 using Conda
RUN conda install -n auto-import-env -c conda-forge omero-py -y && \
    conda install -n auto-import-env -c bioconda bftools -y && \
    conda install -n auto-import-env -c conda-forge psycopg2 libffi==3.3 -y

# Activate the environment by setting the path to environment's bin directory
ENV PATH /opt/conda/envs/auto-import-env/bin:$PATH

# Install git
RUN apt-get update && apt-get install -y git

# Install system prerequisites for building PostgreSQL drivers
RUN apt-get update && apt-get install -y \
    python3-dev \
    libpq-dev \
    build-essential

# Clone the specific branch of the repository
ADD "https://api.github.com/repos/Cellular-Imaging-Amsterdam-UMC/OMERO-Automated-Data-Import/commits?sha=postgres-database&per_page=1" latest_commit
RUN git clone -b postgres-database https://github.com/Cellular-Imaging-Amsterdam-UMC/OMERO-Automated-Data-Import.git /auto-importer

# Install the Python dependencies from the repository
RUN pip install /auto-importer

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

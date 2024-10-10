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

# Install git
RUN apt-get update && apt-get install -y git

# Clone the specific branch of the repository
RUN git clone -b database https://github.com/Cellular-Imaging-Amsterdam-UMC/OMERO-Automated-Data-Import.git /auto-importer

# Install psycopg2 system prerequisites for postgres interaction
RUN apt-get update && apt-get install -y \
    python3-dev \
    libpq-dev \
    build-essential
# Update PATH manually based on known locations
ENV PATH="/usr/pgsql-12/bin:/usr/pgsql-14/bin:${PATH}"

# Install toml
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



# Start from the Miniconda base image to use Conda
FROM continuumio/miniconda3:24.1.2-0

# Set the working directory in the container
WORKDIR /auto-importer

# Create a Conda environment
RUN conda create -n auto-import-env python=3.10 -y

# Install omero-py, bftools, psycopg2, and llvm-openmp using Conda
RUN conda install -n auto-import-env -c conda-forge omero-py -y && \
    conda install -n auto-import-env -c bioconda bftools -y && \
    conda install -n auto-import-env -c conda-forge psycopg2 libffi -y && \
    conda install -n auto-import-env -c conda-forge llvm-openmp -y

# Activate the environment by setting the path to the environment's bin directory
ENV PATH=/opt/conda/envs/auto-import-env/bin:$PATH

# Install git and system prerequisites for building PostgreSQL drivers
RUN apt-get update --fix-missing && apt-get install -y \
    git \
    python3-dev \
    libpq-dev \
    build-essential 

# Create a group and user with specified GID and UID
RUN groupadd -g 1000 autoimportgroup && \
    useradd -m -r -u 1000 -g autoimportgroup autoimportuser

# Clone the specific branch of the repository (or copy the local context)
ADD / /auto-importer

# Install the Python dependencies from the repository
RUN pip install /auto-importer

# Make sure logs directory exists and has correct permissions
RUN mkdir -p /auto-importer/logs

# Ensure proper permissions for all relevant directories in the user's auto-importer directory
RUN chown -R autoimportuser:autoimportgroup /auto-importer/logs

# Ensure your application's startup script is executable
RUN chmod +x /auto-importer/omero_adi/main.py

# Create a simplified development entrypoint script in /usr/local/bin instead
RUN echo '#!/bin/bash' > /usr/local/bin/entrypoint.sh && \
    echo 'pip install -e /auto-importer' >> /usr/local/bin/entrypoint.sh && \
    echo 'exec /bin/bash' >> /usr/local/bin/entrypoint.sh && \
    chmod +x /usr/local/bin/entrypoint.sh

# Switch to the new user for all subsequent commands
USER autoimportuser

# Update the entrypoint path
ENTRYPOINT ["/usr/local/bin/entrypoint.sh"]
CMD ["run"]


# Command for dev
# docker run -it \
#   --name auto-importer-dev \
#   -v $(pwd)/src:/auto-importer/auto-importer \
#   -v $(pwd)/logs:/auto-importer/logs \
#   auto-importer:local dev

# Command for 'prod'
# docker run -d --name auto-importer auto-importer:local
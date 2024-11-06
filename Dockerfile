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

# Install git and system prerequisites for building PostgreSQL drivers
RUN apt-get update && apt-get install -y \
    git \
    python3-dev \
    libpq-dev \
    build-essential \
    fuse-overlayfs \
    podman

# Clone the specific branch of the repository
ADD "https://api.github.com/repos/Cellular-Imaging-Amsterdam-UMC/OMERO-Automated-Data-Import/commits?sha=database&per_page=1" /latest_commit
RUN git clone -b database https://github.com/Cellular-Imaging-Amsterdam-UMC/OMERO-Automated-Data-Import.git /auto-importer

# Install the Python dependencies from the repository
RUN pip install /auto-importer

# Make the logs directory
RUN mkdir /auto-importer/logs

# Ensure your application's startup script is executable
RUN chmod +x /auto-importer/src/main.py

# Create a group and user with specified GID and UID
RUN groupadd -g 1000 autoimportgroup && \
    useradd -m -r -u 1000 -g autoimportgroup autoimportuser

# Pre-create necessary directories in the user's home directory
RUN mkdir -p /home/autoimportuser/.local/share/containers/storage /home/autoimportuser/.config/containers

# Add mappings to /etc/subuid and /etc/subgid
RUN echo -e "autoimportuser:1:999\nautoimportuser:1001:64535" > /etc/subuid && \
    echo -e "autoimportuser:1:999\nautoimportuser:1001:64535" > /etc/subgid

# Ensure proper permissions for all relevant directories in the user's home directory
RUN chown -R autoimportuser:autoimportgroup /home/autoimportuser/.local /home/autoimportuser/.config /auto-importer

# Add container configuration files
ADD /containers.conf /etc/containers/containers.conf
ADD /storage.conf /etc/containers/storage.conf
ADD /podman-containers.conf /home/autoimportuser/.config/containers/containers.conf

# Copy & modify the defaults to provide reference if runtime changes needed
RUN sed -e 's|^#mount_program|mount_program|g' \
           -e '/additionalimage.*/a "/var/lib/shared",' \
           -e 's|^mountopt[[:space:]]*=.*$|mountopt = "nodev,fsync=0"|g' \
           /etc/containers/storage.conf \
           > /etc/containers/storage.conf

# Create necessary directories for shared storage and lock files
RUN mkdir -p /var/lib/shared/overlay-images \
             /var/lib/shared/overlay-layers \
             /var/lib/shared/vfs-images \
             /var/lib/shared/vfs-layers && \
    touch /var/lib/shared/overlay-images/images.lock && \
    touch /var/lib/shared/overlay-layers/layers.lock && \
    touch /var/lib/shared/vfs-images/images.lock && \
    touch /var/lib/shared/vfs-layers/layers.lock

# Set up internal Podman to pass subscriptions down from host to internal container
RUN printf '/run/secrets/etc-pki-entitlement:/run/secrets/etc-pki-entitlement\n/run/secrets/rhsm:/run/secrets/rhsm\n' > /etc/containers/mounts.conf

# Set permissions for Podman tools
RUN chmod 4755 /usr/bin/newgidmap && \
    chmod 4755 /usr/bin/newuidmap

# Set environment variable to allow custom Podman configurations
ENV _CONTAINERS_USERNS_CONFIGURED="" \
    BUILDAH_ISOLATION=chroot

# Switch to the new user for all subsequent commands
USER autoimportuser

# Set the default command or entrypoint to the main script
ENTRYPOINT ["/opt/conda/bin/conda", "run", "-n", "auto-import-env", "python", "src/main.py"]

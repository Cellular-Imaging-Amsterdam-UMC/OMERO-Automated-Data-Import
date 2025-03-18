#!/bin/bash

# Stop and remove the container if it exists
podman stop omeroadi >/dev/null 2>&1 || true
podman rm omeroadi >/dev/null 2>&1 || true

echo "Starting omeroadi container..."
# Run the container with OMERO environment variables and network settings
podman run -d --rm --name omeroadi \
    --privileged \
    --device /dev/fuse \
    --security-opt label=disable \
    -e OMERO_HOST=omeroserver \
    -e OMERO_USER=root \
    -e OMERO_PASSWORD=omero \
    -e OMERO_PORT=4064 \
    --network omero \
    --volume /mnt/datadisk/omero:/OMERO \
    --volume /mnt/L-Drive/basic/divg:/data \
    --volume /opt/omero/logs/omeroadi:/auto-importer/logs:Z \
    --volume /opt/omero/config:/auto-importer/config \
    --volume /opt/omero/OMERO-Automated-Data-Import/omero_adi:/auto-importer/omero_adi \
    --volume /opt/omero/OMERO-Automated-Data-Import/tests:/auto-importer/tests \
    --volume /opt/omero/OMERO-Automated-Data-Import/db:/auto-importer/db \
    --userns=keep-id:uid=1000,gid=1000 \
    localhost/omeroadi:local  
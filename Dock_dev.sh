#!/bin/bash

# Stop and remove the container if it exists
docker stop auto-importer-dev >/dev/null 2>&1 || true
docker rm auto-importer-dev >/dev/null 2>&1 || true

# Run the container with OMERO environment variables and network settings
docker run -it \
  --name auto-importer-dev \
  --network nl-biomero_omero \
  -v $(pwd)/omero_adi:/auto-importer/omero_adi \
  -v $(pwd)/tests:/auto-importer/tests \
  -v $(pwd)/logs:/auto-importer/logs \
  -v $(pwd)/config:/auto-importer/config \
  -v $(pwd)/db:/auto-importer/db \
  -v $(pwd)/simulated_L_Drive:/mnt/L-Drive \
  -e OMERO_HOST=nl-biomero-omeroserver-1 \
  -e OMERO_USER=root \
  -e OMERO_PASSWORD=omero \
  -e OMERO_PORT=4064 \
  auto-importer:local \
  bash -c "python -u /auto-importer/omero_adi/main.py && tail -f /dev/null"

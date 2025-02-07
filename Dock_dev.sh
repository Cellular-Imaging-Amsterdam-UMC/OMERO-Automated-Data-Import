#!/bin/bash

# Stop and remove the container if it exists
docker stop auto-importer-dev >/dev/null 2>&1 || true
docker rm auto-importer-dev >/dev/null 2>&1 || true

# Run the container
docker run -it \
  --name auto-importer-dev \
  -v $(pwd)/omero_adi:/auto-importer/omero_adi \
  -v $(pwd)/tests:/auto-importer/tests \
  -v $(pwd)/logs:/auto-importer/logs \
  auto-importer:local
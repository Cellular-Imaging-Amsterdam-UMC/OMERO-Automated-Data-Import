#!/bin/bash
docker run -it \
  --name auto-importer-dev \
  -v $(pwd)/omero-adi:/auto-importer/omero-adi \
  -v $(pwd)/tests:/auto-importer/tests \
  -v $(pwd)/logs:/auto-importer/logs \
  auto-importer:local dev
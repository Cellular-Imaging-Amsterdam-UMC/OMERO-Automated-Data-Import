#!/bin/bash

podman run -d --rm --name auto-import
    --network omero \
    --volume /mnt/L-Drive/basic/divg:/data \
    --volume /opt/omero/auto-importer/config:/auto-importer/config \
    --userns=keep-id:uid=1000,gid=997 \
    auto-importer

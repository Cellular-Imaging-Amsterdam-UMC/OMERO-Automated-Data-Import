#!/bin/bash
# make_test_env.sh - Setup and diagnostic script for the test environment

set -e

echo -e "\n==== Checking for 'nl-biomero_omero' network ===="
if ! docker network ls | grep -q "nl-biomero_omero"; then
  echo "Network 'nl-biomero_omero' not found. Creating network..."
  docker network create --driver bridge nl-biomero_omero
else
  echo "Network 'nl-biomero_omero' exists."
fi

echo -e "\n==== Inspecting 'nl-biomero_omero' network ===="
docker network inspect nl-biomero_omero

echo -e "\n==== Checking required directories ===="
# On production the L: drive is mounted at /mnt/L-Drive/basic/divg.
# On macOS /mnt is often read-only, so we simulate it in a local directory.
SIMULATED_LDRIVE="./simulated_L_Drive/basic/divg"
if [ ! -d "$SIMULATED_LDRIVE" ]; then
  echo "Directory $SIMULATED_LDRIVE does not exist. Creating it..."
  mkdir -p "$SIMULATED_LDRIVE"
  chmod 777 "$SIMULATED_LDRIVE"
else
  echo "Directory $SIMULATED_LDRIVE exists."
fi

echo -e "\n==== Checking essential containers ===="
containers=(nl-biomero-omeroserver-1 nl-biomero-omeroworker-1-1 nl-biomero-biomero-1 nl-biomero-omeroweb-1 metabase)
for container in "${containers[@]}"; do
  if docker ps --format '{{.Names}}' | grep -q "$container"; then
    echo "Container '$container' is running."
  else
    echo "Warning: Container '$container' is NOT running."
  fi
done

echo -e "\n==== Testing Metabase connectivity ===="
METABASE_URL="http://localhost:3000"
if curl -s --head "$METABASE_URL" | head -n 1 | grep "200" > /dev/null; then
  echo "Metabase at $METABASE_URL is reachable."
else
  echo "Warning: Metabase at $METABASE_URL is not reachable."
fi

echo -e "\n==== Testing OMERO server presence ===="
if docker ps --format '{{.Names}}' | grep -q "nl-biomero-omeroserver-1"; then
  echo "✓ OMERO server container is running"
else
  echo "✗ OMERO server container is not running"
fi

# Show container status details
echo "Container details:"
docker ps --filter name=nl-biomero-omeroserver-1 --format "Status: {{.Status}}\nPorts: {{.Ports}}"

echo -e "\n==== Test Environment Setup Complete ====\n"

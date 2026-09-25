#!/usr/bin/env bash
set -euo pipefail

echo "Building PyStreamFlow Docker image..."
docker build -t pystreamflow:latest .

if [ "${1:-run}" = "compose" ]; then
  echo "Starting with docker-compose..."
  docker compose up -d
else
  echo "Running container..."
  docker run -d --name pystreamflow -p 8000:8000 -p 8080:8080 -v "$(pwd)/workflows:/app/workflows" pystreamflow:latest
fi

echo "PyStreamFlow running at http://localhost:8000"

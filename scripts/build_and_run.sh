#!/usr/bin/env bash
set -e
IMAGE_NAME=${1:-pystreamflow}
TAG=${2:-latest}
docker build -t ${IMAGE_NAME}:${TAG} .
echo "Built ${IMAGE_NAME}:${TAG}"
docker compose -f docker-compose.yml up -d

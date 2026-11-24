#!/usr/bin/env bash
# Build and run the DataGenerator standalone execution demo.
#
# Usage:
#   docker/examples/data_generator/run.sh
#
set -euo pipefail

REPO_ROOT="$(git rev-parse --show-toplevel)"

echo "==> Building base image: artisan-worker:dev"
docker build -f "${REPO_ROOT}/docker/artisan-worker/Dockerfile" -t artisan-worker:dev "${REPO_ROOT}"

echo "==> Building example image: artisan-demo-data-generator:dev"
docker build \
    -f "${REPO_ROOT}/docker/examples/data_generator/Dockerfile" \
    -t artisan-demo-data-generator:dev \
    "${REPO_ROOT}/docker/examples/data_generator/"

echo "==> Running demo container"
docker run --rm \
    artisan-demo-data-generator:dev \
    python /app/demo_in_container.py

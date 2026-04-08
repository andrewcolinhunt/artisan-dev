#!/usr/bin/env bash
# Build and run the DataGenerator cloud worker demo.
#
# Usage:
#   docker/examples/data_generator/run.sh
#
# Produces staged parquet files in /tmp/artisan-demo/ on the host.
set -euo pipefail

# pixi.lock has linux-64 only; force amd64 for all docker commands.
export DOCKER_DEFAULT_PLATFORM=linux/amd64

REPO_ROOT="$(git rev-parse --show-toplevel)"

echo "==> Building base image: artisan-worker:dev"
docker build -f "${REPO_ROOT}/docker/Dockerfile" -t artisan-worker:dev "${REPO_ROOT}"

echo "==> Building example image: artisan-demo-data-generator:dev"
docker build \
    -f "${REPO_ROOT}/docker/examples/data_generator/Dockerfile" \
    -t artisan-demo-data-generator:dev \
    "${REPO_ROOT}/docker/examples/data_generator/"

echo "==> Cleaning previous output"
rm -rf /tmp/artisan-demo
mkdir -p /tmp/artisan-demo

echo "==> Running demo container"
docker run --rm \
    -v /tmp/artisan-demo:/tmp/artisan-demo \
    artisan-demo-data-generator:dev \
    python /app/demo_in_container.py

#!/usr/bin/env bash
set -eo pipefail

IMAGE_TAG="${1:?Usage: ./scripts/build.sh <image-tag>}"

docker build -f ./build/Dockerfile --no-cache -t "$IMAGE_TAG" .
docker push "$IMAGE_TAG"

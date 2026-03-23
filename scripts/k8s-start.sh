#!/usr/bin/env bash
set -eo pipefail

# Deploy into the shared dagster namespace
kubectl apply \
    -f k8s/news-agg-dagster-codeserver.yaml

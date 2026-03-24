#!/usr/bin/env bash
set -eo pipefail

# Delete coin2 code-server from dagster namespace
kubectl delete \
    -f k8s/news-agg-dagster-codeserver.yaml \
    --ignore-not-found

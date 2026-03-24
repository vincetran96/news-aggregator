#!/usr/bin/env bash
set -eo pipefail

# Re-apply manifests (picks up YAML changes), then rolling-restart
# all Deployments so pods pull the latest image.

kubectl apply \
    -f k8s/news-agg-dagster-codeserver.yaml

kubectl rollout restart deployment/news-agg-codeserver -n dagster

#!/usr/bin/env bash
set -euo pipefail

# For each bucket, try to infer the matching local folder path
gcloud storage buckets list --format="value(name,location)" | while read -r BUCKET REGION; do
  REGION=$(echo "$REGION" | tr '[:upper:]' '[:lower:]')
  # e.g. BUCKET=polymarket-raw-polymarket-470619-asia-southeast1
  # REGION=asia-southeast1
  LOCAL_DIR="./all-parquets/${REGION}-parquets/parquets"

  echo "Syncing $BUCKET -> $LOCAL_DIR"
  bash gcloud storage rsync --recursive "gs://$BUCKET/parquets" "$LOCAL_DIR"
done

#!/usr/bin/env bash
set -euo pipefail

# ====== CONFIG ======
PROJECT="${PROJECT:-polymarket-470619}"
ZONE="${ZONE:-us-central1-a}"
REGION="${REGION:-us-central1}"

VM_NAME="${VM_NAME:-polymarket-vm}"
DATA_DISK_NAME="${DATA_DISK_NAME:-polymarket-data}"
SA_NAME="${SA_NAME:-vm-polymarket}"
SA_EMAIL="$SA_NAME@$PROJECT.iam.gserviceaccount.com"
BUCKET="${BUCKET:-polymarket-raw-$PROJECT}"

say(){ echo -e "\n==> $*"; }
gcloud config set project "$PROJECT" >/dev/null

# 1) Delete VM (this also detaches the disk)
say "Deleting VM: $VM_NAME"
gcloud compute instances delete "$VM_NAME" --zone="$ZONE" --quiet || echo "VM not found (ok)."

# 2) Delete the data disk (permanent)
say "Deleting data disk: $DATA_DISK_NAME"
gcloud compute disks delete "$DATA_DISK_NAME" --zone="$ZONE" --quiet || echo "Disk not found (ok)."

# 3) Delete ALL bucket contents, then the bucket (permanent)
say "Deleting ALL objects in gs://$BUCKET (this is irreversible)"
gcloud storage rm -r "gs://$BUCKET/**" || echo "No objects or bucket not found (ok)."

say "Deleting bucket: gs://$BUCKET"
gcloud storage buckets delete "gs://$BUCKET" --quiet || echo "Bucket not found (ok)."

# 4) Remove bucket IAM binding for the SA (in case bucket remained) — best effort
say "Removing bucket IAM binding for service account (if bucket still exists)"
gcloud storage buckets remove-iam-policy-binding "gs://$BUCKET" \
  --member="serviceAccount:$SA_EMAIL" --role="roles/storage.objectAdmin" \
  2>/dev/null || true

# 5) Delete the service account itself (optional, we do it)
say "Deleting service account: $SA_EMAIL"
gcloud iam service-accounts delete "$SA_EMAIL" --quiet || echo "Service account not found (ok)."

say "Teardown complete."
#!/usr/bin/env bash
set -euo pipefail

# ====== CONFIG (export to override) ======
PROJECT="${PROJECT:-polymarket-470619}"

# Regions / zones / replicas (matches your deploy script)
REGIONS="${REGIONS:-europe-west4}"          # CSV list of regions
ZONES="${ZONES:-europe-west4a}"          # CSV list aligned 1:1 with REGIONS (defaults to "<region>-a" if empty)
REPLICAS_PER_REGION="${REPLICAS_PER_REGION:-1}"     # N instances per region

# Naming (matches your deploy script)
VM_NAME_PREFIX="${VM_NAME_PREFIX:-polymarket-vm}"
DATA_DISK_PREFIX="${DATA_DISK_PREFIX:-polymarket-data}"
BUCKET_PREFIX="${BUCKET_PREFIX:-polymarket-raw-$PROJECT}"

# Legacy single-region names (teardown tries these too, best-effort)
VM_NAME="${VM_NAME:-polymarket-vm}"
DATA_DISK_NAME="${DATA_DISK_NAME:-polymarket-data}"
REGION="${REGION:-us-central1}"
ZONE="${ZONE:-us-central1-a}"
BUCKET="${BUCKET:-polymarket-raw-$PROJECT}"

SA_NAME="${SA_NAME:-vm-polymarket}"
SA_EMAIL="$SA_NAME@$PROJECT.iam.gserviceaccount.com"

say(){ echo -e "\n==> $*"; }
gcloud config set project "$PROJECT" >/dev/null

# --- helpers ---
split_csv(){ local s="$1"; IFS=',' read -r -a __arr <<<"$s"; printf '%s\n' "${__arr[@]}"; }

# Build arrays from CSV
mapfile -t REGION_ARR < <(split_csv "$REGIONS")
if [[ -n "${ZONES:-}" ]]; then
  mapfile -t ZONE_ARR < <(split_csv "$ZONES")
else
  ZONE_ARR=(); for r in "${REGION_ARR[@]}"; do ZONE_ARR+=("${r}-a"); done
fi
if [[ "${#ZONE_ARR[@]}" -ne "${#REGION_ARR[@]}" ]]; then
  echo "ZONES count must match REGIONS count when provided"; exit 1
fi

say "Starting multi-region teardown for project $PROJECT"
say "Regions: ${REGIONS} | Zones: ${ZONES:-<default -a>} | Replicas/region: $REPLICAS_PER_REGION"

# ----- Per-region resources -----
for idx in "${!REGION_ARR[@]}"; do
  r="${REGION_ARR[$idx]}"; z="${ZONE_ARR[$idx]}"
  bucket="${BUCKET_PREFIX}-${r}"

  say "Region $r | Zone $z"

  # Delete VMs (best-effort)
  for rep in $(seq 1 "$REPLICAS_PER_REGION"); do
    vm="${VM_NAME_PREFIX}-${r}-${rep}"
    say "Deleting VM: $vm (zone $z)"
    gcloud compute instances delete "$vm" --zone="$z" --quiet || echo "VM $vm not found (ok)."
  done

  # Delete data disks (best-effort)
  for rep in $(seq 1 "$REPLICAS_PER_REGION"); do
    disk="${DATA_DISK_PREFIX}-${r}-${rep}"
    say "Deleting disk: $disk (zone $z)"
    gcloud compute disks delete "$disk" --zone="$z" --quiet || echo "Disk $disk not found (ok)."
  done

  # Empty and delete per-region bucket (best-effort)
  say "Deleting ALL objects in gs://$bucket (irreversible)"
  gcloud storage rm -r "gs://$bucket/**" || echo "No objects or bucket $bucket not found (ok)."

  say "Deleting bucket: gs://$bucket"
  gcloud storage buckets delete "gs://$bucket" --quiet || echo "Bucket $bucket not found (ok)."

  # Remove IAM binding from bucket if it somehow still exists
  say "Removing SA IAM binding on bucket (best-effort)"
  gcloud storage buckets remove-iam-policy-binding "gs://$bucket" --member="serviceAccount:$SA_EMAIL" --role="roles/storage.objectAdmin" 2>/dev/null || true
done

# ----- Legacy single-region cleanup (best-effort) -----
say "Legacy cleanup (single-region names)"
say "Deleting legacy VM: $VM_NAME"
gcloud compute instances delete "$VM_NAME" --zone="$ZONE" --quiet || echo "Legacy VM not found (ok)."

say "Deleting legacy data disk: $DATA_DISK_NAME"
gcloud compute disks delete "$DATA_DISK_NAME" --zone="$ZONE" --quiet || echo "Legacy disk not found (ok)."

say "Deleting ALL objects in legacy bucket gs://$BUCKET"
gcloud storage rm -r "gs://$BUCKET/**" || echo "Legacy bucket empty or not found (ok)."

say "Deleting legacy bucket: gs://$BUCKET"
gcloud storage buckets delete "gs://$BUCKET" --quiet || echo "Legacy bucket not found (ok)."

say "Removing SA IAM binding on legacy bucket (best-effort)"
gcloud storage buckets remove-iam-policy-binding "gs://$BUCKET" --member="serviceAccount:$SA_EMAIL" --role="roles/storage.objectAdmin" 2>/dev/null || true

# ----- Service account cleanup -----
say "Deleting service account: $SA_EMAIL"
gcloud iam service-accounts delete "$SA_EMAIL" --quiet || echo "Service account not found (ok)."

say "Teardown complete."

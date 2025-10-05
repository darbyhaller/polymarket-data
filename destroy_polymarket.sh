#!/usr/bin/env bash
set -euo pipefail

# ====== CONFIG (export to override) ======
PROJECT="${PROJECT:-polymarket-470619}"

# Regions / zones / replicas (matches your deploy script)
REGIONS="${REGIONS:-africa-south1,asia-northeast1,asia-southeast1,australia-southeast1,europe-west4,southamerica-east1,us-central1,us-east1}"
ZONES="${ZONES:-africa-south1-c,asia-northeast1-c,asia-southeast1-b,australia-southeast1-b,europe-west4-a,southamerica-east1-b,us-central1-f,us-east1-b}"
REPLICAS_PER_REGION="${REPLICAS_PER_REGION:-1}"     # N instances per region

# Naming (matches your deploy script)
VM_NAME_PREFIX="${VM_NAME_PREFIX:-polymarket-vm}"
DATA_DISK_PREFIX="${DATA_DISK_PREFIX:-polymarket-data}"
BUCKET_PREFIX="${BUCKET_PREFIX:-polymarket-raw-$PROJECT}"

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
done

say "Teardown complete."

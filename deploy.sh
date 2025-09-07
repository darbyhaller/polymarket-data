#!/usr/bin/env bash
set -euo pipefail

# --- Config (export to override) ---
PROJECT="${PROJECT:-polymarket-470619}"; REGION="${REGION:-us-central1}"; ZONE="${ZONE:-us-central1-a}"
VM_NAME="${VM_NAME:-polymarket-vm}"
MACHINE_TYPE="${MACHINE_TYPE:-}" CUSTOM_CPU="${CUSTOM_CPU:-1}" CUSTOM_MEMORY="${CUSTOM_MEMORY:-4GB}"
VM_SCOPES="${VM_SCOPES:-https://www.googleapis.com/auth/devstorage.read_write}"
DATA_DISK_NAME="${DATA_DISK_NAME:-polymarket-data}" DATA_DISK_SIZE_GB="${DATA_DISK_SIZE_GB:-200}"
BUCKET="${BUCKET:-polymarket-raw-$PROJECT}" ARCHIVE_AFTER_DAYS="${ARCHIVE_AFTER_DAYS:-365}"
REPO_URL="${REPO_URL:-https://github.com/darbyhaller/polymarket-data}" REPO_BRANCH="${REPO_BRANCH:-main}"
APP_DIR="${APP_DIR:-/opt/polymarket}" ENTRYPOINT="${ENTRYPOINT:-capture_real_data.py}"
REQUIREMENTS_FILE="${REQUIREMENTS_FILE:-requirements.txt}" SA_NAME="${SA_NAME:-vm-polymarket}"
SA_EMAIL="$SA_NAME@$PROJECT.iam.gserviceaccount.com" LOCAL_RETENTION_DAYS="${LOCAL_RETENTION_DAYS:-3}"

say(){ echo -e "\n==> $*"; }
need(){ command -v "$1" >/dev/null || { echo "Missing: $1"; exit 1; }; }
ensure_project(){ gcloud config set project "$PROJECT" >/dev/null; }

ensure_bucket(){
  say "Ensuring bucket gs://$BUCKET"
  gcloud storage buckets describe "gs://$BUCKET" >/dev/null 2>&1 || gcloud storage buckets create "gs://$BUCKET" --location=US --uniform-bucket-level-access
  say "Applying lifecycle (ARCHIVE after $ARCHIVE_AFTER_DAYS days)"
  tmp="$(mktemp)"; printf 'rule:\n  - action:\n      type: SetStorageClass\n      storageClass: ARCHIVE\n    condition:\n      age: %s\n' "$ARCHIVE_AFTER_DAYS" > "$tmp"
  gcloud storage buckets update "gs://$BUCKET" --lifecycle-file="$tmp" || true
  rm -f "$tmp"
}

ensure_sa_and_iam(){
  say "Ensuring SA $SA_EMAIL"
  gcloud iam service-accounts describe "$SA_EMAIL" >/dev/null 2>&1 || gcloud iam service-accounts create "$SA_NAME" --display-name="Polymarket VM SA"
  sleep 3
  gcloud storage buckets add-iam-policy-binding "gs://$BUCKET" --member="serviceAccount:$SA_EMAIL" --role="roles/storage.objectAdmin" >/dev/null || true
}

ensure_disk(){
  say "Ensuring disk $DATA_DISK_NAME ${DATA_DISK_SIZE_GB}GB"
  gcloud compute disks describe "$DATA_DISK_NAME" --zone "$ZONE" >/dev/null 2>&1 || gcloud compute disks create "$DATA_DISK_NAME" --size="${DATA_DISK_SIZE_GB}GB" --type=pd-standard --zone "$ZONE"
}

ensure_vm(){
  say "Preparing metadata env"
  tmp_env="$(mktemp)"
  sed -e "s|{{APP_DIR}}|$APP_DIR|g" \
      -e "s|{{REPO_URL}}|$REPO_URL|g" \
      -e "s|{{REPO_BRANCH}}|$REPO_BRANCH|g" \
      -e "s|{{ENTRYPOINT}}|$ENTRYPOINT|g" \
      -e "s|{{REQUIREMENTS_FILE}}|$REQUIREMENTS_FILE|g" \
      -e "s|{{BUCKET}}|$BUCKET|g" \
      -e "s|{{LOCAL_RETENTION_DAYS}}|$LOCAL_RETENTION_DAYS|g" \
      -e "s|{{DATA_DISK_NAME}}|$DATA_DISK_NAME|g" \
      vm/polymarket.env.template > "$tmp_env"
  META=("polymarket-env=$(python3 - <<'PY' < "$tmp_env"
import sys,base64; print(base64.b64encode(sys.stdin.read().encode()).decode())
PY
)")
  rm -f "$tmp_env"

  MF=(); [[ -n "$MACHINE_TYPE" ]] && MF+=(--machine-type="$MACHINE_TYPE") || MF+=(--custom-cpu="$CUSTOM_CPU" --custom-memory="$CUSTOM_MEMORY")
  SC=(--scopes="$VM_SCOPES")

  if ! gcloud compute instances describe "$VM_NAME" --zone "$ZONE" >/dev/null 2>&1; then
    say "Creating VM $VM_NAME"
    gcloud compute instances create "$VM_NAME" --zone "$ZONE" "${MF[@]}" --service-account "$SA_EMAIL" "${SC[@]}" \
      --create-disk=auto-delete=yes,boot=yes,image-family=ubuntu-2204-lts,image-project=ubuntu-os-cloud \
      --disk=name="$DATA_DISK_NAME",mode=rw,auto-delete=no \
      --metadata-from-file startup-script=vm/startup.sh \
      --metadata "${META[@]}"
  else
    say "Updating startup + metadata; ensure disk attached"
    gcloud compute instances add-metadata "$VM_NAME" --zone "$ZONE" --metadata-from-file startup-script=vm/startup.sh --metadata "${META[@]}"
    gcloud compute instances describe "$VM_NAME" --zone "$ZONE" --format="get(disks[].source)" | grep -q "/$DATA_DISK_NAME$" || gcloud compute instances attach-disk "$VM_NAME" --disk "$DATA_DISK_NAME" --zone "$ZONE"
    gcloud compute instances reset "$VM_NAME" --zone "$ZONE"
  fi
}

need gcloud; ensure_project; ensure_bucket; ensure_sa_and_iam; ensure_disk; ensure_vm
say "Done!"
echo "Tail logs: gcloud compute ssh $VM_NAME --zone=$ZONE -- 'journalctl -u polymarket -f'"
echo "Check timers: gcloud compute ssh $VM_NAME --zone=$ZONE -- 'systemctl list-timers | grep polymarket'"
echo "Latest in GCS: gcloud storage ls -r gs://$BUCKET/parquets/event_type=*/year=*/month=*/day=*/hour=*/events-*.parquet | tail -n1"
echo "Wait 30 seconds, then run: gcloud compute ssh polymarket-vm --zone=us-central1-a -- 'sudo journalctl -fu google-startup-scripts.service'"
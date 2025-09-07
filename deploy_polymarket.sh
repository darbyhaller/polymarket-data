#!/usr/bin/env bash
set -euo pipefail

# ===== User-tunable (or export before running) =====
PROJECT="${PROJECT:-polymarket-470619}"
REGION="${REGION:-us-central1}"
ZONE="${ZONE:-us-central1-a}"
VM_NAME="${VM_NAME:-polymarket-vm}"
MACHINE_TYPE="${MACHINE_TYPE:-}"         # e.g. e2-standard-2 (leave empty for custom)
CUSTOM_CPU="${CUSTOM_CPU:-1}"
CUSTOM_MEMORY="${CUSTOM_MEMORY:-4GB}"
VM_SCOPES="${VM_SCOPES:-https://www.googleapis.com/auth/devstorage.read_write}"
DATA_DISK_NAME="${DATA_DISK_NAME:-polymarket-data}"
DATA_DISK_SIZE_GB="${DATA_DISK_SIZE_GB:-200}"
BUCKET="${BUCKET:-polymarket-raw-$PROJECT}"
ARCHIVE_AFTER_DAYS="${ARCHIVE_AFTER_DAYS:-365}"
REPO_URL="${REPO_URL:-https://github.com/darbyhaller/polymarket-data}"
REPO_BRANCH="${REPO_BRANCH:-main}"
APP_DIR="${APP_DIR:-/opt/polymarket}"
ENTRYPOINT="${ENTRYPOINT:-capture_real_data.py}"
REQUIREMENTS_FILE="${REQUIREMENTS_FILE:-requirements.txt}"
SA_NAME="${SA_NAME:-vm-polymarket}"
SA_EMAIL="$SA_NAME@$PROJECT.iam.gserviceaccount.com"
LOCAL_RETENTION_DAYS="${LOCAL_RETENTION_DAYS:-3}"

say(){ echo -e "\n==> $*"; }
need(){ command -v "$1" >/dev/null || { echo "Missing: $1"; exit 1; }; }
ensure_project(){ gcloud config set project "$PROJECT" >/dev/null; }

# ---- Bucket (idempotent) ----
ensure_bucket() {
  say "Ensuring bucket gs://$BUCKET"
  if ! gcloud storage buckets describe "gs://$BUCKET" >/dev/null 2>&1; then
    gcloud storage buckets create "gs://$BUCKET" --location=US --uniform-bucket-level-access
  else
    echo "Bucket exists."
  fi

  say "Applying lifecycle: archive after $ARCHIVE_AFTER_DAYS days"
  tmp_yaml="$(mktemp)"; trap 'rm -f "$tmp_yaml"' EXIT
  sed "s/{{ARCHIVE_AFTER_DAYS}}/$ARCHIVE_AFTER_DAYS/g" files/storage/lifecycle.tpl > "$tmp_yaml"
  if gcloud storage buckets update "gs://$BUCKET" --lifecycle-file="$tmp_yaml"; then
    :
  else
    echo "gcloud lifecycle update failed — trying gsutil"
    tmp_json="$(mktemp)"; trap 'rm -f "$tmp_yaml" "$tmp_json"' EXIT
    sed "s/{{ARCHIVE_AFTER_DAYS}}/$ARCHIVE_AFTER_DAYS/g" files/storage/lifecycle.json.tpl > "$tmp_json"
    gsutil lifecycle set "$tmp_json" "gs://$BUCKET"
  fi

  echo "Current lifecycle:"; gsutil lifecycle get "gs://$BUCKET"
}

# ---- Service Account + IAM (idempotent) ----
ensure_sa_and_iam() {
  say "Ensuring service account $SA_EMAIL"
  if ! gcloud iam service-accounts describe "$SA_EMAIL" >/dev/null 2>&1; then
    gcloud iam service-accounts create "$SA_NAME" --display-name="Polymarket VM SA"
  else
    echo "SA exists."
  fi
  say "Granting roles/storage.objectAdmin on bucket to $SA_EMAIL"
  gcloud storage buckets add-iam-policy-binding "gs://$BUCKET" --member="serviceAccount:$SA_EMAIL" --role="roles/storage.objectAdmin" >/dev/null
}

# ---- Data Disk (idempotent) ----
ensure_disk() {
  say "Ensuring data disk $DATA_DISK_NAME (${DATA_DISK_SIZE_GB}GB)"
  if ! gcloud compute disks describe "$DATA_DISK_NAME" --zone "$ZONE" >/dev/null 2>&1; then
    gcloud compute disks create "$DATA_DISK_NAME" --size="${DATA_DISK_SIZE_GB}GB" --type=pd-standard --zone "$ZONE"
  else
    echo "Disk exists."
  fi
}

# ---- VM (idempotent-ish) ----
ensure_vm() {
  say "Preparing metadata env payload"
  # Build env content from template
  tmp_env="$(mktemp)"; trap 'rm -f "$tmp_env"' EXIT
  sed -e "s|{{APP_DIR}}|$APP_DIR|g" \
      -e "s|{{REPO_URL}}|$REPO_URL|g" \
      -e "s|{{REPO_BRANCH}}|$REPO_BRANCH|g" \
      -e "s|{{ENTRYPOINT}}|$ENTRYPOINT|g" \
      -e "s|{{REQUIREMENTS_FILE}}|$REQUIREMENTS_FILE|g" \
      -e "s|{{BUCKET}}|$BUCKET|g" \
      -e "s|{{LOCAL_RETENTION_DAYS}}|$LOCAL_RETENTION_DAYS|g" \
      -e "s|{{DATA_DISK_NAME}}|$DATA_DISK_NAME|g" \
      files/polymarket.env.template > "$tmp_env"

  # pass the env as plain text metadata (instance attribute)
  METADATA_ARGS=( "--metadata=polymarket-env=$(python3 - <<'PY'
import sys,base64
print(base64.b64encode(sys.stdin.read().encode()).decode())
PY
  < "$tmp_env") )

  MACHINE_FLAGS=()
  if [[ -n "$MACHINE_TYPE" ]]; then
    MACHINE_FLAGS+=( "--machine-type=$MACHINE_TYPE" )
  else
    MACHINE_FLAGS+=( "--custom-cpu=$CUSTOM_CPU" "--custom-memory=$CUSTOM_MEMORY" )
  fi
  SCOPE_FLAGS=( "--scopes=$VM_SCOPES" )

  if ! gcloud compute instances describe "$VM_NAME" --zone "$ZONE" >/dev/null 2>&1; then
    say "Creating VM $VM_NAME"
    gcloud compute instances create "$VM_NAME" --zone "$ZONE" "${MACHINE_FLAGS[@]}" --service-account "$SA_EMAIL" "${SCOPE_FLAGS[@]}" --create-disk=auto-delete=yes,boot=yes,image-family=ubuntu-2204-lts,image-project=ubuntu-os-cloud --disk=name="$DATA_DISK_NAME",mode=rw,auto-delete=no --metadata-from-file startup-script=files/startup.sh "${METADATA_ARGS[@]}"
  else
    say "VM exists; updating startup-script and metadata; ensuring disk attached"
    gcloud compute instances add-metadata "$VM_NAME" --zone "$ZONE" --metadata-from-file startup-script=files/startup.sh "${METADATA_ARGS[@]}"
    if ! gcloud compute instances describe "$VM_NAME" --zone "$ZONE" --format="get(disks[].source)" | grep -q "/$DATA_DISK_NAME$"; then
      gcloud compute instances attach-disk "$VM_NAME" --disk "$DATA_DISK_NAME" --zone "$ZONE"
    fi
    say "Rebooting to apply startup changes"
    gcloud compute instances reset "$VM_NAME" --zone "$ZONE"
  fi
}

# ---- MAIN ----
need gcloud
ensure_project
ensure_bucket
ensure_sa_and_iam
ensure_disk
ensure_vm

say "Done!"
echo "  • Tail ingest logs:  gcloud compute ssh $VM_NAME --zone=$ZONE -- 'journalctl -u polymarket -f'"
echo "  • Check timers:      gcloud compute ssh $VM_NAME --zone=$ZONE -- 'systemctl list-timers | grep polymarket'"
echo "  • Latest in GCS:     gcloud storage ls -r gs://$BUCKET/parquets/event_type=*/year=*/month=*/day=*/hour=*/events-*.parquet | tail -n1"

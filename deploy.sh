#!/usr/bin/env bash
set -euo pipefail

# Multi-region + replicas
# comma-separated: e.g. "us-central1,us-east1"
# optional comma-separated zones aligned with REGIONS; defaults to "<region>-a" if empty
# N per region
# 1 vCPU, 4 GB RAM
# Per-region buckets will be named: $BUCKET_PREFIX-$region
# MUST CHANGE IMAGE_FAMILY to be compatible with MACHINE_TYPE (remove/add -arm-64 from end)
# IMAGE_FAMILY="ubuntu-2204-lts"


PROJECT="${PROJECT:-polymarket-470619}"
REGIONS="${REGIONS:-europe-west4}"
ZONES="${ZONES:-europe-west4-a}"
REPLICAS_PER_REGION="${REPLICAS_PER_REGION:-1}"
VM_NAME_PREFIX="${VM_NAME_PREFIX:-polymarket-vm}"
MACHINE_TYPE="${MACHINE_TYPE:-c4a-standard-1}"
IMAGE_FAMILY="ubuntu-2204-lts-arm64"
VM_SCOPES="${VM_SCOPES:-https://www.googleapis.com/auth/cloud-platform}"
DATA_DISK_PREFIX="${DATA_DISK_PREFIX:-polymarket-data}" DATA_DISK_SIZE_GB="${DATA_DISK_SIZE_GB:-100}"
BUCKET_PREFIX="${BUCKET_PREFIX:-polymarket-raw-$PROJECT}"
ARCHIVE_AFTER_DAYS="${ARCHIVE_AFTER_DAYS:-365}"
REPO_URL="${REPO_URL:-https://github.com/darbyhaller/polymarket-data}" REPO_BRANCH="${REPO_BRANCH:-main}"
APP_DIR="${APP_DIR:-/opt/polymarket}" ENTRYPOINT="${ENTRYPOINT:-capture_real_data.py}"
REQUIREMENTS_FILE="${REQUIREMENTS_FILE:-requirements.txt}" SA_NAME="${SA_NAME:-vm-polymarket}"
SA_EMAIL="$SA_NAME@$PROJECT.iam.gserviceaccount.com" LOCAL_RETENTION_DAYS="${LOCAL_RETENTION_DAYS:-3}"

say(){ echo -e "\n==> $*"; }
need(){ command -v "$1" >/dev/null || { echo "Missing: $1"; exit 1; }; }
ensure_project(){ gcloud config set project "$PROJECT" >/dev/null; }

ensure_sa_project_roles(){
  say "Ensuring SA $SA_EMAIL exists and has Monitoring/Logging writer roles on project $PROJECT"
  gcloud iam service-accounts describe "$SA_EMAIL" >/dev/null 2>&1 || gcloud iam service-accounts create "$SA_NAME" --display-name="Polymarket VM SA"
  # These roles are required for Ops Agent to write metrics and logs
  gcloud projects add-iam-policy-binding "$PROJECT" --member="serviceAccount:$SA_EMAIL" --role="roles/monitoring.metricWriter" >/dev/null
  gcloud projects add-iam-policy-binding "$PROJECT" --member="serviceAccount:$SA_EMAIL" --role="roles/logging.logWriter" >/dev/null
}

split_csv(){ local s="$1"; IFS=',' read -r -a __arr <<<"$s"; printf '%s\n' "${__arr[@]}"; }

ensure_bucket(){
  local bucket="$1" location="$2"
  say "Ensuring bucket gs://$bucket in $location"
  gcloud storage buckets describe "gs://$bucket" >/dev/null 2>&1 || gcloud storage buckets create "gs://$bucket" --location="$location" --uniform-bucket-level-access
  say "Applying lifecycle (ARCHIVE after $ARCHIVE_AFTER_DAYS days) to gs://$bucket"
  tmp="$(mktemp)"; printf 'rule:\n  - action:\n      type: SetStorageClass\n      storageClass: ARCHIVE\n    condition:\n      age: %s\n' "$ARCHIVE_AFTER_DAYS" > "$tmp"
  gcloud storage buckets update "gs://$bucket" --lifecycle-file="$tmp" || true
  rm -f "$tmp"
}

ensure_sa_and_iam_for_bucket(){
  local bucket="$1"
  say "Ensuring SA $SA_EMAIL and IAM on gs://$bucket"
  gcloud iam service-accounts describe "$SA_EMAIL" >/dev/null 2>&1 || gcloud iam service-accounts create "$SA_NAME" --display-name="Polymarket VM SA"
  sleep 3
  gcloud storage buckets add-iam-policy-binding "gs://$bucket" --member="serviceAccount:$SA_EMAIL" --role="roles/storage.objectAdmin" >/dev/null || true
}

ensure_disk(){
  local disk_name="$1" zone="$2"
  say "Ensuring disk $disk_name ${DATA_DISK_SIZE_GB}GB in $zone"
  gcloud compute disks describe "$disk_name" --zone "$zone" >/dev/null 2>&1 || gcloud compute disks create "$disk_name" --size="${DATA_DISK_SIZE_GB}GB" --type=hyperdisk-balanced --zone "$zone"
}

ensure_vm(){
  local vm_name="$1" region="$2" zone="$3" data_disk="$4" shard_id="$5" shard_total="$6" bucket_name="$7"

  say "Preparing metadata env for $vm_name (region=$region zone=$zone shard=$shard_id/$shard_total bucket=$bucket_name)"
  tmp_env="$(mktemp)"
  sed -e "s|{{APP_DIR}}|$APP_DIR|g" \
      -e "s|{{REPO_URL}}|$REPO_URL|g" \
      -e "s|{{REPO_BRANCH}}|$REPO_BRANCH|g" \
      -e "s|{{ENTRYPOINT}}|$ENTRYPOINT|g" \
      -e "s|{{REQUIREMENTS_FILE}}|$REQUIREMENTS_FILE|g" \
      -e "s|{{BUCKET}}|$bucket_name|g" \
      -e "s|{{LOCAL_RETENTION_DAYS}}|$LOCAL_RETENTION_DAYS|g" \
      -e "s|{{DATA_DISK_NAME}}|$data_disk|g" \
      vm/polymarket.env.template > "$tmp_env"

  {
    echo "INSTANCE_NAME=$vm_name"
    echo "REGION=$region"
    echo "ZONE=$zone"
    echo "SHARD_ID=$shard_id"
    echo "SHARD_TOTAL=$shard_total"
    printf "SHARD_PATH=shard=%02d\n" "$shard_id"
  } >> "$tmp_env"

  META=("polymarket-env=$(python3 -c 'import sys,base64; print(base64.b64encode(open(sys.argv[1],"rb").read()).decode())' "$tmp_env")")

  rm -f "$tmp_env"

  MF=(); [[ -n "${MACHINE_TYPE:-}" ]] && MF+=(--machine-type="$MACHINE_TYPE") || MF+=(--custom-cpu="$CUSTOM_CPU" --custom-memory="$CUSTOM_MEMORY")
  SC=(--scopes="$VM_SCOPES")

  if ! gcloud compute instances describe "$vm_name" --zone "$zone" >/dev/null 2>&1; then
    say "Creating VM $vm_name"
    gcloud compute instances create "$vm_name" --zone "$zone" "${MF[@]}" --service-account "$SA_EMAIL" "${SC[@]}" \
      --create-disk=auto-delete=yes,boot=yes,image-family="${IMAGE_FAMILY}",image-project=ubuntu-os-cloud \
      --disk=name="$data_disk",mode=rw,auto-delete=no \
      --metadata-from-file startup-script=vm/startup.sh \
      --metadata "${META[@]}"
  else
    say "Updating startup + metadata; ensure disk attached for $vm_name"
    gcloud compute instances add-metadata "$vm_name" --zone "$zone" --metadata-from-file startup-script=vm/startup.sh --metadata "${META[@]}"
    gcloud compute instances describe "$vm_name" --zone "$zone" --format="get(disks[].source)" | grep -q "/$data_disk$" || gcloud compute instances attach-disk "$vm_name" --disk "$data_disk" --zone "$zone"
    gcloud compute instances reset "$vm_name" --zone "$zone"
  fi
}

need gcloud; ensure_project

# --- Fan out over regions and replicas ---
mapfile -t REGION_ARR < <(split_csv "$REGIONS")
if [[ -n "$ZONES" ]]; then
  mapfile -t ZONE_ARR < <(split_csv "$ZONES")
else
  ZONE_ARR=(); for r in "${REGION_ARR[@]}"; do ZONE_ARR+=("${r}-a"); done
fi
if [[ "${#ZONE_ARR[@]}" -ne "${#REGION_ARR[@]}" ]]; then echo "ZONES count must match REGIONS count when provided"; exit 1; fi

TOTAL_SHARDS=$(( REPLICAS_PER_REGION * ${#REGION_ARR[@]} ))
GLOBAL_SHARD=0

for idx in "${!REGION_ARR[@]}"; do
  r="${REGION_ARR[$idx]}"; z="${ZONE_ARR[$idx]}"
  bucket="${BUCKET_PREFIX}-${r}"

  # Per-region bucket + IAM
  ensure_bucket "$bucket" "$r"
  ensure_sa_and_iam_for_bucket "$bucket"

  for rep in $(seq 1 "$REPLICAS_PER_REGION"); do
    vm="${VM_NAME_PREFIX}-${r}-${rep}"
    disk="${DATA_DISK_PREFIX}-${r}-${rep}"
    ensure_disk "$disk" "$z"
    ensure_vm "$vm" "$r" "$z" "$disk" "$GLOBAL_SHARD" "$TOTAL_SHARDS" "$bucket"
    say "Provisioned $vm (shard $GLOBAL_SHARD / $TOTAL_SHARDS) in $z -> gs://$bucket"
    GLOBAL_SHARD=$(( GLOBAL_SHARD + 1 ))
  done
done

say "Done!"
echo "Latest in GCS per region:"
for r in $(echo "$REGIONS" | tr ',' ' '); do
  echo "Tail logs (example): gcloud compute ssh ${VM_NAME_PREFIX}-${r}-1 --zone=${ZONE} -- 'journalctl -u polymarket -f'"
  echo "Check timers (example): gcloud compute ssh ${VM_NAME_PREFIX}-${r}-1 --zone=${ZONE} -- 'systemctl list-timers | grep polymarket'"
  b="${BUCKET_PREFIX}-${r}"
  echo "  Region $r -> gs://$b"
  echo "  $(gcloud storage ls -r gs://$b/parquets/event_type=*/year=*/month=*/day=*/hour=*/events-*.parquet 2>/dev/null | tail -n1)"
done
echo "sudo journalctl -fu google-startup-scripts.service"
echo "sudo journalctl -fu polymarket"

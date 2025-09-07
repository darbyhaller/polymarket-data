#!/usr/bin/env bash
set -euo pipefail

#############################################
# CONFIG (override by exporting env vars)
#############################################
PROJECT="${PROJECT:-polymarket-470619}"
REGION="${REGION:-us-central1}"
ZONE="${ZONE:-us-central1-a}"

# VM + resources
VM_NAME="${VM_NAME:-polymarket-vm}"

# You used custom type (1 vCPU, 4GB). Leave MACHINE_TYPE empty to use custom flags.
MACHINE_TYPE="${MACHINE_TYPE:-}"          # e.g. e2-standard-2 (leave empty for custom)
CUSTOM_CPU="${CUSTOM_CPU:-1}"             # used when MACHINE_TYPE is empty
CUSTOM_MEMORY="${CUSTOM_MEMORY:-4GB}"     # used when MACHINE_TYPE is empty

# OAuth scopes for the VM (you used devstorage.read_write)
VM_SCOPES="${VM_SCOPES:-https://www.googleapis.com/auth/devstorage.read_write}"

# Data disk
DATA_DISK_NAME="${DATA_DISK_NAME:-polymarket-data}"
DATA_DISK_SIZE_GB="${DATA_DISK_SIZE_GB:-200}"

# Bucket + lifecycle
BUCKET="${BUCKET:-polymarket-raw-$PROJECT}"
ARCHIVE_AFTER_DAYS="${ARCHIVE_AFTER_DAYS:-365}"

# Repo + app
REPO_URL="${REPO_URL:-https://github.com/darbyhaller/polymarket-data}"  # <<< CHANGE ME
REPO_BRANCH="${REPO_BRANCH:-main}"
APP_DIR="${APP_DIR:-/opt/polymarket}"
ENTRYPOINT="${ENTRYPOINT:-capture_real_data.py}"                  # <<< CHANGE if needed
REQUIREMENTS_FILE="${REQUIREMENTS_FILE:-requirements.txt}"        # optional; script tolerates missing file

# Service Account
SA_NAME="${SA_NAME:-vm-polymarket}"
SA_EMAIL="$SA_NAME@$PROJECT.iam.gserviceaccount.com"

# Local retention (days) before deleting local parquet files
LOCAL_RETENTION_DAYS="${LOCAL_RETENTION_DAYS:-3}"

#############################################
# Helpers
#############################################
say() { echo -e "\n==> $*"; }
need() { command -v "$1" >/dev/null || { echo "Missing: $1"; exit 1; }; }

ensure_project() {
  gcloud config set project "$PROJECT" >/dev/null
}

#############################################
# Bucket (idempotent)
#############################################
ensure_bucket() {
  say "Ensuring bucket: gs://$BUCKET"
  if ! gcloud storage buckets describe "gs://$BUCKET" >/dev/null 2>&1; then
    gcloud storage buckets create "gs://$BUCKET" --location=US --uniform-bucket-level-access
  else
    echo "Bucket exists."
  fi

  say "Applying lifecycle: Archive after $ARCHIVE_AFTER_DAYS days (never delete)"

  # Use YAML (gcloud handles this well)
  cat > /tmp/lifecycle.yaml <<YAML
rule:
  - action:
      type: SetStorageClass
      storageClass: ARCHIVE
    condition:
      age: $ARCHIVE_AFTER_DAYS
YAML

  # Try with gcloud first
  if gcloud storage buckets update "gs://$BUCKET" --lifecycle-file=/tmp/lifecycle.yaml; then
    :
  else
    echo "gcloud lifecycle update failed — falling back to gsutil with JSON"
    # Fallback: JSON + gsutil
    cat > /tmp/lifecycle.json <<JSON
{
  "rule": [
    {
      "action": { "type": "SetStorageClass", "storageClass": "ARCHIVE" },
      "condition": { "age": $ARCHIVE_AFTER_DAYS }
    }
  ]
}
JSON
    gsutil lifecycle set /tmp/lifecycle.json "gs://$BUCKET"
  fi

  # Verify
  echo "Current lifecycle:"
  gsutil lifecycle get gs://polymarket-raw-polymarket-470619
}

#############################################
# Service Account + IAM (idempotent)
#############################################
ensure_sa_and_iam() {
  say "Ensuring service account: $SA_EMAIL"
  if ! gcloud iam service-accounts describe "$SA_EMAIL" >/dev/null 2>&1; then
    gcloud iam service-accounts create "$SA_NAME" --display-name="Polymarket VM SA"
  else
    echo "SA exists."
  fi

  say "Granting bucket-level roles/storage.objectAdmin to $SA_EMAIL"
  gcloud storage buckets add-iam-policy-binding "gs://$BUCKET" \
    --member="serviceAccount:$SA_EMAIL" \
    --role="roles/storage.objectAdmin" >/dev/null
}

#############################################
# Data Disk (idempotent)
#############################################
ensure_disk() {
  say "Ensuring data disk: $DATA_DISK_NAME (${DATA_DISK_SIZE_GB}GB)"
  if ! gcloud compute disks describe "$DATA_DISK_NAME" --zone "$ZONE" >/dev/null 2>&1; then
    gcloud compute disks create "$DATA_DISK_NAME" \
      --size="${DATA_DISK_SIZE_GB}GB" \
      --type=pd-standard \
      --zone "$ZONE"
  else
    echo "Disk exists."
  fi
}

#############################################
# VM + Startup Script (idempotent-ish)
#############################################
ensure_vm() {
  say "Preparing startup script"
  cat > /tmp/startup.sh <<EOS
#!/usr/bin/env bash
set -euo pipefail

# Set required environment variables for startup context
export HOME=/root
export USER=root
export PATH=/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin

APP_DIR="$APP_DIR"
MOUNT_POINT="/var/data/polymarket"
REPO_URL="$REPO_URL"
REPO_BRANCH="$REPO_BRANCH"
ENTRYPOINT="$ENTRYPOINT"
REQUIREMENTS_FILE="$REQUIREMENTS_FILE"
BUCKET="$BUCKET"
LOCAL_RETENTION_DAYS="$LOCAL_RETENTION_DAYS"

log() { echo "[startup] \$*"; }

log "Fix any broken dpkg state first"
dpkg --configure -a || true
apt-get update --fix-missing -y || true
apt-get install -f -y || true

log "Update & install base packages"
apt-get update -y
apt-get install -y git python3-venv python3-pip apt-transport-https ca-certificates gnupg curl

if ! command -v gcloud >/dev/null 2>&1; then
  log "Install Google Cloud SDK"
  echo "deb [signed-by=/usr/share/keyrings/cloud.google.gpg] http://packages.cloud.google.com/apt cloud-sdk main" \\
    > /etc/apt/sources.list.d/google-cloud-sdk.list
  curl -fsSL https://packages.cloud.google.com/apt/doc/apt-key.gpg | \\
    gpg --dearmor -o /usr/share/keyrings/cloud.google.gpg
  apt-get update -y && apt-get install -y google-cloud-cli
fi

log "Format & mount data disk to \$MOUNT_POINT (if not already)"
mkdir -p "\$MOUNT_POINT"
DEVICE=""
if lsblk -dn -o NAME | grep -q "^sdb\$"; then
  DEVICE="/dev/sdb"
elif lsblk -dn -o NAME | grep -q "^nvme1n1\$"; then
  DEVICE="/dev/nvme1n1"
fi
if [ -n "\$DEVICE" ]; then
  if ! blkid "\$DEVICE" >/dev/null 2>&1; then
    log "Formatting \$DEVICE as ext4"
    mkfs.ext4 -m 0 -F "\$DEVICE"
  fi
  UUID=\$(blkid -s UUID -o value "\$DEVICE")
  if ! mount | grep -q " \$MOUNT_POINT "; then
    log "Mounting \$DEVICE on \$MOUNT_POINT"
    mount "\$DEVICE" "\$MOUNT_POINT"
  fi
  if ! grep -q "\$UUID" /etc/fstab; then
    echo "UUID=\$UUID  \$MOUNT_POINT  ext4  defaults  0  2" >> /etc/fstab
  fi
else
  log "WARNING: No secondary disk detected. Ingest will still run, but without large local buffer."
fi

log "Setting up swap"
SWAPFILE="\$MOUNT_POINT/swapfile"
if [ ! -f "\$SWAPFILE" ]; then
  fallocate -l 2G "\$SWAPFILE"
  chmod 600 "\$SWAPFILE"
  mkswap "\$SWAPFILE"
  echo "\$SWAPFILE none swap sw 0 0" >> /etc/fstab
fi
swapon -a
log "Swap enabled:"
free -h

log "Prepare app dir & venv"
mkdir -p "\$APP_DIR"
cd "\$APP_DIR"

# Configure git first
git config --global user.name "VM User"
git config --global user.email "vm@polymarket.com"
git config --global --add safe.directory "\$APP_DIR"

if [ ! -d .git ]; then
  log "Cloning \$REPO_URL (\$REPO_BRANCH)"
  git clone --branch "\$REPO_BRANCH" "\$REPO_URL" .
else
  log "Repo exists; updating"
  git remote set-url origin "\$REPO_URL" || true
  git fetch origin "\$REPO_BRANCH" || true
  git reset --hard "origin/\$REPO_BRANCH" || true
fi

chown -R dhaller:dhaller "\$APP_DIR"

python3 -m venv .venv
source .venv/bin/activate
pip install --upgrade pip
if [ -f "\$REQUIREMENTS_FILE" ]; then
  pip install -r "\$REQUIREMENTS_FILE" || true
fi

log "Install systemd units"

# Stop existing services if they're running (for updates)
if systemctl is-active --quiet polymarket; then
  log "Stopping existing polymarket service for update"
  systemctl stop polymarket
fi
if systemctl is-active --quiet polymarket-sync.timer; then
  log "Stopping existing sync timer for update"
  systemctl stop polymarket-sync.timer
fi
if systemctl is-active --quiet polymarket-clean.timer; then
  log "Stopping existing clean timer for update"
  systemctl stop polymarket-clean.timer
fi

cat >/etc/systemd/system/polymarket.service <<UNIT
[Unit]
Description=Polymarket WebSocket Ingest
After=network-online.target
Wants=network-online.target

[Service]
User=root
WorkingDirectory=\$APP_DIR
Environment=DATA_ROOT=\$MOUNT_POINT
Environment=PARQUET_ROOT=\$MOUNT_POINT
Environment=PYTHONUNBUFFERED=1
Environment=ROTATE_MB=512
ExecStart=\$APP_DIR/.venv/bin/python \$APP_DIR/\$ENTRYPOINT
Restart=always
RestartSec=3
LimitNOFILE=262144

[Install]
WantedBy=multi-user.target
UNIT

cat >/usr/local/bin/sync_to_gcs.sh <<SYNC
#!/usr/bin/env bash
set -euo pipefail
SRC="\$MOUNT_POINT/parquets"
DST="gs://\$BUCKET/parquets"
CURHOUR=\\\$(date -u +%H)
# Exclude current UTC hour to avoid uploading in-progress parquet files
# New structure: event_type=*/year=*/month=*/day=*/hour=*/events-*.parquet
gcloud storage rsync -r -x "hour=\\\${CURHOUR}(/|\$)" "\\\$SRC" "\\\$DST"
SYNC
chmod +x /usr/local/bin/sync_to_gcs.sh

cat >/etc/systemd/system/polymarket-sync.service <<SYNCUNIT
[Unit]
Description=Sync local archive to GCS
[Service]
Type=oneshot
ExecStart=/usr/local/bin/sync_to_gcs.sh
SYNCUNIT

cat >/etc/systemd/system/polymarket-sync.timer <<SYNCTIMER
[Unit]
Description=Run GCS sync hourly, offset by 30 minutes
[Timer]
OnCalendar=*:30
Persistent=true
[Install]
WantedBy=timers.target
SYNCTIMER

cat >/usr/local/bin/clean_old_local.sh <<'CLEAN'
#!/usr/bin/env bash
set -euo pipefail
# Defaults if not passed via Environment/EnvironmentFile
PARQUET_ROOT="${PARQUET_ROOT:-/var/data/polymarket}"
LOCAL_RETENTION_DAYS="${LOCAL_RETENTION_DAYS:-3}"
ROOT="$PARQUET_ROOT/parquets"
[ -d "$ROOT" ] || exit 0
CURHOUR="$(date -u +%H)"
RETENTION_MINUTES=$(( LOCAL_RETENTION_DAYS * 24 * 60 ))
# Delete parquet files older than retention, excluding the current UTC hour dir
find "$ROOT" -type f -name '*.parquet' \
  ! -path "*/hour=${CURHOUR}/*" \
  -mmin +$RETENTION_MINUTES -print -delete
# Optional: prune empty directories left behind (best-effort)
find "$ROOT" -type d -empty -prune -exec rmdir -p -- {} + 2>/dev/null || true
CLEAN
chmod +x /usr/local/bin/clean_old_local.sh

cat >/etc/systemd/system/polymarket-clean.service <<CLEANUNIT
[Unit]
Description=Delete old local capture files
[Service]
Type=oneshot
ExecStart=/usr/local/bin/clean_old_local.sh
CLEANUNIT

cat >/etc/systemd/system/polymarket-clean.timer <<CLEANTIMER
[Unit]
Description=Run local cleanup daily
[Timer]
OnCalendar=daily
Persistent=true
[Install]
WantedBy=timers.target
CLEANTIMER

log "Enable & start services"
systemctl daemon-reload
systemctl enable --now polymarket
systemctl enable --now polymarket-sync.timer
systemctl enable --now polymarket-clean.timer

log "Startup complete."
EOS

  # choose machine flags (standard vs custom)
  MACHINE_FLAGS=()
  if [[ -n "$MACHINE_TYPE" ]]; then
    MACHINE_FLAGS+=( "--machine-type=$MACHINE_TYPE" )
  else
    MACHINE_FLAGS+=( "--custom-cpu=$CUSTOM_CPU" "--custom-memory=$CUSTOM_MEMORY" )
  fi

  SCOPE_FLAGS=( "--scopes=$VM_SCOPES" )

  if ! gcloud compute instances describe "$VM_NAME" --zone "$ZONE" >/dev/null 2>&1; then
    say "Creating VM $VM_NAME"
    gcloud compute instances create "$VM_NAME" \
      --zone "$ZONE" \
      "${MACHINE_FLAGS[@]}" \
      --service-account "$SA_EMAIL" \
      "${SCOPE_FLAGS[@]}" \
      --create-disk=auto-delete=yes,boot=yes,image-family=ubuntu-2204-lts,image-project=ubuntu-os-cloud \
      --disk=name="$DATA_DISK_NAME",mode=rw,auto-delete=no \
      --metadata-from-file startup-script=/tmp/startup.sh
  else
    say "VM exists; updating startup-script, ensuring disk attached"
    gcloud compute instances add-metadata "$VM_NAME" \
      --zone "$ZONE" \
      --metadata-from-file startup-script=/tmp/startup.sh

    if ! gcloud compute instances describe "$VM_NAME" --zone "$ZONE" \
      --format="get(disks[].source)" | grep -q "/$DATA_DISK_NAME$"; then
      gcloud compute instances attach-disk "$VM_NAME" --disk "$DATA_DISK_NAME" --zone "$ZONE"
    fi

    say "Rebooting VM to apply startup-script"
    gcloud compute instances reset "$VM_NAME" --zone "$ZONE"
  fi
}

#############################################
# MAIN
#############################################
need gcloud
ensure_project
ensure_bucket
ensure_sa_and_iam
ensure_disk
ensure_vm

say "Done!"
echo "Useful next steps:"
echo "  • Tail ingest logs:  gcloud compute ssh $VM_NAME --zone=$ZONE -- 'journalctl -u polymarket -f'"
echo "  • Check timers:      gcloud compute ssh $VM_NAME --zone=$ZONE -- 'systemctl list-timers | grep polymarket'"
echo "  • Latest in GCS:     gcloud storage ls -r gs://$BUCKET/parquets/event_type=*/year=*/month=*/day=*/hour=*/events-*.parquet | tail -n1"

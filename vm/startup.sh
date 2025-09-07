#!/usr/bin/env bash
# Robust startup: load env first (with nounset off), set defaults, then proceed.
set -euo pipefail

log(){ echo "[startup] $*"; }

export HOME=/root
export USER=root
export PATH=/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin

# --- 1) Load environment from instance metadata (base64 or plain), safely ---
ENV_FILE="/etc/polymarket/polymarket.env"
mkdir -p /etc/polymarket

set +u  # disable nounset while we fetch/source env
META_VAL="$(curl -fsS -H "Metadata-Flavor: Google" \
  http://metadata.google.internal/computeMetadata/v1/instance/attributes/polymarket-env || true)"

if [ -n "$META_VAL" ]; then
  # try base64 decode; if it fails, treat as plain text
  if printf '%s' "$META_VAL" | base64 -d >/tmp/p.env 2>/dev/null; then
    install -m 0644 /tmp/p.env "$ENV_FILE"
  else
    printf '%s' "$META_VAL" > "$ENV_FILE"
    chmod 0644 "$ENV_FILE"
  fi
fi

# ensure file exists even if metadata missing
[ -f "$ENV_FILE" ] || : > "$ENV_FILE"

# export all vars from env file (if any)
set -a
. "$ENV_FILE" || true

# apply SAFE DEFAULTS so reads can’t explode later
: "${APP_DIR:=/opt/polymarket}"
: "${REPO_URL:=https://github.com/darbyhaller/polymarket-data}"
: "${REPO_BRANCH:=main}"
: "${ENTRYPOINT:=capture_real_data.py}"
: "${REQUIREMENTS_FILE:=requirements.txt}"
: "${BUCKET:=polymarket-raw-polymarket-470619}"
: "${LOCAL_RETENTION_DAYS:=3}"
: "${DATA_DISK_NAME:=polymarket-data}"
: "${PARQUET_ROOT:=/var/data/polymarket}"
: "${ROTATE_MB:=512}"
set +a
set -u  # re-enable nounset

log "Env loaded: APP_DIR=$APP_DIR REPO_BRANCH=$REPO_BRANCH BUCKET=$BUCKET"

# --- 2) Base packages + gcloud ---
dpkg --configure -a || true
apt-get update --fix-missing -y || true
apt-get install -f -y || true
apt-get update -y
apt-get install -y git python3-venv python3-pip apt-transport-https ca-certificates gnupg curl

if ! command -v gcloud >/dev/null 2>&1; then
  log "Installing Google Cloud SDK"
  echo "deb [signed-by=/usr/share/keyrings/cloud.google.gpg] http://packages.cloud.google.com/apt cloud-sdk main" > /etc/apt/sources.list.d/google-cloud-sdk.list
  curl -fsSL https://packages.cloud.google.com/apt/doc/apt-key.gpg | gpg --dearmor -o /usr/share/keyrings/cloud.google.gpg
  apt-get update -y && apt-get install -y google-cloud-cli
fi

# --- 3) Data disk mount (ext4, fstab) ---
MOUNT_POINT="$PARQUET_ROOT"
mkdir -p "$MOUNT_POINT"
DEVICE=""
if lsblk -dn -o NAME | grep -q "^sdb$"; then DEVICE="/dev/sdb"; fi
if lsblk -dn -o NAME | grep -q "^nvme1n1$"; then DEVICE="/dev/nvme1n1"; fi

if [ -n "${DEVICE:-}" ]; then
  if ! blkid "$DEVICE" >/dev/null 2>&1; then
    log "Formatting $DEVICE as ext4"
    mkfs.ext4 -m 0 -F "$DEVICE"
  fi
  UUID="$(blkid -s UUID -o value "$DEVICE")"
  if ! mount | grep -q " $MOUNT_POINT "; then
    log "Mounting $DEVICE on $MOUNT_POINT"
    mount "$DEVICE" "$MOUNT_POINT"
  fi
  grep -q "$UUID" /etc/fstab || echo "UUID=$UUID  $MOUNT_POINT  ext4  defaults  0  2" >> /etc/fstab
else
  log "WARNING: No secondary disk detected. Continuing without large buffer."
fi

# --- 4) Swap on data disk ---
SWAPFILE="$MOUNT_POINT/swapfile"
if [ ! -f "$SWAPFILE" ]; then
  fallocate -l 2G "$SWAPFILE"
  chmod 600 "$SWAPFILE"
  mkswap "$SWAPFILE"
  grep -q "^$SWAPFILE " /etc/fstab || echo "$SWAPFILE none swap sw 0 0" >> /etc/fstab
fi
swapon -a || true

# --- 5) App checkout + venv ---
mkdir -p "$APP_DIR"
cd "$APP_DIR"

git config --global user.name "VM User"
git config --global user.email "vm@polymarket.com"
git config --global --add safe.directory "$APP_DIR"

if [ ! -d .git ]; then
  log "Cloning repo $REPO_URL branch $REPO_BRANCH"
  git clone --branch "$REPO_BRANCH" "$REPO_URL" .
else
  log "Updating repo $REPO_URL branch $REPO_BRANCH"
  git remote set-url origin "$REPO_URL" || true
  git fetch origin "$REPO_BRANCH" || true
  git reset --hard "origin/$REPO_BRANCH" || true
fi

python3 -m venv .venv
source .venv/bin/activate
pip install --upgrade pip
if [ -f "$REQUIREMENTS_FILE" ]; then
  pip install -r "$REQUIREMENTS_FILE" || true
fi

# --- 6) Install services/scripts from repo (matches your new ops/ layout) ---
install -m 0755 -D "$APP_DIR/vm/sync/sync_to_gcs.sh" /usr/local/bin/sync_to_gcs.sh
install -m 0755 -D "$APP_DIR/vm/clean/clean_old_local.sh" /usr/local/bin/clean_old_local.sh
install -m 0644 -D "$APP_DIR/vm/polymarket.service" /etc/systemd/system/polymarket.service
install -m 0644 -D "$APP_DIR/vm/sync/polymarket-sync.service" /etc/systemd/system/polymarket-sync.service
install -m 0644 -D "$APP_DIR/vm/sync/polymarket-sync.timer" /etc/systemd/system/polymarket-sync.timer
install -m 0644 -D "$APP_DIR/vm/clean/polymarket-clean.service" /etc/systemd/system/polymarket-clean.service
install -m 0644 -D "$APP_DIR/vm/clean/polymarket-clean.timer" /etc/systemd/system/polymarket-clean.timer
systemctl daemon-reload
systemctl enable --now polymarket
systemctl enable --now polymarket-sync.timer
systemctl enable --now polymarket-clean.timer


log "Startup complete."

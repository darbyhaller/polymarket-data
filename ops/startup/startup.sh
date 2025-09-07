#!/usr/bin/env bash
set -euo pipefail
log(){ echo "[startup] $*"; }

export HOME=/root USER=root PATH=/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin

# env from metadata → /etc/polymarket/polymarket.env
mkdir -p /etc/polymarket
curl -fsS -H "Metadata-Flavor: Google" "http://metadata.google.internal/computeMetadata/v1/instance/attributes/polymarket-env" | base64 -d > /etc/polymarket/polymarket.env
set -a; source /etc/polymarket/polymarket.env; set +a

APP_DIR="${APP_DIR:-/opt/polymarket}" MOUNT_POINT="${PARQUET_ROOT:-/var/data/polymarket}"

# base deps + gcloud
dpkg --configure -a || true
apt-get update --fix-missing -y || true; apt-get install -f -y || true
apt-get update -y; apt-get install -y git python3-venv python3-pip apt-transport-https ca-certificates gnupg curl
command -v gcloud >/dev/null 2>&1 || { echo "deb [signed-by=/usr/share/keyrings/cloud.google.gpg] http://packages.cloud.google.com/apt cloud-sdk main" > /etc/apt/sources.list.d/google-cloud-sdk.list; curl -fsSL https://packages.cloud.google.com/apt/doc/apt-key.gpg | gpg --dearmor -o /usr/share/keyrings/cloud.google.gpg; apt-get update -y && apt-get install -y google-cloud-cli; }

# mount data disk
mkdir -p "$MOUNT_POINT"
DEVICE=""; lsblk -dn -o NAME | grep -q "^sdb$" && DEVICE="/dev/sdb" || true; lsblk -dn -o NAME | grep -q "^nvme1n1$" && DEVICE="/dev/nvme1n1" || true
if [ -n "$DEVICE" ]; then blkid "$DEVICE" >/dev/null 2>&1 || mkfs.ext4 -m 0 -F "$DEVICE"; UUID=$(blkid -s UUID -o value "$DEVICE"); mount | grep -q " $MOUNT_POINT " || mount "$DEVICE" "$MOUNT_POINT"; grep -q "$UUID" /etc/fstab || echo "UUID=$UUID  $MOUNT_POINT  ext4  defaults  0  2" >> /etc/fstab; else log "WARNING: no secondary disk"; fi

# swap
SWAPFILE="$MOUNT_POINT/swapfile"; [ -f "$SWAPFILE" ] || { fallocate -l 2G "$SWAPFILE"; chmod 600 "$SWAPFILE"; mkswap "$SWAPFILE"; echo "$SWAPFILE none swap sw 0 0" >> /etc/fstab; }
swapon -a || true

# app
mkdir -p "$APP_DIR"; cd "$APP_DIR"
git config --global user.name "VM User"; git config --global user.email "vm@polymarket.com"; git config --global --add safe.directory "$APP_DIR"
[ -d .git ] || git clone --branch "$REPO_BRANCH" "$REPO_URL" .
git remote set-url origin "$REPO_URL" || true; git fetch origin "$REPO_BRANCH" || true; git reset --hard "origin/$REPO_BRANCH" || true
python3 -m venv .venv; source .venv/bin/activate; pip install --upgrade pip; [ -f "$REQUIREMENTS_FILE" ] && pip install -r "$REQUIREMENTS_FILE" || true

# install system bits from repo (no heredocs)
install -m 0755 -D "$APP_DIR/ops/sync/sync_to_gcs.sh" /usr/local/bin/sync_to_gcs.sh
install -m 0755 -D "$APP_DIR/ops/clean/clean_old_local.sh" /usr/local/bin/clean_old_local.sh
install -m 0644 -D "$APP_DIR/ops/startup/polymarket.service" /etc/systemd/system/polymarket.service
install -m 0644 -D "$APP_DIR/ops/sync/polymarket-sync.service" /etc/systemd/system/polymarket-sync.service
install -m 0644 -D "$APP_DIR/ops/sync/polymarket-sync.timer" /etc/systemd/system/polymarket-sync.timer
install -m 0644 -D "$APP_DIR/ops/clean/polymarket-clean.service" /etc/systemd/system/polymarket-clean.service
install -m 0644 -D "$APP_DIR/ops/clean/polymarket-clean.timer" /etc/systemd/system/polymarket-clean.timer

systemctl daemon-reload
systemctl enable --now polymarket
systemctl enable --now polymarket-sync.timer
systemctl enable --now polymarket-clean.timer
log "Startup complete"

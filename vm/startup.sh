#!/usr/bin/env bash
set -euo pipefail

# env so git/venv behave
export HOME=/root
export USER=root
export PATH=/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin
mkdir -p "$HOME"

# --- defaults (always set) ---
APP_DIR=${APP_DIR:-/opt/polymarket}
REPO_URL=${REPO_URL:-https://github.com/darbyhaller/polymarket-data}
REPO_BRANCH=${REPO_BRANCH:-main}
ENTRYPOINT=${ENTRYPOINT:-capture_real_data.py}
REQUIREMENTS_FILE=${REQUIREMENTS_FILE:-requirements.txt}
BUCKET=${BUCKET:-polymarket-raw-polymarket-470619}
LOCAL_RETENTION_DAYS=${LOCAL_RETENTION_DAYS:-3}
DATA_DISK_NAME=${DATA_DISK_NAME:-polymarket-data}
PARQUET_ROOT=${PARQUET_ROOT:-/var/data/polymarket}

# --- overlay from instance metadata if present (base64 or plain) ---
set +u
META_VAL="$(curl -fsS -H "Metadata-Flavor: Google" http://metadata.google.internal/computeMetadata/v1/instance/attributes/polymarket-env || true)"
if [ -n "$META_VAL" ]; then
  if printf '%s' "$META_VAL" | base64 -d >/tmp/p.env 2>/dev/null; then . /tmp/p.env || true
  else printf '%s\n' "$META_VAL" >/tmp/p.env; . /tmp/p.env || true; fi
fi
set -u

# --- write merged env for services ---
mkdir -p /etc/polymarket
cat >/etc/polymarket/polymarket.env <<EOF
APP_DIR="$APP_DIR"
REPO_URL="$REPO_URL"
REPO_BRANCH="$REPO_BRANCH"
ENTRYPOINT="$ENTRYPOINT"
REQUIREMENTS_FILE="$REQUIREMENTS_FILE"
BUCKET="$BUCKET"
LOCAL_RETENTION_DAYS="$LOCAL_RETENTION_DAYS"
DATA_DISK_NAME="$DATA_DISK_NAME"
PARQUET_ROOT="$PARQUET_ROOT"
EOF
chmod 0644 /etc/polymarket/polymarket.env

# --- base packages + gcloud ---
dpkg --configure -a || true
apt-get update --fix-missing -y || true
apt-get install -f -y || true
apt-get update -y
apt-get install -y git python3-venv python3-pip ca-certificates curl gnupg
if ! command -v gcloud >/dev/null 2>&1; then
  echo "deb [signed-by=/usr/share/keyrings/cloud.google.gpg] http://packages.cloud.google.com/apt cloud-sdk main" >/etc/apt/sources.list.d/google-cloud-sdk.list
  curl -fsSL https://packages.cloud.google.com/apt/doc/apt-key.gpg | gpg --dearmor -o /usr/share/keyrings/cloud.google.gpg
  apt-get update -y && apt-get install -y google-cloud-cli
fi

if ! dpkg -s google-cloud-ops-agent >/dev/null 2>&1; then
  curl -sS https://dl.google.com/cloudagents/add-google-cloud-ops-agent-repo.sh | bash
  apt-get update -y
  apt-get install -y google-cloud-ops-agent
fi

mkdir -p /etc/google-cloud-ops-agent
cat >/etc/google-cloud-ops-agent/config.yaml <<'YAML'
logging:
  receivers:
    polymarket_journald:
      type: systemd_journald
    startup_journald:
      type: systemd_journald
  service:
    pipelines:
      polymarket:
        receivers: [polymarket_journald]
      startup:
        receivers: [startup_journald]
metrics:
  receivers:
    hostmetrics:
      type: hostmetrics
  service:
    pipelines:
      default:
        receivers: [hostmetrics]
YAML

systemctl enable --now google-cloud-ops-agent
systemctl restart google-cloud-ops-agent || true

# --- data disk mount + fstab ---
MOUNT_POINT="$PARQUET_ROOT"
mkdir -p "$MOUNT_POINT"
DEVICE=""; lsblk -dn -o NAME | grep -q '^sdb$' && DEVICE=/dev/sdb || true
lsblk -dn -o NAME | grep -q '^nvme1n1$' && DEVICE=/dev/nvme1n1 || true
if [ -n "$DEVICE" ]; then
  blkid "$DEVICE" >/dev/null 2>&1 || mkfs.ext4 -m 0 -F "$DEVICE"
  UUID="$(blkid -s UUID -o value "$DEVICE")"
  mount | grep -q " $MOUNT_POINT " || mount "$DEVICE" "$MOUNT_POINT"
  grep -q "$UUID" /etc/fstab || echo "UUID=$UUID  $MOUNT_POINT  ext4  defaults  0  2" >> /etc/fstab
fi

# --- swap on data disk (no dup fstab) ---
SWAPFILE="$MOUNT_POINT/swapfile"
if [ ! -f "$SWAPFILE" ]; then
  fallocate -l 2G "$SWAPFILE"; chmod 600 "$SWAPFILE"; mkswap "$SWAPFILE"
fi
grep -q "^$SWAPFILE " /etc/fstab || echo "$SWAPFILE none swap sw 0 0" >> /etc/fstab
swapon -a || true

# --- app checkout + venv ---
mkdir -p "$APP_DIR"; cd "$APP_DIR"
git config --system user.name "VM User" || true
git config --system user.email "vm@polymarket.com" || true
git config --system --add safe.directory "$APP_DIR" || true

[ -d .git ] || git clone --branch "$REPO_BRANCH" "$REPO_URL" .
git remote set-url origin "$REPO_URL" || true
git fetch origin "$REPO_BRANCH" || true
git reset --hard "origin/$REPO_BRANCH" || true

python3 -m venv .venv
. .venv/bin/activate
pip install --upgrade pip
[ -f "$REQUIREMENTS_FILE" ] && pip install -r "$REQUIREMENTS_FILE" || true

# --- install scripts/units from vm/ and start ---
install -m 0755 -D "$APP_DIR/vm/sync/sync_to_gcs.sh"      /usr/local/bin/sync_to_gcs.sh
install -m 0755 -D "$APP_DIR/vm/clean/clean_old_local.sh"  /usr/local/bin/clean_old_local.sh
install -m 0644 -D "$APP_DIR/vm/polymarket.service"                /etc/systemd/system/polymarket.service
install -m 0644 -D "$APP_DIR/vm/sync/polymarket-sync.service"      /etc/systemd/system/polymarket-sync.service
install -m 0644 -D "$APP_DIR/vm/sync/polymarket-sync.timer"        /etc/systemd/system/polymarket-sync.timer
install -m 0644 -D "$APP_DIR/vm/clean/polymarket-clean.service"    /etc/systemd/system/polymarket-clean.service
install -m 0644 -D "$APP_DIR/vm/clean/polymarket-clean.timer"      /etc/systemd/system/polymarket-clean.timer

systemctl daemon-reload
systemctl enable --now polymarket-sync.timer
systemctl enable --now polymarket-clean.timer
systemctl enable --now polymarket

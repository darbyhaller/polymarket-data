#!/usr/bin/env bash
set -Eeuo pipefail

# -------- CONFIG --------
INST="${INST:-polymarket-vm}"          # VM name
ZONE="${ZONE:-us-central1-a}"          # VM zone
SIZE_GB="${SIZE_GB:-373}"              # desired PD-SSD size if we must create
DISK_PREFIX="${DISK_PREFIX:-scratch-ssd-}"
TMP_MNT="${TMP_MNT:-/mnt/scratch}"
PY_CMD='/opt/polymarket/.venv/bin/python /opt/polymarket/preprocess_to_l1.py /var/data/polymarket/year=2025/month=08/day=31 --cloud-output /var/data/polymarket/l1 --verbose'

# If you also want to delete a reused scratch disk on failure, set:
DELETE_EXISTING_ON_FAILURE="${DELETE_EXISTING_ON_FAILURE:-0}"
# ------------------------

REGION="${ZONE%-*}"
CREATED_NEW=0
ATTACHED_BY_US=0
DISK="${DISK:-}"  # will set later

cleanup() {
  status=$?
  echo "[cleanup] status=$status"
  # Try to unmount on the VM (only if we attached)
  if [[ "${ATTACHED_BY_US}" -eq 1 && -n "${DISK:-}" ]]; then
    echo "[cleanup] attempting remote unmount ${TMP_MNT}"
    gcloud compute ssh "$INST" --zone "$ZONE" --command "sudo umount ${TMP_MNT} || true" >/dev/null 2>&1 || true
  fi
  # Detach if we attached
  if [[ "${ATTACHED_BY_US}" -eq 1 && -n "${DISK:-}" ]]; then
    echo "[cleanup] detaching disk ${DISK} from ${INST}"
    gcloud compute instances detach-disk "$INST" --disk "$DISK" --zone "$ZONE" --quiet || true
  fi
  # Delete:
  # - ALWAYS delete if we created it this run
  # - If failure and we attached an existing disk AND DELETE_EXISTING_ON_FAILURE=1, delete it too
  if [[ -n "${DISK:-}" ]]; then
    if [[ "${CREATED_NEW}" -eq 1 ]]; then
      echo "[cleanup] deleting created disk ${DISK}"
      gcloud compute disks delete "$DISK" --zone "$ZONE" --quiet || true
    elif [[ "$status" -ne 0 && "${ATTACHED_BY_US}" -eq 1 && "${DELETE_EXISTING_ON_FAILURE}" -eq 1 ]]; then
      echo "[cleanup] deleting reused disk ${DISK} due to failure (DELETE_EXISTING_ON_FAILURE=1)"
      gcloud compute disks delete "$DISK" --zone "$ZONE" --quiet || true
    fi
  fi
  exit $status
}
trap cleanup EXIT INT TERM

echo "Instance: $INST  Zone: $ZONE  Region: $REGION"

# 1) Reuse an existing scratch disk in this zone, else create one
EXISTING_DISK="$(gcloud compute disks list \
  --filter="zone:($ZONE) AND name~^${DISK_PREFIX}" \
  --format="value(name)" | head -n1 || true)"

if [[ -n "$EXISTING_DISK" ]]; then
  DISK="$EXISTING_DISK"
  echo "Found existing scratch disk: $DISK"
else
  # Best-effort quota check (non-fatal if CLI format differs)
  read -r LIMIT USAGE <<<"$(gcloud compute regions describe "$REGION" --flatten="quotas[]" \
    --format="value(quotas.metric,quotas.limit,quotas.usage)" 2>/dev/null \
    | awk '$1=="SSD_TOTAL_GB"{print $2,$3}')"
  if [[ -n "${LIMIT:-}" && -n "${USAGE:-}" ]]; then
    REM=$(( ${LIMIT%.*} - ${USAGE%.*} ))
    echo "SSD_TOTAL_GB: limit=${LIMIT} GiB, usage=${USAGE} GiB, remaining=${REM} GiB"
    if (( REM <= 0 )); then
      echo "No SSD quota remaining in $REGION"; exit 1
    fi
    if (( SIZE_GB > REM )); then
      echo "Requested ${SIZE_GB}GiB > remaining ${REM}GiB; reducing to ${REM}GiB"
      SIZE_GB="$REM"
    fi
  fi
  DISK="${DISK_PREFIX}$(date +%s)"
  echo "Creating $DISK (${SIZE_GB}GiB pd-ssd)..."
  gcloud compute disks create "$DISK" --size="${SIZE_GB}GB" --type=pd-ssd --zone "$ZONE" --quiet
  CREATED_NEW=1
fi

# 2) Attach if needed
USERS="$(gcloud compute disks describe "$DISK" --zone "$ZONE" --format='value(users)' || true)"
if [[ -z "$USERS" ]]; then
  echo "Attaching $DISK to $INST..."
  gcloud compute instances attach-disk "$INST" --disk "$DISK" --zone "$ZONE" --quiet
  ATTACHED_BY_US=1
else
  if grep -q "/instances/${INST}$" <<<"$USERS"; then
    echo "Disk $DISK is already attached to $INST."
  else
    echo "Disk $DISK is attached to another instance:"
    echo "  $USERS"
    echo "Detach there or choose a different disk."
    exit 1
  fi
fi

# 3) Prepare FS, mount, run job (remote)
gcloud compute ssh "$INST" --zone "$ZONE" --command "bash -lc 'set -euo pipefail
DISK=\"$DISK\"
TMP_MNT=\"$TMP_MNT\"
PY_CMD=\"$PY_CMD\"

# Wait for device to appear and find the actual device path
echo \"Waiting for disk \$DISK to be available...\"
for i in {1..30}; do
  # First try the by-id paths
  if DEV=\$(ls /dev/disk/by-id/google-\$DISK 2>/dev/null); then
    echo \"Found device via google-\$DISK: \$DEV\"
    break
  elif DEV=\$(ls /dev/disk/by-id/scsi-0Google_PersistentDisk_\$DISK 2>/dev/null); then
    echo \"Found device via scsi path: \$DEV\"
    break
  else
    # Find the most recent google-persistent-disk that points to an unformatted device
    DEV=\$(ls -t /dev/disk/by-id/google-persistent-disk-* 2>/dev/null | head -1)
    if [[ -n \"\$DEV\" ]]; then
      # Check if this device has no filesystem (newly attached)
      if ! sudo blkid \"\$DEV\" >/dev/null 2>&1; then
        echo \"Found newest unformatted device: \$DEV\"
        break
      fi
    fi
    sleep 2
  fi
  if [[ \$i -eq 30 ]]; then
    echo \"Error: Disk device not found after 60 seconds.\"
    echo \"Available Google devices:\"
    ls -la /dev/disk/by-id/google-* 2>/dev/null || true
    echo \"Block devices:\"
    lsblk || true
    exit 1
  fi
done
echo \"Using device: \$DEV\"

# Install mkfs.ext4 if missing
if ! command -v mkfs.ext4 >/dev/null 2>&1; then
  if command -v apt-get >/dev/null 2>&1; then sudo apt-get update -y && sudo apt-get install -y e2fsprogs;
  elif command -v dnf >/dev/null 2>&1; then sudo dnf install -y e2fsprogs;
  elif command -v yum >/dev/null 2>&1; then sudo yum install -y e2fsprogs;
  else echo Need e2fsprogs >&2; exit 1; fi
fi

# Only mkfs if no FS (avoid wiping)
if ! sudo blkid \"\$DEV\" >/dev/null 2>&1; then
  sudo mkfs.ext4 -F -m1 -E lazy_itable_init=0,lazy_journal_init=0 \"\$DEV\"
fi

sudo mkdir -p \"\$TMP_MNT\"
sudo mount \"\$DEV\" \"\$TMP_MNT\" || true
sudo chown \"\$USER\":\"\$USER\" \"\$TMP_MNT\"
df -h \"\$TMP_MNT\"

echo Running job with TMPDIR=\$TMP_MNT ...
sudo systemd-run --wait --collect --setenv=TMPDIR=\"\$TMP_MNT\" \
  -p MemoryMax=2G -p MemorySwapMax=0 \
  \$PY_CMD

echo Job complete on VM.
'"

echo "Success path completed."
# Normal exit will still hit trap/cleanup:
# - detach (if we attached)
# - delete if we created

echo "THIS IS DEPRECATED, RUNS SUPER SLOW. JUST RUN PREPROCESS LOCALLY"
#!/usr/bin/env bash
set -Eeuo pipefail

# -------- CONFIG --------
INST="${INST:-polymarket-vm}"          # VM name
ZONE="${ZONE:-us-central1-a}"          # VM zone
SIZE_GB="${SIZE_GB:-373}"              # desired PD-SSD size if we must create
DISK_PREFIX="${DISK_PREFIX:-scratch-ssd-}"
TMP_MNT="${TMP_MNT:-/mnt/scratch}"
PY_CMD='/opt/polymarket/.venv/bin/python /opt/polymarket/preprocess_to_l1.py /var/data/polymarket/year=2025/month=08/day=31/hour=00 --cloud-output /var/data/polymarket/l1 --verbose'

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
  gcloud compute instances attach-disk "$INST" --disk "$DISK" --zone "$ZONE" --quiet --device-name "$DISK"
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
echo "[local] preparing remote runner..."
REMOTE_TMP="/tmp/run_preprocess_remote.$(date +%s).sh"

# write the remote script locally (single-quoted heredoc: no local expansion)
cat > /tmp/_remote_runner.sh <<'REMOTE'
#!/usr/bin/env bash
set -euo pipefail
set -x
PS4='+ remote:${LINENO}: '

: "${DISK:?missing DISK}"
: "${TMP_MNT:?missing TMP_MNT}"
: "${PY_CMD:?missing PY_CMD}"

echo "[remote] env: DISK=${DISK} TMP_MNT=${TMP_MNT}"
sudo udevadm settle || true

DEV=""
for i in {1..60}; do
  if [[ -e "/dev/disk/by-id/google-${DISK}" ]]; then
    DEV="$(readlink -f "/dev/disk/by-id/google-${DISK}")"
    echo "[remote] found by device-name: ${DEV}"
    break
  fi
  cand=$(ls -t /dev/disk/by-id/google-persistent-disk-* 2>/dev/null | head -1 || true)
  if [[ -n "${cand}" ]]; then
    real="$(readlink -f "${cand}")"
    if ! findmnt -S "${real}" >/dev/null 2>&1; then
      DEV="${real}"
      echo "[remote] fallback candidate: ${cand} -> ${DEV}"
      break
    fi
  fi
  sleep 2
done

if [[ -z "${DEV}" ]]; then
  echo "[remote] ERROR: disk not found after 120s"
  ls -la /dev/disk/by-id/google-* || true
  lsblk -f || true
  exit 1
fi
echo "[remote] using device: ${DEV}"

FSTYPE="$(lsblk -no FSTYPE "${DEV}" | tr -d '[:space:]' || true)"
if [[ -z "${FSTYPE}" ]]; then
  echo "[remote] mkfs.ext4 on ${DEV}"
  mkfs.ext4 -F -m1 -E lazy_itable_init=0,lazy_journal_init=0 "${DEV}"
fi

mkdir -p "${TMP_MNT}"
if ! findmnt "${TMP_MNT}" >/dev/null 2>&1; then
  mount -o noatime,nodiratime "${DEV}" "${TMP_MNT}"
fi
chown "${SUDO_USER:-$USER}:${SUDO_USER:-$USER}" "${TMP_MNT}" || true
df -h "${TMP_MNT}" || true

echo "[remote] running job with TMPDIR=${TMP_MNT}"
systemd-run --wait --collect --pty --setenv=TMPDIR="${TMP_MNT}" \
  -p MemoryMax=2G -p MemorySwapMax=0 \
  ${PY_CMD}

echo "[remote] job complete."
REMOTE

# ship it to the VM and run it
gcloud compute scp --zone "$ZONE" /tmp/_remote_runner.sh "$INST:$REMOTE_TMP"
rm -f /tmp/_remote_runner.sh

echo "[local] executing remote runner..."
gcloud compute ssh "$INST" --zone "$ZONE" -- \
  "sudo DISK=$(printf %q "$DISK") TMP_MNT=$(printf %q "$TMP_MNT") PY_CMD=$(printf %q "$PY_CMD") bash -xe $REMOTE_TMP; rc=\$?; rm -f $REMOTE_TMP; exit \$rc"

echo "[local] remote script finished."
echo "THIS IS DEPRECATED, RUNS SUPER SLOW. JUST RUN PREPROCESS LOCALLY"
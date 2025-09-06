# Polymarket Data Capture — Developer Guide

## 🔑 Quick Cheat Sheet (Most Used Commands)

### SSH in
```bash
gcloud compute ssh polymarket-vm --zone=us-central1-a
```

### One-time env vars (in any shell)
```bash
PROJECT=$(gcloud config get-value project)
BUCKET="polymarket-raw-$PROJECT"
echo "Using bucket: gs://$BUCKET"
```

### Restart capture service & watch logs
```bash
sudo systemctl restart polymarket
journalctl -u polymarket -f
```

### Peek latest 5 events in GCS (zsh-safe, newest lines)
```zsh
setopt noglob
LATEST=$(gcloud storage ls -r gs://$BUCKET/parquets/year=*/month=*/day=*/hour=*/events-*.jsonl.gz | tail -n1)
unsetopt noglob
gcloud storage cat "$LATEST" | gunzip -c | \
python3 - <<'PYCODE'
import sys, json
from collections import deque
buf = deque(maxlen=5)
for line in sys.stdin:
    buf.append(line)
for line in buf:
    try:
        print(json.dumps(json.loads(line), indent=2))
    except Exception:
        print(line.strip())
PYCODE
```

### Download yesterday's data locally
```bash
Y=$(date -u -d "yesterday" +%Y)
M=$(date -u -d "yesterday" +%m)
D=$(date -u -d "yesterday" +%d)
gcloud storage cp -r gs://$BUCKET/parquets/year=$Y/month=$M/day=$D ./data/
```

### Check sync timer
```bash
systemctl list-timers | grep polymarket-sync
journalctl -u polymarket-sync.service -n 20 --no-pager
```

### Switch gcloud accounts
```bash
gcloud config configurations activate personal   # or work
gcloud config list --format="value(core.account, core.project)"
```

---

## 1. VM + Disk

- **VM**: custom 1 vCPU / 4 GB RAM in us-central1-a
- **Data disk**: 200 GB mounted at `/var/data/polymarket`
- **Code**: `/opt/polymarket` (Git repo + venv)

**Tip**: set defaults so you don't need `--zone` each time:
```bash
gcloud config set compute/zone us-central1-a
```

## 2. Git Workflow

### Clone your repo into /opt/polymarket
```bash
cd /opt
sudo git clone git@github.com:youruser/polymarket.git
cd polymarket
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

If using the HTTPS URL instead of SSH, use:
```bash
git clone https://github.com/youruser/polymarket.git
```

### Deploy changes
```bash
git pull origin main
sudo systemctl restart polymarket
```

## 3. Systemd Services

### Ingest (`polymarket.service`)
Runs `capture_real_data.py` continuously.

```bash
sudo systemctl status polymarket --no-pager
sudo systemctl restart polymarket
journalctl -u polymarket -f
```

### Sync (`polymarket-sync.timer`)
Runs hourly → syncs `/var/data/polymarket` → `gs://polymarket-raw-<PROJECT>/parquets`.

```bash
systemctl list-timers | grep polymarket-sync
journalctl -u polymarket-sync.service --since "6 hours ago" --no-pager
```

### Cleanup (`polymarket-clean.timer`)
Runs daily → deletes local `.jsonl.gz` older than 3 days (skips current UTC hour).

```bash
systemctl list-timers | grep polymarket-clean
journalctl -u polymarket-clean.service --since "2 days ago" --no-pager
```

## 4. GCS Lifecycle

Objects transition to **ARCHIVE** after 365 days, never deleted.

### Check lifecycle policy
```bash
gsutil lifecycle get gs://polymarket-raw-$PROJECT
# or
gcloud storage buckets describe gs://polymarket-raw-$PROJECT --format=json | jq .lifecycle
```

## 5. Inspecting Data

### Find latest file
```bash
LATEST=$(gcloud storage ls -r gs://$BUCKET/parquets/year=*/month=*/day=*/hour=*/events-*.jsonl.gz | tail -n1)
```

### Preview events (safe pretty-print per line)
```bash
gcloud storage cat "$LATEST" | gunzip -c | \
python3 -c 'import sys, json
from itertools import islice
for line in islice(sys.stdin, 5):
    print(json.dumps(json.loads(line), indent=2))'
```

### Count lines
```bash
gcloud storage cat "$LATEST" | gunzip -c | wc -l
```

## 6. Downloading for Analysis

### Yesterday's partition
```bash
Y=$(date -u -d "yesterday" +%Y)
M=$(date -u -d "yesterday" +%m)
D=$(date -u -d "yesterday" +%d)
gcloud storage cp -r gs://$BUCKET/parquets/year=$Y/month=$M/day=$D ./data/
```

### Specific hours
```bash
gcloud storage cp -r gs://$BUCKET/parquets/year=2025/month=08/day=30/hour=1[2-5]/ ./data/
```

## 7. gcloud Configs

### List configs
```bash
gcloud config configurations list
```

### Create & switch
```bash
gcloud config configurations create personal
gcloud auth login
gcloud config set project polymarket-470619
gcloud config configurations activate personal
```

### Check active configuration
```bash
gcloud config list --format="value(core.account, core.project)"
```

## 8. Health Checks

### Service status
```bash
sudo systemctl is-active polymarket
```

### Disk usage
```bash
df -h /var/data/polymarket
```

### Sync logs
```bash
journalctl -u polymarket-sync.service -n 20 --no-pager
```

## 9. Forcing a Sync to GCS (debugging)

Normally the sync runs hourly via `polymarket-sync.timer`.  
To force it right now:

```bash
# Run the sync service manually
sudo systemctl start polymarket-sync.service

# Or call the sync script directly
/usr/local/bin/sync_to_gcs.sh
```
# Polymarket Data Capture — Developer Guide

## 🔑 Quick Cheat Sheet (Most Used Commands)

### Creation Chat
[ChatGPT](https://chatgpt.com/c/68b34acb-76e4-8320-9783-e8ae2299c4b3)

### Restart capture service & watch logs
```bash
sudo systemctl restart polymarket
journalctl -u polymarket -f
```

### Peek latest 5 events in GCS
```bash
LATEST=$(gcloud storage ls -r gs://$BUCKET/raw/year=*/month=*/day=*/hour=*/events-*.jsonl.gz | tail -n1)
gcloud storage cat "$LATEST" | gunzip -c | head -n 5
```

### Download yesterday's data locally
```bash
Y=$(date -u -d "yesterday" +%Y)
M=$(date -u -d "yesterday" +%m)
D=$(date -u -d "yesterday" +%d)
gcloud storage cp -r gs://$BUCKET/raw/year=$Y/month=$M/day=$D ./data/
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

- **VM**: e2-standard-1 in us-central1-a
- **Data disk**: 200 GB mounted at `/var/data/polymarket`
- **Code**: `/opt/polymarket` (Git repo + venv)

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
Runs hourly → syncs `/var/data/polymarket` → `gs://polymarket-raw-<PROJECT>/raw`.

```bash
systemctl list-timers | grep polymarket-sync
journalctl -u polymarket-sync.service --since "6 hours ago" --no-pager
```

## 4. GCS Lifecycle

Objects transition to **ARCHIVE** after 365 days, never deleted.

### Check lifecycle policy
```bash
gsutil lifecycle get gs://polymarket-raw-<PROJECT>
```

## 5. Inspecting Data

### Find latest file
```bash
LATEST=$(gcloud storage ls -r gs://$BUCKET/raw/year=*/month=*/day=*/hour=*/events-*.jsonl.gz | tail -n1)
```

### Preview events
```bash
gcloud storage cat "$LATEST" | gunzip -c | head -n 5
```

### Pretty-print with jq
```bash
gcloud storage cat "$LATEST" | gunzip -c | head -n 5 | jq .
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
gcloud storage cp -r gs://$BUCKET/raw/year=$Y/month=$M/day=$D ./data/
```

### Specific hours
```bash
gcloud storage cp -r gs://$BUCKET/raw/year=2025/month=08/day=30/hour=1[2-5]/ ./data/
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
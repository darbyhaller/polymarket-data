# Polymarket VM System Architecture

## Overview

This deployment runs a single Google Cloud VM hosting the Polymarket application stack. The VM executes Python services, persists market data, and mounts a dedicated persistent disk for storage. Swap space is configured for stability under memory pressure.

## Components

### 1. Compute

**Google Compute Engine VM:**

- Machine type: `e2-custom-1-4096` (1 vCPU, 4 GB RAM)
- Zone: `us-central1-a`
- OS: Ubuntu 22.04 LTS
- Service account attached with `devstorage.read_write` scope

**Swap configuration:**

- 2 GB swap file on the attached data disk (`/var/data/polymarket/swapfile`)
- Enabled via `deploy_polymarket.sh` to prevent OOM kills

### 2. Storage

**Boot disk:** 10 GB root filesystem, OS + app code under `/opt/polymarket`

**Persistent data disk** (`/dev/sdb`, 200 GB):

- Mounted at `/var/data/polymarket`
- Stores large files, logs, caches, and swap file
- Marked `auto-delete=no` to survive VM recreation

### 3. Application

**Source code location:** `/opt/polymarket`

**Language/runtime:** Python 3 with a local virtual environment (`.venv`)

**Key scripts:**

- `writer.py` — main service process
- `fetch_markets.py`, `capture_real_data.py` — data ingestion scripts
- `health_check.sh` — prints CPU, memory, disk, and process status

**Process behavior:**

- Python service typically runs at ~30–40% CPU, ~13% memory usage
- Logs and caches can be relocated to `/var/data/polymarket` to offload the small root disk

### 4. Version Control

- Git repository initialized under `/opt/polymarket`
- Safe directory configured for Git (`git config --global --add safe.directory /opt/polymarket`)
- `.git` and `.venv` are the largest hidden directories (~500 MB combined)

### 5. Deployment

**Script:** `deploy_polymarket.sh`

- Installs dependencies
- Configures swap file (idempotent logic)
- Activates Python virtual environment
- Starts the application

## Operational Considerations

**Monitoring:** Use `health_check.sh` or `uptime`, `free -h`, `df -h` for quick checks.

**Scaling:** Current VM is suitable for light workloads. If sustained CPU load approaches 1.0 (100%), consider scaling to 2 vCPUs.

**Persistence:** Application data and swap live on the 200 GB persistent disk, ensuring durability beyond VM lifecycle.

**Maintenance:**

- Clean `.git` and `.venv` if disk pressure occurs
- Ensure logs and caches are redirected to `/var/data/polymarket`
- Apply security updates (`unattended-upgrades`)

## Diagram (simplified)

```
+----------------------------+
| Google Cloud Project       |
|   polymarket-470619        |
+----------------------------+
            |
            v
+----------------------------+
| Compute Engine VM          |
| - e2-custom-1-4096         |
| - Ubuntu 22.04 LTS         |
| - Service account attached |
+----------------------------+
   |           |
   |           +----------------------+
   |                                  |
   v                                  v
/opt/polymarket                /var/data/polymarket
(App code + venv + .git)       (Persistent disk: 200GB)
                               - Swapfile
                               - Logs / Cache / Output

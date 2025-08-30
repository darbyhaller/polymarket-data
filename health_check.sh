#!/usr/bin/env bash
set -euo pipefail

echo "===== VM Health Check ====="

# CPU load (1m, 5m, 15m averages)
echo
echo "[CPU Load]"
uptime

# RAM usage
echo
echo "[Memory]"
free -h

# Top 5 memory-hogging processes
echo
echo "[Top 5 processes by RAM]"
ps -eo pid,ppid,cmd,%mem,%cpu --sort=-%mem | head -n 6

# Disk usage root + data mount
echo
echo "[Disk Usage]"
df -h / /var/data/polymarket || true

# Inode usage (safety check for too many files)
echo
echo "[Inodes]"
df -i /var/data/polymarket || true

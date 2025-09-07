#!/usr/bin/env bash
set -euo pipefail
set -a; [ -f /etc/polymarket/polymarket.env ] && source /etc/polymarket/polymarket.env; set +a

ROOT="${PARQUET_ROOT:-/var/data/polymarket}/parquets"
[ -d "$ROOT" ] || exit 0
CURHOUR="$(date -u +%H)"
RETENTION_MINUTES=$(( ${LOCAL_RETENTION_DAYS:-3} * 24 * 60 ))

find "$ROOT" -type f -name '*.parquet' ! -path "*/hour=${CURHOUR}/*" -mmin +$RETENTION_MINUTES -print -delete
find "$ROOT" -type d -empty -prune -exec rmdir -p -- {} + 2>/dev/null || true

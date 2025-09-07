#!/usr/bin/env bash
set -euo pipefail
set -a; [ -f /etc/polymarket/polymarket.env ] && source /etc/polymarket/polymarket.env; set +a
SRC="${PARQUET_ROOT:-/var/data/polymarket}/parquets"
DST="gs://${BUCKET}/parquets"
CURHOUR="$(date -u +%H)"
gcloud storage rsync -r -x "(\.inprogress$|\.tmp$|hour=${CURHOUR}(/|$))" "$SRC" "$DST"

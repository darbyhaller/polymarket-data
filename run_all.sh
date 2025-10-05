#!/usr/bin/env zsh
set -euo pipefail

if (( $# == 0 )); then
  echo "usage: $0 <command to run remotely>"
  exit 1
fi

CMD="$*"

while read -r NAME ZONE; do
  echo "Running on $NAME ($ZONE): $CMD"
  gcloud compute ssh "$NAME" --zone "$ZONE" --command "$CMD" -- -n
done < <(gcloud compute instances list --format="value(name,zone)")

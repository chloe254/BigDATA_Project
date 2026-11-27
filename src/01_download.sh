#!/usr/bin/env bash
# Download the raw ATP match files (Jeff Sackmann, CC BY-NC-SA 4.0) and check the 1-5 MB rule.
# Usage: SEASONS="2021 2022 2023 2024 2025" bash src/01_download.sh
set -euo pipefail

SEASONS="${SEASONS:-2021 2022 2023 2024 2025}"
BASE_URL="https://huggingface.co/datasets/Aneeshers/tennis-sackmann-archive/resolve/main/atp"
OUT_DIR="${OUT_DIR:-data/raw}"
mkdir -p "$OUT_DIR"

for season in $SEASONS; do
  curl -fsSL "$BASE_URL/atp_matches_${season}.csv" -o "$OUT_DIR/atp_matches_${season}.csv"
done

# Provenance file: required in the README (download date, source, size)
total_bytes=$(cat "$OUT_DIR"/atp_matches_*.csv | wc -c)
{
  echo "source=$BASE_URL"
  echo "license=CC BY-NC-SA 4.0"
  echo "downloaded_at=$(date -u +%Y-%m-%dT%H:%M:%SZ)"
  echo "seasons=$SEASONS"
  echo "total_bytes=$total_bytes"
  (cd "$OUT_DIR" && sha256sum atp_matches_*.csv)
} > "$OUT_DIR/MANIFEST.txt"

ls -l "$OUT_DIR"
echo "Total raw size: $total_bytes bytes ($(awk "BEGIN{printf \"%.2f\", $total_bytes/1000000}") MB)"

# Project rule: 1 MB <= raw size <= 5 MB (-1 point otherwise). Checked against 5,000,000 bytes to stay safe.
if (( total_bytes < 1000000 || total_bytes > 5000000 )); then
  echo "ERROR: raw size out of the 1-5 MB range, adjust SEASONS." >&2
  exit 1
fi
echo "OK: size within the 1-5 MB range."

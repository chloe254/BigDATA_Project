#!/usr/bin/env bash
# Upload the raw files to the bronze layer, unchanged (bronze = exact copy of the source).
# Alternative used in the report: k8s/job-upload-bronze.yaml runs the ingestion inside the cluster.
set -euo pipefail

: "${DATA_ROOT:?DATA_ROOT must be set, e.g. s3://$KUBERNETES_NAMESPACE/bigdata_project}"
IN_DIR="${IN_DIR:-data/raw}"

# Offline mode (DATA_ROOT is a local folder): plain copy, same layout as on S3
if [[ "$DATA_ROOT" != s3://* ]]; then
  mkdir -p "$DATA_ROOT/bronze/atp"
  cp "$IN_DIR"/atp_matches_*.csv "$IN_DIR"/MANIFEST.txt "$DATA_ROOT/bronze/atp/"
  ls -l "$DATA_ROOT/bronze/atp/"
  exit 0
fi

PROFILE_OPT=()
[[ -n "${AWS_PROFILE_NAME:-}" ]] && PROFILE_OPT=(--profile "$AWS_PROFILE_NAME")
ENDPOINT_OPT=()
[[ -n "${S3_ENDPOINT_URL:-}" ]] && ENDPOINT_OPT=(--endpoint-url "$S3_ENDPOINT_URL")

aws s3 "${PROFILE_OPT[@]}" "${ENDPOINT_OPT[@]}" cp "$IN_DIR/" "$DATA_ROOT/bronze/atp/" \
  --recursive --exclude "*" --include "atp_matches_*.csv" --include "MANIFEST.txt"

# Evidence: listing of the bronze prefix (screenshot it with the Onyxia ID and the date)
date
aws s3 "${PROFILE_OPT[@]}" "${ENDPOINT_OPT[@]}" ls "$DATA_ROOT/bronze/atp/" --recursive --human-readable --summarize

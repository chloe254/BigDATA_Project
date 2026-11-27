# Big Data project: does the court surface change who wins in ATP tennis?

## Authors
- LESTIC Chloé
- NOM2 Prénom2

## Prerequisites
- Onyxia service `vscode-pyspark` (4 CPU / 4 Gi), with the S3 profile `default` configured (course, S3 lab)
- `uv`, `aws` CLI, `kubectl` (included in the image). Maven Central reachable (Iceberg jar downloaded by Spark)
- Offline development: Docker (`docker compose up -d` starts a local MinIO S3), Java 17+

## Dataset
| | |
|---|---|
| Name | ATP Tennis Rankings, Results, and Stats — main tour matches 2021-2025 (`atp_matches_YYYY.csv`) |
| URL | https://huggingface.co/datasets/Aneeshers/tennis-sackmann-archive (`atp/` folder): archive of https://github.com/JeffSackmann/tennis_atp, returning 404 on 2026-11-27 |
| License | CC BY-NC-SA 4.0 (Jeff Sackmann / Tennis Abstract) |
| Download date | 2026-11-27 (see `bronze/atp/MANIFEST.txt`) |
| Size | 3.07 MB (3,074,476 bytes), 5 files, 49 columns |

## Problem statement
Between 2021 and 2025, does the surface (hard, clay, grass) change how often the better-ranked player wins, and how dominant the serve is?
We measure, per surface, the share of matches won by the better-ranked player and the share of service points won.

## S3 paths
Bucket: `user-c-lestic-ece` — root `DATA_ROOT=s3://user-c-lestic-ece/bigdata_project`
| Layer | Prefix | Format |
|---|---|---|
| Raw (bronze) | `bronze/atp/` | CSV, unchanged + `MANIFEST.txt` (sha256, date) |
| Silver | `warehouse/silver/atp_matches/` · `silver/quarantine/` | Iceberg (partitioned by surface) · Parquet |
| Gold | `gold/surface_summary/` · `gold/surface_season/` (+ `warehouse/gold/`) | Parquet (+ Iceberg) |

## Reproduction
```bash
git clone https://github.com/chloe254/BigDATA_Project.git && cd BigDATA_Project
uv sync
cp .env.example .env            # Onyxia block: uncomment it, comment the local block
set -a && source .env && set +a
# 1. Ingestion (in the cluster): see the commands at the top of k8s/job-upload-bronze.yaml
kubectl apply -f k8s/job-upload-bronze.yaml && kubectl logs -f job/atp-upload-bronze
#    or from the terminal: bash src/01_download.sh && bash src/02_upload_bronze.sh
# 2. Processing
bash src/run_pipeline.sh 3      # profiling -> silver -> gold -> verification
uv run python src/07_benchmark.py   # measures used in the report
```
**Restart**: every step overwrites its outputs (idempotent). After a failure, rerun from the failed step: `bash src/run_pipeline.sh <step>` (3 = profiling, 4 = silver, 5 = gold, 6 = verification).
`ExpiredToken` error: restart the Onyxia service (temporary credentials), re-run `source .env`, recreate the `s3-credentials` Secret.
**Verification**: `src/06_verify_duckdb.py` recomputes the gold table from the raw CSV with DuckDB and fails if a single value differs from the Spark output.

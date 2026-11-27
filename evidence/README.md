# Evidence

Each screenshot must show the **Onyxia ID / namespace** (`user-...`) **and the date and time**.
Tip: before each capture, run `echo "$KUBERNETES_NAMESPACE - $(date)"` in the terminal so both appear on screen.

| File | Content | Command / page |
|---|---|---|
| `01_onyxia_services.png` | Onyxia "My services" page: vscode-pyspark running | Onyxia portal |
| `02_s3_bronze.png` | Bronze prefix with the raw CSV files and sizes | `aws s3 ls $DATA_ROOT/bronze/atp/ --human-readable --summarize` |
| `03_k8s_job_upload.png` | Ingestion Job completed + its logs | `kubectl get jobs` + `kubectl logs job/atp-upload-bronze` |
| `04_profiling.png` | Quality issues with row counts | `uv run python src/03_profiling.py` |
| `05_spark_silver.png` | Bronze -> silver trace (rows, quarantine) | `uv run python src/04_bronze_to_silver.py` |
| `06_spark_ui.png` | Spark UI (jobs / stages / DAG) during a run | port 4040 of the service |
| `07_spark_gold.png` | Gold tables printed | `uv run python src/05_silver_to_gold.py` |
| `08_s3_processed.png` | Silver (Iceberg: data + metadata) and gold prefixes | `aws s3 ls $DATA_ROOT/ --recursive --human-readable` |
| `09_verification.png` | "VERIFICATION REUSSIE" | `uv run python src/06_verify_duckdb.py` |
| `10_benchmark.png` | Spark vs DuckDB, CSV vs Parquet | `uv run python src/07_benchmark.py` |
| `11_s3_browser.png` | Onyxia file explorer showing the bucket | Onyxia portal > Mes fichiers |

"""Bronze -> Silver with Spark DataFrames.

- explicit typing (no inferSchema: reproducible and faster)
- deduplication on the business key (tourney_id, match_num)
- quarantine of unusable rows (walkovers, unknown surface), with the reason
- quality flags kept for the gold layer (retirement, ranks present, serve stats valid)
- output: Iceberg table lake.silver.atp_matches (or Parquet if USE_ICEBERG=0)
Idempotent: every run overwrites its outputs.
Usage: uv run python src/04_bronze_to_silver.py
"""
import time

from pyspark.sql import Window
from pyspark.sql import functions as F

from config import SILVER, SURFACES, USE_ICEBERG, bronze_files, get_spark, spark_path

spark = get_spark("atp-bronze-to-silver")
t0 = time.perf_counter()

INT_COLS = [
    "draw_size", "match_num", "winner_id", "winner_seed", "winner_ht", "loser_id", "loser_seed",
    "loser_ht", "best_of", "minutes", "winner_rank", "winner_rank_points", "loser_rank",
    "loser_rank_points",
] + [f"{p}_{c}" for p in ("w", "l") for c in
     ("ace", "df", "svpt", "1stIn", "1stWon", "2ndWon", "SvGms", "bpSaved", "bpFaced")]
DOUBLE_COLS = ["winner_age", "loser_age"]

# 1. Read: all columns as strings, then explicit casts
raw = (spark.read.option("header", True).option("inferSchema", False)
       .csv([spark_path(f) for f in bronze_files()])
       .withColumn("source_file", F.element_at(F.split(F.input_file_name(), "/"), -1)))
n_bronze = raw.count()

df = raw
for c in INT_COLS:
    # try_cast: a malformed value becomes NULL instead of failing the job
    df = df.withColumn(c, F.expr(f"try_cast(`{c}` AS INT)"))
for c in DOUBLE_COLS:
    df = df.withColumn(c, F.expr(f"try_cast(`{c}` AS DOUBLE)"))
df = (df.withColumn("match_date", F.to_date(F.col("tourney_date").cast("string"), "yyyyMMdd"))
        .withColumn("season", F.year("match_date"))
        .withColumn("score", F.trim("score"))
        .withColumn("surface", F.initcap(F.trim("surface"))))

# 2. Deduplicate on the business key (keep the first occurrence)
w = Window.partitionBy("tourney_id", "match_num").orderBy("source_file")
df = df.withColumn("_rn", F.row_number().over(w))
duplicates = df.filter("_rn > 1").withColumn("reason", F.lit("duplicate"))
df = df.filter("_rn = 1").drop("_rn")
duplicates = duplicates.drop("_rn")

# 3. Quarantine: rows that cannot answer the research question
reason = (F.when(F.upper("score").contains("W/O"), "walkover")
           .when(~F.col("surface").isin(*SURFACES) | F.col("surface").isNull(), "unknown_surface")
           .when(F.col("match_date").isNull(), "invalid_date"))
df = df.withColumn("reason", reason)
quarantine = df.filter("reason IS NOT NULL").unionByName(duplicates)
silver = df.filter("reason IS NULL").drop("reason")

# 4. Quality flags and derived columns (kept in silver, used by gold)
stats_ok = " AND ".join([
    f"{p}_svpt > 0 AND {p}_1stIn <= {p}_svpt AND {p}_1stWon <= {p}_1stIn "
    f"AND {p}_1stWon + {p}_2ndWon <= {p}_svpt AND {p}_bpSaved <= {p}_bpFaced"
    for p in ("w", "l")])
silver = (silver
          .withColumn("is_retired", F.upper("score").rlike("RET|DEF"))
          .withColumn("has_ranks", F.col("winner_rank").isNotNull() & F.col("loser_rank").isNotNull())
          .withColumn("has_valid_serve_stats", F.coalesce(F.expr(stats_ok), F.lit(False)))
          .withColumn("better_ranked_won",
                      F.when(F.col("has_ranks"), F.col("winner_rank") < F.col("loser_rank"))))

# 5. Write
if USE_ICEBERG:
    spark.sql("CREATE NAMESPACE IF NOT EXISTS lake.silver")
    (silver.writeTo("lake.silver.atp_matches")
           .using("iceberg").partitionedBy(F.col("surface")).createOrReplace())
    target = "lake.silver.atp_matches (Iceberg)"
else:
    silver.write.mode("overwrite").partitionBy("surface").parquet(spark_path(SILVER) + "/atp_matches")
    target = SILVER + "/atp_matches (Parquet)"
quarantine.write.mode("overwrite").parquet(spark_path(SILVER) + "/quarantine")

# 6. Execution trace (screenshot for the evidence folder)
n_silver, n_quar = silver.count(), quarantine.count()
print(f"bronze rows      : {n_bronze}")
print(f"silver rows      : {n_silver}  -> {target}")
print(f"quarantined rows : {n_quar}")
quarantine.groupBy("reason").count().orderBy("reason").show()
silver.groupBy("surface").agg(
    F.count("*").alias("matches"),
    F.sum(F.col("is_retired").cast("int")).alias("retired"),
    F.sum((~F.col("has_ranks")).cast("int")).alias("missing_ranks"),
    F.sum((~F.col("has_valid_serve_stats")).cast("int")).alias("invalid_or_missing_stats"),
).orderBy("surface").show()
assert n_silver + n_quar == n_bronze, "row count mismatch between bronze and silver+quarantine"
print(f"OK in {time.perf_counter() - t0:.1f}s")
spark.stop()

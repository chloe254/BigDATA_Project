"""Silver -> Gold with Spark: answer the research question, per surface.

Question: does the surface change how often the better-ranked player wins,
and how dominant the serve is?

Rules (kept simple on purpose so that 06_verify_duckdb.py can recompute them from the raw CSV):
- retirements / defaults (is_retired) are excluded from both metrics: the match was not played out
- "better-ranked won" uses matches where both ranks are known (has_ranks)
- serve metrics use matches whose serve stats passed the silver checks (has_valid_serve_stats)
- serve points won = (1stWon + 2ndWon) / svpt, summed over both players; ace rate = ace / svpt

Outputs (overwritten at each run, so the script is idempotent):
- gold/surface_summary/  one row per surface             (Parquet, read by DuckDB in step 06)
- gold/surface_season/   one row per surface and season  (Parquet)
- lake.gold.surface_summary / lake.gold.surface_season   (Iceberg, if USE_ICEBERG=1)
Usage: uv run python src/05_silver_to_gold.py
"""
import time

from pyspark.sql import functions as F

from config import GOLD, SILVER, USE_ICEBERG, get_spark, spark_path

spark = get_spark("atp-silver-to-gold")
t0 = time.perf_counter()

# 1. Read silver (same table that 04_bronze_to_silver.py wrote)
if USE_ICEBERG:
    silver = spark.table("lake.silver.atp_matches")
else:
    silver = spark.read.parquet(spark_path(SILVER) + "/atp_matches")
# an empty score gives is_retired = NULL: count it as a played match, not a retirement
silver = silver.withColumn("is_retired", F.coalesce(F.col("is_retired"), F.lit(False)))

played = silver.filter(~F.col("is_retired"))
ranked = F.col("has_ranks")
serve_ok = F.col("has_valid_serve_stats")


def sum_if(cond, value):
    return F.sum(F.when(cond, value).otherwise(F.lit(0))).cast("long")


AGGS = [
    F.count("*").alias("matches"),
    sum_if(ranked, F.lit(1)).alias("ranked_matches"),
    sum_if(ranked & F.col("better_ranked_won"), F.lit(1)).alias("better_ranked_wins"),
    sum_if(serve_ok, F.lit(1)).alias("serve_matches"),
    sum_if(serve_ok, F.col("w_svpt") + F.col("l_svpt")).alias("serve_points"),
    sum_if(serve_ok, F.col("w_1stWon") + F.col("w_2ndWon")
           + F.col("l_1stWon") + F.col("l_2ndWon")).alias("serve_points_won"),
    sum_if(serve_ok, F.col("w_ace") + F.col("l_ace")).alias("aces"),
]


def with_rates(df):
    """Percentages rounded to 2 decimals; counts are kept so every rate can be checked."""
    return (df.withColumn("better_ranked_win_pct",
                          F.round(100 * F.col("better_ranked_wins") / F.col("ranked_matches"), 2))
              .withColumn("serve_points_won_pct",
                          F.round(100 * F.col("serve_points_won") / F.col("serve_points"), 2))
              .withColumn("ace_pct", F.round(100 * F.col("aces") / F.col("serve_points"), 2)))


summary = with_rates(played.groupBy("surface").agg(*AGGS)).orderBy("surface")
by_season = with_rates(played.groupBy("surface", "season").agg(*AGGS)).orderBy("surface", "season")

# 2. Write: Parquet (one small file per table) + Iceberg copies
for name, df in (("surface_summary", summary), ("surface_season", by_season)):
    df.coalesce(1).write.mode("overwrite").parquet(spark_path(GOLD) + f"/{name}")
    if USE_ICEBERG:
        spark.sql("CREATE NAMESPACE IF NOT EXISTS lake.gold")
        df.writeTo(f"lake.gold.{name}").using("iceberg").createOrReplace()

# 3. Execution trace (screenshot 07_spark_gold.png)
n_silver, n_retired = silver.count(), silver.filter(F.col("is_retired")).count()
print(f"silver rows          : {n_silver}")
print(f"excluded retirements : {n_retired}")
print(f"gold written to      : {GOLD}/surface_summary, {GOLD}/surface_season"
      + (" (+ lake.gold.* Iceberg)" if USE_ICEBERG else ""))
print("\n== surface_summary ==")
summary.select("surface", "matches", "ranked_matches", "better_ranked_win_pct",
               "serve_matches", "serve_points_won_pct", "ace_pct").show()
print("== surface_season ==")
by_season.select("surface", "season", "matches", "better_ranked_win_pct",
                 "serve_points_won_pct", "ace_pct").show(30)

assert summary.agg(F.sum("matches")).first()[0] == n_silver - n_retired, \
    "gold matches do not add up to silver minus retirements"
print(f"OK in {time.perf_counter() - t0:.1f}s")
spark.stop()

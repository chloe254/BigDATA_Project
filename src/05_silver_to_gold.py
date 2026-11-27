import time

from pyspark.sql import functions as F

from config import GOLD, SILVER, USE_ICEBERG, get_spark, spark_path

spark = get_spark("atp-silver-to-gold")
t0 = time.perf_counter()

if USE_ICEBERG:
    silver = spark.table("lake.silver.atp_matches")
else:
    silver = spark.read.parquet(spark_path(SILVER) + "/atp_matches")
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
    return (df.withColumn("better_ranked_win_pct",
                          F.round(100 * F.col("better_ranked_wins") / F.col("ranked_matches"), 2))
              .withColumn("serve_points_won_pct",
                          F.round(100 * F.col("serve_points_won") / F.col("serve_points"), 2))
              .withColumn("ace_pct", F.round(100 * F.col("aces") / F.col("serve_points"), 2)))


summary = with_rates(played.groupBy("surface").agg(*AGGS)).orderBy("surface")
by_season = with_rates(played.groupBy("surface", "season").agg(*AGGS)).orderBy("surface", "season")

for name, df in (("surface_summary", summary), ("surface_season", by_season)):
    df.coalesce(1).write.mode("overwrite").parquet(spark_path(GOLD) + f"/{name}")
    if USE_ICEBERG:
        spark.sql("CREATE NAMESPACE IF NOT EXISTS lake.gold")
        df.writeTo(f"lake.gold.{name}").using("iceberg").createOrReplace()

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

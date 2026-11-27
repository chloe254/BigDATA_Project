import time

from pyspark.sql import functions as F

from config import GOLD, SILVER, USE_ICEBERG, get_spark, spark_path

spark = get_spark("atp-silver-vers-gold")
debut = time.perf_counter()

if USE_ICEBERG:
    silver = spark.table("lake.silver.atp_matches")
else:
    silver = spark.read.parquet(spark_path(SILVER) + "/atp_matches")
silver = silver.withColumn("is_retired", F.coalesce(F.col("is_retired"), F.lit(False)))

joues = silver.filter(~F.col("is_retired"))
classes = F.col("has_ranks")
service_ok = F.col("has_valid_serve_stats")


def somme_si(condition, valeur):
    return F.sum(F.when(condition, valeur).otherwise(F.lit(0))).cast("long")


AGREGATS = [
    F.count("*").alias("matches"),
    somme_si(classes, F.lit(1)).alias("ranked_matches"),
    somme_si(classes & F.col("better_ranked_won"), F.lit(1)).alias("better_ranked_wins"),
    somme_si(service_ok, F.lit(1)).alias("serve_matches"),
    somme_si(service_ok, F.col("w_svpt") + F.col("l_svpt")).alias("serve_points"),
    somme_si(service_ok, F.col("w_1stWon") + F.col("w_2ndWon")
             + F.col("l_1stWon") + F.col("l_2ndWon")).alias("serve_points_won"),
    somme_si(service_ok, F.col("w_ace") + F.col("l_ace")).alias("aces"),
]


def avec_taux(df):
    return (df.withColumn("better_ranked_win_pct",
                          F.round(100 * F.col("better_ranked_wins") / F.col("ranked_matches"), 2))
              .withColumn("serve_points_won_pct",
                          F.round(100 * F.col("serve_points_won") / F.col("serve_points"), 2))
              .withColumn("ace_pct", F.round(100 * F.col("aces") / F.col("serve_points"), 2)))


par_surface = avec_taux(joues.groupBy("surface").agg(*AGREGATS)).orderBy("surface")
par_saison = avec_taux(joues.groupBy("surface", "season").agg(*AGREGATS)).orderBy("surface", "season")

for nom, df in (("surface_summary", par_surface), ("surface_season", par_saison)):
    df.coalesce(1).write.mode("overwrite").parquet(spark_path(GOLD) + f"/{nom}")
    if USE_ICEBERG:
        spark.sql("CREATE NAMESPACE IF NOT EXISTS lake.gold")
        df.writeTo(f"lake.gold.{nom}").using("iceberg").createOrReplace()

n_silver, n_abandons = silver.count(), silver.filter(F.col("is_retired")).count()
print(f"lignes silver        : {n_silver}")
print(f"abandons exclus      : {n_abandons}")
print(f"gold écrit dans      : {GOLD}/surface_summary, {GOLD}/surface_season"
      + (" (+ tables Iceberg lake.gold.*)" if USE_ICEBERG else ""))
print("\n== surface_summary (par surface) ==")
par_surface.select("surface", "matches", "ranked_matches", "better_ranked_win_pct",
                   "serve_matches", "serve_points_won_pct", "ace_pct").show()
print("== surface_season (par surface et saison) ==")
par_saison.select("surface", "season", "matches", "better_ranked_win_pct",
                  "serve_points_won_pct", "ace_pct").show(30)

assert par_surface.agg(F.sum("matches")).first()[0] == n_silver - n_abandons, \
    "le total des matchs gold ne correspond pas au silver moins les abandons"
print(f"OK en {time.perf_counter() - debut:.1f}s")
spark.stop()

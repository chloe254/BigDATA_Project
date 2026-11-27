import statistics
import time

from config import SILVER, bronze_files, get_duckdb, get_spark, spark_path

REPETITIONS = 3

t = time.perf_counter()
spark = get_spark("atp-benchmark")
demarrage_spark = time.perf_counter() - t
con = get_duckdb()

fichiers_csv = bronze_files()
dossier_parquet = SILVER + "/atp_matches"
liste_csv = ", ".join(f"'{f}'" for f in fichiers_csv)

REQUETE = """
SELECT surface, COUNT(*) AS matchs, ROUND(AVG(TRY_CAST(minutes AS DOUBLE)), 1) AS duree_moyenne
FROM {source}
WHERE surface IN ('Hard', 'Clay', 'Grass')
GROUP BY surface ORDER BY surface"""


def spark_csv():
    (spark.read.option("header", True).csv([spark_path(f) for f in fichiers_csv])
     .createOrReplaceTempView("bench_csv"))
    return [tuple(r) for r in spark.sql(REQUETE.format(source="bench_csv")).collect()]


def spark_parquet():
    spark.read.parquet(spark_path(dossier_parquet)).createOrReplaceTempView("bench_parquet")
    return [tuple(r) for r in spark.sql(REQUETE.format(source="bench_parquet")).collect()]


def duckdb_csv():
    source = f"read_csv([{liste_csv}], header = true, all_varchar = true)"
    return con.execute(REQUETE.format(source=source)).fetchall()


def duckdb_parquet():
    source = f"read_parquet('{dossier_parquet}/*/*.parquet', hive_partitioning = true)"
    return con.execute(REQUETE.format(source=source)).fetchall()


def mesurer(fonction):
    resultat = fonction()
    durees = []
    for _ in range(REPETITIONS):
        t = time.perf_counter()
        fonction()
        durees.append(time.perf_counter() - t)
    return statistics.median(durees), resultat


def taille_octets(chemins):
    jvm = spark._jvm
    conf = spark._jsc.hadoopConfiguration()
    total = 0
    for c in chemins:
        chemin = jvm.org.apache.hadoop.fs.Path(spark_path(c))
        total += chemin.getFileSystem(conf).getContentSummary(chemin).getLength()
    return total


mesures = []
resultats = {}
for moteur, format_, fonction in (("Spark", "CSV (bronze)", spark_csv),
                                  ("Spark", "Parquet (silver)", spark_parquet),
                                  ("DuckDB", "CSV (bronze)", duckdb_csv),
                                  ("DuckDB", "Parquet (silver)", duckdb_parquet)):
    secondes, resultat = mesurer(fonction)
    resultats[(moteur, format_)] = [(r[0], int(r[1])) for r in resultat]
    mesures.append((moteur, format_, secondes))

taille_csv = taille_octets(fichiers_csv)
taille_parquet = taille_octets([dossier_parquet])

print(f"\nRequête : nombre de matchs et durée moyenne par surface "
      f"(médiane de {REPETITIONS} exécutions après 1 passage à blanc)")
print(f"{'moteur':<8} {'format':<18} {'secondes':>8}")
for moteur, format_, secondes in mesures:
    print(f"{moteur:<8} {format_:<18} {secondes:>8.3f}")
print(f"\nDémarrage de la session Spark : {demarrage_spark:.1f} s "
      "(payé une fois par job, inexistant avec DuckDB)")
print(f"\nStockage   CSV bronze     : {taille_csv / 1e6:.2f} Mo")
print(f"           Parquet silver : {taille_parquet / 1e6:.2f} Mo "
      f"({100 * taille_parquet / taille_csv:.0f} % du CSV, typé, partitionné par surface)")

comptes_csv = resultats[("DuckDB", "CSV (bronze)")]
comptes_parquet = resultats[("DuckDB", "Parquet (silver)")]
memes_resultats = all(resultats[("Spark", f)] == resultats[("DuckDB", f)]
                      for f in ("CSV (bronze)", "Parquet (silver)"))
print(f"\nMême résultat Spark et DuckDB sur chaque format : {'OUI' if memes_resultats else 'NON'}")
print(f"Matchs par surface, bronze -> silver            : {comptes_csv} -> {comptes_parquet}"
      " (écart = forfaits retirés dans le silver)")
spark.stop()

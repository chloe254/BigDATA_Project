import statistics
import time

from config import SILVER, bronze_files, get_duckdb, get_spark, spark_path

RUNS = 3

t = time.perf_counter()
spark = get_spark("atp-benchmark")
spark_startup = time.perf_counter() - t
con = get_duckdb()

csv_files = bronze_files()
parquet_dir = SILVER + "/atp_matches"
csv_list = ", ".join(f"'{f}'" for f in csv_files)

QUERY = """
SELECT surface, COUNT(*) AS matches, ROUND(AVG(TRY_CAST(minutes AS DOUBLE)), 1) AS avg_minutes
FROM {src}
WHERE surface IN ('Hard', 'Clay', 'Grass')
GROUP BY surface ORDER BY surface"""


def spark_csv():
    (spark.read.option("header", True).csv([spark_path(f) for f in csv_files])
     .createOrReplaceTempView("bench_csv"))
    return [tuple(r) for r in spark.sql(QUERY.format(src="bench_csv")).collect()]


def spark_parquet():
    spark.read.parquet(spark_path(parquet_dir)).createOrReplaceTempView("bench_parquet")
    return [tuple(r) for r in spark.sql(QUERY.format(src="bench_parquet")).collect()]


def duckdb_csv():
    src = f"read_csv([{csv_list}], header = true, all_varchar = true)"
    return con.execute(QUERY.format(src=src)).fetchall()


def duckdb_parquet():
    src = f"read_parquet('{parquet_dir}/*/*.parquet', hive_partitioning = true)"
    return con.execute(QUERY.format(src=src)).fetchall()


def measure(fn):
    result = fn()
    times = []
    for _ in range(RUNS):
        t = time.perf_counter()
        fn()
        times.append(time.perf_counter() - t)
    return statistics.median(times), result


def size_bytes(paths):
    jvm = spark._jvm
    conf = spark._jsc.hadoopConfiguration()
    total = 0
    for p in paths:
        path = jvm.org.apache.hadoop.fs.Path(spark_path(p))
        total += path.getFileSystem(conf).getContentSummary(path).getLength()
    return total


rows = []
results = {}
for engine, fmt, fn in (("Spark", "CSV (bronze)", spark_csv),
                        ("Spark", "Parquet (silver)", spark_parquet),
                        ("DuckDB", "CSV (bronze)", duckdb_csv),
                        ("DuckDB", "Parquet (silver)", duckdb_parquet)):
    seconds, result = measure(fn)
    results[(engine, fmt)] = [(r[0], int(r[1])) for r in result]
    rows.append((engine, fmt, seconds))

csv_size = size_bytes(csv_files)
parquet_size = size_bytes([parquet_dir])

print(f"\nQuery: matches and average duration per surface (median of {RUNS} runs after 1 warm-up)")
print(f"{'engine':<8} {'format':<18} {'seconds':>8}")
for engine, fmt, seconds in rows:
    print(f"{engine:<8} {fmt:<18} {seconds:>8.3f}")
print(f"\nSpark session startup : {spark_startup:.1f} s (paid once per job, not by DuckDB)")
print(f"\nStorage   CSV bronze     : {csv_size / 1e6:.2f} MB")
print(f"          Parquet silver : {parquet_size / 1e6:.2f} MB "
      f"({100 * parquet_size / csv_size:.0f}% of CSV, typed, partitioned by surface)")

csv_counts = results[("DuckDB", "CSV (bronze)")]
pq_counts = results[("DuckDB", "Parquet (silver)")]
same_engine = all(results[("Spark", f)] == results[("DuckDB", f)]
                  for f in ("CSV (bronze)", "Parquet (silver)"))
print(f"\nSame result Spark vs DuckDB on each format : {same_engine}")
print(f"Bronze vs silver counts per surface        : {csv_counts} -> {pq_counts}"
      " (gap = walkovers removed in silver)")
spark.stop()

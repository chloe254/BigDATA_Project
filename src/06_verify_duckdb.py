import sys
import time

from config import GOLD, SURFACES, bronze_files, get_duckdb

t0 = time.perf_counter()
con = get_duckdb()

files = ", ".join(f"'{f}'" for f in bronze_files())
surfaces = ", ".join(f"'{s}'" for s in SURFACES)
stats_ok = " AND ".join(
    f"{p}_svpt > 0 AND {p}_1stIn <= {p}_svpt AND {p}_1stWon <= {p}_1stIn "
    f"AND {p}_1stWon + {p}_2ndWon <= {p}_svpt AND {p}_bpSaved <= {p}_bpFaced"
    for p in ("w", "l"))
int_cols = ["match_num", "winner_rank", "loser_rank"] + [
    f"{p}_{c}" for p in ("w", "l")
    for c in ("ace", "svpt", "1stIn", "1stWon", "2ndWon", "bpSaved", "bpFaced")]
casts = ",\n        ".join(f'TRY_CAST("{c}" AS INTEGER) AS "{c}"' for c in int_cols)

con.execute(f"""
CREATE OR REPLACE TEMP VIEW typed AS
SELECT
    tourney_id,
    {casts},
    TRIM(score) AS score,
    UPPER(SUBSTR(TRIM(surface), 1, 1)) || LOWER(SUBSTR(TRIM(surface), 2)) AS surface,
    TRY_STRPTIME(tourney_date, '%Y%m%d')::DATE AS match_date,
    regexp_extract(filename, '[^/]+$') AS source_file
FROM read_csv([{files}], header = true, all_varchar = true, filename = true)
""")

con.execute(f"""
CREATE OR REPLACE TEMP VIEW silver AS
WITH dedup AS (
    SELECT * FROM typed
    QUALIFY ROW_NUMBER() OVER (PARTITION BY tourney_id, match_num ORDER BY source_file) = 1
)
SELECT
    *,
    YEAR(match_date) AS season,
    COALESCE(regexp_matches(UPPER(score), 'RET|DEF'), FALSE) AS is_retired,
    winner_rank IS NOT NULL AND loser_rank IS NOT NULL AS has_ranks,
    COALESCE({stats_ok}, FALSE) AS has_valid_serve_stats,
    winner_rank < loser_rank AS better_ranked_won
FROM dedup
WHERE NOT COALESCE(UPPER(score) LIKE '%W/O%', FALSE)
  AND surface IN ({surfaces})
  AND match_date IS NOT NULL
""")

AGGS = """
    COUNT(*) AS matches,
    SUM(CASE WHEN has_ranks THEN 1 ELSE 0 END) AS ranked_matches,
    SUM(CASE WHEN has_ranks AND better_ranked_won THEN 1 ELSE 0 END) AS better_ranked_wins,
    SUM(CASE WHEN has_valid_serve_stats THEN 1 ELSE 0 END) AS serve_matches,
    SUM(CASE WHEN has_valid_serve_stats THEN w_svpt + l_svpt ELSE 0 END) AS serve_points,
    SUM(CASE WHEN has_valid_serve_stats
             THEN w_1stWon + w_2ndWon + l_1stWon + l_2ndWon ELSE 0 END) AS serve_points_won,
    SUM(CASE WHEN has_valid_serve_stats THEN w_ace + l_ace ELSE 0 END) AS aces
"""
COUNTS = ["matches", "ranked_matches", "better_ranked_wins", "serve_matches",
          "serve_points", "serve_points_won", "aces"]
RATES = ["better_ranked_win_pct", "serve_points_won_pct", "ace_pct"]


def expected(keys):
    return f"""
    SELECT {keys}, {AGGS.strip()}
    FROM silver WHERE NOT is_retired GROUP BY {keys}"""


def with_rates(sql):
    return f"""
    SELECT *,
        ROUND(100.0 * better_ranked_wins / ranked_matches, 2) AS better_ranked_win_pct,
        ROUND(100.0 * serve_points_won / serve_points, 2) AS serve_points_won_pct,
        ROUND(100.0 * aces / serve_points, 2) AS ace_pct
    FROM ({sql})"""


def compare(table, keys):
    key_list = [k.strip() for k in keys.split(",")]
    on = " AND ".join(f"d.{k} = s.{k}" for k in key_list)
    diffs = " OR ".join(
        [f"d.{c} IS DISTINCT FROM s.{c}" for c in COUNTS]
        + [f"ABS(d.{c} - s.{c}) > 0.011" for c in RATES])
    sql = f"""
    WITH d AS ({with_rates(expected(keys))}),
         s AS (SELECT * FROM '{GOLD}/{table}/*.parquet')
    SELECT {", ".join(f"COALESCE(d.{k}, s.{k}) AS {k}" for k in key_list)},
           d.matches AS duckdb_matches, s.matches AS spark_matches,
           d.better_ranked_win_pct AS duckdb_brw_pct, s.better_ranked_win_pct AS spark_brw_pct,
           d.serve_points_won_pct AS duckdb_spw_pct, s.serve_points_won_pct AS spark_spw_pct,
           (d.matches IS NULL OR s.matches IS NULL OR {diffs}) AS mismatch
    FROM d FULL OUTER JOIN s ON {on}
    ORDER BY {keys}"""
    rows = con.execute(sql).fetchall()
    n_bad = sum(1 for r in rows if r[-1])
    print(f"\n== {table}: {len(rows)} rows compared, {n_bad} mismatch(es) ==")
    con.sql(sql).show(max_rows=40, max_width=250)
    return n_bad


def scalar(sql):
    return con.execute(sql).fetchall()[0][0]


n_bronze = scalar(f"SELECT COUNT(*) FROM read_csv([{files}], header = true, all_varchar = true)")
n_silver = scalar("SELECT COUNT(*) FROM silver")
print(f"bronze rows (DuckDB, raw CSV) : {n_bronze}")
print(f"silver rows (DuckDB rebuild)  : {n_silver}")

bad = compare("surface_summary", "surface") + compare("surface_season", "surface, season")

print(f"done in {time.perf_counter() - t0:.1f}s")
if bad:
    print(f"VERIFICATION FAILED: {bad} row(s) differ between DuckDB and Spark")
    sys.exit(1)
print("VERIFICATION PASSED: DuckDB recomputation matches the Spark gold tables")

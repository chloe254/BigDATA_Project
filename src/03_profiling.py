"""Data quality profiling of the bronze layer with DuckDB.

Counts every quality issue on the raw files: the numbers go straight into the
"Data quality issues" section of the report (real examples + affected rows).
Usage: uv run python src/03_profiling.py
"""
import time
from pathlib import Path

from config import bronze_files, get_duckdb

con = get_duckdb()
files = bronze_files()
t0 = time.perf_counter()

# union_by_name: seasons may not have exactly the same columns
con.execute(f"""
    CREATE TABLE bronze AS
    SELECT * EXCLUDE (filename), filename AS source_file
    FROM read_csv({files!r}, union_by_name = true, filename = true, header = true)
""")

print("== Rows per file")
print(con.sql("SELECT source_file, count(*) AS n_rows FROM bronze GROUP BY ALL ORDER BY 1"))

print("== Schema (bronze)")
print(con.sql("SELECT column_name, column_type, null_percentage FROM (SUMMARIZE bronze)"))

# One line per quality rule: name, SQL condition. Same rules as src/04_bronze_to_silver.py
RULES = {
    "duplicate key (tourney_id, match_num), all copies counted": """
        (tourney_id, match_num) IN (SELECT tourney_id, match_num FROM bronze
                                    GROUP BY ALL HAVING count(*) > 1)""",
    "walkover (score = W/O)": "score ILIKE '%W/O%'",
    "retirement or default (RET/DEF)": "score ILIKE '%RET%' OR score ILIKE '%DEF%'",
    "surface missing or not Hard/Clay/Grass": "surface IS NULL OR surface NOT IN ('Hard','Clay','Grass')",
    "winner_rank or loser_rank missing": "winner_rank IS NULL OR loser_rank IS NULL",
    "serve stats missing (w_svpt or l_svpt)": "w_svpt IS NULL OR l_svpt IS NULL",
    "serve stats inconsistent (1stIn > svpt, 1stWon > 1stIn, bpSaved > bpFaced...)": """
        w_1stIn > w_svpt OR l_1stIn > l_svpt OR w_1stWon > w_1stIn OR l_1stWon > l_1stIn
        OR w_bpSaved > w_bpFaced OR l_bpSaved > l_bpFaced OR w_svpt = 0 OR l_svpt = 0""",
    "minutes missing": "minutes IS NULL",
    "minutes outlier (<= 0 or > 400)": "minutes <= 0 OR minutes > 400",
    "winner_ht or loser_ht missing": "winner_ht IS NULL OR loser_ht IS NULL",
}

total = con.execute("SELECT count(*) FROM bronze").fetchone()[0]
rows = []
for name, cond in RULES.items():
    n = con.execute(f"SELECT count(*) FROM bronze WHERE {cond}").fetchone()[0]
    example = con.execute(f"""
        SELECT tourney_name || ' ' || tourney_date || ' | ' || winner_name || ' d. ' || loser_name
               || ' | score=' || coalesce(score, 'NULL')
        FROM bronze WHERE {cond} LIMIT 1""").fetchone()
    rows.append((name, n, round(100 * n / total, 2), example[0] if example else ""))

print(f"== Quality issues (total rows: {total})")
for name, n, pct, ex in rows:
    print(f"{n:>6} rows ({pct:>5}%)  {name}\n          e.g. {ex}")

out = Path("docs/quality_report.csv")
out.parent.mkdir(exist_ok=True)
con.execute("CREATE TABLE q(rule VARCHAR, n_rows INT, pct DOUBLE, example VARCHAR)")
con.executemany("INSERT INTO q VALUES (?, ?, ?, ?)", rows)
con.execute(f"COPY q TO '{out}' (HEADER)")
print(f"\nReport written to {out} in {time.perf_counter() - t0:.2f}s")

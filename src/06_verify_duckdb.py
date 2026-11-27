import sys
import time

from config import GOLD, SURFACES, bronze_files, get_duckdb

debut = time.perf_counter()
con = get_duckdb()

fichiers = ", ".join(f"'{f}'" for f in bronze_files())
surfaces = ", ".join(f"'{s}'" for s in SURFACES)
stats_valides = " AND ".join(
    f"{p}_svpt > 0 AND {p}_1stIn <= {p}_svpt AND {p}_1stWon <= {p}_1stIn "
    f"AND {p}_1stWon + {p}_2ndWon <= {p}_svpt AND {p}_bpSaved <= {p}_bpFaced"
    for p in ("w", "l"))
colonnes_entieres = ["match_num", "winner_rank", "loser_rank"] + [
    f"{p}_{c}" for p in ("w", "l")
    for c in ("ace", "svpt", "1stIn", "1stWon", "2ndWon", "bpSaved", "bpFaced")]
conversions = ",\n        ".join(f'TRY_CAST("{c}" AS INTEGER) AS "{c}"' for c in colonnes_entieres)

con.execute(f"""
CREATE OR REPLACE TEMP VIEW type AS
SELECT
    tourney_id,
    {conversions},
    TRIM(score) AS score,
    UPPER(SUBSTR(TRIM(surface), 1, 1)) || LOWER(SUBSTR(TRIM(surface), 2)) AS surface,
    TRY_STRPTIME(tourney_date, '%Y%m%d')::DATE AS match_date,
    regexp_extract(filename, '[^/]+$') AS fichier_source
FROM read_csv([{fichiers}], header = true, all_varchar = true, filename = true)
""")

con.execute(f"""
CREATE OR REPLACE TEMP VIEW silver AS
WITH dedoublonne AS (
    SELECT * FROM type
    QUALIFY ROW_NUMBER() OVER (PARTITION BY tourney_id, match_num ORDER BY fichier_source) = 1
)
SELECT
    *,
    YEAR(match_date) AS season,
    COALESCE(regexp_matches(UPPER(score), 'RET|DEF'), FALSE) AS is_retired,
    winner_rank IS NOT NULL AND loser_rank IS NOT NULL AS has_ranks,
    COALESCE({stats_valides}, FALSE) AS has_valid_serve_stats,
    winner_rank < loser_rank AS better_ranked_won
FROM dedoublonne
WHERE NOT COALESCE(UPPER(score) LIKE '%W/O%', FALSE)
  AND surface IN ({surfaces})
  AND match_date IS NOT NULL
""")

AGREGATS = """
    COUNT(*) AS matches,
    SUM(CASE WHEN has_ranks THEN 1 ELSE 0 END) AS ranked_matches,
    SUM(CASE WHEN has_ranks AND better_ranked_won THEN 1 ELSE 0 END) AS better_ranked_wins,
    SUM(CASE WHEN has_valid_serve_stats THEN 1 ELSE 0 END) AS serve_matches,
    SUM(CASE WHEN has_valid_serve_stats THEN w_svpt + l_svpt ELSE 0 END) AS serve_points,
    SUM(CASE WHEN has_valid_serve_stats
             THEN w_1stWon + w_2ndWon + l_1stWon + l_2ndWon ELSE 0 END) AS serve_points_won,
    SUM(CASE WHEN has_valid_serve_stats THEN w_ace + l_ace ELSE 0 END) AS aces
"""
COMPTES = ["matches", "ranked_matches", "better_ranked_wins", "serve_matches",
           "serve_points", "serve_points_won", "aces"]
TAUX = ["better_ranked_win_pct", "serve_points_won_pct", "ace_pct"]


def attendu(cles):
    return f"""
    SELECT {cles}, {AGREGATS.strip()}
    FROM silver WHERE NOT is_retired GROUP BY {cles}"""


def avec_taux(sql):
    return f"""
    SELECT *,
        ROUND(100.0 * better_ranked_wins / ranked_matches, 2) AS better_ranked_win_pct,
        ROUND(100.0 * serve_points_won / serve_points, 2) AS serve_points_won_pct,
        ROUND(100.0 * aces / serve_points, 2) AS ace_pct
    FROM ({sql})"""


def comparer(table, cles):
    liste_cles = [k.strip() for k in cles.split(",")]
    jointure = " AND ".join(f"d.{k} = s.{k}" for k in liste_cles)
    ecarts = " OR ".join(
        [f"d.{c} IS DISTINCT FROM s.{c}" for c in COMPTES]
        + [f"ABS(d.{c} - s.{c}) > 0.011" for c in TAUX])
    sql = f"""
    WITH d AS ({avec_taux(attendu(cles))}),
         s AS (SELECT * FROM '{GOLD}/{table}/*.parquet')
    SELECT {", ".join(f"COALESCE(d.{k}, s.{k}) AS {k}" for k in liste_cles)},
           d.matches AS matchs_duckdb, s.matches AS matchs_spark,
           d.better_ranked_win_pct AS pct_mieux_classe_duckdb,
           s.better_ranked_win_pct AS pct_mieux_classe_spark,
           d.serve_points_won_pct AS pct_service_duckdb,
           s.serve_points_won_pct AS pct_service_spark,
           (d.matches IS NULL OR s.matches IS NULL OR {ecarts}) AS ecart
    FROM d FULL OUTER JOIN s ON {jointure}
    ORDER BY {cles}"""
    lignes = con.execute(sql).fetchall()
    n_ecarts = sum(1 for ligne in lignes if ligne[-1])
    print(f"\n== {table} : {len(lignes)} lignes comparées, {n_ecarts} écart(s) ==")
    con.sql(sql).show(max_rows=40, max_width=250)
    return n_ecarts


def valeur(sql):
    return con.execute(sql).fetchall()[0][0]


n_bronze = valeur(f"SELECT COUNT(*) FROM read_csv([{fichiers}], header = true, all_varchar = true)")
n_silver = valeur("SELECT COUNT(*) FROM silver")
print(f"lignes bronze (DuckDB, CSV brut)  : {n_bronze}")
print(f"lignes silver (recalcul DuckDB)   : {n_silver}")

total_ecarts = comparer("surface_summary", "surface") + comparer("surface_season", "surface, season")

print(f"terminé en {time.perf_counter() - debut:.1f}s")
if total_ecarts:
    print(f"VERIFICATION ECHOUEE : {total_ecarts} ligne(s) différente(s) entre DuckDB et Spark")
    sys.exit(1)
print("VERIFICATION REUSSIE : le recalcul DuckDB correspond exactement aux tables gold de Spark")

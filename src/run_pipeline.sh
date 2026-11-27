#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")/.."

ETAPE_DEPART="${1:-3}"
PY="${PYTHON:-python3}"

if [[ -z "${DATA_ROOT:-}" ]]; then
  echo "DATA_ROOT non défini : lancer avant  set -a && source .env && set +a" >&2
  exit 2
fi

declare -A SCRIPTS=(
  [3]="src/03_profiling.py"
  [4]="src/04_bronze_to_silver.py"
  [5]="src/05_silver_to_gold.py"
  [6]="src/06_verify_duckdb.py"
)
declare -A NOMS=(
  [3]="profilage qualité du bronze (DuckDB)"
  [4]="bronze -> silver (Spark)"
  [5]="silver -> gold (Spark)"
  [6]="vérification indépendante (DuckDB)"
)

if [[ -z "${SCRIPTS[$ETAPE_DEPART]:-}" ]]; then
  echo "Étape inconnue : $ETAPE_DEPART (valeurs possibles : 3, 4, 5, 6)" >&2
  echo "Usage : bash src/run_pipeline.sh [3|4|5|6]" >&2
  exit 2
fi

echo "Pipeline ATP - namespace ${KUBERNETES_NAMESPACE:-local} - $(date)"
echo "DATA_ROOT=$DATA_ROOT  USE_ICEBERG=${USE_ICEBERG:-1}  départ à l'étape $ETAPE_DEPART"

debut_total=$SECONDS
for etape in 3 4 5 6; do
  (( etape < ETAPE_DEPART )) && continue
  echo
  echo "===== Étape $etape : ${NOMS[$etape]} ====="
  debut=$SECONDS
  if ! "$PY" "${SCRIPTS[$etape]}"; then
    echo
    echo "ÉCHEC à l'étape $etape (${SCRIPTS[$etape]})." >&2
    echo "Corriger puis reprendre avec : bash src/run_pipeline.sh $etape" >&2
    exit 1
  fi
  echo "----- Étape $etape terminée en $((SECONDS - debut)) s"
done

echo
echo "PIPELINE TERMINÉ en $((SECONDS - debut_total)) s (étapes $ETAPE_DEPART à 6)"

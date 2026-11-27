# Projet Big Data : la surface change-t-elle qui gagne au tennis ATP ?

## Auteurs
- LESTIC Chloé
- MENUT Edouard

## Prérequis
- Service Onyxia `vscode-pyspark` (4 CPU / 4 Gi, **réservation mémoire ≤ 512 Mi**, voir « Limites »). Les identifiants S3 sont injectés par Onyxia dans les variables `AWS_*`
- `aws` CLI, `kubectl`, Spark 4.3 et DuckDB (inclus dans l'image)
- Développement hors ligne : Docker (`docker compose up -d` démarre un MinIO S3 local), Java 17+

## Jeu de données
| | |
|---|---|
| Nom | ATP Tennis Rankings, Results, and Stats : matchs du circuit principal 2021-2025 (`atp_matches_AAAA.csv`) |
| URL | https://huggingface.co/datasets/Aneeshers/tennis-sackmann-archive (dossier `atp/`), archive de https://github.com/JeffSackmann/tennis_atp, qui renvoyait une erreur 404 le 2026-11-27 |
| Licence | CC BY-NC-SA 4.0 (Jeff Sackmann / Tennis Abstract) |
| Date de téléchargement | 2026-11-27 (voir `bronze/atp/MANIFEST.txt`) |
| Taille | 3,07 Mo (3 074 476 octets), 5 fichiers, 49 colonnes, 14 656 matchs |

## Problématique
Entre 2021 et 2025, la surface (dur, terre battue, gazon) change-t-elle la fréquence de victoire du joueur le mieux classé, et la domination du service ?
On mesure, par surface, la part des matchs gagnés par le mieux classé et la part des points gagnés au service.

## Architecture (médaillon)
```
source (Hugging Face)
   │  01_download.sh  (contrôle 1-5 Mo + MANIFEST sha256)
   ▼
BRONZE  s3://…/bronze/atp/            CSV bruts, inchangés
   │  02_upload_bronze.sh  ou  Job Kubernetes k8s/job-upload-bronze.yaml
   │  03_profiling.py (DuckDB)  ──►  docs/quality_report.csv
   ▼
SILVER  s3://…/silver/atp_matches/    Parquet typé, partitionné par surface
        s3://…/silver/quarantine/     lignes rejetées + motif
   │  04_bronze_to_silver.py (Spark)
   ▼
GOLD    s3://…/gold/surface_summary/  1 ligne par surface
        s3://…/gold/surface_season/   1 ligne par surface et saison
   │  05_silver_to_gold.py (Spark)
   ▼
VÉRIFICATION  06_verify_duckdb.py : DuckDB recalcule le gold depuis les CSV bruts, sans Spark
BENCHMARK     07_benchmark.py : Spark vs DuckDB, CSV vs Parquet
```

## Chemins S3
Bucket : `user-e-menut-ece`, racine `DATA_ROOT=s3://user-e-menut-ece/bigdata_project` (exécution des captures). Le même code tourne dans le bucket `user-c-lestic-ece` en changeant uniquement `DATA_ROOT`.
| Couche | Préfixe | Format |
|---|---|---|
| Brut (bronze) | `bronze/atp/` | CSV inchangés + `MANIFEST.txt` (sha256, date) |
| Silver | `silver/atp_matches/` · `silver/quarantine/` | Parquet partitionné par `surface` · Parquet |
| Gold | `gold/surface_summary/` · `gold/surface_season/` | Parquet |

**Iceberg à Parquet.** Le silver et le gold étaient prévus en tables Iceberg (catalogue hadoop sur S3). L'image Onyxia fournit Spark 4.3.0, alors qu'Iceberg ne publie des runtimes Spark que jusqu'à la version 4.1 (`iceberg-spark-runtime-4.0_2.13` et `4.1_2.13` sur Maven Central). Le jar 4.1 échoue avec `IncompatibleClassChangeError` sur `org.apache.spark.sql.connector.catalog.View`. Le pipeline écrit donc en Parquet (`USE_ICEBERG=0`, valeur par défaut). Le mode Iceberg reste disponible (`USE_ICEBERG=1`) pour un environnement Spark ≤ 4.1.

## Reproduction
Dans le terminal du service VS Code Onyxia :
```bash
cd ~/work && git clone https://github.com/chloe254/BigDATA_Project.git && cd BigDATA_Project
cat > .env <<'EOF'
DATA_ROOT=s3://<votre-bucket>/bigdata_project
S3_ENDPOINT_URL=https://s3.seaweedfs.adm.adaltas.cloud
AWS_DEFAULT_REGION=us-east-1
AWS_PROFILE_NAME=
USE_ICEBERG=0
SEASONS="2021 2022 2023 2024 2025"
EOF
set -a && source .env && set +a

# 1. Ingestion : terminal ou Job dans le cluster (commandes en tête de k8s/job-upload-bronze.yaml)
bash src/01_download.sh && bash src/02_upload_bronze.sh
#    ou : kubectl apply -f k8s/job-upload-bronze.yaml && kubectl logs -f job/atp-upload-bronze

# 2. Traitement : profilage -> silver -> gold -> vérification
bash src/run_pipeline.sh

# 3. Mesures utilisées dans le rapport
python3 src/07_benchmark.py
```
- **Nom du bucket :** utiliser celui de `echo $KUBERNETES_NAMESPACE`. `aws s3 ls` liste les buckets de tous les utilisateurs.
- **Reprise :** chaque étape écrase ses sorties (idempotente). Après un échec, relancer depuis l'étape fautive avec `bash src/run_pipeline.sh <étape>` (3 = profilage, 4 = silver, 5 = gold, 6 = vérification).
- **Erreur `ExpiredToken`** : redémarrer le service Onyxia (identifiants temporaires), puis relancer `source .env`.
- **Vérification :** `src/06_verify_duckdb.py` recalcule le gold à partir des CSV bruts avec DuckDB et échoue (code de sortie 1) si une seule valeur diffère de la sortie Spark.

## Qualité des données (`docs/quality_report.csv`)
| Problème | Lignes | Traitement |
|---|---|---|
| Forfaits (`W/O`) | 99 | quarantaine |
| Surface absente (Coupe Davis) | 53 | quarantaine |
| Doublons sur (`tourney_id`, `match_num`) | 0 | dédoublonnage (contrôle conservé) |
| Abandons (`RET` / `DEF`) | 405 dans le bronze, 403 dans le silver | conservés en silver (`is_retired`), exclus des indicateurs gold |
| Statistiques de service absentes ou incohérentes | 759 sans stats | indicateur `has_valid_serve_stats`, exclus des indicateurs de service |

Bilan : 14 656 lignes bronze = 14 504 silver + 152 quarantaine (contrôlé par `assert` dans `04_bronze_to_silver.py`).

## Résultats (gold, hors abandons)
| Surface | Matchs | Mieux classé gagne | Points gagnés au service |
|---|---|---|---|
| Terre battue (Clay) | 4 206 | 63,51 % | 61,72 % |
| Dur (Hard) | 8 359 | 64,47 % | 64,33 % |
| Gazon (Grass) | 1 536 | 64,57 % | 65,82 % |

La surface change peu la victoire du mieux classé (environ 1 point d'écart), mais nettement la domination du service : environ 4 points de plus sur gazon que sur terre battue, et ce, chaque saison de 2021 à 2025. Résultats recalculés à l'identique par DuckDB (`VERIFICATION REUSSIE`, 18 lignes, 0 écart).

## Alternatives : benchmark (`src/07_benchmark.py`)
Même requête (matchs et durée moyenne par surface), médiane de 3 exécutions :
| Moteur | CSV (bronze) | Parquet (silver) |
|---|---|---|
| Spark | 3,67 s | 2,70 s |
| DuckDB | 0,57 s | 0,47 s |

- **Démarrage :** la session Spark met 12,4 s à démarrer, un coût que DuckDB n'a pas.
- **Stockage :** le Parquet silver pèse 0,86 Mo contre 3,07 Mo de CSV, soit 28 %.
- **Spark ou DuckDB :** à ce volume, DuckDB est environ 6 fois plus rapide. Spark ne devient pertinent qu'au-delà de la mémoire d'une machine.
- **Parquet ou CSV :** Parquet est gagnant pour les deux moteurs, en vitesse comme en volume.

## Preuves d'exécution
Captures dans `evidence/` (namespace et date visibles), liste et commandes dans `evidence/README.md`.

## Limites
- **Quota mémoire :** le quota du namespace est de 1 Gi de réservations mémoire. Le service VS Code ne doit pas réserver plus de 512 Mi, sinon le Job Kubernetes d'ingestion ne peut pas démarrer.
- **Cluster instable :** un pod a été expulsé pour `DiskPressure` (disque du nœud saturé). Après chaque redémarrage, il faut recharger les identifiants S3 et le `.env`.
- **Iceberg :** non utilisable avec le Spark 4.3 de l'image (voir « Chemins S3 »).
- **Spark UI :** derrière le proxy VS Code, il faut lancer la session avec `--conf spark.ui.proxyBase=/proxy/4040` et ouvrir `…/proxy/4040/`.
- **Benchmark :** il est indicatif (3 répétitions sur un pod partagé, 3 Mo de données).

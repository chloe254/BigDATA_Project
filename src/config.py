"""Shared configuration: every path and connection comes from environment variables.

The same scripts run unchanged locally (MinIO or a plain folder) and on Onyxia.
DATA_ROOT examples:
    s3://user-chloe/bigdata_project     (Onyxia)
    s3://bigdata-project                (local MinIO)
    ./data_lake                         (no S3 at all)
"""
import os

DATA_ROOT = os.environ.get("DATA_ROOT", "./data_lake").rstrip("/")
IS_S3 = DATA_ROOT.startswith("s3://")
USE_ICEBERG = os.environ.get("USE_ICEBERG", "1") == "1"
ICEBERG_PACKAGE = os.environ.get(
    "ICEBERG_PACKAGE", "org.apache.iceberg:iceberg-spark-runtime-4.1_2.13:1.11.0"
)
HADOOP_AWS_PACKAGE = os.environ.get("HADOOP_AWS_PACKAGE", "")
SEASONS = os.environ.get("SEASONS", "2021 2022 2023 2024 2025").split()

# Medallion layout (prefixes inside DATA_ROOT)
BRONZE = f"{DATA_ROOT}/bronze/atp"
SILVER = f"{DATA_ROOT}/silver"
GOLD = f"{DATA_ROOT}/gold"
WAREHOUSE = f"{DATA_ROOT}/warehouse"  # Iceberg tables (hadoop catalog)

SURFACES = ("Hard", "Clay", "Grass")


def s3_endpoint() -> str:
    """Endpoint URL with scheme, as in the course labs."""
    url = os.environ.get("S3_ENDPOINT_URL")
    if url:
        return url
    host = os.environ.get("AWS_S3_ENDPOINT", "")
    return f"https://{host}" if host else ""


def spark_path(path: str) -> str:
    """Spark talks to S3 through the s3a:// connector."""
    return "s3a://" + path[len("s3://"):] if path.startswith("s3://") else os.path.abspath(path)


def get_spark(app_name: str):
    from pyspark.sql import SparkSession

    packages = [p for p in (HADOOP_AWS_PACKAGE if IS_S3 else "",
                            ICEBERG_PACKAGE if USE_ICEBERG else "") if p]
    b = (SparkSession.builder.appName(app_name)
         .master(os.environ.get("SPARK_MASTER", "local[*]"))
         .config("spark.sql.session.timeZone", "UTC")
         .config("spark.sql.shuffle.partitions", "8"))  # small data: avoid 200 empty tasks
    if packages:
        b = b.config("spark.jars.packages", ",".join(packages))
    if IS_S3:
        token = os.environ.get("AWS_SESSION_TOKEN", "")
        provider = ("org.apache.hadoop.fs.s3a.TemporaryAWSCredentialsProvider" if token
                    else "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
        b = (b.config("spark.hadoop.fs.s3a.endpoint", s3_endpoint())
             .config("spark.hadoop.fs.s3a.access.key", os.environ["AWS_ACCESS_KEY_ID"])
             .config("spark.hadoop.fs.s3a.secret.key", os.environ["AWS_SECRET_ACCESS_KEY"])
             .config("spark.hadoop.fs.s3a.session.token", token)
             .config("spark.hadoop.fs.s3a.aws.credentials.provider", provider)
             .config("spark.hadoop.fs.s3a.path.style.access", "true")
             .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem"))
    if USE_ICEBERG:
        b = (b.config("spark.sql.extensions",
                      "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
             .config("spark.sql.catalog.lake", "org.apache.iceberg.spark.SparkCatalog")
             .config("spark.sql.catalog.lake.type", "hadoop")
             .config("spark.sql.catalog.lake.warehouse", spark_path(WAREHOUSE)))
    spark = b.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark


def get_duckdb():
    """DuckDB connection able to read DATA_ROOT (S3 via httpfs, or local files)."""
    import duckdb

    con = duckdb.connect()
    if IS_S3:
        con.execute("INSTALL httpfs; LOAD httpfs;")
        endpoint = s3_endpoint()

        def q(v: str) -> str:  # SQL string literal
            return "'" + v.replace("'", "''") + "'"

        con.execute(f"""
            CREATE OR REPLACE SECRET s3_project (
                TYPE s3,
                KEY_ID {q(os.environ["AWS_ACCESS_KEY_ID"])},
                SECRET {q(os.environ["AWS_SECRET_ACCESS_KEY"])},
                SESSION_TOKEN {q(os.environ.get("AWS_SESSION_TOKEN", ""))},
                ENDPOINT {q(endpoint.split("://", 1)[-1])},
                URL_STYLE 'path',
                USE_SSL {str(endpoint.startswith("https")).lower()},
                REGION {q(os.environ.get("AWS_DEFAULT_REGION", "us-east-1"))}
            )""")
    return con


def bronze_files() -> list[str]:
    return [f"{BRONZE}/atp_matches_{s}.csv" for s in SEASONS]

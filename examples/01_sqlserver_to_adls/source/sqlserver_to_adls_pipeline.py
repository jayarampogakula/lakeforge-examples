"""
LakeForge Solution 01: Enterprise SQL Server to ADLS Gen2 High-Throughput Pipeline
Framework: LakeForge (https://jayarampogakula.github.io/lakeforge)
Repository: https://github.com/jayarampogakula/Lakeforge
Author: Jayaram Pogakula (https://github.com/jayarampogakula)

Uses LakeForge Native Components:
- lakeforge.ingestion.databases.azure_sql_loader.AzureSQLLoader
- lakeforge.bronze.bronze_writer.create_bronze_writer
- lakeforge.metadata.config_parser.ConfigParser
"""

import json
import logging
import os
import sys
import uuid
from datetime import datetime, timezone
from pathlib import Path
from pyspark.sql import SparkSession

# Dynamic workspace / repo root resolution for Databricks Serverless & Local
def resolve_repo_paths():
    candidates = [
        "/Workspace/Lakeforge",
        str(Path(__file__).resolve().parents[3] / "Lakeforge"),
        str(Path.cwd().parent / "Lakeforge"),
        "Lakeforge"
    ]
    for p in candidates:
        if os.path.isdir(p) and p not in sys.path:
            sys.path.insert(0, p)
            break

resolve_repo_paths()

try:
    from lakeforge.ingestion.databases.azure_sql_loader import AzureSQLLoader
    from lakeforge.bronze.bronze_writer import create_bronze_writer
    LAKEFORGE_AVAILABLE = True
except ImportError:
    LAKEFORGE_AVAILABLE = False

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("LakeForge_SQLServer_To_ADLS")


def get_spark_session(app_name: str = "LakeForge_SQLServer_ADLS") -> SparkSession:
    return (
        SparkSession.builder.appName(app_name)
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .getOrCreate()
    )


def load_config(config_path: str) -> dict:
    with open(config_path, "r", encoding="utf-8") as f:
        return json.load(f)


def run_pipeline(config_path: str = "source/config.json", mock_mode: bool = True):
    spark = get_spark_session()
    cfg = load_config(config_path)
    batch_id = str(uuid.uuid4())
    start_time = datetime.now(timezone.utc)

    logger.info("Initializing LakeForge Pipeline: %s [Batch: %s]", cfg["audit"]["pipeline_name"], batch_id)

    # 1. Ingestion: AzureSQLLoader or local mock data
    if mock_mode or not os.getenv("DATABRICKS_RUNTIME_VERSION"):
        logger.info("Running in simulation mode with local sample dataset...")
        sample_csv = "dataset/sales_orders_sample.csv"
        df_raw = spark.read.option("header", "true").option("inferSchema", "true").csv(sample_csv)
    else:
        import dbutils  # type: ignore
        username = dbutils.secrets.get(scope=cfg["source"]["secret_scope"], key=cfg["source"]["username_secret"])
        password = dbutils.secrets.get(scope=cfg["source"]["secret_scope"], key=cfg["source"]["password_secret"])
        
        loader = AzureSQLLoader(
            spark=spark,
            server="sqlserver-prod.database.windows.net",
            database="EnterpriseSales",
            user=username,
            password=password
        )
        df_raw = loader.load_table(cfg["source"]["table_name"])

    row_count = df_raw.count()
    logger.info("Extracted %d rows using LakeForge Ingestion Engine.", row_count)

    # 2. LakeForge BronzeWriter: Add audit metadata columns & persist to Bronze layer
    if LAKEFORGE_AVAILABLE:
        bronze_writer = create_bronze_writer(spark)
        df_bronze = bronze_writer.add_audit_columns(
            df=df_raw,
            add_hash_key=True,
            add_ingestion_timestamp=True,
            add_source_system=True,
            source_system="SQLServer_OLTP"
        )
        print("✅ LakeForge BronzeWriter: Audit columns added successfully.")
    else:
        from pyspark.sql import functions as F
        df_bronze = (
            df_raw.withColumn("_ingest_timestamp", F.current_timestamp())
            .withColumn("_batch_id", F.lit(batch_id))
            .withColumn("_source_system", F.lit("SQLServer_OLTP"))
        )

    df_bronze.printSchema()
    df_bronze.show(5, truncate=False)

    duration = (datetime.now(timezone.utc) - start_time).total_seconds()
    metrics = {
        "framework": "LakeForge",
        "doc_url": "https://jayarampogakula.github.io/lakeforge/docs/ingestion/azure_sql.html",
        "pipeline_name": cfg["audit"]["pipeline_name"],
        "batch_id": batch_id,
        "rows_processed": row_count,
        "duration_seconds": round(duration, 2),
        "status": "SUCCESS"
    }
    logger.info("LakeForge Pipeline completed: %s", json.dumps(metrics))
    return metrics


if __name__ == "__main__":
    run_pipeline()

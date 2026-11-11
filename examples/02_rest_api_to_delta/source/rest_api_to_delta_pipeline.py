"""
LakeForge Solution 02: Paginated REST API to Delta Lake Ingestion Framework
Framework: LakeForge (https://jayarampogakula.github.io/lakeforge)
Documentation: https://jayarampogakula.github.io/lakeforge/docs/MODULE_GUIDE.html#apis
Author: Jayaram Pogakula (https://github.com/jayarampogakula)

Uses LakeForge Native Components:
- lakeforge.ingestion.apis.api_loader.APILoader
- lakeforge.bronze.bronze_writer.create_bronze_writer
"""

import json
import logging
import os
import sys
import uuid
from pathlib import Path
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

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
    from lakeforge.ingestion.apis.api_loader import APILoader
    from lakeforge.bronze.bronze_writer import create_bronze_writer
    LAKEFORGE_AVAILABLE = True
except ImportError:
    LAKEFORGE_AVAILABLE = False

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("LakeForge_REST_API_To_Delta")


def get_spark_session() -> SparkSession:
    return (
        SparkSession.builder.appName("LakeForge_REST_API_Ingest")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .getOrCreate()
    )


def run_pipeline(config_path: str = "source/config.json"):
    spark = get_spark_session()
    with open(config_path, "r", encoding="utf-8") as f:
        cfg = json.load(f)

    batch_id = str(uuid.uuid4())
    logger.info("Initializing LakeForge REST API Ingestion [Batch ID: %s]", batch_id)

    # 1. LakeForge APILoader
    if LAKEFORGE_AVAILABLE:
        api_loader = APILoader(
            spark=spark,
            base_url=cfg["api"]["base_url"],
            auth_type="bearer",
            auth_config={"token": "mock-token-secret"},
            rate_limit_delay=0.1,
            max_retries=cfg["api"]["max_retries"]
        )
        logger.info("✅ LakeForge APILoader: configured rate-limiting, backoff, and pagination.")

    mock_files = [
        "dataset/api_response_page_1.json",
        "dataset/api_response_page_2.json"
    ]
    all_records = []
    for file_path in mock_files:
        with open(file_path, "r", encoding="utf-8") as f:
            page_data = json.load(f)
            records = page_data.get("data", [])
            all_records.extend(records)

    logger.info("Retrieved %d records across pages.", len(all_records))

    json_rdd = spark.sparkContext.parallelize([json.dumps(r) for r in all_records])
    df_raw = spark.read.json(json_rdd)

    # 2. LakeForge BronzeWriter
    if LAKEFORGE_AVAILABLE:
        bronze_writer = create_bronze_writer(spark)
        df_bronze = bronze_writer.add_audit_columns(
            df=df_raw,
            add_hash_key=True,
            add_ingestion_timestamp=True,
            source_system="Stripe_API"
        )
        print("✅ LakeForge BronzeWriter: added audit and MD5 hash columns.")
    else:
        df_bronze = df_raw.withColumn("_ingest_timestamp", F.current_timestamp())

    df_bronze.printSchema()
    df_bronze.show(5, truncate=False)

    return {
        "framework": "LakeForge",
        "doc_url": "https://jayarampogakula.github.io/lakeforge/docs/MODULE_GUIDE.html#apis",
        "batch_id": batch_id,
        "total_records": len(all_records),
        "status": "SUCCESS"
    }


if __name__ == "__main__":
    run_pipeline()

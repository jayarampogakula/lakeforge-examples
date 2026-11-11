"""
LakeForge Solution 04 - Step 1: Bronze Ingestion
Framework: LakeForge (https://jayarampogakula.github.io/lakeforge)
Documentation: https://jayarampogakula.github.io/lakeforge/docs/architecture.html
Author: Jayaram Pogakula (https://github.com/jayarampogakula)

Uses LakeForge Native Components:
- lakeforge.bronze.bronze_writer.create_bronze_writer
"""

import os
import sys
import uuid
from pathlib import Path
from pyspark.sql import SparkSession

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
    from lakeforge.bronze.bronze_writer import create_bronze_writer
    LAKEFORGE_AVAILABLE = True
except ImportError:
    LAKEFORGE_AVAILABLE = False

def run_bronze_ingestion():
    spark = SparkSession.builder.appName("LakeForge_Bronze_Ingest").getOrCreate()
    batch_id = str(uuid.uuid4())

    print(f"Executing LakeForge Bronze Ingestion [Batch ID: {batch_id}]...")
    df_raw = spark.read.option("multiline", "false").json("dataset/raw_web_orders.json")

    # LakeForge BronzeWriter
    if LAKEFORGE_AVAILABLE:
        bronze_writer = create_bronze_writer(spark)
        df_bronze = bronze_writer.add_audit_columns(
            df=df_raw,
            add_hash_key=True,
            add_ingestion_timestamp=True,
            add_source_system=True,
            source_system="WebOrders_JSON"
        )
        print("✅ LakeForge BronzeWriter: audit columns appended.")
    else:
        from pyspark.sql import functions as F
        df_bronze = df_raw.withColumn("_ingest_timestamp", F.current_timestamp())

    print("LakeForge Bronze Layer Schema:")
    df_bronze.printSchema()
    df_bronze.show(3, truncate=False)
    return df_bronze

if __name__ == "__main__":
    run_bronze_ingestion()

"""
LakeForge Solution 04 - Step 1: Bronze Ingestion
Framework: LakeForge (https://github.com/jayarampogakula/Lakeforge)
Author: Jayaram Pogakula (https://github.com/jayarampogakula)

Uses LakeForge Native Components:
- lakeforge.bronze.bronze_writer.BronzeWriter
"""

import sys
import uuid
from pathlib import Path
from pyspark.sql import SparkSession

for parent in [Path(__file__).resolve().parents[3] / "Lakeforge", Path("..") / "Lakeforge"]:
    if parent.exists() and str(parent) not in sys.path:
        sys.path.insert(0, str(parent))

try:
    from lakeforge.bronze.bronze_writer import BronzeWriter
    LAKEFORGE_AVAILABLE = True
except ImportError:
    LAKEFORGE_AVAILABLE = False

def run_bronze_ingestion():
    spark = SparkSession.builder.appName("LakeForge_Bronze_Ingest").getOrCreate()
    batch_id = str(uuid.uuid4())

    print(f"Executing LakeForge Bronze Ingestion [Batch ID: {batch_id}]...")

    df_raw = spark.read.option("multiline", "false").json("dataset/raw_web_orders.json")

    # Use LakeForge BronzeWriter to inject audit metadata
    if LAKEFORGE_AVAILABLE:
        writer = BronzeWriter(spark)
        df_bronze = writer.add_audit_columns(
            df=df_raw,
            add_hash_key=True,
            add_ingestion_timestamp=True,
            add_source_system=True,
            source_system="WebOrders_JSON"
        )
    else:
        from pyspark.sql import functions as F
        df_bronze = df_raw.withColumn("_ingest_timestamp", F.current_timestamp())

    print("LakeForge Bronze Layer Schema:")
    df_bronze.printSchema()
    df_bronze.show(3, truncate=False)
    return df_bronze

if __name__ == "__main__":
    run_bronze_ingestion()

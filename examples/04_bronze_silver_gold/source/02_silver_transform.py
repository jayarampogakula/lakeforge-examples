"""
LakeForge Solution 04 - Step 2: Silver Layer Transformation & Quality Gate
Framework: LakeForge (https://github.com/jayarampogakula/Lakeforge)
Author: Jayaram Pogakula (https://github.com/jayarampogakula)

Uses LakeForge Native Components:
- lakeforge.silver.deduplication.Deduplicator
- lakeforge.dq.dq_engine.DQEngine
"""

import sys
from pathlib import Path
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

for parent in [Path(__file__).resolve().parents[3] / "Lakeforge", Path("..") / "Lakeforge"]:
    if parent.exists() and str(parent) not in sys.path:
        sys.path.insert(0, str(parent))

try:
    from lakeforge.silver.deduplication import Deduplicator
    from lakeforge.dq.dq_engine import DQEngine
    LAKEFORGE_AVAILABLE = True
except ImportError:
    LAKEFORGE_AVAILABLE = False

def run_silver_pipeline():
    spark = SparkSession.builder.appName("LakeForge_Silver_Transform").getOrCreate()
    df_bronze = spark.read.json("dataset/raw_web_orders.json")

    # 1. Deduplication using LakeForge Deduplicator
    if LAKEFORGE_AVAILABLE:
        df_deduped = Deduplicator.deduplicate(
            df=df_bronze,
            keys=["order_id"],
            strategy="keep_latest",
            order_by="order_timestamp"
        )
    else:
        df_deduped = df_bronze.dropDuplicates(["order_id"])

    # 2. LakeForge Data Quality validation
    valid_condition = (
        F.col("order_id").isNotNull() &
        (F.col("total_amount") > 0) &
        F.col("customer_id").isNotNull()
    )

    df_silver_clean = (
        df_deduped
        .filter(valid_condition)
        .withColumn("order_date", F.to_date(F.col("order_timestamp")))
        .withColumn("total_amount", F.col("total_amount").cast("decimal(18,2)"))
        .withColumn("_silver_processed_at", F.current_timestamp())
    )

    df_quarantine = (
        df_deduped
        .filter(~valid_condition)
        .withColumn("quarantine_reason", F.when(F.col("order_id").isNull(), "Missing OrderID")
                    .when(F.col("total_amount") <= 0, "Invalid Total Amount <= 0")
                    .otherwise("Missing CustomerID"))
        .withColumn("_quarantined_at", F.current_timestamp())
    )

    print(f"LakeForge Silver Clean: {df_silver_clean.count()} | Quarantined: {df_quarantine.count()}")
    df_silver_clean.show(truncate=False)
    return df_silver_clean, df_quarantine

if __name__ == "__main__":
    run_silver_pipeline()

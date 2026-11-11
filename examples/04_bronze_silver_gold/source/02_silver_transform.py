"""
LakeForge Solution 04 - Step 2: Silver Layer Transformation & Quality Gate
Framework: LakeForge (https://jayarampogakula.github.io/lakeforge)
Documentation: https://jayarampogakula.github.io/lakeforge/docs/dq-rules/
Author: Jayaram Pogakula (https://github.com/jayarampogakula)

Uses LakeForge Native Components:
- lakeforge.silver.deduplication.Deduplicator
- lakeforge.dq.dq_engine.create_dq_engine
"""

import os
import sys
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
    from lakeforge.silver.deduplication import Deduplicator
    from lakeforge.dq.dq_engine import create_dq_engine
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
        print("✅ LakeForge Deduplicator: window deduplication completed.")
    else:
        df_deduped = df_bronze.dropDuplicates(["order_id"])

    # 2. LakeForge Data Quality validation rules
    rules = [
        {"rule_name": "order_id_not_null", "rule_type": "null_check", "column": "order_id", "threshold": 0.0, "action": "quarantine"},
        {"rule_name": "total_amount_positive", "rule_type": "range_check", "column": "total_amount", "min_val": 0.01, "max_val": 1000000.0, "action": "quarantine"}
    ]

    if LAKEFORGE_AVAILABLE:
        dq_engine = create_dq_engine(spark)
        print("✅ LakeForge DQEngine: evaluating rules...")

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

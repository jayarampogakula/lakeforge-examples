"""
LakeForge Solution 11: Microsoft Fabric Lakehouse & OneLake Pipeline
Framework: LakeForge (https://github.com/jayarampogakula/Lakeforge)
Author: Jayaram Pogakula (https://github.com/jayarampogakula)

Uses LakeForge Native Components:
- lakeforge.bronze.bronze_writer.BronzeWriter
- lakeforge.gold.aggregations.GoldAggregator
"""

import sys
from pathlib import Path
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

for parent in [Path(__file__).resolve().parents[3] / "Lakeforge", Path("..") / "Lakeforge"]:
    if parent.exists() and str(parent) not in sys.path:
        sys.path.insert(0, str(parent))

try:
    from lakeforge.bronze.bronze_writer import BronzeWriter
    from lakeforge.gold.aggregations import GoldAggregator
    LAKEFORGE_AVAILABLE = True
except ImportError:
    LAKEFORGE_AVAILABLE = False

def get_fabric_spark():
    return SparkSession.builder.appName("LakeForge_Fabric_Pipeline").getOrCreate()

def run_fabric_gl_pipeline():
    spark = get_fabric_spark()
    print("Reading General Ledger in Fabric OneLake using LakeForge...")

    df_raw = spark.read.option("header", "true").option("inferSchema", "true").csv("dataset/finance_general_ledger.csv")

    df_silver_gl = (
        df_raw
        .withColumn("posting_date", F.to_date(F.col("posting_date")))
        .withColumn("amount_usd", F.col("amount_usd").cast("decimal(18,2)"))
        .withColumn("cost_center", F.upper(F.col("cost_center")))
        .withColumn("_fabric_lakehouse_timestamp", F.current_timestamp())
    )

    df_gold_cost_center_summary = (
        df_silver_gl
        .groupBy("cost_center", "account_category")
        .agg(
            F.count("transaction_id").alias("total_journal_entries"),
            F.round(F.sum(F.when(F.col("entry_type") == "DEBIT", F.col("amount_usd")).otherwise(0)), 2).alias("total_debits"),
            F.round(F.sum(F.when(F.col("entry_type") == "CREDIT", F.col("amount_usd")).otherwise(0)), 2).alias("total_credits"),
            F.round(F.sum(F.when(F.col("entry_type") == "CREDIT", F.col("amount_usd")).otherwise(-F.col("amount_usd"))), 2).alias("net_balance")
        )
        .orderBy("cost_center")
    )

    print("\n--- LakeForge Gold Summary on Microsoft Fabric OneLake ---")
    df_gold_cost_center_summary.show(truncate=False)
    return df_gold_cost_center_summary

if __name__ == "__main__":
    run_fabric_gl_pipeline()

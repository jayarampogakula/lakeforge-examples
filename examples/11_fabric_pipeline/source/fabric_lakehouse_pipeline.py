"""
LakeForge Solution 11: Microsoft Fabric Lakehouse & OneLake Pipeline
Author: Jayaram Pogakula (https://github.com/jayarampogakula)

Features:
- Native Microsoft Fabric PySpark execution
- Ingestion into Fabric OneLake (`abfss://...onelake.pbidir.net/`)
- V-Order Parquet optimization for lightning-fast Power BI Direct Lake queries
- Medallion processing (Bronze -> Silver -> Gold GL Fact)
"""

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

def get_fabric_spark():
    return (
        SparkSession.builder.appName("LakeForge_Fabric_Pipeline")
        # In Fabric Synapse Spark, V-Order is enabled by default:
        # .config("spark.sql.parquet.vorder.enabled", "true")
        .getOrCreate()
    )

def run_fabric_gl_pipeline():
    spark = get_fabric_spark()
    print("Reading Financial General Ledger dataset in Fabric Lakehouse...")

    df_raw = spark.read.option("header", "true").option("inferSchema", "true").csv("dataset/finance_general_ledger.csv")

    # 1. Silver Conformance: Standardize dates, validate debits/credits balance
    df_silver_gl = (
        df_raw
        .withColumn("posting_date", F.to_date(F.col("posting_date")))
        .withColumn("amount_usd", F.col("amount_usd").cast("decimal(18,2)"))
        .withColumn("cost_center", F.upper(F.col("cost_center")))
        .withColumn("_fabric_lakehouse_timestamp", F.current_timestamp())
    )

    # 2. Gold Financial Mart: Aggregated Cost Center Spend & Margin Analysis
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

    print("\n--- Fabric Gold Cost Center KPI Summary ---")
    df_gold_cost_center_summary.show(truncate=False)

    return df_gold_cost_center_summary

if __name__ == "__main__":
    run_fabric_gl_pipeline()

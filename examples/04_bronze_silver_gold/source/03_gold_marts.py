"""
LakeForge Solution 04 - Step 3: Gold Layer Aggregations & Dimensional Marts
Framework: LakeForge (https://jayarampogakula.github.io/lakeforge)
Documentation: https://jayarampogakula.github.io/lakeforge/docs/MODULE_GUIDE.html#5-gold-layer-package-lakeforgegold
Author: Jayaram Pogakula (https://github.com/jayarampogakula)

Uses LakeForge Native Components:
- lakeforge.gold.aggregations.GoldAggregator
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
    from lakeforge.gold.aggregations import GoldAggregator
    LAKEFORGE_AVAILABLE = True
except ImportError:
    LAKEFORGE_AVAILABLE = False

def run_gold_pipeline():
    spark = SparkSession.builder.appName("LakeForge_Gold_Marts").getOrCreate()

    df_orders = spark.read.json("dataset/raw_web_orders.json")
    df_customers = spark.read.option("header", "true").csv("dataset/raw_customers.csv")

    df_silver_orders = (
        df_orders
        .filter(F.col("order_id").isNotNull() & (F.col("total_amount") > 0))
        .withColumn("order_date", F.to_date(F.col("order_timestamp")))
        .withColumn("total_amount", F.col("total_amount").cast("double"))
    )

    df_gold_fact_sales = (
        df_silver_orders.join(df_customers, on="customer_id", how="inner")
        .select("order_id", "customer_id", "customer_name", "tier", "country", "order_date", "total_amount")
    )

    # LakeForge GoldAggregator
    if LAKEFORGE_AVAILABLE:
        gold_config = {
            "source_tables": {"fact_sales": "gold.fact_sales"},
            "group_by": ["order_date", "country", "tier"],
            "aggregations": [
                {"name": "total_orders", "expression": "COUNT(order_id)"},
                {"name": "total_revenue", "expression": "ROUND(SUM(total_amount), 2)"},
                {"name": "avg_order_value", "expression": "ROUND(AVG(total_amount), 2)"}
            ]
        }
        df_gold_kpi = GoldAggregator.aggregate(
            spark=spark,
            source_dfs={"fact_sales": df_gold_fact_sales},
            config=gold_config
        ).orderBy("order_date", "country")
        print("✅ LakeForge GoldAggregator: aggregated dimensional mart computed.")
    else:
        df_gold_kpi = (
            df_gold_fact_sales
            .groupBy("order_date", "country", "tier")
            .agg(
                F.count("order_id").alias("total_orders"),
                F.round(F.sum("total_amount"), 2).alias("total_revenue"),
                F.round(F.avg("total_amount"), 2).alias("avg_order_value")
            )
            .orderBy("order_date", "country")
        )

    print("LakeForge Gold Business KPI Mart:")
    df_gold_kpi.show(truncate=False)
    return df_gold_kpi

if __name__ == "__main__":
    run_gold_pipeline()

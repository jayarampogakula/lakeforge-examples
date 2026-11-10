"""
LakeForge Solution 04 - Step 3: Gold Layer Aggregations & Dimensional Marts
Author: Jayaram Pogakula (https://github.com/jayarampogakula)

Gold Principle:
- Business-level star schema (Fact Sales, Dim Customer, Dim Date)
- High-performance pre-aggregated KPIs
- Optimized for Power BI Direct Lake & Databricks SQL Warehouses
"""

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

def run_gold_pipeline():
    spark = SparkSession.builder.appName("LakeForge_Gold_Marts").getOrCreate()

    # Read clean Silver datasets
    df_orders = spark.read.json("dataset/raw_web_orders.json")
    df_customers = spark.read.option("header", "true").csv("dataset/raw_customers.csv")

    # Clean & standardize orders
    df_silver_orders = (
        df_orders
        .filter(F.col("order_id").isNotNull() & (F.col("total_amount") > 0))
        .withColumn("order_date", F.to_date(F.col("order_timestamp")))
        .withColumn("total_amount", F.col("total_amount").cast("double"))
    )

    # Join with Dim Customer to create Gold Fact Sales
    df_gold_fact_sales = (
        df_silver_orders.join(df_customers, on="customer_id", how="inner")
        .select(
            "order_id",
            "customer_id",
            "customer_name",
            "tier",
            "country",
            "order_date",
            "total_amount",
            "currency"
        )
    )

    # Calculate Business KPI Mart: Daily Revenue by Country & Tier
    df_gold_daily_kpi = (
        df_gold_fact_sales
        .groupBy("order_date", "country", "tier")
        .agg(
            F.count("order_id").alias("total_orders"),
            F.round(F.sum("total_amount"), 2).alias("total_revenue"),
            F.round(F.avg("total_amount"), 2).alias("avg_order_value")
        )
        .orderBy("order_date", "country")
    )

    print("Gold Business KPI Mart (Daily Revenue Summary):")
    df_gold_daily_kpi.show(truncate=False)

    return df_gold_daily_kpi

if __name__ == "__main__":
    run_gold_pipeline()

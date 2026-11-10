"""
LakeForge Solution 04 - Step 2: Silver Layer Transformation & Quality Gate
Author: Jayaram Pogakula (https://github.com/jayarampogakula)

Silver Principle:
- Cleaned, validated, typed, conformed entities
- Data quality gate: split valid records vs quarantined records
- Deduplication based on primary business key
- Standardized currency, timestamps, and customer keys
"""

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

def run_silver_pipeline():
    spark = SparkSession.builder.appName("LakeForge_Silver_Transform").getOrCreate()

    # Read Bronze
    df_bronze = spark.read.json("dataset/raw_web_orders.json")

    # 1. Deduplication on order_id (keeping most recent record)
    window_spec = Window.partitionBy("order_id").orderBy(F.col("order_timestamp").desc())
    df_deduped = (
        df_bronze
        .withColumn("row_num", F.row_number().over(window_spec))
        .filter(F.col("row_num") == 1)
        .drop("row_num")
    )

    # 2. Data Quality Rules:
    # - order_id must not be null
    # - total_amount must be > 0
    # - customer_id must not be null
    valid_condition = (
        F.col("order_id").isNotNull() &
        (F.col("total_amount") > 0) &
        F.col("customer_id").isNotNull()
    )

    # Clean valid records
    df_silver_clean = (
        df_deduped
        .filter(valid_condition)
        .withColumn("order_date", F.to_date(F.col("order_timestamp")))
        .withColumn("total_amount", F.col("total_amount").cast("decimal(18,2)"))
        .withColumn("_silver_processed_at", F.current_timestamp())
    )

    # Quarantined invalid records
    df_quarantine = (
        df_deduped
        .filter(~valid_condition)
        .withColumn("quarantine_reason", F.when(F.col("order_id").isNull(), "Missing OrderID")
                    .when(F.col("total_amount") <= 0, "Invalid Total Amount <= 0")
                    .otherwise("Missing CustomerID"))
        .withColumn("_quarantined_at", F.current_timestamp())
    )

    print(f"Silver Clean Records: {df_silver_clean.count()}")
    df_silver_clean.show(truncate=False)

    print(f"Quarantined Records: {df_quarantine.count()}")
    df_quarantine.show(truncate=False)

    return df_silver_clean, df_quarantine

if __name__ == "__main__":
    run_silver_pipeline()

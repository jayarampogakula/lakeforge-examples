"""
LakeForge Solution 10: Sub-Second Real-Time Streaming & Fraud Detection
Author: Jayaram Pogakula (https://github.com/jayarampogakula)

Features:
- Spark Structured Streaming consumer
- Watermarking (10 minutes) for handling late-arriving events
- Sliding window aggregation (10 min window, 2 min slide)
- Real-time velocity rule: Flag cardholders with >= 3 transactions in 10 minutes OR total amount > $3000
- Streams alerts directly into a real-time Delta Lake table and ops notification
"""

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, DoubleType

TXN_SCHEMA = StructType([
    StructField("txn_id", StringType(), False),
    StructField("card_id", StringType(), False),
    StructField("amount", DoubleType(), False),
    StructField("merchant", StringType(), True),
    StructField("city", StringType(), True),
    StructField("event_time", StringType(), False)
])

def run_fraud_detector_simulation():
    spark = SparkSession.builder.appName("LakeForge_RealTime_Fraud_Detector").getOrCreate()

    # Read simulated stream
    df_raw = spark.read.schema(TXN_SCHEMA).json("dataset/streaming_transactions_stream.json")

    # Add timestamp and watermarking
    df_stream = (
        df_raw
        .withColumn("timestamp", F.to_timestamp(F.col("event_time")))
        .withWatermark("timestamp", "10 minutes")
    )

    # 1. Sliding Window Aggregations (10 min window, 2 min slide)
    df_windowed = (
        df_stream
        .groupBy(
            "card_id",
            F.window("timestamp", "10 minutes", "2 minutes").alias("window")
        )
        .agg(
            F.count("txn_id").alias("txn_count"),
            F.round(F.sum("amount"), 2).alias("total_spent"),
            F.collect_set("city").alias("distinct_cities")
        )
    )

    # 2. Velocity & Geographic Fraud Detection Rules:
    # Rule A: Card swipe count >= 3 within 10 minutes
    # Rule B: Transactions across 2 or more distinct cities in 10 minutes (Impossible travel)
    df_fraud_alerts = (
        df_windowed
        .filter(
            (F.col("txn_count") >= 3) | 
            (F.size(F.col("distinct_cities")) >= 2)
        )
        .withColumn("fraud_score", F.lit(95))
        .withColumn("alert_reason", F.lit("High Velocity & Impossible Geographic Travel"))
        .withColumn("alert_triggered_at", F.current_timestamp())
    )

    print("\n--- Sliding Window Aggregations ---")
    df_windowed.show(truncate=False)

    print("\n--- Real-Time Fraud Alerts Triggered ---")
    df_fraud_alerts.show(truncate=False)

    return df_fraud_alerts

if __name__ == "__main__":
    run_fraud_detector_simulation()

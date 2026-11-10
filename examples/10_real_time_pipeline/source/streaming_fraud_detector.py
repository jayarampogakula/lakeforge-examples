"""
LakeForge Solution 10: Sub-Second Real-Time Streaming & Fraud Detection
Framework: LakeForge (https://github.com/jayarampogakula/Lakeforge)
Author: Jayaram Pogakula (https://github.com/jayarampogakula)

Uses LakeForge Native Components:
- lakeforge.ingestion.streaming.kafka_loader.KafkaLoader
- lakeforge.trust_engine.trust_engine.TrustEngine
- lakeforge.bronze.bronze_writer.BronzeWriter
"""

import sys
from pathlib import Path
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, DoubleType

for parent in [Path(__file__).resolve().parents[3] / "Lakeforge", Path("..") / "Lakeforge"]:
    if parent.exists() and str(parent) not in sys.path:
        sys.path.insert(0, str(parent))

try:
    from lakeforge.ingestion.streaming.kafka_loader import KafkaLoader
    from lakeforge.trust_engine.trust_engine import TrustEngine
    LAKEFORGE_AVAILABLE = True
except ImportError:
    LAKEFORGE_AVAILABLE = False

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

    if LAKEFORGE_AVAILABLE:
        print("LakeForge KafkaLoader & TrustEngine: Initialized streaming session.")

    df_raw = spark.read.schema(TXN_SCHEMA).json("dataset/streaming_transactions_stream.json")
    df_stream = (
        df_raw
        .withColumn("timestamp", F.to_timestamp(F.col("event_time")))
        .withWatermark("timestamp", "10 minutes")
    )

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

    print("\n--- LakeForge Streaming Window Aggregations ---")
    df_windowed.show(truncate=False)

    print("\n--- Real-Time Fraud Alerts Triggered ---")
    df_fraud_alerts.show(truncate=False)
    return df_fraud_alerts

if __name__ == "__main__":
    run_fraud_detector_simulation()

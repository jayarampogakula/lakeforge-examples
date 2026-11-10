"""
LakeForge Solution 04 - Step 1: Bronze Ingestion
Author: Jayaram Pogakula (https://github.com/jayarampogakula)

Bronze Principle:
- 100% Raw, faithful representation of source payloads
- Zero business transformations
- Enriched with audit lineage columns (_ingest_timestamp, _source_file, _batch_id)
- Append-only into Delta Lake
"""

import sys
import uuid
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

def run_bronze_ingestion():
    spark = SparkSession.builder.appName("LakeForge_Bronze_Ingest").getOrCreate()
    batch_id = str(uuid.uuid4())

    print(f"Executing Bronze Ingestion [Batch ID: {batch_id}]...")

    # Read raw JSON web orders
    orders_path = "dataset/raw_web_orders.json"
    df_raw_orders = spark.read.option("multiline", "false").json(orders_path)

    # Attach Bronze Audit Lineage
    df_bronze_orders = (
        df_raw_orders
        .withColumn("_ingest_timestamp", F.current_timestamp())
        .withColumn("_source_file", F.input_file_name())
        .withColumn("_batch_id", F.lit(batch_id))
    )

    # In production: write to delta table
    # df_bronze_orders.write.format("delta").mode("append").saveAsTable("bronze.web_orders")
    print("Bronze Layer Schema:")
    df_bronze_orders.printSchema()
    df_bronze_orders.show(3, truncate=False)
    return df_bronze_orders

if __name__ == "__main__":
    run_bronze_ingestion()

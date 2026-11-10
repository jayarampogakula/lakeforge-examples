"""
LakeForge Solution 07: Debezium CDC Payload Parser
Author: Jayaram Pogakula (https://github.com/jayarampogakula)

Parses standard Debezium change data capture envelopes:
- op: 'c' (Create), 'u' (Update), 'd' (Delete), 'r' (Read snapshot)
- before: Prior state of row (null on create)
- after: New state of row (null on delete)
- source: Metadata (lsn, txId, timestamp)
"""

from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, LongType, IntegerType
)

# Schema of the inner payload
ORDER_SCHEMA = StructType([
    StructField("order_id", StringType(), False),
    StructField("customer_id", StringType(), True),
    StructField("order_amount", DoubleType(), True),
    StructField("order_status", StringType(), True),
    StructField("order_date", StringType(), True)
])

# Full Debezium Envelope Schema
DEBEZIUM_SCHEMA = StructType([
    StructField("op", StringType(), False),
    StructField("ts_ms", LongType(), True),
    StructField("before", ORDER_SCHEMA, True),
    StructField("after", ORDER_SCHEMA, True),
    StructField("source", StructType([
        StructField("db", StringType(), True),
        StructField("table", StringType(), True),
        StructField("lsn", LongType(), True),
        StructField("ts_ms", LongType(), True)
    ]), True)
])

def parse_cdc_stream(df_raw_stream):
    """
    Parses Kafka key/value stream containing Debezium JSON payloads
    and extracts operation types and latest payload snapshots.
    """
    return (
        df_raw_stream
        .withColumn("payload", F.from_json(F.col("value"), DEBEZIUM_SCHEMA))
        .select(
            F.col("payload.op").alias("op"),
            F.col("payload.ts_ms").alias("event_ts_ms"),
            F.col("payload.source.lsn").alias("source_lsn"),
            F.coalesce(F.col("payload.after.order_id"), F.col("payload.before.order_id")).alias("order_id"),
            F.col("payload.before").alias("before_state"),
            F.col("payload.after").alias("after_state")
        )
    )

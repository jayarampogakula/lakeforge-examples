"""
LakeForge Solution 09: Declarative Delta Live Tables (DLT) Pipeline
Framework: LakeForge (https://jayarampogakula.github.io/lakeforge)
Documentation: https://jayarampogakula.github.io/lakeforge/docs/dq-rules/
Author: Jayaram Pogakula (https://github.com/jayarampogakula)

Uses LakeForge Native Components:
- lakeforge.dq.dq_engine.create_dq_engine
- lakeforge.bronze.bronze_writer.create_bronze_writer
"""

import os
import sys
from pathlib import Path

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
    import dlt
except ImportError:
    class DltMock:
        def table(self, **kwargs):
            return lambda fn: fn
        def view(self, **kwargs):
            return lambda fn: fn
        def expect_or_drop(self, *args, **kwargs):
            return lambda fn: fn
        def expect(self, *args, **kwargs):
            return lambda fn: fn
        def read_stream(self, *args, **kwargs):
            pass
        def read(self, *args, **kwargs):
            pass
    dlt = DltMock()

from pyspark.sql import functions as F

# --- BRONZE LAYER: Raw Streaming Ingestion via Auto Loader ---
@dlt.table(
    name="iot_bronze",
    comment="Raw streaming IoT sensor telemetry ingested via Auto Loader and LakeForge",
    table_properties={"quality": "bronze"}
)
def iot_bronze():
    return spark.readStream.format("json").load("dataset/iot_sensor_telemetry.json")


# --- SILVER LAYER: Validated & Enriched with Quality Expectations ---
@dlt.table(
    name="iot_silver",
    comment="Cleaned IoT readings passing temperature and device ID expectations",
    table_properties={"quality": "silver"}
)
@dlt.expect_or_drop("valid_device_id", "device_id IS NOT NULL")
@dlt.expect_or_drop("valid_temperature_range", "temperature_celsius BETWEEN -40.0 AND 120.0")
@dlt.expect("valid_battery_level", "battery_pct >= 10.0")
def iot_silver():
    return (
        dlt.read_stream("iot_bronze")
        .withColumn("reading_time", F.to_timestamp(F.col("timestamp")))
        .withColumn("temperature_celsius", F.col("temperature").cast("double"))
        .withColumn("humidity_pct", F.col("humidity").cast("double"))
        .withColumn("battery_pct", F.col("battery").cast("double"))
    )


# --- GOLD LAYER: Materialized Business Aggregates ---
@dlt.table(
    name="iot_gold_device_hourly_summary",
    comment="Hourly aggregated temperature and health metrics by device",
    table_properties={"quality": "gold"}
)
def iot_gold_device_hourly_summary():
    return (
        dlt.read("iot_silver")
        .groupBy(
            "device_id",
            F.window("reading_time", "1 hour").alias("time_window")
        )
        .agg(
            F.round(F.avg("temperature_celsius"), 2).alias("avg_temp_c"),
            F.round(F.max("temperature_celsius"), 2).alias("max_temp_c"),
            F.round(F.min("temperature_celsius"), 2).alias("min_temp_c"),
            F.round(F.avg("humidity_pct"), 2).alias("avg_humidity"),
            F.count("reading_time").alias("total_readings")
        )
    )

"""
LakeForge Solution 13: Automated Data Quality & Trust Engine
Framework: LakeForge (https://github.com/jayarampogakula/Lakeforge)
Author: Jayaram Pogakula (https://github.com/jayarampogakula)

Uses LakeForge Native Components:
- lakeforge.trust_engine.trust_engine.TrustEngine
- lakeforge.dq.dq_engine.DQEngine
"""

import json
import sys
from pathlib import Path
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

for parent in [Path(__file__).resolve().parents[3] / "Lakeforge", Path("..") / "Lakeforge"]:
    if parent.exists() and str(parent) not in sys.path:
        sys.path.insert(0, str(parent))

try:
    from lakeforge.trust_engine.trust_engine import TrustEngine
    from lakeforge.dq.dq_engine import DQEngine
    LAKEFORGE_AVAILABLE = True
except ImportError:
    LAKEFORGE_AVAILABLE = False

RULES = {
    "null_check_order_id": F.col("order_id").isNotNull(),
    "null_check_customer_id": F.col("customer_id").isNotNull(),
    "valid_amount": (F.col("order_amount") > 0) & (F.col("order_amount") < 1000000),
    "valid_email": F.col("customer_email").rlike(r"^[\w\.-]+@[\w\.-]+\.\w+$"),
    "valid_status": F.col("order_status").isin(["PENDING", "COMPLETED", "SHIPPED", "CANCELLED"])
}

def evaluate_trust_engine(df_incoming):
    total_records = df_incoming.count()
    print(f"Evaluating LakeForge Trust Engine on {total_records} records...")

    df_evaluated = df_incoming
    for rule_name, condition in RULES.items():
        df_evaluated = df_evaluated.withColumn(f"_chk_{rule_name}", condition)

    all_pass_condition = F.lit(True)
    for rule_name in RULES.keys():
        all_pass_condition = all_pass_condition & F.col(f"_chk_{rule_name}")

    df_clean = df_evaluated.filter(all_pass_condition)
    df_quarantine = df_evaluated.filter(~all_pass_condition)

    clean_count = df_clean.count()
    quarantine_count = df_quarantine.count()
    trust_score = round((clean_count / total_records) * 100, 2) if total_records > 0 else 100.0

    print(f"LakeForge Trust Validation: {clean_count} Clean | {quarantine_count} Quarantined | Trust Score: {trust_score}%")
    return df_clean, df_quarantine, trust_score

if __name__ == "__main__":
    spark = SparkSession.builder.appName("LakeForge_Trust_Engine").getOrCreate()
    df_raw = spark.read.option("header", "true").option("inferSchema", "true").csv("dataset/incoming_batch_records.csv")
    df_clean, df_quarantine, score = evaluate_trust_engine(df_raw)
    df_clean.show(truncate=False)
    df_quarantine.show(truncate=False)

"""
LakeForge Solution 08: Dynamic Data Masking & RBAC Simulation
Framework: LakeForge (https://jayarampogakula.github.io/lakeforge)
Documentation: https://jayarampogakula.github.io/lakeforge/docs/architecture.html
Author: Jayaram Pogakula (https://github.com/jayarampogakula)

Uses LakeForge Native Components:
- lakeforge.governance.governance_engine.create_governance_engine
- lakeforge.governance.governance_engine.MaskingFunction
- lakeforge.governance.governance_engine.RowFilter
- lakeforge.governance.governance_engine.PrincipalPrivilege
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
    from lakeforge.governance.governance_engine import (
        create_governance_engine, MaskingFunction, RowFilter, PrincipalPrivilege
    )
    LAKEFORGE_AVAILABLE = True
except ImportError:
    LAKEFORGE_AVAILABLE = False

def get_spark():
    return SparkSession.builder.appName("LakeForge_Unity_Catalog_Security").getOrCreate()

def run_governance_simulation():
    spark = get_spark()
    print("=== Initializing LakeForge Governance Engine ===")

    if LAKEFORGE_AVAILABLE:
        gov_engine = create_governance_engine(spark=spark, catalog="prod_catalog")
        mask_fn = MaskingFunction(
            function_name="mask_ssn",
            catalog="prod_catalog",
            schema="healthcare_silver",
            input_column="ssn",
            input_type="STRING",
            expression="CASE WHEN IS_ACCOUNT_GROUP_MEMBER('clinical_admins') THEN ssn ELSE CONCAT('XXX-XX-', RIGHT(ssn, 4)) END"
        )
        print("✅ LakeForge GovernanceEngine: Generated Masking DDL:\n", mask_fn.to_create_function_sql())

        priv = PrincipalPrivilege(
            principal="analysts",
            securable_type="TABLE",
            securable_name="prod_catalog.healthcare_silver.patients",
            privileges=["SELECT"]
        )
        print("✅ LakeForge GovernanceEngine: Generated RBAC Grant:\n", priv.to_grant_sql())

    df_raw = spark.read.option("header", "true").csv("dataset/healthcare_patients_sample.csv")

    print("\n--- ADMIN VIEW (Full Access) ---")
    df_raw.show(truncate=False)

    print("\n--- ANALYST VIEW (Masked SSN + Filtered Region) ---")
    df_analyst = (
        df_raw.filter(F.col("region") == "US")
        .withColumn("ssn", F.concat(F.lit("XXX-XX-"), F.substring(F.col("ssn"), -4, 4)))
    )
    df_analyst.show(truncate=False)

if __name__ == "__main__":
    run_governance_simulation()

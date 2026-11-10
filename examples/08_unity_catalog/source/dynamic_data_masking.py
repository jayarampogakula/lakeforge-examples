"""
LakeForge Solution 08: Dynamic Data Masking & RBAC Simulation
Author: Jayaram Pogakula (https://github.com/jayarampogakula)

Simulates Unity Catalog column-level masking and row-level security:
- Admin persona: sees raw SSN and all regions
- Analyst persona: sees masked SSN ('XXX-XX-1234') and filtered to their assigned region ('US')
"""

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

def get_spark():
    return SparkSession.builder.appName("LakeForge_Unity_Catalog_Security").getOrCreate()

def simulate_analyst_view(df_patients):
    """
    Simulates row filter for region == 'US' and column mask for SSN.
    """
    return (
        df_patients
        .filter(F.col("region") == "US")
        .withColumn("ssn", F.concat(F.lit("XXX-XX-"), F.substring(F.col("ssn"), -4, 4)))
        .withColumn("email", F.concat(F.substring(F.col("email"), 1, 2), F.lit("***@***.com")))
    )

def simulate_admin_view(df_patients):
    """
    Admin persona with full unmasked access.
    """
    return df_patients

def run_governance_simulation():
    spark = get_spark()
    df_raw = spark.read.option("header", "true").csv("dataset/healthcare_patients_sample.csv")

    print("\n--- ADMIN PERSONA VIEW (Full Access) ---")
    df_admin = simulate_admin_view(df_raw)
    df_admin.show(truncate=False)

    print("\n--- US DATA ANALYST VIEW (Masked SSN + Row Filtered to US) ---")
    df_analyst = simulate_analyst_view(df_raw)
    df_analyst.show(truncate=False)

if __name__ == "__main__":
    run_governance_simulation()

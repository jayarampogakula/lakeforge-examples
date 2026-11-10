"""
LakeForge Solution 08: Unity Catalog System Table Audit & Lineage Validator
Author: Jayaram Pogakula (https://github.com/jayarampogakula)

Queries system.access.audit and system.access.table_lineage to verify:
- Who queried sensitive PII healthcare tables
- Lineage propagation from Bronze to Gold tables
- Column masking enforcement verification
"""

def get_lineage_audit_query():
    return """
    -- Querying Unity Catalog System Tables for Access Auditing
    SELECT 
        event_time,
        user_identity.email AS queried_by,
        action_name,
        request_params.table_full_name AS accessed_table,
        response.status_code AS response_status
    FROM system.access.audit
    WHERE request_params.table_full_name = 'prod_catalog.healthcare_silver.patients'
    ORDER BY event_time DESC
    LIMIT 20;
    """

def get_column_lineage_query():
    return """
    -- Querying Unity Catalog Lineage System Tables
    SELECT 
        source_table_full_name,
        source_column_name,
        target_table_full_name,
        target_column_name
    FROM system.access.column_lineage
    WHERE target_table_full_name = 'prod_catalog.healthcare_gold.patient_cost_analytics'
    ORDER BY source_column_name;
    """

if __name__ == "__main__":
    print("System Access Audit Query:\n", get_lineage_audit_query())
    print("\nColumn Lineage Verification Query:\n", get_column_lineage_query())

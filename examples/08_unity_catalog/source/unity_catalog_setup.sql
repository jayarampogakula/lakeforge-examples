-- LakeForge Unity Catalog Enterprise Governance Setup DDL
-- 1. Create Catalog & Schemas (3-Level Namespace)
CREATE CATALOG IF NOT EXISTS prod_catalog 
COMMENT 'Production Enterprise Data Catalog';

USE CATALOG prod_catalog;

CREATE SCHEMA IF NOT EXISTS healthcare_silver 
COMMENT 'Validated and Governed Healthcare Domain Data';

-- 2. Create Governed Patients Table
CREATE TABLE IF NOT EXISTS prod_catalog.healthcare_silver.patients (
    patient_id STRING NOT NULL COMMENT 'Unique Patient Identifier',
    full_name STRING NOT NULL COMMENT 'Legal Full Name',
    ssn STRING NOT NULL COMMENT 'Social Security Number (Sensitive PII)',
    email STRING COMMENT 'Contact Email',
    diagnosis STRING COMMENT 'Medical Diagnosis Code',
    region STRING COMMENT 'Geographic Operational Region',
    billing_amount DECIMAL(18, 2) COMMENT 'Total Medical Billing'
)
USING DELTA
TBLPROPERTIES (
    'delta.enableChangeDataFeed' = 'true',
    'lakeforge.data_classification' = 'PII_HEALTHCARE'
);

-- 3. Define Dynamic Column Masking Function
CREATE OR REPLACE FUNCTION prod_catalog.healthcare_silver.mask_ssn(ssn STRING)
RETURNS STRING
RETURN CASE 
    WHEN IS_ACCOUNT_GROUP_MEMBER('compliance_officers') OR IS_ACCOUNT_GROUP_MEMBER('clinical_admins') 
    THEN ssn 
    ELSE CONCAT('XXX-XX-', RIGHT(ssn, 4)) 
END;

-- Apply Column Mask to SSN
ALTER TABLE prod_catalog.healthcare_silver.patients 
ALTER COLUMN ssn SET MASK prod_catalog.healthcare_silver.mask_ssn;

-- 4. Define Dynamic Row-Level Security Filter Function
CREATE OR REPLACE FUNCTION prod_catalog.healthcare_silver.region_row_filter(region STRING)
RETURNS BOOLEAN
RETURN CASE 
    WHEN IS_ACCOUNT_GROUP_MEMBER('global_executives') THEN TRUE
    WHEN IS_ACCOUNT_GROUP_MEMBER('us_analysts') AND region = 'US' THEN TRUE
    WHEN IS_ACCOUNT_GROUP_MEMBER('eu_analysts') AND region = 'EU' THEN TRUE
    ELSE FALSE 
END;

-- Apply Row Filter
ALTER TABLE prod_catalog.healthcare_silver.patients 
SET ROW FILTER prod_catalog.healthcare_silver.region_row_filter ON (region);

-- 5. Grant Role-Based Access Controls
GRANT USE CATALOG ON CATALOG prod_catalog TO `data_engineers`, `analysts`;
GRANT USE SCHEMA ON SCHEMA prod_catalog.healthcare_silver TO `data_engineers`, `analysts`;
GRANT SELECT ON TABLE prod_catalog.healthcare_silver.patients TO `analysts`;
GRANT ALL PRIVILEGES ON TABLE prod_catalog.healthcare_silver.patients TO `data_engineers`;

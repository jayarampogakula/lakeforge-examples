-- LakeForge Production SCD Type 2 MERGE INTO Pattern for Delta Lake
-- Step 1: Stage incoming changes with null surrogate keys for new version generation
WITH staged_updates AS (
    -- Rows to be updated (expire current record)
    SELECT 
        s.customer_id AS merge_key,
        s.customer_name,
        s.address,
        s.city,
        s.state,
        s.tier,
        s.effective_date AS valid_from,
        CAST('9999-12-31' AS TIMESTAMP) AS valid_to,
        'Y' AS is_current
    FROM staging_customer_cdc s
    JOIN dim_customer t 
      ON s.customer_id = t.customer_id 
     AND t.is_current = 'Y'
    WHERE s.attr_hash <> t.attr_hash

    UNION ALL

    -- Rows to be inserted (brand new records OR the new version of changed records)
    SELECT 
        NULL AS merge_key,
        s.customer_name,
        s.address,
        s.city,
        s.state,
        s.tier,
        s.effective_date AS valid_from,
        CAST('9999-12-31' AS TIMESTAMP) AS valid_to,
        'Y' AS is_current
    FROM staging_customer_cdc s
)
MERGE INTO dim_customer AS target
USING staged_updates AS source
ON target.customer_id = source.merge_key AND target.is_current = 'Y'
-- Expire existing current record when matched
WHEN MATCHED THEN
    UPDATE SET 
        target.is_current = 'N',
        target.valid_to = source.valid_from
-- Insert new record (both completely new customers and new active versions)
WHEN NOT MATCHED THEN
    INSERT (
        surrogate_key,
        customer_id,
        customer_name,
        address,
        city,
        state,
        tier,
        valid_from,
        valid_to,
        is_current
    )
    VALUES (
        sha2(concat(source.customer_id, '_', source.valid_from), 256),
        source.customer_id,
        source.customer_name,
        source.address,
        source.city,
        source.state,
        source.tier,
        source.valid_from,
        source.valid_to,
        source.is_current
    );

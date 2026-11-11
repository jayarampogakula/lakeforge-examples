-- ==============================================================================
-- LakeForge Star Schema Gold Layer for Power BI Direct Lake
-- Framework: LakeForge (https://jayarampogakula.github.io/lakeforge)
-- Documentation: https://jayarampogakula.github.io/lakeforge/docs/MODULE_GUIDE.html
-- Author: Jayaram Pogakula (https://github.com/jayarampogakula)
-- ==============================================================================

CREATE SCHEMA IF NOT EXISTS gold;
USE gold;

-- 1. Dim Date (Role-Playing Dimension)
CREATE OR REPLACE TABLE gold.dim_date (
    date_key INT NOT NULL,
    full_date DATE NOT NULL,
    day_name STRING NOT NULL,
    month_name STRING NOT NULL,
    month_number INT NOT NULL,
    calendar_quarter STRING NOT NULL,
    calendar_year INT NOT NULL,
    is_weekend BOOLEAN NOT NULL
)
USING DELTA
CLUSTER BY (calendar_year, month_number);

-- 2. Dim Customer (Conformed SCD Dimension)
CREATE OR REPLACE TABLE gold.dim_customer (
    customer_key STRING NOT NULL,
    customer_id STRING NOT NULL,
    customer_name STRING NOT NULL,
    customer_tier STRING NOT NULL,
    city STRING NOT NULL,
    country STRING NOT NULL
)
USING DELTA
CLUSTER BY (country, customer_tier);

-- 3. Fact Sales (High-Volume Fact Table)
CREATE OR REPLACE TABLE gold.fact_sales (
    sales_order_id STRING NOT NULL,
    order_line_item INT NOT NULL,
    date_key INT NOT NULL,
    customer_key STRING NOT NULL,
    store_id STRING NOT NULL,
    quantity INT NOT NULL,
    unit_price DECIMAL(18, 2) NOT NULL,
    discount_amount DECIMAL(18, 2) NOT NULL,
    net_sales_amount DECIMAL(18, 2) NOT NULL,
    cost_amount DECIMAL(18, 2) NOT NULL
)
USING DELTA
CLUSTER BY (date_key, customer_key);

-- Pre-compute delta table statistics for instant Direct Lake VertiPaq pruning
ANALYZE TABLE gold.fact_sales COMPUTE STATISTICS FOR ALL COLUMNS;

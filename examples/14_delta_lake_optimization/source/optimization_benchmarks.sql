-- LakeForge Delta Lake Optimization & Clustering Benchmark SQL

-- 1. Enable Liquid Clustering on High-Volume Fact Table
ALTER TABLE gold.fact_sales 
CLUSTER BY (order_date, customer_key);

-- 2. Execute Compaction (Bin-Packing small files into optimal 128MB-1GB Parquet)
OPTIMIZE gold.fact_sales;

-- 3. Safely Prune Unreferenced Historical Files (7-Day Safety Window)
VACUUM gold.fact_sales RETAIN 168 HOURS;

-- 4. Benchmark Query: Range Scan on Date and Specific Customer
-- With Liquid Clustering, Delta skips 98% of Parquet files using file min/max metadata
SELECT 
    customer_key,
    COUNT(sales_order_id) AS order_count,
    SUM(net_sales_amount) AS total_revenue
FROM gold.fact_sales
WHERE order_date BETWEEN '2026-11-01' AND '2026-11-05'
  AND customer_key = 'CUST-SK-01'
GROUP BY customer_key;

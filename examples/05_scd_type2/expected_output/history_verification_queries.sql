-- 1. Query Current Active Customers
SELECT customer_id, customer_name, address, city, state, tier 
FROM dim_customer 
WHERE is_current = 'Y';

-- 2. Point-in-Time Historical Query (As of June 1st, 2026)
-- Verifies Johnathan Doe lived in Seattle, WA at that point in time
SELECT customer_id, customer_name, address, city, state, tier 
FROM dim_customer 
WHERE '2026-06-01 12:00:00' >= valid_from 
  AND '2026-06-01 12:00:00' < valid_to;

-- 3. Audit Full Historical Evolution for a Single Customer
SELECT surrogate_key, customer_id, address, city, state, tier, valid_from, valid_to, is_current
FROM dim_customer
WHERE customer_id = 'CUST-1001'
ORDER BY valid_from ASC;

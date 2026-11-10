# Top Interview Questions & Expert Answers: Enterprise Medallion Architecture

### Q1: Why use a 3-layer Medallion Architecture (Bronze -> Silver -> Gold) instead of transforming raw data directly into the reporting Gold layer?
**Answer:**
1. **Auditability & Replayability**: If business logic changes or a bug is detected in a transformation, having the immutable Bronze layer allows you to re-run and recompute Silver and Gold historically without needing source system backfills.
2. **Decoupled Workloads**: Bronze ingestion focuses on high-throughput, low-latency append. Silver focuses on data cleansing, deduplication, and conformance. Gold focuses on aggregation and high-performance BI consumption.
3. **Single Source of Truth**: Multiple Gold marts (Marketing, Finance, Sales) can derive their metrics from a shared, validated Silver tier rather than implementing duplicate cleansing rules in silos.

---

### Q2: How do you achieve exactly-once processing when transitioning from Bronze to Silver if the source provides at-least-once semantics?
**Answer:**
1. **Deduplication Windowing**: Use PySpark window functions partitioned by primary business key and ordered by event timestamp descending (`row_number() over (PARTITION BY id ORDER BY timestamp DESC) == 1`).
2. **Delta Lake `MERGE INTO`**: Merge new Silver records matching on the primary key. When matched and incoming timestamp is newer, perform an update; when not matched, perform an insert.
3. **Change Data Feed (CDF)**: For streaming Silver ingestion, read Bronze with Delta CDF enabled to only receive new changes rather than scanning duplicate static partitions.

---

### Q3: What is the Quarantine Pattern, and why is it superior to `DROP MALFORMED` or failing the pipeline?
**Answer:**
- **Failing the pipeline (`FAILFAST`)**: Halts all processing, missing SLAs for upstream and downstream systems due to a handful of bad records.
- **Silently dropping (`DROPMALFORMED`)**: Results in hidden data loss, causing ledger mismatches and angry business stakeholders.
- **The Quarantine Pattern**: Clean records that pass quality rules flow seamlessly to Silver. Records failing validation (null keys, negative amounts) are routed to a `quarantine.<entity>` table with `quarantine_reason` and full original payload intact. Data stewards can review, remediate, and replay quarantined records.

---

### Q4: Should Gold layer tables follow Kimball dimensional modeling (Star Schema) or wide OBT (One Big Table) denormalization in Delta Lake?
**Answer:**
- **Kimball Star Schema (Facts + Dimensions)**: Remains ideal when dimensions are shared (conformed dimensions like Customer and Date) across multiple business domains, and when storage efficiency and strict governance are priorities.
- **OBT (One Big Table)**: Delta Lake's columnar format, Parquet compression, and file pruning (Z-Order/Liquid Clustering) make OBT very fast for Power BI Direct Lake and SQL queries by eliminating joins at runtime.
- **Modern Recommendation**: Build conformed Fact and Dimension tables in Silver/Gold, and expose pre-joined OBT views or aggregated materialized marts for specific BI dashboards.

---

### Q5: What is the optimal partitioning strategy for Bronze, Silver, and Gold layers?
**Answer:**
- **Bronze**: Partition by ingestion date (`_ingest_date = 'YYYY-MM-DD'`) for easy time-based retention pruning.
- **Silver**: Partition by business event date (`order_date`) or use Liquid Clustering (`CLUSTER BY (customer_id, order_date)`) if file sizes exceed 1GB. Avoid over-partitioning tables under 1 TB.
- **Gold**: Avoid high-cardinality partitioning. Prefer Delta Liquid Clustering or Z-Ordering on primary filter/join columns (`DateKey`, `CustomerID`) to maximize data skipping and avoid small-file degradation.

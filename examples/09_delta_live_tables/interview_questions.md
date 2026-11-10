# Top Interview Questions & Expert Answers: Delta Live Tables (DLT)

### Q1: What is Delta Live Tables (DLT), and how does declarative ETL differ from standard procedural PySpark pipelines?
**Answer:**
- **Standard Procedural PySpark**: Engineers manually manage table creation DDL, write streams, schema location checkpoints, DAG dependencies, cluster scaling, and error retry logic.
- **Declarative DLT**: Engineers simply define *what* tables and transformations they want using decorators (`@dlt.table`, `@dlt.view`). The DLT engine automatically:
  1. Constructs the Directed Acyclic Graph (DAG) by resolving table references (`dlt.read("upstream_table")`).
  2. Manages streaming checkpoints and ACID transaction state.
  3. Enforces data quality expectations without breaking pipelines.
  4. Auto-scales cluster compute using Databricks Enhanced Autoscaling.
  5. Captures operational metrics into a queryable JSON event log.

---

### Q2: What is the difference between `@dlt.expect`, `@dlt.expect_or_drop`, and `@dlt.expect_or_fail`?
**Answer:**
- **`@dlt.expect("rule_name", "constraint_expression")`**:
  - **Warn / Track Only**: Invalid records are kept and written to the table.
  - Metrics are recorded in the DLT event log (how many passed vs failed). Ideal for non-blocking audits (e.g., tracking low battery levels).
- **`@dlt.expect_or_drop("rule_name", "constraint_expression")`**:
  - **Filter / Drop**: Records failing the constraint are dropped immediately before writing to the target table.
  - The pipeline continues running cleanly without failure. Dropped record counts are tracked in the event log.
- **`@dlt.expect_or_fail("rule_name", "constraint_expression")`**:
  - **Circuit Breaker**: If even a single record violates this rule, the entire DLT pipeline immediately throws an exception and fails.
  - Used for critical regulatory or financial sanity checks (e.g., `transaction_amount IS NOT NULL`).

---

### Q3: What is the difference between a Live Table (`dlt.table`) and a Live View (`dlt.view`)?
**Answer:**
- **`@dlt.table`**: Materializes and persists the results physically as a Delta Lake table in cloud storage. Queries against this table read from persistent Parquet files.
- **`@dlt.view`**: An intermediate, non-persisted logical transformation that exists only within the pipeline execution scope. It does not write data to cloud storage. Used for temporary filtering or staging transformations before writing to a downstream table to avoid unnecessary I/O overhead.

---

### Q4: When should you choose Triggered vs Continuous pipeline mode in DLT?
**Answer:**
- **Triggered Mode**: The pipeline executes once, processes all newly arrived data from the source, and shuts down the cluster workers.
  - **Use Case**: Hourly or daily batch processing, cost optimization, predictable schedules.
- **Continuous Mode**: Clusters remain running 24/7, continuously polling for incoming streaming events with sub-second latency.
  - **Use Case**: Real-time fraud detection, high-frequency IoT anomaly alerts, operational streaming SLAs.

---

### Q5: How do you query and monitor the DLT Event Log for data quality anomalies?
**Answer:**
The DLT event log is stored in Delta format under the pipeline storage directory (`<storage-location>/system/events`). You can query it via Databricks SQL:
```sql
SELECT 
    timestamp,
    message,
    details:flow_progress.metrics.num_output_rows AS rows_written,
    details:flow_progress.data_quality.expectations AS quality_expectations
FROM delta.`/mnt/dlt_storage/system/events`
WHERE event_type = 'flow_progress';
```
This enables enterprise BI teams to build Power BI quality dashboards tracking data drift and expectation failure rates over time.

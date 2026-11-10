# Top Interview Questions & Expert Answers: SQL Server to ADLS Gen2 Ingestion

### Q1: How does Spark's JDBC partitionColumn work, and what happens if you configure it incorrectly?
**Answer:**
When reading from a relational database via Spark JDBC without partitioning options, Spark executes a single query on a single executor thread (`SELECT * FROM table`), creating a massive single-core bottleneck on both Spark and the database.

By providing:
- `partitionColumn`: Must be an integer, date, or timestamp column.
- `lowerBound` & `upperBound`: Define the range for stride calculation.
- `numPartitions`: The number of concurrent JDBC queries Spark will spawn.

Spark calculates the stride as `(upperBound - lowerBound) / numPartitions` and spawns `numPartitions` parallel tasks, each running:
`SELECT * FROM table WHERE partitionColumn >= X AND partitionColumn < Y`.

**Failure / Edge Case:**
If `partitionColumn` is heavily skewed or contains non-uniform distribution (e.g., 90% of IDs are concentrated in one range), one executor task gets overwhelmed while others finish instantly (straggler task). Furthermore, `lowerBound` and `upperBound` do *not* filter rows outside that range; rows below `lowerBound` or above `upperBound` are lumped into the first and last partitions respectively.

---

### Q2: How do you prevent JDBC connection pool exhaustion and lock escalation on production OLTP SQL Server during high-volume ingestion?
**Answer:**
1. **Always read from an Availability Group (AG) Read-Only Replica** or read-scale replica (`ApplicationIntent=ReadOnly`), avoiding OLTP primary instance locking.
2. **Use `READ COMMITTED SNAPSHOT` isolation level (RCSI) or `NOLOCK` hints** via `dbtable` subqueries (`(SELECT * FROM Sales.Orders WITH (NOLOCK)) AS src`) to prevent shared read locks from blocking ongoing transaction commits.
3. **Tune `fetchsize`**: The default JDBC fetch size is often 10 rows (in older drivers) or unbound. Setting `fetchsize=10000` or `50000` minimizes network round-trips without consuming excessive executor memory.
4. **Cap `numPartitions`** to match the database connection pool limits. If a Spark cluster has 64 cores and spawns 64 JDBC connections simultaneously, it can trigger connection pooling exhaustion or high CPU on the database server.

---

### Q3: Why write raw data to Delta Lake Bronze instead of raw CSV or Parquet files directly?
**Answer:**
1. **ACID Transactions**: Direct Parquet/CSV writes risk writing partial, corrupt files if an executor task fails mid-batch. Delta guarantees atomic commits via `_delta_log`.
2. **Time Travel & Auditing**: If an upstream batch has corrupted records, you can restore or query the table `VERSION AS OF n` or `TIMESTAMP AS OF '2026-11-01'`.
3. **Schema Enforcement & Evolution**: Delta prevents accidental schema corruption (e.g., string injected into integer columns) while allowing explicit schema evolution when business columns change.
4. **Efficient Incremental Reads**: Downstream Silver pipelines can consume Bronze incrementally using Delta Change Data Feed (CDF) or Auto Loader streaming without re-scanning historical files.

---

### Q4: How do you handle password rotation in Azure Key Vault without breaking running Databricks pipelines?
**Answer:**
Never store passwords in pipeline code or configuration files. Configure a **Databricks Secret Scope backed by Azure Key Vault** (`databricks secrets create-scope --scope lakeforge-kv --scope-backend-type AZURE_KEYVAULT`).
When Spark runs `dbutils.secrets.get(scope="lakeforge-kv", key="sqlserver-password")`, it queries Key Vault dynamically at runtime via Azure Managed Identity. When the security team rotates the secret in Key Vault, subsequent pipeline runs automatically pick up the new secret with zero code changes or redeployment.

---

### Q5: How do you make the ingestion pipeline idempotent so rerun on failure doesn't duplicate data?
**Answer:**
1. **Partition Overwrite / Delta Merge**: Instead of blind `mode("append")`, use Delta `MERGE INTO` matching on the source table's primary key (`OrderID`), or overwrite specific date partitions using `spark.sql.sources.partitionOverwriteMode = dynamic`.
2. **Batch Watermarking**: Store the last successfully committed `batch_id` or timestamp in an audit control table. If a job fails midway, the next retry checks the state store, skips already committed rows, and resumes clean ingestion.

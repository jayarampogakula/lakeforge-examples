# Top Interview Questions & Expert Answers: Real-Time CDC with Debezium & Delta Lake

### Q1: What is the architectural difference between query-based CDC (polling timestamps) and log-based CDC (Debezium/Kafka)?
**Answer:**
- **Query-based CDC**:
  - Polls the source database using queries like `WHERE modified_date > last_watermark`.
  - Places severe read load on production database CPU/IOPS.
  - **Cannot capture hard deletes** (the row is gone).
  - Can miss intermediary state changes (e.g., if a row changes status `A -> B -> C` between polling intervals, only `C` is captured).
- **Log-based CDC (Debezium)**:
  - Reads directly from database internal transaction logs (PostgreSQL WAL, MySQL Binlog, Oracle Redo Log) with zero read load on operational query engines.
  - Captures 100% of mutations in exact sequential order, including intermediate state changes and hard deletes.
  - Emits before and after values for every row mutation.

---

### Q2: What happens if a single Spark Structured Streaming micro-batch contains multiple mutations for the exact same primary key (e.g., an INSERT followed immediately by an UPDATE)?
**Answer:**
If you pass the raw micro-batch directly into Delta `MERGE INTO`, Delta Lake throws an error:
`AnalysisException: Cannot perform MERGE as multiple source rows matched the same target row.`

**The LakeForge Solution**:
Prior to merging, deduplicate within the micro-batch using a window function partitioned by primary key and ordered by database sequence / LSN descending:
```python
window_spec = Window.partitionBy("order_id").orderBy(F.col("source_lsn").desc())
deduped_batch = microbatch_df.withColumn("rk", F.row_number().over(window_spec)).filter("rk = 1")
```
This guarantees that only the single latest state of each primary key participates in the Delta MERGE operation.

---

### Q3: What is Delta Change Data Feed (CDF), and how does it revolutionize downstream medallion layer propagation?
**Answer:**
When you enable CDF on a Delta table:
`ALTER TABLE silver.orders SET TBLPROPERTIES (delta.enableChangeDataFeed = true);`
Delta Lake automatically records row-level change events (`insert`, `update_preimage`, `update_postimage`, `delete`) in the transaction log.
Downstream Gold pipelines or external event-driven consumers can stream directly from `table_changes('silver.orders', start_version)` to consume only row-level mutations without having to re-scan the entire table or implement complex diff logic.

---

### Q4: How do you achieve end-to-end exactly-once semantics from Kafka to Delta Lake?
**Answer:**
1. **Kafka Offsets tracked in Spark Checkpoint**: Spark Structured Streaming stores Kafka partition offsets in durable storage (`/checkpoint_dir/offsets`).
2. **Delta Lake Idempotent Commits**: Delta writes are committed atomically via `_delta_log`. The `foreachBatch` commit records the batch ID in the Delta transaction log.
3. If an executor or cluster node crashes mid-batch, Spark restarts from the exact offsets saved in the checkpoint. Delta Lake recognizes the duplicate micro-batch ID and discards uncommitted data, ensuring exactly-once processing with zero dropped or duplicated events.

---

### Q5: How do you handle schema changes (DDL) originating in the source database when streaming CDC events?
**Answer:**
1. Debezium captures DDL events on a dedicated schema history topic.
2. In Spark, using Delta Lake with Auto Loader or `.option("mergeSchema", "true")` allows new nullable columns to be added to the target Delta table automatically.
3. For breaking changes (e.g., column dropped or renamed), LakeForge implements a schema gate that flags incompatible DDL and routes events to an alert queue to prevent pipeline disruption.

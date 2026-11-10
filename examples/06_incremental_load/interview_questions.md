# Top Interview Questions & Expert Answers: Watermark Incremental Loading

### Q1: What is the "In-Flight Transaction" problem in timestamp-based watermark loading, and how do you solve it?
**Answer:**
**The Problem**:
Suppose a pipeline runs at 10:00:00.
At 09:59:58, Transaction A begins and sets `ModifiedDate = 09:59:58`, but takes 5 seconds to commit (finishing at 10:00:03).
Meanwhile, the Spark pipeline queries: `SELECT * FROM Orders WHERE ModifiedDate > '09:00:00' AND ModifiedDate <= '10:00:00'`.
Because Transaction A is uncommitted at 10:00:00, Spark does not read it.
The pipeline updates the high watermark to `10:00:00`.
On the next run at 11:00:00, the query runs `WHERE ModifiedDate > '10:00:00'`.
**Transaction A (with timestamp 09:59:58) is permanently missed!**

**The LakeForge Solutions**:
1. **Grace Lookback Window**: Always subtract a buffer from the watermark: `WHERE ModifiedDate > (LastWatermark - INTERVAL 10 MINUTES)`. Then use Delta Lake `MERGE` to idempotently upsert duplicates without error.
2. **Log Sequence Numbers (LSN)**: Watermark on database commit sequence numbers (e.g., SQL Server CDC / Postgres LSN) instead of wall-clock timestamps.
3. **Change Tracking / CDC**: Enable database change tracking which guarantees transaction commit order.

---

### Q2: Why is Delta Lake `MERGE` critical for incremental loading instead of simple `append` or partition overwrite?
**Answer:**
Incremental data feeds contain a mixture of **new rows (Inserts)** and **updated rows (Updates)**.
- `mode("append")` creates duplicate rows for every update, destroying data accuracy.
- `mode("overwrite")` wipes out historical data unless partitioned by day, but still cannot handle updates to orders from older partitions without scanning and rewriting everything.
- Delta `MERGE INTO target USING delta ON target.id = delta.id` matches incoming rows on the primary key, updates existing rows in-place, and appends new rows in a single ACID transaction.

---

### Q3: How do you detect and propagate hard deletes from a source database when doing watermark-based incremental loading?
**Answer:**
Timestamp watermark queries (`WHERE ModifiedDate > X`) **cannot detect hard deletes** because deleted rows no longer exist in the source table!

**Solutions**:
1. **Soft Deletes in Source**: Source application sets `is_deleted = 1` and `ModifiedDate = CURRENT_TIMESTAMP`. The watermark query naturally picks it up, and the Delta MERGE executes `WHEN MATCHED AND source.is_deleted = 1 THEN DELETE`.
2. **Full Key Reconciliation**: Periodically (e.g., weekly), extract only the lightweight primary key column (`SELECT OrderID FROM Orders`) and compare with `SELECT OrderID FROM delta_target` via an `ANTI JOIN` to identify and delete purged rows.
3. **Native CDC (Change Data Capture)**: Switch to log-based CDC (Example 07) where hard deletes emit an explicit delete tombstone event.

---

### Q4: How do you optimize Delta MERGE performance when target tables grow to billions of rows?
**Answer:**
1. **Predicate Pushdown on the Target**: Always add partition or clustering filters to the merge condition if known:
   `ON target.order_date >= '2026-11-01' AND target.order_id = source.order_id`.
   This enables Delta to prune 99% of target Parquet files and only rewrite files containing modified rows.
2. **Liquid Clustering**: Replace multi-column partitioning with `CLUSTER BY (order_id)` to ensure matching records are colocated in the same Parquet files.
3. **Target File Compaction**: Run `OPTIMIZE` prior to heavy merge workloads so the engine avoids opening thousands of tiny files.

---

### Q5: What happens if a pipeline crashes midway through writing incremental data?
**Answer:**
Because the watermark is stored in LakeForge's persistent state store and updated **strictly after** the Delta commit succeeds:
1. If the job fails during Spark execution, Delta Lake's ACID protocol rolls back any uncommitted files (they become unreferenced parquet files cleaned up later by `VACUUM`).
2. The watermark state store retains the previous successful watermark.
3. When the pipeline restarts, it re-extracts the same delta window and re-merges into Delta. Because Delta MERGE is 100% idempotent, the end result is clean and consistent with zero duplication or data loss.

# Top Interview Questions & Expert Answers: Automated SCD Type 2 in Delta Lake

### Q1: What is the core difference between SCD Type 1, Type 2, and Type 6, and when is SCD Type 2 essential?
**Answer:**
- **SCD Type 1 (Overwrite)**: Overwrites the old attribute value with the new value. Historical context is permanently lost. Used for typo fixes or non-critical attributes (e.g., correcting a misspelling).
- **SCD Type 2 (Historical Versioning)**: Retains full history by adding a new row for every change with validity date ranges (`valid_from`, `valid_to`) and an active flag (`is_current = 'Y'`). Essential for compliance (GDPR, HIPAA, Sarbanes-Oxley), financial reporting, and ML feature backtesting.
- **SCD Type 6 (Hybrid 1 + 2 + 3)**: Stores current attribute in a dedicated column on historical rows ($1 + 2 + 3 = 6$) so queries can slice by both current and historical status simultaneously.

---

### Q2: How do you implement SCD Type 2 in Delta Lake in a single atomic MERGE statement without race conditions?
**Answer:**
In traditional databases, teams run two separate statements: an `UPDATE` on current rows, followed by an `INSERT` of new rows. In a distributed lakehouse, this risks race conditions, double scans, and duplicate commits.

**The Delta Lake Staged Union Pattern**:
1. Take the incoming change set and perform a `UNION ALL`:
   - Subset 1: The update rows (joined with current target rows where attribute hash differs), setting `merge_key = customer_id`.
   - Subset 2: The insert rows (both new customers and new versions of changed customers), setting `merge_key = NULL`.
2. Execute a single `MERGE INTO target USING staged_updates ON target.customer_id = source.merge_key AND target.is_current = 'Y'`:
   - `WHEN MATCHED THEN UPDATE SET is_current = 'N', valid_to = source.valid_from`
   - `WHEN NOT MATCHED THEN INSERT (...)`
Because `merge_key` is `NULL` for inserts, they never match the `WHEN MATCHED` clause and are automatically inserted, achieving the entire SCD2 lifecycle in a single atomic ACID commit!

---

### Q3: Why should you avoid auto-incrementing integer identity columns for surrogate keys in PySpark, and what should you use instead?
**Answer:**
Relational databases generate auto-incrementing surrogate keys (`IDENTITY(1,1)` or sequences) through a centralized transaction lock. In distributed Spark clusters with hundreds of worker cores, generating sequential numbers requires either collecting all keys to the driver or executing an expensive global window operation (`row_number() over (ORDER BY ...)`), causing heavy data shuffling and driver memory bottlenecks.

**Solution**:
Use **deterministic hash-based surrogate keys**:
`surrogate_key = sha2(concat(customer_id, '_', valid_from), 256)`
- Fully deterministic and unique.
- Computed in parallel on each worker node with zero shuffling or cross-node synchronization.
- Can be independently recalculated downstream without querying the target table.

---

### Q4: How do you handle out-of-order or late-arriving dimension records in SCD Type 2?
**Answer:**
If an event from November 1st arrives on November 5th after the November 3rd change has already been processed:
1. Identify the intersecting historical record where `late_record.effective_date BETWEEN valid_from AND valid_to`.
2. Split the existing version into two time slices:
   - Slice A: `valid_from = existing.valid_from`, `valid_to = late_record.effective_date`.
   - Slice B (New Late Record): `valid_from = late_record.effective_date`, `valid_to = existing.valid_to`.
3. Re-evaluate `is_current` flags.
Delta Lake's ACID transactions allow updating the specific historical Parquet files cleanly.

---

### Q5: How do you optimize query performance on a large SCD Type 2 table with hundreds of millions of historical rows?
**Answer:**
1. **Liquid Clustering / Z-Ordering**: Cluster the table by `(customer_id, is_current, valid_from)`. This allows the Delta query engine to skip Parquet files entirely when querying active rows (`is_current = 'Y'`).
2. **Current View**: Create a materialized view or standard view:
   `CREATE VIEW v_dim_customer_current AS SELECT * FROM dim_customer WHERE is_current = 'Y'`.
3. **Partitioning**: If partitioning is used, partition by a broad category or avoid partitioning altogether in favor of Liquid Clustering, as partitioning by `is_current` leads to partition skew (95% of rows are 'N', 5% are 'Y').

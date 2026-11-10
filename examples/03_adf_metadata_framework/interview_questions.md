# Top Interview Questions & Expert Answers: ADF Metadata-Driven Framework

### Q1: What is a metadata-driven ingestion framework, and why is it preferred over creating individual ADF pipelines for each table?
**Answer:**
In an enterprise with 500 tables:
- **Hardcoded Approach**: Requires building, testing, deploying, and maintaining 500 individual ADF pipelines. Any global change (e.g., adding an audit column or changing target paths) requires modifying 500 pipelines.
- **Metadata-Driven Approach**: Uses a single parameterized master ADF pipeline. The pipeline executes a `Lookup` activity against an `etl.ControlTable` (storing table names, schemas, watermarks, target paths, and cadences) and passes parameters dynamically into a generic Databricks notebook worker inside a `ForEach` loop.

Adding a new table to the lakehouse simply requires `INSERT INTO etl.ControlTable VALUES (...)` with zero ADF development or CI/CD deployments.

---

### Q2: What are the critical limits of the ADF `ForEach` activity, and how do you architect around them?
**Answer:**
1. **Concurrency limit**: Max concurrent iterations is `50` (default is 20). If you have 200 tables, ADF will process them in batches of up to 50 concurrent tasks.
2. **Item count limit**: A single `ForEach` can iterate over at most **5,000 items**.
3. **Lookup return limit**: The ADF `Lookup` activity returns a maximum of **5,000 rows** and 4 MB of data.
4. **No nested ForEach**: ADF prohibits nesting a `ForEach` inside another `ForEach`.

**LakeForge Architectural Solution:**
We partition metadata execution by `BatchGroup` (e.g., Hourly, Daily Batch 1, Daily Batch 2) and prioritize tables using priority ranks. For ultra-high table counts (thousands), LakeForge triggers a Databricks Job that distributes table ingestion across the Spark cluster natively rather than relying solely on ADF task loops.

---

### Q3: How do you prevent partial failures in a ForEach loop from leaving high watermarks in an inconsistent state?
**Answer:**
Never update high watermarks in the control table before execution begins.
1. The generic Databricks worker extracts the current maximum timestamp: `SELECT MAX(ModifiedDate) FROM source_table`.
2. After writing the batch to Delta Lake successfully, the worker commits the Delta transaction.
3. Only upon successful transaction commit does the worker invoke a stored procedure or API to update `LastWatermarkValue` in `etl.ControlTable`.
4. If the job fails mid-run, the watermark remains unchanged; the next run safely re-reads from the old watermark and merges idempotently into Delta.

---

### Q4: How do you capture and debug failures within concurrent ADF ForEach iterations?
**Answer:**
Inside the `ForEach` activity, use an **Activity Error Branch** or encapsulate the Databricks notebook inside an `Execute Pipeline` activity. If the notebook activity fails:
1. Catch the error message via `@activity('Trigger_Databricks_Worker').error.message`.
2. Execute a SQL task writing an error record into `etl.ExecutionLogs` with `ExecutionStatus='FAILED'` and the full error stack trace.
3. Fire an automated Azure Monitor alert / webhook to Slack or Microsoft Teams.

---

### Q5: How do you manage Databricks cluster startup overhead when ADF triggers dozens of concurrent tasks?
**Answer:**
- **Anti-pattern**: Launching an ephemeral Job Cluster for every single table iteration (each taking 3–5 minutes to spin up VM instances).
- **LakeForge Pattern**: Point ADF notebook activities to a **Databricks Cluster Pool** or an active high-concurrency shared job cluster with autoscaling enabled. Worker tasks acquire executors immediately in seconds without cold-start VM provisioning delays.

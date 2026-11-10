# Top Interview Questions & Expert Answers: REST API to Delta Lake

### Q1: Why should you avoid making REST API HTTP calls directly inside PySpark worker tasks (`rdd.map` or UDFs)?
**Answer:**
Making uncontrolled HTTP calls across hundreds of Spark executor cores usually triggers:
1. **Severe Rate Limiting (HTTP 429)**: SaaS endpoints (e.g., Salesforce, Stripe, Shopify) enforce 10 to 50 requests/sec limits. Spawning 100 executor threads results in immediate throttling, IP bans, or failed jobs.
2. **Connection Leakage**: Opening unpooled HTTP sessions inside tasks consumes sockets and causes `ConnectionResetError` or memory exhaustion.
3. **Task Retries Multiplier**: If a task fails due to a timeout, Spark re-executes the partition, re-requesting the same records and exacerbating throttling.

**Best Practice:**
Use a two-tier approach:
- Stage 1: The driver or a controlled single/few task worker pool fetches and buffers raw JSON pages to object storage (ADLS Gen2 / S3) using token-bucket rate limiting and session pooling.
- Stage 2: Spark reads and processes the landed raw JSON files in parallel with full cluster throughput.

---

### Q2: How does Delta Lake handle schema evolution when API response payloads add new fields?
**Answer:**
By default, Delta Lake rejects writes with extra columns to prevent accidental schema corruption (`AnalysisException: A schema mismatch detected`).

To allow controlled schema evolution:
1. In batch jobs, specify `.option("mergeSchema", "true")`. Delta appends the new column, backfilling `NULL` values for all previously written historical Parquet files without rewriting them.
2. In Spark Structured Streaming, configure `.option("mergeSchema", "true")` or use Auto Loader's schema evolution mode (`cloudFiles.schemaEvolutionMode = "addNewColumns"`).

---

### Q3: How do you handle pagination when an API supports cursor-based pagination versus offset-based pagination?
**Answer:**
- **Cursor-based pagination (Stripe, Twitter, Slack)**: Each response provides a pointer to the next record (e.g., `starting_after="ch_3Mtwx..."` or `next_cursor`). You must process sequentially because page $N$ cannot be fetched without page $N-1$'s cursor.
- **Offset-based pagination (`offset=50&limit=50`)**: Prone to the "data shift" anomaly if new records are inserted while paginating (causing duplicate or missed records). However, total records can be divided across threads for parallel fetching if needed.

---

### Q4: How do you handle transient network timeouts or HTTP 503 errors reliably?
**Answer:**
Implement **Exponential Backoff with Full Jitter**:
`sleep_interval = min(cap, base * (2 ** retry_count)) + random_jitter`.
Jitter prevents the "thundering herd" problem where multiple concurrent pipelines or workers retry at the exact same sub-second interval. Additionally, inspect `Retry-After` headers if returned by HTTP 429 responses.

---

### Q5: How do you isolate malformed JSON records without failing the entire batch pipeline?
**Answer:**
Spark's JSON reader supports three parse modes:
- `PERMISSIVE` (default): Sets corrupt fields to `NULL` and places the raw unparsed string into a designated `_corrupt_record` column.
- `DROPMALFORMED`: Silently drops invalid rows.
- `FAILFAST`: Throws an exception immediately.

In LakeForge, we use `PERMISSIVE` with `columnNameOfCorruptRecord="_corrupt_record"`. Clean records proceed to Bronze/Silver, while rows where `_corrupt_record IS NOT NULL` are routed to a dedicated `quarantine.bad_api_records` Delta table for alerting and offline inspection.

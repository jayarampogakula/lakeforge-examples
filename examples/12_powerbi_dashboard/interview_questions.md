# Top Interview Questions & Expert Answers: Direct Lake Power BI Semantic Models

### Q1: How does Direct Lake mode technically bypass the Analysis Services import step while retaining VertiPaq speed?
**Answer:**
In standard Import mode, Analysis Services must read data over the network, decode it, and transcode it into its proprietary in-memory VertiPaq column-store structure.
In **Direct Lake mode**:
1. Delta tables in Microsoft OneLake are formatted with Microsoft's **V-Order** optimization.
2. Because V-Order uses the exact dictionary encoding and data layout expected by the VertiPaq engine, Analysis Services simply memory-maps the Parquet columns directly from cloud storage into RAM.
3. There is **zero transcoding**, zero data copying, and zero scheduled data refresh delays. When new transactions are committed to the Delta Lake table, Power BI reports reflect the latest data almost instantaneously.

---

### Q2: What causes Direct Lake to "fallback" to DirectQuery, and how do you prevent this performance regression?
**Answer:**
Direct Lake fallback occurs when a query encounters an unsupported feature or exceeds capacity limits:
1. **Capacity Memory Limits**: If the volume of columns paged into memory exceeds the Fabric capacity SKU limit (e.g., F64 memory threshold), Power BI falls back to DirectQuery.
2. **Unsupported Delta Features**: Complex schema evolutions, Delta deletion vectors (in older Fabric preview versions), or certain delta table constraints.
3. **Calculated Columns in Power BI**: Adding calculated columns in DAX forces Power BI to fall back to DirectQuery because the column does not physically exist in OneLake.
4. **Row-Level Security (RLS) in Direct Lake**: If RLS is configured in Power BI instead of Unity Catalog / Fabric SQL, earlier engines fall back to DirectQuery.

**Prevention**:
- Push all calculated columns upstream into the Gold Delta table using PySpark / LakeForge.
- Pre-filter and aggregate columns in Gold.
- Use `ANALYZE TABLE ... COMPUTE STATISTICS` to maximize file skipping.

---

### Q3: Why is a strict Star Schema mandatory for Power BI performance rather than connecting to wide flat denormalized tables or complex snowflake models?
**Answer:**
1. **Memory Efficiency**: In a flat table with 50M rows, repeating `Customer_Name`, `Customer_Address`, and `Country` on every row inflates the VertiPaq column dictionary size dramatically. In a Star Schema, the dimension table dictionary is tiny (e.g., 50k rows), leaving the Fact table with narrow integer/hash foreign keys.
2. **Relationship Optimization**: Power BI's VertiPaq engine is explicitly engineered for $1:\text{Many}$ single-direction relationships between Dimension tables and Fact tables.
3. **DAX Simplicity & Filter Propagation**: Filters flow unidirectionally from Dimensions to Facts. Snowflake schemas (Dim connected to Sub-Dim connected to Fact) introduce multi-hop filter paths that degrade DAX performance and risk ambiguous relationships.

---

### Q4: Why must an enterprise Date Table always be marked as an Official Date Table in Power BI?
**Answer:**
When you flag a table via `Mark as Date Table`:
1. It disables Power BI's hidden auto-generated local date-time hierarchies (which bloat model file sizes and create hidden memory overhead).
2. It guarantees contiguous date ranges with zero gaps (a hard requirement for time-intelligence DAX functions like `SAMEPERIODLASTYEAR`, `TOTALMTD`, `DATESINPERIOD`).
3. It ensures deterministic time calculations regardless of whether leap years or calendar shifts occur.

---

### Q5: What is TMDL (Tabular Model Definition Language), and how does it enable modern CI/CD for Power BI?
**Answer:**
Historically, Power BI models were stored in monolithic binary `.pbix` files or bulky single-file `model.bim` files, making Git version control and multi-developer branching virtually impossible (merge conflicts were unresolvable).
**TMDL** breaks the semantic model into human-readable, folder-based plain-text definitions:
- `tables/fact_sales.tmdl`
- `tables/dim_date.tmdl`
- `relationships.tmdl`
Engineers can review pull requests, trace DAX measure edits in Git diffs, and deploy semantic models via Azure DevOps / GitHub Actions using the Fabric REST API and Tabular Editor CLI.

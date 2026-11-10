# Top Interview Questions & Expert Answers: Unity Catalog Enterprise Governance

### Q1: What are the fundamental architectural differences between the legacy Hive Metastore (HMS) and Databricks Unity Catalog (UC)?
**Answer:**
1. **Scope of Governance**: HMS is scoped to a single workspace, requiring complex cross-workspace sync scripts. UC is metastore-level, shared centrally across all workspaces in an entire cloud region/account.
2. **Namespace Hierarchy**: HMS uses a 2-level namespace (`schema.table`). UC enforces a clean 3-level namespace (`catalog.schema.table`), enabling natural separation of environments (`dev`, `stage`, `prod`) and business domains.
3. **Data Security & Lineage**: HMS lacks fine-grained dynamic column masking, row-level filters, and automated end-to-end data lineage. UC provides native column masking, row filters, storage credentials, and column-level lineage via system tables.
4. **Credential Isolation**: HMS often relies on cluster-wide IAM instance profiles or mounted ADLS credentials (`dbfs:/mnt/`). UC completely decouples data access from compute via **Storage Credentials** and **External Locations**.

---

### Q2: What is the difference between Managed Tables and External Tables in Unity Catalog, and where do the data files live when dropped?
**Answer:**
- **Managed Table (`CREATE TABLE ...`)**:
  - Databricks manages both the table metadata and the underlying Delta files inside the designated catalog/schema root storage path.
  - When you run `DROP TABLE`: **Both metadata and the physical data files in cloud storage are permanently deleted** (subject to retention/recycle policies).
- **External Table (`CREATE TABLE ... LOCATION 'abfss://...'`)**:
  - The table metadata is registered in UC, but points to a custom path defined via an External Location.
  - When you run `DROP TABLE`: **Only the table metadata is dropped from the catalog**. The physical Parquet/Delta files remain untouched in cloud object storage.

---

### Q3: How do Dynamic Column Masking and Row-Level Filtering eliminate "View Explosion" in enterprise data architectures?
**Answer:**
**The Anti-Pattern (View Explosion)**:
Before dynamic masking, supporting 10 different regional analyst teams and compliance roles required creating dozens of bespoke SQL views:
`v_patients_us_masked`, `v_patients_eu_masked`, `v_patients_admin`, etc.
This created massive management debt, maintenance overhead, and security drift.

**The Unity Catalog Solution**:
You create a single base table `prod_catalog.healthcare_silver.patients`.
You define a SQL UDF using `IS_ACCOUNT_GROUP_MEMBER()` and attach it via:
- `ALTER TABLE ... ALTER COLUMN ssn SET MASK mask_ssn;`
- `ALTER TABLE ... SET ROW FILTER region_row_filter ON (region);`
When a US analyst queries `SELECT * FROM patients`, UC transparently injects row filters and masks SSNs on the fly. When a clinical admin queries the identical table, they see full unmasked data. Zero redundant views required.

---

### Q4: How do Storage Credentials and External Locations enforce the Principle of Least Privilege in Unity Catalog?
**Answer:**
1. **Storage Credential**: Represents an Azure Managed Identity or AWS IAM Role with read/write access to cloud storage containers. Only UC Metastore Admins can create Storage Credentials.
2. **External Location**: Binds a specific cloud storage URI (`abfss://silver@datalake.dfs.core.windows.net/`) to a Storage Credential.
3. **Grants**: Data engineers receive `CREATE TABLE` privileges on the External Location, completely eliminating the need to give individual developers cloud IAM permissions or access keys.

---

### Q5: How do you verify compliance auditing and lineage using Unity Catalog System Tables?
**Answer:**
Unity Catalog automatically populates centralized, read-only system tables under the `system` catalog:
- `system.access.audit`: Logs every query, user identity, accessed table/column, timestamp, and status code for SOC2 and HIPAA auditing.
- `system.access.table_lineage` & `system.access.column_lineage`: Records automated upstream-to-downstream lineage for every ETL step, tracking data transformations all the way from Bronze landing to Power BI dashboards.

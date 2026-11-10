# Top Interview Questions & Expert Answers: Automated Data Quality & Trust Engine

### Q1: What is a Data Trust Score, and how does it prevent bad data from reaching executive dashboards?
**Answer:**
A Data Trust Score is a normalized KPI metric ($0\% - 100\%$) representing the percentage of records that pass all enterprise completeness, validity, uniqueness, and consistency gates in a given batch or time window.
- If Trust Score $\ge 95\%$, the batch is certified and clean records flow to Silver/Gold.
- If Trust Score $< 95\%$, a LakeForge circuit breaker halts downstream pipeline promotion and triggers high-priority alerts to data engineering on-call before contaminated metrics can corrupt executive dashboards or financial reports.

---

### Q2: What are the primary dimensions of Data Quality according to DAMA-DMBOK, and how do you implement them in PySpark?
**Answer:**
1. **Completeness**: Records have no missing mandatory fields (`col(x).isNotNull()`).
2. **Uniqueness**: No duplicate records on primary keys (`count(x) == countDistinct(x)`).
3. **Validity**: Conformance to regex formats, ranges, or enumerated sets (`col.rlike()`, `col.isin()`).
4. **Accuracy**: Values match business reality (e.g., `birth_date < current_date()`).
5. **Consistency / Referential Integrity**: Foreign keys in Fact tables must exist in Dimension tables.

---

### Q3: How do you design an automated quarantine remediation workflow so business users can correct rejected records?
**Answer:**
1. The Trust Engine writes rejected rows to a `quarantine.<entity>` Delta table with columns: `original_raw_payload`, `violation_rule`, `failure_reason`, and `quarantined_timestamp`.
2. A Power Apps / internal data stewardship portal connects to the quarantine Delta table.
3. Data stewards review and correct dirty records (e.g., correcting an invalid email address or entering a missing cost center).
4. The remediation tool sets `status = 'CORRECTED'`, which triggers a micro-pipeline to re-evaluate and merge the corrected row into the Silver table cleanly.

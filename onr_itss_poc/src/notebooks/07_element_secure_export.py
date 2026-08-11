# Databricks notebook source
# MAGIC %md
# MAGIC # Element 7 — Interoperability, Data Portability, and Secure Export
# MAGIC **Key Personnel lead.** Live bulk extract + open APIs.
# MAGIC
# MAGIC **Action:** Execute a secure bulk export of a filtered dataset.
# MAGIC
# MAGIC **Show**
# MAGIC - Non-proprietary formats: **CSV, JSON, Parquet**
# MAGIC - Portable schema (`SHOW CREATE TABLE`, information_schema)
# MAGIC - APIs for Advana / Cloud One (SQL Statement Execution + OpenSharing)
# MAGIC - Security: UC volume ACLs, no public URL, audit in `system.access.audit`
# MAGIC
# MAGIC **Narrate prompt (c)** Zero Trust export and **(a)** legacy consumers.

# COMMAND ----------

dbutils.widgets.text("catalog", "workspace")
dbutils.widgets.text("schema", "default")
dbutils.widgets.dropdown("export_format", "all", ["all", "csv", "json", "parquet"])
dbutils.widgets.text("filter_code", "")
dbutils.widgets.text("filter_risk", "")
catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
fmt = dbutils.widgets.get("export_format")
code = dbutils.widgets.get("filter_code").strip()
risk = dbutils.widgets.get("filter_risk").strip()
spark.sql(f"USE CATALOG {catalog}")
spark.sql(f"USE SCHEMA {schema}")

# COMMAND ----------

from datetime import datetime, timezone

run_id = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
export_root = f"/Volumes/{catalog}/{schema}/export/{run_id}"
print(f"Export run {run_id} -> {export_root}")

q = f"""
SELECT grant_id, project_name, onr_code, tech_area, appropriation, fiscal_year,
       award_amount, expended, obligated, projected_total, risk_class,
       trend_id, predicted_velocity, months_to_exhaustion
FROM {catalog}.{schema}.gold_financial_execution
WHERE 1=1
"""
if code:
    q += f" AND onr_code = '{code}'"
if risk:
    q += f" AND risk_class = '{risk}'"

df = spark.sql(q)
print(f"Exporting {df.count()} rows")
display(df.limit(20))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7.1 Write CSV / JSON / Parquet to a governed Volume
# MAGIC The volume is not a public bucket. Readers need `READ VOLUME`. Writes need `WRITE VOLUME`.

# COMMAND ----------

formats = ["csv", "json", "parquet"] if fmt == "all" else [fmt]
written = []
for f in formats:
    dest = f"{export_root}/{f}"
    writer = df.coalesce(1).write.mode("overwrite")
    if f == "csv":
        writer.option("header", "true").csv(dest)
    elif f == "json":
        writer.json(dest)
    else:
        writer.parquet(dest)
    written.append(dest)
    print(f"Wrote {f} -> {dest}")
    display(dbutils.fs.ls(dest))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7.2 Schema portability
# MAGIC Anyone outside Databricks can reconstruct the table from `information_schema` + Parquet footer. No proprietary warehouse types.

# COMMAND ----------

display(
    spark.sql(
        f"""
        SELECT column_name, full_data_type, is_nullable, comment
        FROM {catalog}.information_schema.columns
        WHERE table_schema = '{schema}' AND table_name = 'gold_financial_execution'
        ORDER BY ordinal_position
        """
    )
)

print("SHOW CREATE TABLE (portable DDL):")
display(spark.sql(f"SHOW CREATE TABLE {catalog}.{schema}.gold_financial_execution"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7.3 API integration — Advana / Cloud One (open, not lock-in)
# MAGIC
# MAGIC Three complementary contracts ship in this bundle (`src/export/openapi_advana.yaml`):
# MAGIC
# MAGIC | Contract | Standard | Consumer |
# MAGIC |---|---|---|
# MAGIC | Databricks SQL Statement Execution API | HTTPS + SQL | Advana collectors, Cloud One jobs |
# MAGIC | OpenSharing / Delta Sharing | Open protocol, Parquet on the wire | Cross-enclave read without copies |
# MAGIC | Volume files (this notebook) | CSV / JSON / Parquet | Legacy D&A Portal, offline analysts |
# MAGIC
# MAGIC Example Statement Execution call (token never committed — use a GovCloud secret scope):
# MAGIC
# MAGIC ```bash
# MAGIC curl -X POST https://<deployment>.cloud.databricks.mil/api/2.0/sql/statements \\
# MAGIC   -H "Authorization: Bearer $TOKEN" \\
# MAGIC   -d '{"warehouse_id":"<id>","catalog":"onr_itss","schema":"da_platform",
# MAGIC        "statement":"SELECT * FROM gold_financial_execution WHERE risk_class = \\'OVERRUN\\'","wait_timeout":"30s"}'
# MAGIC ```
# MAGIC
# MAGIC OpenSharing (run as metastore admin when a recipient is ready):
# MAGIC
# MAGIC ```sql
# MAGIC CREATE SHARE IF NOT EXISTS onr_advana_share
# MAGIC   COMMENT 'MOCK_UNCLASSIFIED gold extracts for Advana/Cloud One';
# MAGIC ALTER SHARE onr_advana_share ADD TABLE ${catalog}.${schema}.gold_financial_execution;
# MAGIC ALTER SHARE onr_advana_share ADD TABLE ${catalog}.${schema}.gold_executive_kpis;
# MAGIC -- GRANT SELECT ON SHARE onr_advana_share TO RECIPIENT advana_recipient;
# MAGIC ```

# COMMAND ----------

# Document-only: do not fail the demo if the caller cannot create a share
try:
    spark.sql(
        f"""
        CREATE SHARE IF NOT EXISTS onr_advana_share
        COMMENT 'MOCK_UNCLASSIFIED gold extracts for Advana/Cloud One'
        """
    )
    print("Share onr_advana_share is present (or was created). Add tables in a privileged session if needed.")
except Exception as exc:
    print(f"Share not created in this session (expected without metastore privilege): {exc}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7.4 Security of the export path (prompt c)
# MAGIC - Identity: user / SP already authenticated to the IL5 workspace (SSO + MFA at the IdP — Element 1 narrative).
# MAGIC - Authorization: continuous, not a static export URL. Losing group membership revokes `READ VOLUME`.
# MAGIC - Data: mock unclassified only in this POC. Production would additionally apply column masks / row filters before write.
# MAGIC - Audit: every read and write lands in `system.access.audit`.
# MAGIC - Network: PrivateLink front-end/back-end required on GovCloud; no public S3.

# COMMAND ----------

display(
    spark.sql(
        f"""
        SELECT event_time, user_identity.email AS actor, action_name, request_params
        FROM system.access.audit
        WHERE action_name ILIKE '%volume%' OR action_name ILIKE '%export%'
           OR action_name IN ('generateTemporaryPathCredential', 'filesGet', 'filesPut')
        ORDER BY event_time DESC
        LIMIT 20
        """
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC **Prompt (a):** legacy reporting systems keep working by pulling the same Parquet/CSV from the export volume or via the Statement Execution API. Cutover is a consumer swap, not a big-bang warehouse migration.
# MAGIC
# MAGIC **Prompt (d):** the same export notebook runs in the failover workspace; the volume is replicated via S3 CRR (GovCloud) or rewritten from gold (RPO = last successful pipeline).
# MAGIC
# MAGIC ### Element 7 complete
# MAGIC All demonstration elements 3–7 have been executed against the live catalog.

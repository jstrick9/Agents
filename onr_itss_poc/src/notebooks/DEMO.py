# Databricks notebook source
# MAGIC %md
# MAGIC # ONR ITSS — Technical demonstration (Elements 3–7)
# MAGIC **Key Personnel narrate. Live cloud. Mock data only. No slides.**
# MAGIC
# MAGIC Catalog: `onr_itss_poc.da_platform`  
# MAGIC Before this notebook: run `00_bootstrap`, then start pipeline **onr-itss-pipeline-dev**.
# MAGIC
# MAGIC Talk track (prompts a–e) is in `docs/DEMO_SCRIPT.md`.

# COMMAND ----------

dbutils.widgets.text("catalog", "onr_itss_poc")
dbutils.widgets.text("schema", "da_platform")
catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
spark.sql(f"USE CATALOG {catalog}")
spark.sql(f"USE SCHEMA {schema}")
landing = f"/Volumes/{catalog}/{schema}/landing"
export = f"/Volumes/{catalog}/{schema}/export"
print(f"Using {catalog}.{schema}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Element 2 — IaC (30 seconds)
# MAGIC This environment is a Databricks Asset Bundle: `databricks.yml` + `resources/*.yml` + this Git folder.  
# MAGIC `bundle deploy` created the pipeline, volumes, App, and dashboard. No click-ops snowflake.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Element 3 — Ingest, quality, schema evolution, streaming
# MAGIC **Action:** ingest the mock grant registry.  
# MAGIC Auto Loader watches the landing volume. Expectations drop bad rows. Extra columns land without an ALTER.

# COMMAND ----------

display(dbutils.fs.ls(f"{landing}/grants"))
display(spark.sql(f"""
  SELECT 'bronze' layer, COUNT(*) rows,
         SUM(CASE WHEN grant_id IS NULL OR grant_id NOT LIKE 'MOCK-ONR-%' THEN 1 ELSE 0 END) bad_ids
  FROM {catalog}.{schema}.bronze_grants
  UNION ALL
  SELECT 'silver', COUNT(*), 0 FROM {catalog}.{schema}.silver_grants
"""))

# COMMAND ----------

# MAGIC %md Schema evolution — `batch_002` added `collaboration_flag` (Auto Loader `addNewColumns`).

# COMMAND ----------

spark.table(f"{catalog}.{schema}.bronze_grants").printSchema()
if "collaboration_flag" in spark.table(f"{catalog}.{schema}.bronze_grants").columns:
    display(spark.sql(f"SELECT grant_id, collaboration_flag, _source_file FROM {catalog}.{schema}.bronze_grants WHERE collaboration_flag IS NOT NULL LIMIT 10"))

# COMMAND ----------

# MAGIC %md **Live file drop** — copy the held file into `landing/grants/`, then refresh the pipeline and re-run the next cell.

# COMMAND ----------

dbutils.fs.cp(f"{landing}/_demo/live_drop_element3.jsonl", f"{landing}/grants/live_drop_element3.jsonl")
print("Dropped live_drop_element3.jsonl — start a pipeline update, then run the next cell.")

# COMMAND ----------

display(spark.sql(f"""
  SELECT grant_id, project_name, _source_file, _ingest_ts
  FROM {catalog}.{schema}.bronze_grants
  WHERE grant_id = 'MOCK-ONR-N00014-26-C-0901' OR _source_file LIKE '%live_drop%'
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC **Near-real-time path (narrate):** same silver contract can read Kinesis (`spark.readStream.format("kinesis")`) in GovCloud. This demo uses Auto Loader file-arrival so we do not need a live stream.
# MAGIC
# MAGIC **Prompt (a):** legacy ETL keeps writing extracts here — additive, zero gap.  
# MAGIC **Prompt (d):** checkpoints live on the Volume; restart the pipeline to resume.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Element 4 — Catalog, quality, lineage
# MAGIC Open **Catalog Explorer** on `onr_itss_poc.da_platform` in another tab (Lineage graph).

# COMMAND ----------

display(spark.sql(f"SHOW TABLES IN {catalog}.{schema}"))
display(spark.table(f"{catalog}.{schema}.gold_data_quality"))
display(spark.table(f"{catalog}.{schema}.gold_vendors").select("vendor_name", "dataset_name", "status", "gap_status", "days_to_renewal"))

# COMMAND ----------

display(spark.sql(f"""
  SELECT source_table_full_name, target_table_full_name, event_time
  FROM system.access.table_lineage
  WHERE target_table_full_name LIKE '{catalog}.{schema}.%'
     OR source_table_full_name LIKE '{catalog}.{schema}.%'
  ORDER BY event_time DESC
  LIMIT 20
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC **Prompt (e):** lapsed subscription = `DATA_GAP` on the vendor table and on the App.  
# MAGIC **Prompt (c):** executives read gold only; landing writes are a different principal.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Element 5 — Analytics / model (live)
# MAGIC Train a small risk model on gold execution and write structured outputs: risk, velocity, trend ID.

# COMMAND ----------

import mlflow
import mlflow.sklearn
from sklearn.ensemble import GradientBoostingClassifier
from sklearn.model_selection import train_test_split

pdf = spark.table(f"{catalog}.{schema}.gold_financial_execution").toPandas()
assert len(pdf) > 0, "Pipeline has not published gold yet — start onr-itss-pipeline-dev"
feat = ["award_amount", "expended", "obligated", "monthly_burn", "remaining_months"]
X = pdf[feat].fillna(0)
y = pdf["risk_class"].fillna("UNKNOWN")
Xtr, Xte, ytr, yte = train_test_split(X, y, test_size=0.3, random_state=7)

mlflow.set_registry_uri("databricks-uc")
with mlflow.start_run(run_name="onr-risk"):
    clf = GradientBoostingClassifier(random_state=7).fit(Xtr, ytr)
    mlflow.log_metric("accuracy", float(clf.score(Xte, yte)))
    mlflow.sklearn.log_model(clf, "model", registered_model_name=f"{catalog}.{schema}.onr_execution_risk")

pdf["model_risk_class"] = clf.predict(X)
pdf["model_trend_id"] = "TRD-" + pdf["model_risk_class"] + "-BURN"
scored = spark.createDataFrame(pdf[["grant_id", "project_name", "onr_code", "award_amount", "risk_class", "model_risk_class", "predicted_velocity", "model_trend_id"]])
scored.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{catalog}.{schema}.gold_predictive_velocity")
display(spark.table(f"{catalog}.{schema}.gold_predictive_velocity").orderBy("award_amount", ascending=False))

# COMMAND ----------

# MAGIC %md
# MAGIC **Prompt (b):** OVERRUN → reprogram; UNDER_EXEC → prevent lapse; `predicted_velocity` feeds budget formulation.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Element 6 — Dashboard, App, automation
# MAGIC 1. Open Lakeview **ONR Executive D and A** — KPI strip for the brief.
# MAGIC 2. Open the Streamlit App — search, filter, extract, **Approve** an anomaly (no code).
# MAGIC
# MAGIC Automated summary / `route_to` come from the pipeline. Approve writes `gold_approval_log`.

# COMMAND ----------

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {catalog}.{schema}.gold_approval_log (
  grant_id STRING,
  anomaly_type STRING,
  decision STRING,
  decided_by STRING,
  decided_ts TIMESTAMP
) USING DELTA
COMMENT 'Writable approval log for the App (Element 6). Pipeline MV gold_anomalies stays append-only.'
""")
display(spark.table(f"{catalog}.{schema}.gold_executive_kpis"))
display(spark.table(f"{catalog}.{schema}.gold_executive_summary"))
display(spark.table(f"{catalog}.{schema}.gold_anomalies"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Element 7 — Secure export (CSV / JSON / Parquet)
# MAGIC Filtered gold → governed Volume. Same contract Advana / Cloud One would call via SQL Statement Execution.

# COMMAND ----------

from datetime import datetime, timezone

run_id = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
df = spark.table(f"{catalog}.{schema}.gold_financial_execution").filter("risk_class <> 'UNKNOWN'")
root = f"{export}/{run_id}"
df.coalesce(1).write.mode("overwrite").option("header", "true").csv(f"{root}/csv")
df.coalesce(1).write.mode("overwrite").json(f"{root}/json")
df.coalesce(1).write.mode("overwrite").parquet(f"{root}/parquet")
print(root)
display(dbutils.fs.ls(f"{root}/csv"))
display(spark.sql(f"SHOW CREATE TABLE {catalog}.{schema}.gold_financial_execution"))

# COMMAND ----------

# MAGIC %md
# MAGIC **Advana / Cloud One API** — same filtered extract, open SQL over HTTPS. Bearer token is continuous authorization (no static export URL).

# COMMAND ----------

host = spark.conf.get("spark.databricks.workspaceUrl", "dbc-ae83c2ba-d87c.cloud.databricks.com")
print(f"""curl -sS -X POST "https://{host}/api/2.0/sql/statements" \\
  -H "Authorization: Bearer $DATABRICKS_TOKEN" \\
  -H "Content-Type: application/json" \\
  -d '{{"warehouse_id":"<sql-warehouse-id>","catalog":"{catalog}","schema":"{schema}","wait_timeout":"30s","statement":"SELECT grant_id, risk_class, projected_total FROM gold_financial_execution WHERE risk_class = \\'OVERRUN\\'"}}'""")

# COMMAND ----------

# MAGIC %md
# MAGIC **Prompt (c):** no public URL — `READ VOLUME` / API token is continuous authorization.  
# MAGIC **Prompt (a):** legacy reports pull this Parquet (or the same SQL) until they cut over.
# MAGIC
# MAGIC ### Done
# MAGIC Stay in the App for search / filter / extract. Narrate any remaining prompt from `docs/DEMO_SCRIPT.md`.

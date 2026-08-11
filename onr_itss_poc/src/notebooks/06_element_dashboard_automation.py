# Databricks notebook source
# MAGIC %md
# MAGIC # Element 6 — Unified Dashboard, Visualizations, and Process Automation
# MAGIC **Key Personnel lead.** Non-technical leader path — no code required after this notebook.
# MAGIC
# MAGIC **Action:** Open the Lakeview executive dashboard and the Databricks App.
# MAGIC
# MAGIC **Show**
# MAGIC - Search / filter / extract without backend access
# MAGIC - Automated summaries
# MAGIC - Approval routing
# MAGIC - Anomaly flagging
# MAGIC
# MAGIC **Narrate prompt (b)** budget tracking on the KPI strip and **(e)** vendor-gap alerts.

# COMMAND ----------

dbutils.widgets.text("catalog", "onr_itss_dev")
dbutils.widgets.text("schema", "da_platform")
catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
spark.sql(f"USE CATALOG {catalog}")
spark.sql(f"USE SCHEMA {schema}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6.1 Executive KPI strip (what Lakeview binds)

# COMMAND ----------

display(spark.table(f"{catalog}.{schema}.gold_executive_kpis"))
display(spark.table(f"{catalog}.{schema}.gold_executive_summary"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6.2 Search / filter / extract — SQL a leader never has to write
# MAGIC The App exposes these as sidebar widgets. This cell is the same query the App runs.

# COMMAND ----------

dbutils.widgets.text("filter_code", "")
dbutils.widgets.text("filter_risk", "")
dbutils.widgets.text("search_text", "")
code = dbutils.widgets.get("filter_code").strip()
risk = dbutils.widgets.get("filter_risk").strip()
search = dbutils.widgets.get("search_text").strip()

q = f"SELECT grant_id, project_name, onr_code, tech_area, award_amount, expended, projected_total, risk_class, trend_id FROM {catalog}.{schema}.gold_financial_execution WHERE 1=1"
if code:
    q += f" AND onr_code = '{code}'"
if risk:
    q += f" AND risk_class = '{risk}'"
if search:
    q += f" AND (lower(project_name) LIKE '%{search.lower()}%' OR lower(grant_id) LIKE '%{search.lower()}%')"
q += " ORDER BY award_amount DESC"

filtered = spark.sql(q)
print(f"{filtered.count()} rows after filter")
display(filtered.limit(50))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Extract (Element 6 + 7 preview)
# MAGIC A leader clicks **Extract CSV** in the App. Equivalent:

# COMMAND ----------

export_dir = f"/Volumes/{catalog}/{schema}/export/leader_extract"
(
    filtered.coalesce(1)
    .write.mode("overwrite")
    .option("header", "true")
    .csv(f"{export_dir}/csv")
)
print(f"Extract written to {export_dir}/csv")
display(dbutils.fs.ls(f"{export_dir}/csv"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6.3 Process automation
# MAGIC
# MAGIC | Workflow | How it runs | Where a leader sees it |
# MAGIC |---|---|---|
# MAGIC | Automated summary | Pipeline MV `gold_executive_summary` (deterministic template — no external LLM) | Lakeview + App banner |
# MAGIC | Anomaly flagging | Pipeline MV `gold_anomalies` (overrun / under-exec / vendor gap) | App Anomalies tab |
# MAGIC | Approval routing | Pipeline MV `gold_approval_queue` routes to `financial_execution_lead` or `data_vendor_manager` with SLA | App Approvals tab |
# MAGIC
# MAGIC These refresh whenever the file-arrival job runs the pipeline. No analyst ticket.

# COMMAND ----------

display(spark.table(f"{catalog}.{schema}.gold_anomalies"))
display(spark.table(f"{catalog}.{schema}.gold_approval_queue"))
display(spark.table(f"{catalog}.{schema}.gold_vendor_lifecycle"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6.4 Open the live UI
# MAGIC After `databricks bundle deploy`:
# MAGIC
# MAGIC 1. **Lakeview** — workspace → Dashboards → *ONR Executive D and A {target}*
# MAGIC    Filters and KPI counters for the battle-rhythm brief.
# MAGIC 2. **Databricks App** — workspace → Apps → *onr-exec-app-{target}*
# MAGIC    GovCloud URL shape: `https://<app>.aws-gov.databricksapps.us`
# MAGIC    DoD URL shape: `https://<app>.aws-dod.databricksapps.mil`
# MAGIC
# MAGIC Walk a non-technical leader through: filter Code 08 → search "Near-Real-Time" → extract CSV → open Approvals → show the lapsed vendor (DATA_GAP) so they see a dashboard that *tells them the feed is dark*.
# MAGIC
# MAGIC **Prompt (d):** Lakeview is served from a serverless SQL warehouse. If the warehouse is down, the App fails closed (no stale silent numbers). HA is a second warehouse in the failover workspace.
# MAGIC
# MAGIC **Prompt (a):** the App is the modern replacement for the legacy D&A Portal *views*; the portal remains the system of record for data entry until cutover.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Element 6 complete
# MAGIC Next: `07_element_secure_export`.

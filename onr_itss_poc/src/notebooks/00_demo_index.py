# Databricks notebook source
# MAGIC %md
# MAGIC # ONR ITSS POC — demonstration index
# MAGIC Open these in order during the 50-minute recording. Key Personnel only.
# MAGIC
# MAGIC | Order | Notebook / UI | Element |
# MAGIC |---|---|---|
# MAGIC | 0 | This Git folder + `databricks.yml` | 2 — IaC |
# MAGIC | 1 | `03_element_ingest_demo` | 3 — Auto Loader, quality, schema evolution, live file drop |
# MAGIC | 2 | Pipeline UI → Expectations | 3/4 — drop counts |
# MAGIC | 3 | `04_element_governance_catalog` + Catalog Explorer Lineage | 4 |
# MAGIC | 4 | `05_element_analytics_ml` | 5 — forecast, velocity, trend IDs |
# MAGIC | 5 | Lakeview *ONR Executive D and A* + App `onr-exec-app-*` | 6 |
# MAGIC | 6 | `06_element_dashboard_automation` | 6 — extract + automation tables |
# MAGIC | 7 | `07_element_secure_export` | 7 — CSV/JSON/Parquet + APIs |
# MAGIC | 8 | `docs/STRATEGIC_PROMPTS.md` | 11.4 a–e (spoken, no slides) |
# MAGIC
# MAGIC Script: `docs/DEMO_SCRIPT.md`. Install: `INSTALL.md`.
# MAGIC
# MAGIC **Data constraint:** mock unclassified only. If you see a real name, SSN, or CUI marking — stop the recording.

# COMMAND ----------

dbutils.widgets.text("catalog", "onr_itss_dev")
dbutils.widgets.text("schema", "da_platform")
catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
print(f"Target {catalog}.{schema}")
print("Tables:")
display(spark.sql(f"SHOW TABLES IN {catalog}.{schema}"))

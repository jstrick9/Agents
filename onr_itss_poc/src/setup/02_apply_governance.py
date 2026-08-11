# Databricks notebook source
# MAGIC %md
# MAGIC # 02 — Post-publish grants, comments, and gold SELECT
# MAGIC Run after the first successful pipeline update so analysts/executives can see gold
# MAGIC without bronze write access.

# COMMAND ----------

dbutils.widgets.text("catalog", "onr_itss_dev")
dbutils.widgets.text("schema", "da_platform")
dbutils.widgets.text("analysts_group", "onr_analysts")
dbutils.widgets.text("executives_group", "onr_executives")
catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
analysts = dbutils.widgets.get("analysts_group")
execs = dbutils.widgets.get("executives_group")

# COMMAND ----------

gold = [
    "gold_grant_portfolio",
    "gold_financial_execution",
    "gold_anomalies",
    "gold_approval_queue",
    "gold_vendor_lifecycle",
    "gold_data_quality_scores",
    "gold_executive_kpis",
    "gold_executive_summary",
    "gold_predictive_velocity",
]

for tbl in gold:
    fqn = f"{catalog}.{schema}.{tbl}"
    if not spark.catalog.tableExists(fqn):
        print(f"skip (missing) {fqn}")
        continue
    spark.sql(f"GRANT SELECT ON TABLE {fqn} TO `{analysts}`")
    spark.sql(f"GRANT SELECT ON TABLE {fqn} TO `{execs}`")
    spark.sql(
        f"""
        ALTER TABLE {fqn} SET TAGS (
          'classification' = 'MOCK_UNCLASSIFIED',
          'cui' = 'false',
          'pii' = 'false'
        )
        """
    )
    print(f"granted SELECT on {fqn}")

spark.sql(f"GRANT READ VOLUME ON VOLUME {catalog}.{schema}.export TO `{analysts}`")
print("Governance pass complete.")

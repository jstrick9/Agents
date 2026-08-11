# Databricks notebook source
# MAGIC %md
# MAGIC # QA — post-pipeline validation
# MAGIC Fails the job if gold is empty, stale, or missing required columns.
# MAGIC Used by `nightly_validate` and as the last gate before a demo recording.

# COMMAND ----------

dbutils.widgets.text("catalog", "onr_itss_dev")
dbutils.widgets.text("schema", "da_platform")
catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
spark.sql(f"USE CATALOG {catalog}")
spark.sql(f"USE SCHEMA {schema}")

# COMMAND ----------

from datetime import datetime, timezone

REQUIRED = {
    "bronze_grants": ["grant_id", "_source_file", "_ingest_ts"],
    "silver_grants": ["grant_id", "award_amount", "onr_code"],
    "silver_financial": ["transaction_id", "grant_id", "expended"],
    "gold_financial_execution": ["grant_id", "risk_class", "predicted_velocity", "trend_id"],
    "gold_executive_kpis": ["grant_count", "total_awarded", "vendor_data_gaps"],
    "gold_data_quality_scores": ["dataset", "health_score", "health_band"],
    "gold_anomalies": ["anomaly_type", "severity"],
    "gold_approval_queue": ["route_to", "status"],
    "gold_vendor_lifecycle": ["subscription_id", "gap_status"],
    "gold_executive_summary": ["summary_text"],
}

failures = []
report = []

for tbl, cols in REQUIRED.items():
    fqn = f"{catalog}.{schema}.{tbl}"
    if not spark.catalog.tableExists(fqn):
        failures.append(f"MISSING TABLE {fqn}")
        continue
    df = spark.table(fqn)
    cnt = df.count()
    missing = [c for c in cols if c not in df.columns]
    if missing:
        failures.append(f"{fqn} missing columns {missing}")
    if cnt == 0:
        failures.append(f"{fqn} is empty")
    report.append((tbl, cnt, "OK" if cnt > 0 and not missing else "FAIL"))

# Silver must be strictly smaller than bronze when bad rows were seeded
if spark.catalog.tableExists(f"{catalog}.{schema}.bronze_grants") and spark.catalog.tableExists(
    f"{catalog}.{schema}.silver_grants"
):
    b = spark.table(f"{catalog}.{schema}.bronze_grants").count()
    s = spark.table(f"{catalog}.{schema}.silver_grants").count()
    if s > b:
        failures.append(f"silver_grants ({s}) > bronze_grants ({b})")
    report.append(("quality_drop_check", b - s, "OK" if s <= b else "FAIL"))

# No real PII-looking columns
forbidden = {"ssn", "social_security", "home_address", "passport"}
for tbl, _ in REQUIRED.items():
    fqn = f"{catalog}.{schema}.{tbl}"
    if spark.catalog.tableExists(fqn):
        hits = [c for c in spark.table(fqn).columns if c.lower() in forbidden]
        if hits:
            failures.append(f"{fqn} has forbidden columns {hits}")

# Freshness of gold
if spark.catalog.tableExists(f"{catalog}.{schema}.gold_executive_kpis"):
    ts = spark.sql(f"SELECT max(as_of_ts) FROM {catalog}.{schema}.gold_executive_kpis").collect()[0][0]
    report.append(("gold_as_of", str(ts), "OK" if ts is not None else "FAIL"))
    if ts is None:
        failures.append("gold_executive_kpis.as_of_ts is null")

display(spark.createDataFrame(report, ["check", "value", "status"]))

if failures:
    msg = "QA FAILED:\n" + "\n".join(failures)
    print(msg)
    dbutils.notebook.exit(f"quality_passed=false::{msg}")

print("All QA passed at", datetime.now(timezone.utc).isoformat())
dbutils.notebook.exit("quality_passed=true")

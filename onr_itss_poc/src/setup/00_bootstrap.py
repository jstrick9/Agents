# Databricks notebook source
# MAGIC %md
# MAGIC # 00 — Bootstrap (run once)
# MAGIC Creates catalog **`onr_itss_poc.da_platform`**, volumes, and mock landing files.
# MAGIC
# MAGIC Requires `CREATE CATALOG` (you now have admin). Then start the pipeline and open `DEMO`.

# COMMAND ----------

dbutils.widgets.text("catalog", "onr_itss_poc")
dbutils.widgets.text("schema", "da_platform")
catalog = dbutils.widgets.get("catalog").strip()
schema = dbutils.widgets.get("schema").strip()
assert catalog.replace("_", "").isalnum() and schema.replace("_", "").isalnum()

# COMMAND ----------

def run(sql: str) -> None:
    spark.sql(sql)
    print("OK", " ".join(sql.split())[:120])

run(f"CREATE CATALOG IF NOT EXISTS {catalog} COMMENT 'ONR ITSS POC — mock unclassified only'")
run(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{schema} COMMENT 'bronze / silver / gold'")
for vol, comment in [
    ("landing", "Auto Loader landing"),
    ("export", "Element 7 export"),
    ("checkpoints", "Auto Loader checkpoints"),
]:
    run(f"CREATE VOLUME IF NOT EXISTS {catalog}.{schema}.{vol} COMMENT '{comment}'")

spark.sql(f"USE CATALOG {catalog}")
spark.sql(f"USE SCHEMA {schema}")

# COMMAND ----------

import json
import sys
from pathlib import Path

for parent in [Path.cwd(), *Path.cwd().parents]:
    if (parent / "src" / "common" / "mock_data.py").exists():
        sys.path.insert(0, str(parent))
        break

from src.common.mock_data import (
    build_demo_drop_file,
    build_financial,
    build_grants,
    build_grants_schema_evolution,
    build_vendors,
    csv_text,
)

landing = f"/Volumes/{catalog}/{schema}/landing"
grants = build_grants()
write = lambda path, text: dbutils.fs.put(path, text, overwrite=True)
write_jsonl = lambda path, rows: write(path, "\n".join(json.dumps(r) for r in rows) + "\n")

write_jsonl(f"{landing}/grants/batch_001.jsonl", grants)
write_jsonl(f"{landing}/grants/batch_002_schema_evolution.jsonl", build_grants_schema_evolution())
write(f"{landing}/_demo/live_drop_element3.jsonl", json.dumps(build_demo_drop_file()) + "\n")
write(f"{landing}/financial/fy26_execution.csv", csv_text(build_financial(grants)))
write_jsonl(f"{landing}/vendors/subscriptions.jsonl", build_vendors())

run(
    f"""
    CREATE TABLE IF NOT EXISTS {catalog}.{schema}.gold_approval_log (
      grant_id STRING,
      anomaly_type STRING,
      decision STRING,
      decided_by STRING,
      decided_ts TIMESTAMP
    ) USING DELTA
    COMMENT 'App writes Approve/Reject here. Element 6.'
    """
)

print("Seeded", landing)
display(dbutils.fs.ls(f"{landing}/grants"))
display(dbutils.fs.ls(f"{landing}/financial"))
display(dbutils.fs.ls(f"{landing}/vendors"))

# COMMAND ----------

# MAGIC %md
# MAGIC Next:
# MAGIC 1. Start pipeline **onr-itss-pipeline-dev**
# MAGIC 2. Open notebook **DEMO** and Run all
# MAGIC 3. Open the App and Lakeview dashboard

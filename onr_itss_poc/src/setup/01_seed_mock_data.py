# Databricks notebook source
# MAGIC %md
# MAGIC # 01 — Seed mock landing files
# MAGIC **Purpose:** Write sanitized mock S&T grant, financial ERP, and vendor subscription files into the Unity Catalog landing volume so Auto Loader / file-arrival can pick them up.
# MAGIC
# MAGIC **Data constraints:** MOCK only. No CUI, PII, or classified data.
# MAGIC
# MAGIC **Outputs**
# MAGIC - `/Volumes/{catalog}/{schema}/landing/grants/*.jsonl`
# MAGIC - `/Volumes/{catalog}/{schema}/landing/financial/*.csv`
# MAGIC - `/Volumes/{catalog}/{schema}/landing/vendors/*.json`
# MAGIC
# MAGIC Re-run is safe: files are overwritten in place. The Element 3 live-drop file is written to `landing/grants/_held/` so the presenter can copy it during the demo.

# COMMAND ----------

dbutils.widgets.text("catalog", "onr_itss_dev")
dbutils.widgets.text("schema", "da_platform")
catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")

# COMMAND ----------

import json
import sys
from pathlib import Path

# Bundle source is synced next to this notebook on deploy.
# Fall back to generating in-process if the helper module is not on the path.
candidates = [
    Path.cwd(),
    Path("/Workspace"),
]
for c in candidates:
    if (c / "src" / "common" / "mock_data.py").exists():
        sys.path.insert(0, str(c))
        break
# Also try the notebook's parent tree
nb_dir = Path.cwd()
for parent in [nb_dir, *nb_dir.parents]:
    if (parent / "src" / "common" / "mock_data.py").exists():
        sys.path.insert(0, str(parent))
        break

from src.common.mock_data import (  # noqa: E402
    build_demo_drop_file,
    build_financial,
    build_financial_schema_variant,
    build_grants,
    build_grants_schema_evolution,
    build_vendors,
    csv_text,
)

# COMMAND ----------

landing = f"/Volumes/{catalog}/{schema}/landing"
spark.sql(f"USE CATALOG {catalog}")
spark.sql(f"USE SCHEMA {schema}")

# Confirm volumes exist (bootstrap must have run)
for vol in ("landing", "export", "checkpoints"):
    assert spark.sql(f"SHOW VOLUMES IN {catalog}.{schema}").filter(f"volume_name = '{vol}'").count() == 1, (
        f"Missing volume {catalog}.{schema}.{vol} — run 00_uc_bootstrap first"
    )

# COMMAND ----------

grants = build_grants()
evolved = build_grants_schema_evolution()
financial = build_financial(grants)
financial_variant = build_financial_schema_variant(financial)
vendors = build_vendors()
drop = build_demo_drop_file()

def write_text(path: str, text: str) -> None:
    dbutils.fs.put(path, text, overwrite=True)

def write_jsonl(path: str, rows) -> None:
    write_text(path, "\n".join(json.dumps(r) for r in rows) + "\n")

# JSON landing files are JSONL so Auto Loader (multiLine=false) reads one record per line.
# Held demo file lives OUTSIDE landing/grants so Auto Loader does not pick it up early.
write_jsonl(f"{landing}/grants/batch_001.jsonl", grants)
write_jsonl(f"{landing}/grants/batch_002_schema_evolution.jsonl", evolved)
write_text(f"{landing}/_demo/live_drop_element3.jsonl", json.dumps(drop) + "\n")
write_text(f"{landing}/financial/fy26_execution.csv", csv_text(financial))
write_text(f"{landing}/financial/fy26_execution_variant.csv", csv_text(financial_variant))
write_jsonl(f"{landing}/vendors/subscriptions.jsonl", vendors)

# COMMAND ----------

print("Seeded landing volume:")
for p in [
    f"{landing}/grants/batch_001.jsonl",
    f"{landing}/grants/batch_002_schema_evolution.jsonl",
    f"{landing}/grants/_held/live_drop_element3.json",
    f"{landing}/financial/fy26_execution.csv",
    f"{landing}/financial/fy26_execution_variant.csv",
    f"{landing}/vendors/subscriptions.json",
]:
    print(" ", p)

display(dbutils.fs.ls(f"{landing}/grants"))
display(dbutils.fs.ls(f"{landing}/financial"))
display(dbutils.fs.ls(f"{landing}/vendors"))

# COMMAND ----------

# MAGIC %md
# MAGIC Files are in the volume. Start the Lakeflow pipeline (or the file-arrival job). Held file `landing/_demo/live_drop_element3.jsonl` is copied into `landing/grants/` during Element 3 of the demo.

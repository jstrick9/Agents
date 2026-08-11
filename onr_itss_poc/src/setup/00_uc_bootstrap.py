# Databricks notebook source
# MAGIC %md
# MAGIC # 00 — Unity Catalog bootstrap (run first)
# MAGIC Creates schema, volumes, and tags for the ONR ITSS POC.
# MAGIC
# MAGIC **This commercial POC workspace:** `https://dbc-ae83c2ba-d87c.cloud.databricks.com/?o=7474653232339519`  
# MAGIC **Folder:** `/Workspace/Users/joshua.strickland@satsyil.com/onr_itss_poc`
# MAGIC
# MAGIC Safe to re-run. If `CREATE CATALOG` is denied, set `catalog` to an existing catalog (`main` or `workspace`) and re-run.
# MAGIC Group grants are **off** by default so a personal workspace without `onr_*` groups still succeeds.

# COMMAND ----------

dbutils.widgets.text("catalog", "onr_itss_dev")
dbutils.widgets.text("schema", "da_platform")
dbutils.widgets.dropdown("apply_group_grants", "false", ["false", "true"])
dbutils.widgets.text("engineers_group", "onr_data_engineers")
dbutils.widgets.text("analysts_group", "onr_analysts")
dbutils.widgets.text("executives_group", "onr_executives")

catalog = dbutils.widgets.get("catalog").strip()
schema = dbutils.widgets.get("schema").strip()
apply_grants = dbutils.widgets.get("apply_group_grants") == "true"
engineers = dbutils.widgets.get("engineers_group")
analysts = dbutils.widgets.get("analysts_group")
execs = dbutils.widgets.get("executives_group")

for name, value in (("catalog", catalog), ("schema", schema)):
    if not value.replace("_", "").isalnum():
        raise ValueError(f"Unsafe {name}: {value}")

# COMMAND ----------

def try_sql(sql: str, ok: str, warn: str) -> bool:
    try:
        spark.sql(sql)
        print("OK ", ok)
        return True
    except Exception as exc:
        print("SKIP", warn, "—", exc)
        return False

# COMMAND ----------

created_catalog = try_sql(
    f"CREATE CATALOG IF NOT EXISTS {catalog} COMMENT 'ONR ITSS POC — mock unclassified data only. No CUI/PII.'",
    f"catalog {catalog}",
    f"CREATE CATALOG {catalog} (use an existing catalog such as main if you lack privilege)",
)

try_sql(
    f"CREATE SCHEMA IF NOT EXISTS {catalog}.{schema} COMMENT 'ONR ITSS POC bronze/silver/gold + volumes'",
    f"schema {catalog}.{schema}",
    f"CREATE SCHEMA {catalog}.{schema}",
)
try_sql(
    f"CREATE SCHEMA IF NOT EXISTS {catalog}.qa COMMENT 'ONR ITSS POC quarantine / validation'",
    f"schema {catalog}.qa",
    f"CREATE SCHEMA {catalog}.qa",
)

for vol, comment in [
    ("landing", "Auto Loader landing zone for mock grants, ERP, vendor files"),
    ("export", "Element 7 secure export (CSV / JSON / Parquet)"),
    ("checkpoints", "Auto Loader schema and checkpoint locations"),
]:
    try_sql(
        f"CREATE VOLUME IF NOT EXISTS {catalog}.{schema}.{vol} COMMENT '{comment}'",
        f"volume {catalog}.{schema}.{vol}",
        f"CREATE VOLUME {vol}",
    )

# COMMAND ----------

try_sql(
    f"ALTER SCHEMA {catalog}.{schema} SET TAGS ('classification' = 'MOCK_UNCLASSIFIED', 'cui' = 'false', 'pii' = 'false')",
    "schema tags",
    "schema tags",
)
for vol in ("landing", "export"):
    try_sql(
        f"ALTER VOLUME {catalog}.{schema}.{vol} SET TAGS ('classification' = 'MOCK_UNCLASSIFIED', 'cui' = 'false', 'pii' = 'false')",
        f"{vol} tags",
        f"{vol} tags",
    )

# COMMAND ----------

if apply_grants:
    grants = [
        f"GRANT USE CATALOG ON CATALOG {catalog} TO `{engineers}`",
        f"GRANT USE SCHEMA, CREATE TABLE, CREATE VOLUME, CREATE FUNCTION, CREATE MODEL ON SCHEMA {catalog}.{schema} TO `{engineers}`",
        f"GRANT READ VOLUME, WRITE VOLUME ON VOLUME {catalog}.{schema}.landing TO `{engineers}`",
        f"GRANT READ VOLUME, WRITE VOLUME ON VOLUME {catalog}.{schema}.export TO `{engineers}`",
        f"GRANT READ VOLUME, WRITE VOLUME ON VOLUME {catalog}.{schema}.checkpoints TO `{engineers}`",
        f"GRANT USE CATALOG ON CATALOG {catalog} TO `{analysts}`",
        f"GRANT USE SCHEMA ON SCHEMA {catalog}.{schema} TO `{analysts}`",
        f"GRANT READ VOLUME ON VOLUME {catalog}.{schema}.export TO `{analysts}`",
        f"GRANT USE CATALOG ON CATALOG {catalog} TO `{execs}`",
        f"GRANT USE SCHEMA ON SCHEMA {catalog}.{schema} TO `{execs}`",
    ]
    for g in grants:
        try_sql(g, g, g)
else:
    print("Group grants skipped (apply_group_grants=false). Fine for a personal POC workspace.")

# COMMAND ----------

try_sql(
    f"""
    CREATE OR REPLACE FUNCTION {catalog}.{schema}.mask_if_restricted(val STRING)
    RETURNS STRING
    RETURN CASE WHEN val IS NULL THEN NULL ELSE val END
    """,
    "mask_if_restricted UDF",
    "mask UDF",
)

print(f"Bootstrap finished for {catalog}.{schema}")
print("Next: run 01_seed_mock_data")
if not created_catalog:
    print("NOTE: catalog create was skipped. If schema create also failed, set catalog=main and re-run.")

# Databricks notebook source
# MAGIC %md
# MAGIC # 00 — Unity Catalog bootstrap (run first)
# MAGIC Uses the existing **`workspace.default`** catalog/schema. Does **not** create a catalog.
# MAGIC
# MAGIC This metastore (`metastore_aws_us_east_2`) denies `CREATE CATALOG`. That is expected.
# MAGIC
# MAGIC **Widgets (change these if an old run left `onr_itss_dev`):**
# MAGIC - `catalog` = `workspace`
# MAGIC - `schema` = `default`
# MAGIC
# MAGIC Then **Run all**. Next: `01_seed_mock_data` with the same widgets.

# COMMAND ----------

dbutils.widgets.text("catalog", "workspace")
dbutils.widgets.text("schema", "default")
dbutils.widgets.dropdown("apply_group_grants", "false", ["false", "true"])
dbutils.widgets.text("engineers_group", "onr_data_engineers")
dbutils.widgets.text("analysts_group", "onr_analysts")
dbutils.widgets.text("executives_group", "onr_executives")

requested_catalog = dbutils.widgets.get("catalog").strip()
schema = dbutils.widgets.get("schema").strip()
apply_grants = dbutils.widgets.get("apply_group_grants") == "true"
engineers = dbutils.widgets.get("engineers_group")
analysts = dbutils.widgets.get("analysts_group")
execs = dbutils.widgets.get("executives_group")

for name, value in (("catalog", requested_catalog), ("schema", schema)):
    if not value.replace("_", "").isalnum():
        raise ValueError(f"Unsafe {name}: {value}")

# COMMAND ----------

def short_err(exc: Exception) -> str:
    msg = str(exc).split("JVM stacktrace:")[0].strip()
    return " ".join(msg.split())[:400]


def try_sql(sql: str) -> tuple[bool, str]:
    try:
        spark.sql(sql)
        return True, ""
    except Exception as exc:
        return False, short_err(exc)


def catalog_names() -> list[str]:
    df = spark.sql("SHOW CATALOGS")
    col = "catalog" if "catalog" in df.columns else df.columns[0]
    return [r[col] for r in df.collect()]


EXCLUDE = {"system", "samples", "hive_metastore", "__databricks_internal"}
PREFERRED = ["workspace", "main"]

available = catalog_names()
print("Catalogs you can see:")
for c in available:
    print(" -", c)

# COMMAND ----------

def pick_catalog(requested: str, names: list[str]) -> str:
    if requested in names:
        return requested
    print(f"Requested catalog '{requested}' is not available (CREATE CATALOG is denied on this metastore).")
    for cand in PREFERRED:
        if cand in names:
            print(f"Falling back to existing catalog '{cand}'.")
            return cand
    usable = [c for c in names if c not in EXCLUDE]
    if usable:
        print(f"Falling back to existing catalog '{usable[0]}'.")
        return usable[0]
    raise RuntimeError(
        "No usable Unity Catalog catalog found. Ask a metastore admin to grant "
        "USE CATALOG + CREATE SCHEMA on 'main' or 'workspace'."
    )


catalog = pick_catalog(requested_catalog, available)
print(f"Using catalog: {catalog}")

# COMMAND ----------

ok, err = try_sql(f"USE CATALOG `{catalog}`")
if not ok:
    raise RuntimeError(f"Cannot USE CATALOG {catalog}: {err}")
ok, err = try_sql(f"USE SCHEMA `{schema}`")
if not ok:
    # Schema missing — try to create it, but workspace.default should already exist.
    created, cerr = try_sql(f"CREATE SCHEMA IF NOT EXISTS `{catalog}`.`{schema}`")
    if not created:
        raise RuntimeError(f"Cannot USE or CREATE SCHEMA {catalog}.{schema}: {err} / {cerr}")
    spark.sql(f"USE SCHEMA `{schema}`")
print(f"OK using {catalog}.{schema}")

for vol, comment in [
    ("landing", "Auto Loader landing zone for mock grants, ERP, vendor files"),
    ("export", "Element 7 secure export (CSV / JSON / Parquet)"),
    ("checkpoints", "Auto Loader schema and checkpoint locations"),
]:
    ok, err = try_sql(
        f"CREATE VOLUME IF NOT EXISTS {catalog}.{schema}.{vol} COMMENT '{comment}'"
    )
    print(("OK " if ok else "SKIP ") + f"volume {catalog}.{schema}.{vol}" + ("" if ok else f" — {err}"))

# COMMAND ----------

try_sql(
    f"ALTER SCHEMA {catalog}.{schema} SET TAGS ('classification' = 'MOCK_UNCLASSIFIED', 'cui' = 'false', 'pii' = 'false')"
)
for vol in ("landing", "export"):
    try_sql(
        f"ALTER VOLUME {catalog}.{schema}.{vol} SET TAGS ('classification' = 'MOCK_UNCLASSIFIED', 'cui' = 'false', 'pii' = 'false')"
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
        ok, err = try_sql(g)
        print(("OK " if ok else "SKIP ") + g + ("" if ok else f" — {err}"))
else:
    print("Group grants skipped (apply_group_grants=false). Fine for a personal POC workspace.")

# COMMAND ----------

ok, err = try_sql(
    f"""
    CREATE OR REPLACE FUNCTION {catalog}.{schema}.mask_if_restricted(val STRING)
    RETURNS STRING
    RETURN CASE WHEN val IS NULL THEN NULL ELSE val END
    """
)
print(("OK " if ok else "SKIP ") + "mask_if_restricted UDF" + ("" if ok else f" — {err}"))

print()
print("=" * 60)
print("Resolved location (use these widgets everywhere else)")
print(f"  catalog = {catalog}")
print(f"  schema  = {schema}")
print(f"  landing = /Volumes/{catalog}/{schema}/landing")
print(f"  export  = /Volumes/{catalog}/{schema}/export")
print("=" * 60)
print("Next: run 01_seed_mock_data with the same catalog/schema widgets.")

try:
    dbutils.jobs.taskValues.set(key="catalog", value=catalog)
    dbutils.jobs.taskValues.set(key="schema", value=schema)
except Exception:
    pass

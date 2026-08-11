# Databricks notebook source
# MAGIC %md
# MAGIC # Element 3 — Automated Ingestion, Data Operations, and Streaming
# MAGIC **Key Personnel lead.** Live cloud + live repo. Mock data only.
# MAGIC
# MAGIC **Action:** Ingest a raw / semi-structured mock research-grant registry.
# MAGIC
# MAGIC **Show**
# MAGIC 1. Auto Loader detects incoming files (file-arrival job + `cloudFiles`).
# MAGIC 2. Automated quality checks (Lakeflow expectations).
# MAGIC 3. Schema variations (`addNewColumns` — `collaboration_flag`).
# MAGIC 4. Near-real-time path (Kinesis architecture — documented, not required to stand up).
# MAGIC
# MAGIC **Narrate prompt (a)** legacy ETL coexistence and **(d)** streaming RTO/RPO while this runs.

# COMMAND ----------

dbutils.widgets.text("catalog", "onr_itss_dev")
dbutils.widgets.text("schema", "da_platform")
catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
spark.sql(f"USE CATALOG {catalog}")
spark.sql(f"USE SCHEMA {schema}")
landing = f"/Volumes/{catalog}/{schema}/landing"

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3.1 Landing zone (UC Volume — no DBFS mounts)
# MAGIC Files already seeded by `01_seed_mock_data`. Batch 002 includes extra columns to prove schema evolution.

# COMMAND ----------

print("Grants landing:")
display(dbutils.fs.ls(f"{landing}/grants"))
print("Financial landing:")
display(dbutils.fs.ls(f"{landing}/financial"))
print("Vendors landing:")
display(dbutils.fs.ls(f"{landing}/vendors"))
print("Held live-drop (not yet visible to Auto Loader):")
display(dbutils.fs.ls(f"{landing}/_demo"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3.2 How Auto Loader detects files
# MAGIC The Lakeflow pipeline uses `spark.readStream.format("cloudFiles")` with:
# MAGIC - `cloudFiles.schemaEvolutionMode = addNewColumns`
# MAGIC - `cloudFiles.rescuedDataColumn = _rescued_data`
# MAGIC - schema location on the `checkpoints` volume
# MAGIC
# MAGIC The DAB job `file_arrival_ingest` watches `/Volumes/{catalog}/{schema}/landing/grants` and starts a pipeline update when a new file arrives (60s debounce). CI lives at `infra/ci/databricks-ci.yml` (copy into `.github/workflows/` in a repo that allows workflow files).

# COMMAND ----------

print("Pipeline source (first 80 lines):")
src = dbutils.fs.head(
    f"/Workspace/Users/{spark.sql('SELECT current_user()').collect()[0][0]}/.bundle/onr_itss_poc/dev/files/src/pipelines/bronze_silver_gold.py",
    4000,
) if False else None

# Always show the repo copy that DAB synced next to notebooks when possible
import os
candidates = [
    "../pipelines/bronze_silver_gold.py",
    "src/pipelines/bronze_silver_gold.py",
]
for c in candidates:
    if os.path.exists(c):
        with open(c) as fh:
            print("".join(fh.readlines()[:80]))
        break
else:
    print("Open src/pipelines/bronze_silver_gold.py in the repo for the live code walk-through.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3.3 Current bronze / silver state (quality already applied)

# COMMAND ----------

for tbl in ["bronze_grants", "silver_grants", "bronze_financial", "silver_financial", "bronze_vendors"]:
    fqn = f"{catalog}.{schema}.{tbl}"
    if spark.catalog.tableExists(fqn):
        cnt = spark.table(fqn).count()
        print(f"{fqn}: {cnt} rows")
    else:
        print(f"{fqn}: NOT YET CREATED — run the medallion pipeline first")

# COMMAND ----------

if spark.catalog.tableExists(f"{catalog}.{schema}.bronze_grants"):
    display(
        spark.sql(
            f"""
            SELECT grant_id, project_name, award_amount, _source_file, _ingest_ts
            FROM {catalog}.{schema}.bronze_grants
            ORDER BY _ingest_ts DESC
            LIMIT 20
            """
        )
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3.4 Quality checks — what was dropped
# MAGIC Expectations on `silver_grants`:
# MAGIC - `valid_grant_id` (must start with `MOCK-ONR-`) — **drop**
# MAGIC - `non_negative_award` — **drop**
# MAGIC - `known_onr_code` / `valid_trl` — **warn** (kept, scored)
# MAGIC
# MAGIC Seed data includes a null grant_id and a negative award so the presenter can point at expectation metrics in the pipeline UI (**Expectations** tab).

# COMMAND ----------

if spark.catalog.tableExists(f"{catalog}.{schema}.bronze_grants") and spark.catalog.tableExists(
    f"{catalog}.{schema}.silver_grants"
):
    display(
        spark.sql(
            f"""
            SELECT 'bronze' AS layer, COUNT(*) AS rows,
                   SUM(CASE WHEN grant_id IS NULL OR grant_id NOT LIKE 'MOCK-ONR-%' THEN 1 ELSE 0 END) AS bad_ids,
                   SUM(CASE WHEN TRY_CAST(award_amount AS DOUBLE) < 0 THEN 1 ELSE 0 END) AS negative_awards
            FROM {catalog}.{schema}.bronze_grants
            UNION ALL
            SELECT 'silver', COUNT(*), 0, 0 FROM {catalog}.{schema}.silver_grants
            """
        )
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3.5 Schema variation — extra columns already landed
# MAGIC `batch_002_schema_evolution.jsonl` adds `collaboration_flag` and `international_partner`.
# MAGIC Auto Loader `addNewColumns` evolves bronze without a manual ALTER.

# COMMAND ----------

if spark.catalog.tableExists(f"{catalog}.{schema}.bronze_grants"):
    print("bronze_grants schema (look for collaboration_flag):")
    spark.table(f"{catalog}.{schema}.bronze_grants").printSchema()
    if "collaboration_flag" in spark.table(f"{catalog}.{schema}.bronze_grants").columns:
        display(
            spark.sql(
                f"""
                SELECT grant_id, collaboration_flag, international_partner, _source_file
                FROM {catalog}.{schema}.bronze_grants
                WHERE collaboration_flag IS NOT NULL
                LIMIT 20
                """
            )
        )
    else:
        print("collaboration_flag not present yet — pipeline may not have processed batch_002.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3.6 LIVE file drop — Auto Loader detection
# MAGIC Copy the held JSONL file into `landing/grants/`. The file-arrival job (or a manual pipeline start) picks it up. Wait ~1 minute, then re-query.

# COMMAND ----------

held = f"{landing}/_demo/live_drop_element3.jsonl"
dest = f"{landing}/grants/live_drop_element3.jsonl"
dbutils.fs.cp(held, dest)
print(f"Dropped {held} -> {dest}")
print("Start a pipeline update now (or wait for file-arrival). Then run the next cell.")

# COMMAND ----------

# Re-run after the pipeline update
if spark.catalog.tableExists(f"{catalog}.{schema}.bronze_grants"):
    display(
        spark.sql(
            f"""
            SELECT grant_id, project_name, demo_marker, _source_file, _ingest_ts
            FROM {catalog}.{schema}.bronze_grants
            WHERE grant_id = 'MOCK-ONR-N00014-26-C-0901'
               OR _source_file LIKE '%live_drop_element3%'
            """
        )
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3.7 Near-real-time streaming architecture (narrate — Kinesis not required)
# MAGIC
# MAGIC ```
# MAGIC Legacy D&A Portal / ERP extracts ──► UC Volume (this demo)
# MAGIC                                      Auto Loader  (file arrival, schema evolution)
# MAGIC Command event bus (future) ────────► Amazon Kinesis Data Streams (GovCloud)
# MAGIC                                      spark.readStream.format("kinesis")
# MAGIC                                           .option("streamName", "onr-da-events")
# MAGIC                                           .option("region", "us-gov-west-1")
# MAGIC                                           .option("initialPosition", "TRIM_HORIZON")
# MAGIC                                      ──► bronze_* streaming tables (same silver expectations)
# MAGIC ```
# MAGIC
# MAGIC **Why this is open architecture:** Auto Loader and Kinesis are interchangeable sources behind the same silver contract. Kafka is equally valid (`format("kafka")`). No proprietary ingest lock-in.
# MAGIC
# MAGIC **Prompt (d) — resilience:** streaming checkpoints live on the `checkpoints` volume (Delta-backed, multi-AZ). RPO for Auto Loader is the last committed file; RPO for Kinesis is the shard checkpoint. RTO is a pipeline restart on serverless (minutes) or classic job cluster (policy-controlled). Failover is a second workspace / metastore replica — see `docs/DR_RTO_RPO.md`.
# MAGIC
# MAGIC **Prompt (a) — legacy:** this landing volume is the *strangler* target. Existing ETL continues to write extracts here; nothing in the legacy portal is cut over until gold reconciles. Zero service gap.
# MAGIC
# MAGIC **Prompt (c):** ingest identities are service principals with `WRITE VOLUME` on landing only. Pipeline compute cannot `SELECT` executive gold if not granted. No public S3.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Element 3 complete
# MAGIC Next: notebook `04_element_governance_catalog` (catalog, metadata, health scores, lineage).

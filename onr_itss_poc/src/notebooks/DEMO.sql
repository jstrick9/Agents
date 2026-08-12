-- Databricks notebook source
-- MAGIC %md
-- MAGIC # ONR ITSS — Technical demonstration (Elements 3–7)
-- MAGIC **Key Personnel narrate. Live cloud. Mock data only. No slides. No clusters.**
-- MAGIC
-- MAGIC Attach this notebook to your **Serverless SQL warehouse** and run it top to bottom.
-- MAGIC Every cell is SQL — the pipeline does the compute-heavy work on serverless pipeline compute.
-- MAGIC
-- MAGIC Catalog: `onr_itss_poc.da_platform`
-- MAGIC Before this notebook: `00_bootstrap` run, seed files uploaded, pipeline **onr-itss-pipeline-dev** started once.
-- MAGIC
-- MAGIC Talk track (prompts a–e) is in `docs/DEMO_SCRIPT.md`.

-- COMMAND ----------
-- MAGIC %md
-- MAGIC ## Element 2 — Infrastructure as Code (30 seconds)
-- MAGIC
-- MAGIC The whole platform is declared in this Git folder: `databricks.yml`,
-- MAGIC `resources/pipelines.yml`, the pipeline source `src/pipelines/medallion.py`.
-- MAGIC The pipeline running in Workflows was created from that file — the Git folder is the
-- MAGIC source of truth, not click-ops. In a workspace with the CLI, `databricks bundle deploy`
-- MAGIC does the same thing.

-- COMMAND ----------
-- MAGIC %md
-- MAGIC ## Element 3 — Ingest, quality, schema evolution, streaming
-- MAGIC **Action:** ingest the mock S&T grant registry.
-- MAGIC
-- MAGIC Auto Loader watches the landing Volume for new files (no manual import step).
-- MAGIC Expectations drop bad rows. Extra columns land without an ALTER.

-- COMMAND ----------

LIST '/Volumes/onr_itss_poc/da_platform/landing/grants';

-- COMMAND ----------
-- MAGIC %md
-- MAGIC Bronze keeps every row; Silver drops the rows that fail quality expectations
-- MAGIC (bad grant ids, negative awards) — the **quality gate is in the pipeline**.

-- COMMAND ----------

SELECT 'bronze_grants' AS layer, COUNT(*) AS rows,
       COUNT_IF(grant_id IS NULL OR grant_id NOT LIKE 'MOCK-ONR-%') AS bad_ids
FROM onr_itss_poc.da_platform.bronze_grants
UNION ALL
SELECT 'silver_grants', COUNT(*), 0
FROM onr_itss_poc.da_platform.silver_grants
UNION ALL
SELECT 'bronze_financial', COUNT(*), COUNT_IF(transaction_id IS NULL)
FROM onr_itss_poc.da_platform.bronze_financial
UNION ALL
SELECT 'silver_financial', COUNT(*), 0
FROM onr_itss_poc.da_platform.silver_financial;

-- COMMAND ----------
-- MAGIC %md
-- MAGIC **Schema evolution** — `batch_002_schema_evolution.jsonl` arrived with two new columns
-- MAGIC (`collaboration_flag`, `international_partner`). Auto Loader's `addNewColumns` mode
-- MAGIC absorbed them with zero pipeline reconfiguration:

-- COMMAND ----------

SELECT grant_id, project_name, collaboration_flag, international_partner, _source_file
FROM onr_itss_poc.da_platform.bronze_grants
WHERE collaboration_flag IS NOT NULL
LIMIT 10;

-- COMMAND ----------
-- MAGIC %md
-- MAGIC **Live file drop (Element 3 action, on camera):**
-- MAGIC
-- MAGIC 1. Open **Catalog Explorer** → `onr_itss_poc` → `da_platform` → `landing` → `grants`
-- MAGIC 2. Drag **`data/mock/grants/live_drop_element3.jsonl`** (in this Git folder) into the folder
-- MAGIC 3. Go to **Workflows → Lakeflow pipelines → onr-itss-pipeline-dev → Start update**
-- MAGIC 4. When it reaches **Completed**, re-run the cell below
-- MAGIC
-- MAGIC A brand-new grant arrives with no code change, no schema change, no restart.

-- COMMAND ----------

SELECT grant_id, project_name, onr_code, _source_file, _ingest_ts
FROM onr_itss_poc.da_platform.bronze_grants
WHERE grant_id = 'MOCK-ONR-N00014-26-C-0901' OR _source_file LIKE '%live_drop%';

-- COMMAND ----------
-- MAGIC %md
-- MAGIC **Near-real-time path (narrate):** the same silver contract can read Kinesis
-- MAGIC (`spark.readStream.format("kinesis")`) in GovCloud; this demo uses Auto Loader
-- MAGIC file-arrival so we don't need a live stream on camera.
-- MAGIC
-- MAGIC **Prompt (a):** legacy ETL keeps writing extracts to this landing Volume — additive,
-- MAGIC zero service gap. **Prompt (d):** checkpoints live on the Volume; restart the pipeline
-- MAGIC to resume — RPO is the last Delta commit, RTO is the restart.

-- COMMAND ----------
-- MAGIC %md
-- MAGIC ## Element 4 — Catalog, quality, lineage
-- MAGIC Open **Catalog Explorer** on `onr_itss_poc.da_platform` in another tab and click
-- MAGIC **Lineage** on `gold_financial_execution` — bronze → silver → gold end to end.

-- COMMAND ----------

SHOW TABLES IN onr_itss_poc.da_platform;

-- COMMAND ----------

SELECT dataset, row_count, ROUND(health_score, 1) AS health_score, health_band, computed_ts
FROM onr_itss_poc.da_platform.gold_data_quality;

-- COMMAND ----------

SELECT vendor_name, dataset_name, status, gap_status, days_to_renewal
FROM onr_itss_poc.da_platform.gold_vendors
ORDER BY CASE gap_status WHEN 'DATA_GAP' THEN 1 WHEN 'RENEWAL_DUE' THEN 2 ELSE 3 END;

-- COMMAND ----------
-- MAGIC %md
-- MAGIC Lineage events are queryable too (this workspace uses `system.access.table_lineage`;
-- MAGIC enable it once in `00_bootstrap` if it is not already):

-- COMMAND ----------

SELECT source_table_full_name, target_table_full_name, event_time
FROM system.access.table_lineage
WHERE target_table_full_name LIKE 'onr_itss_poc.da_platform.%'
   OR source_table_full_name LIKE 'onr_itss_poc.da_platform.%'
ORDER BY event_time DESC
LIMIT 20;

-- COMMAND ----------
-- MAGIC %md
-- MAGIC **Prompt (e):** the lapsed mock subscription shows as `DATA_GAP` here and on the App —
-- MAGIC a dark feed cannot silently degrade the forecast. **Prompt (c):** executives read gold
-- MAGIC only; landing writes are a different principal — least privilege.

-- COMMAND ----------
-- MAGIC %md
-- MAGIC ## Element 5 — Analytics / model (live)
-- MAGIC
-- MAGIC The pipeline trains a **Spark ML risk model** on gold execution and registers it in
-- MAGIC **UC Models** (`onr_execution_risk`) — every full refresh re-trains and adds a version.
-- MAGIC Open **Models** in the sidebar to see the registered model, then run the queries below.

-- COMMAND ----------

SHOW MODELS IN onr_itss_poc.da_platform;

-- COMMAND ----------

SELECT grant_id, project_name, onr_code, award_amount, risk_class, model_risk_class,
       ROUND(predicted_velocity, 0) AS predicted_velocity, trend_id
FROM onr_itss_poc.da_platform.gold_predictive_velocity
ORDER BY award_amount DESC;

-- COMMAND ----------

SELECT risk_class, COUNT(*) AS grants, ROUND(SUM(award_amount), 0) AS awarded
FROM onr_itss_poc.da_platform.gold_financial_execution
GROUP BY risk_class
ORDER BY awarded DESC;

-- COMMAND ----------
-- MAGIC %md
-- MAGIC **Prompt (b):** OVERRUN → reprogram; UNDER_EXEC → prevent lapse; `predicted_velocity`
-- MAGIC feeds budget formulation for the next cycle.

-- COMMAND ----------
-- MAGIC %md
-- MAGIC ## Element 6 — Dashboard, App, automation
-- MAGIC 1. Open Lakeview **ONR Executive D and A** — KPI strip for the brief.
-- MAGIC 2. Open the Streamlit **App** — search, filter, extract, then **Approve** an anomaly (no code).
-- MAGIC
-- MAGIC Automated summary + routing come from the pipeline. Approve writes `gold_approval_log`.

-- COMMAND ----------

SELECT * FROM onr_itss_poc.da_platform.gold_executive_kpis;

-- COMMAND ----------

SELECT summary_text, generated_ts FROM onr_itss_poc.da_platform.gold_executive_summary;

-- COMMAND ----------

SELECT a.grant_id, a.anomaly_type, a.severity, a.description, a.route_to, a.detected_ts,
       COALESCE(d.decision, 'OPEN') AS status
FROM onr_itss_poc.da_platform.gold_anomalies a
LEFT JOIN (
  SELECT grant_id, anomaly_type, decision,
         ROW_NUMBER() OVER (PARTITION BY grant_id, anomaly_type ORDER BY decided_ts DESC) AS rn
  FROM onr_itss_poc.da_platform.gold_approval_log
) d ON a.grant_id = d.grant_id AND a.anomaly_type = d.anomaly_type AND d.rn = 1
ORDER BY a.detected_ts DESC;

-- COMMAND ----------
-- MAGIC %md
-- MAGIC ## Element 7 — Secure export (CSV / JSON / Parquet) + open API
-- MAGIC
-- MAGIC In the **App → Extract** tab, download the filtered portfolio as **CSV, JSON, or Parquet**
-- MAGIC (non-proprietary formats). The schema stays portable — prove it:

-- COMMAND ----------

SHOW CREATE TABLE onr_itss_poc.da_platform.gold_financial_execution;

-- COMMAND ----------
-- MAGIC %md
-- MAGIC **Advana / Cloud One integration** — the same filtered extract over SQL Statement
-- MAGIC Execution (open HTTP API, bearer token = continuous authorization, no static export URL):
-- MAGIC
-- MAGIC ```bash
-- MAGIC curl -sS -X POST "https://dbc-ae83c2ba-d87c.cloud.databricks.com/api/2.0/sql/statements" \
-- MAGIC   -H "Authorization: Bearer $DATABRICKS_TOKEN" \
-- MAGIC   -H "Content-Type: application/json" \
-- MAGIC   -d '{"warehouse_id":"<sql-warehouse-id>","catalog":"onr_itss_poc","schema":"da_platform",
-- MAGIC        "wait_timeout":"30s",
-- MAGIC        "statement":"SELECT grant_id, risk_class, projected_total
-- MAGIC                      FROM onr_itss_poc.da_platform.gold_financial_execution
-- MAGIC                      WHERE risk_class = '\''OVERRUN'\''"}'
-- MAGIC ```

-- COMMAND ----------
-- MAGIC %md
-- MAGIC **Prompt (c):** no public URL — `READ VOLUME` / API token is continuous authorization.
-- MAGIC **Prompt (a):** legacy reports pull this Parquet (or the same SQL) until they cut over.
-- MAGIC
-- MAGIC ### Done
-- MAGIC Stay in the App for search / filter / extract. Narrate any remaining prompt from `docs/DEMO_SCRIPT.md`.

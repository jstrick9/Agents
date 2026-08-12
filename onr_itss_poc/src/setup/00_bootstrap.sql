-- Databricks notebook source
-- MAGIC %md
-- MAGIC # 00 — Bootstrap (run once, on your Serverless SQL warehouse)
-- MAGIC
-- MAGIC Creates catalog **`onr_itss_poc.da_platform`**, the three Volumes, and the App's
-- MAGIC `gold_approval_log` — **no cluster needed**. All cells are SQL.
-- MAGIC
-- MAGIC 1. In the compute picker, attach this notebook to your **Serverless SQL warehouse**.
-- MAGIC 2. **Run all** (you need `CREATE CATALOG` — you have admin).
-- MAGIC 3. Then do the one-time prep in the last cells: upload seed files + start the pipeline.

-- COMMAND ----------

CREATE CATALOG IF NOT EXISTS onr_itss_poc COMMENT 'ONR ITSS POC - mock unclassified only';

-- COMMAND ----------

CREATE SCHEMA IF NOT EXISTS onr_itss_poc.da_platform COMMENT 'bronze / silver / gold';

-- COMMAND ----------

CREATE VOLUME IF NOT EXISTS onr_itss_poc.da_platform.landing COMMENT 'Auto Loader landing (Element 3)';
CREATE VOLUME IF NOT EXISTS onr_itss_poc.da_platform.export COMMENT 'Element 7 export';
CREATE VOLUME IF NOT EXISTS onr_itss_poc.da_platform.checkpoints COMMENT 'Auto Loader checkpoints';

-- COMMAND ----------
-- MAGIC %md
-- MAGIC `gold_approval_log` is the table the **App** writes Approve / Reject decisions to (Element 6).
-- MAGIC It is created here and again defensively when the App opens.

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS onr_itss_poc.da_platform.gold_approval_log (
  grant_id STRING,
  anomaly_type STRING,
  decision STRING,
  decided_by STRING,
  decided_ts TIMESTAMP
) USING DELTA
COMMENT 'App writes Approve/Reject here. Element 6.';

-- COMMAND ----------
-- MAGIC %md
-- MAGIC (Optional) Enable lineage history for Element 4. Admin-only; if this fails on your
-- MAGIC workspace, just skip this cell — Catalog Explorer lineage still works.
-- MAGIC
-- MAGIC ```sql
-- MAGIC SELECT system.enable_system_table('access');
-- MAGIC ```

-- COMMAND ----------
-- MAGIC %md
-- MAGIC ## One-time prep (≈3 minutes, no compute)
-- MAGIC
-- MAGIC **1. Upload the seed files** into the Volumes (Catalog Explorer → `onr_itss_poc` →
-- MAGIC `da_platform` → `landing` → **Add data / Upload files to a volume**):
-- MAGIC
-- MAGIC | Repo file | Upload to |
-- MAGIC |---|---|
-- MAGIC | `data/mock/grants/batch_001.jsonl` | `landing/grants/` |
-- MAGIC | `data/mock/grants/batch_002_schema_evolution.jsonl` | `landing/grants/` |
-- MAGIC | `data/mock/financial/fy26_execution.csv` | `landing/financial/` |
-- MAGIC | `data/mock/financial/fy26_execution_variant.csv` | `landing/financial/` |
-- MAGIC | `data/mock/vendors/subscriptions.jsonl` | `landing/vendors/` |
-- MAGIC
-- MAGIC > Keep `data/mock/grants/live_drop_element3.jsonl` **out** — that is the Element 3
-- MAGIC > live file drop, uploaded on camera during the demo.
-- MAGIC
-- MAGIC **2. Create + start the pipeline** — see `INSTALL.md` step 4 (create manually in
-- MAGIC Workflows → Lakeflow pipelines, compute = **Serverless**).
-- MAGIC
-- MAGIC When both are done, re-run the three `LIST` cells below to confirm the files are visible.

-- COMMAND ----------

LIST '/Volumes/onr_itss_poc/da_platform/landing/grants';

-- COMMAND ----------

LIST '/Volumes/onr_itss_poc/da_platform/landing/financial';

-- COMMAND ----------

LIST '/Volumes/onr_itss_poc/da_platform/landing/vendors';

-- Databricks notebook source
-- MAGIC %md
-- MAGIC # 00 — Unity Catalog bootstrap (run first)
-- MAGIC **Purpose:** Create the IL5-safe catalog, schema, managed volumes, tags, and group grants for the ONR ITSS POC.
-- MAGIC
-- MAGIC **Catalog/schema:** widgets `catalog` / `schema` (defaults `onr_itss_dev.da_platform`)
-- MAGIC
-- MAGIC **IL5 notes**
-- MAGIC - Names contain no CUI, PII, or program identifiers (GovCloud name-field rule).
-- MAGIC - Managed volumes replace DBFS mounts. Do not create `dbfs:/mnt/...` paths.
-- MAGIC - Groups-only grants. Do not GRANT to individual users in prod.
-- MAGIC - This notebook is **idempotent** (`IF NOT EXISTS` / `CREATE OR REPLACE FUNCTION` where possible).
-- MAGIC
-- MAGIC **Requires:** Metastore admin or `CREATE CATALOG` on the GovCloud metastore.

-- COMMAND ----------

CREATE WIDGET TEXT catalog DEFAULT 'onr_itss_dev';
CREATE WIDGET TEXT schema DEFAULT 'da_platform';
CREATE WIDGET TEXT engineers_group DEFAULT 'onr_data_engineers';
CREATE WIDGET TEXT analysts_group DEFAULT 'onr_analysts';
CREATE WIDGET TEXT executives_group DEFAULT 'onr_executives';

-- COMMAND ----------

-- MAGIC %md ### Catalog, schema, volumes

-- COMMAND ----------

-- If your metastore requires a managed location, uncomment and set the GovCloud S3 bucket:
-- CREATE CATALOG IF NOT EXISTS IDENTIFIER(:catalog)
--   MANAGED LOCATION 's3://<govcloud-uc-bucket>/onr_itss/'
--   COMMENT 'ONR ITSS POC — mock unclassified data only';

CREATE CATALOG IF NOT EXISTS IDENTIFIER(:catalog)
  COMMENT 'ONR ITSS POC — mock unclassified data only. No CUI/PII.';

CREATE SCHEMA IF NOT EXISTS IDENTIFIER(:catalog || '.' || :schema)
  COMMENT 'Data and Analytics platform schema (bronze/silver/gold + landing volumes).';

CREATE SCHEMA IF NOT EXISTS IDENTIFIER(:catalog || '.qa')
  COMMENT 'Quarantine and validation results.';

CREATE VOLUME IF NOT EXISTS IDENTIFIER(:catalog || '.' || :schema || '.landing')
  COMMENT 'Auto Loader landing zone for mock grants, financial ERP, and vendor files.';

CREATE VOLUME IF NOT EXISTS IDENTIFIER(:catalog || '.' || :schema || '.export')
  COMMENT 'Secure bulk export target (CSV / JSON / Parquet) for Element 7.';

CREATE VOLUME IF NOT EXISTS IDENTIFIER(:catalog || '.' || :schema || '.checkpoints')
  COMMENT 'Auto Loader schema and checkpoint locations.';

-- COMMAND ----------

-- MAGIC %md ### Classification tags (portable metadata — Element 4)

-- COMMAND ----------

ALTER CATALOG IDENTIFIER(:catalog) SET TAGS ('classification' = 'MOCK_UNCLASSIFIED', 'cui' = 'false', 'pii' = 'false', 'domain' = 'onr_snt_mock');
ALTER SCHEMA IDENTIFIER(:catalog || '.' || :schema) SET TAGS ('classification' = 'MOCK_UNCLASSIFIED', 'cui' = 'false', 'pii' = 'false', 'layer' = 'da_platform');
ALTER VOLUME IDENTIFIER(:catalog || '.' || :schema || '.landing') SET TAGS ('classification' = 'MOCK_UNCLASSIFIED', 'element' = '3');
ALTER VOLUME IDENTIFIER(:catalog || '.' || :schema || '.export') SET TAGS ('classification' = 'MOCK_UNCLASSIFIED', 'element' = '7');

-- COMMAND ----------

-- MAGIC %md ### Least-privilege grants (prompt c — Zero Trust)

-- COMMAND ----------

-- Engineers: build
GRANT USE CATALOG ON CATALOG IDENTIFIER(:catalog) TO IDENTIFIER(:engineers_group);
GRANT USE SCHEMA, CREATE TABLE, CREATE VOLUME, CREATE FUNCTION, CREATE MODEL
  ON SCHEMA IDENTIFIER(:catalog || '.' || :schema) TO IDENTIFIER(:engineers_group);
GRANT READ VOLUME, WRITE VOLUME ON VOLUME IDENTIFIER(:catalog || '.' || :schema || '.landing') TO IDENTIFIER(:engineers_group);
GRANT READ VOLUME, WRITE VOLUME ON VOLUME IDENTIFIER(:catalog || '.' || :schema || '.export') TO IDENTIFIER(:engineers_group);
GRANT READ VOLUME, WRITE VOLUME ON VOLUME IDENTIFIER(:catalog || '.' || :schema || '.checkpoints') TO IDENTIFIER(:engineers_group);

-- Analysts: consume silver/gold (table-level grants applied after pipeline publish)
GRANT USE CATALOG ON CATALOG IDENTIFIER(:catalog) TO IDENTIFIER(:analysts_group);
GRANT USE SCHEMA ON SCHEMA IDENTIFIER(:catalog || '.' || :schema) TO IDENTIFIER(:analysts_group);
GRANT READ VOLUME ON VOLUME IDENTIFIER(:catalog || '.' || :schema || '.export') TO IDENTIFIER(:analysts_group);

-- Executives: catalog visibility only; gold SELECT granted post-publish
GRANT USE CATALOG ON CATALOG IDENTIFIER(:catalog) TO IDENTIFIER(:executives_group);
GRANT USE SCHEMA ON SCHEMA IDENTIFIER(:catalog || '.' || :schema) TO IDENTIFIER(:executives_group);

-- COMMAND ----------

-- MAGIC %md ### Masking UDF (defense in depth — no real PII exists in this POC)

-- COMMAND ----------

CREATE OR REPLACE FUNCTION IDENTIFIER(:catalog || '.' || :schema || '.mask_if_restricted')(val STRING)
RETURNS STRING
RETURN CASE
  WHEN IS_ACCOUNT_GROUP_MEMBER('onr_data_engineers') THEN val
  WHEN val IS NULL THEN NULL
  ELSE '***'
END;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Bootstrap complete. Next: run `01_seed_mock_data` to land files in the volume, then deploy/run the Lakeflow pipeline.

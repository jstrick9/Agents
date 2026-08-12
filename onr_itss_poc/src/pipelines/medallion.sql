-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Medallion — SQL streaming tables + materialized views (Lakeflow-free)
-- MAGIC
-- MAGIC The entire bronze → silver → gold medallion as **plain SQL**, run on your
-- MAGIC **Serverless SQL warehouse**. **There is no pipeline to create, start, or update** —
-- MAGIC no `Workflows → Lakeflow pipelines`, no cluster, no `medallion.py`.
-- MAGIC
-- MAGIC - `CREATE OR REFRESH` here is the whole deploy. Every cell is **idempotent**:
-- MAGIC   re-running rebuilds/refreshes in place, so this notebook is also the DR runbook.
-- MAGIC - Bronze uses Auto Loader (`STREAM read_files`) with `addNewColumns` schema
-- MAGIC   evolution — new columns arrive with zero reconfiguration.
-- MAGIC - Silver enforces the quality gates as `CONSTRAINT ... EXPECT ... DROP ROW`
-- MAGIC   (same semantics as the old pipeline's `expect_or_drop`), tracked in each
-- MAGIC   streaming table's **event log**.
-- MAGIC - Gold materialized views refresh automatically as the streaming tables update;
-- MAGIC   Databricks backs them with serverless-managed Lakeflow (visible in Catalog
-- MAGIC   Explorer) — nothing for you to operate.
-- MAGIC
-- MAGIC Catalog: `onr_itss_poc.da_platform` · Seed files must already be in the landing
-- MAGIC Volume (`00_bootstrap` + the upload table) before the first run.
-- MAGIC `DEMO.sql` runs this notebook via `%run` in its opening cell.

-- COMMAND ----------
-- MAGIC %md
-- MAGIC ## Bronze — Auto Loader on the landing Volume (Element 3)
-- MAGIC
-- MAGIC - Schema is inferred and **evolves** (`addNewColumns`) — `batch_002` adds
-- MAGIC   `collaboration_flag` / `international_partner`, the live drop adds `demo_marker`.
-- MAGIC - Schema versions are written to the `checkpoints` Volume (visible, DR-friendly).
-- MAGIC - `_rescued_data` catches type mismatches; `_metadata.file_path` becomes
-- MAGIC   `_source_file`; `_ingest_ts` stamps arrival time.

-- COMMAND ----------

CREATE OR REFRESH STREAMING TABLE onr_itss_poc.da_platform.bronze_grants
COMMENT 'Raw mock S&T grants. Element 3.'
AS
SELECT
  *,
  _metadata.file_path  AS _source_file,
  current_timestamp()  AS _ingest_ts
FROM STREAM read_files(
  '/Volumes/onr_itss_poc/da_platform/landing/grants',
  format => 'json',
  schemaLocation => '/Volumes/onr_itss_poc/da_platform/checkpoints/schemas/bronze_grants'
);

-- COMMAND ----------

CREATE OR REFRESH STREAMING TABLE onr_itss_poc.da_platform.bronze_financial
COMMENT 'Raw mock ERP. Element 3.'
AS
SELECT
  *,
  _metadata.file_path  AS _source_file,
  current_timestamp()  AS _ingest_ts
FROM STREAM read_files(
  '/Volumes/onr_itss_poc/da_platform/landing/financial',
  format => 'csv',
  header => true,
  schemaLocation => '/Volumes/onr_itss_poc/da_platform/checkpoints/schemas/bronze_financial'
);

-- COMMAND ----------

CREATE OR REFRESH STREAMING TABLE onr_itss_poc.da_platform.bronze_vendors
COMMENT 'Raw mock subscriptions. Prompt e.'
AS
SELECT
  *,
  _metadata.file_path  AS _source_file,
  current_timestamp()  AS _ingest_ts
FROM STREAM read_files(
  '/Volumes/onr_itss_poc/da_platform/landing/vendors',
  format => 'json',
  schemaLocation => '/Volumes/onr_itss_poc/da_platform/checkpoints/schemas/bronze_vendors'
);

-- COMMAND ----------
-- MAGIC %md
-- MAGIC ## Silver — cleansed, with quality gates (Element 3)
-- MAGIC
-- MAGIC Each silver table re-declares its schema explicitly and drops rows that fail the
-- MAGIC same expectations the old pipeline enforced. Expectation results land in the
-- MAGIC streaming table's event log (`event_log('<schema>.<table>')`) — see the Element 4
-- MAGIC cell in `DEMO.sql`.

-- COMMAND ----------

CREATE OR REFRESH STREAMING TABLE onr_itss_poc.da_platform.silver_grants (
  grant_id        STRING COMMENT 'Upper-cased, trimmed MOCK-ONR-* id',
  project_name    STRING,
  performing_org  STRING,
  onr_code        STRING,
  tech_area       STRING,
  start_date      DATE,
  end_date        DATE,
  award_amount    DOUBLE,
  status          STRING,
  trl             INT,
  appropriation   STRING,
  fiscal_year     INT,
  investigator_id STRING,
  source_system   STRING,
  classification  STRING,
  CONSTRAINT valid_grant_id EXPECT (grant_id IS NOT NULL AND grant_id LIKE 'MOCK-ONR-%') ON VIOLATION DROP ROW,
  CONSTRAINT non_negative_award EXPECT (award_amount IS NOT NULL AND award_amount >= 0) ON VIOLATION DROP ROW
)
COMMENT 'Cleansed grants. Bad ids/amounts dropped.'
AS
SELECT DISTINCT
  upper(trim(grant_id))        AS grant_id,
  project_name,
  performing_org,
  onr_code,
  tech_area,
  cast(start_date AS DATE)     AS start_date,
  cast(end_date AS DATE)       AS end_date,
  cast(award_amount AS DOUBLE) AS award_amount,
  status,
  cast(trl AS INT)             AS trl,
  appropriation,
  cast(fiscal_year AS INT)     AS fiscal_year,
  investigator_id,
  source_system,
  classification
FROM STREAM(onr_itss_poc.da_platform.bronze_grants);

-- COMMAND ----------

CREATE OR REFRESH STREAMING TABLE onr_itss_poc.da_platform.silver_financial (
  transaction_id STRING,
  grant_id       STRING,
  fiscal_year    INT,
  period         STRING,
  period_date    DATE,
  budget_line    STRING,
  appropriation  STRING,
  budgeted       DOUBLE,
  obligated      DOUBLE,
  expended       DOUBLE,
  cost_center    STRING,
  source_system  STRING,
  classification STRING,
  CONSTRAINT valid_txn EXPECT (transaction_id IS NOT NULL) ON VIOLATION DROP ROW,
  CONSTRAINT valid_grant EXPECT (grant_id LIKE 'MOCK-ONR-%') ON VIOLATION DROP ROW,
  CONSTRAINT non_negative_expended EXPECT (expended IS NULL OR expended >= 0) ON VIOLATION DROP ROW
)
COMMENT 'Cleansed ERP. Negative expended dropped.'
AS
SELECT DISTINCT
  transaction_id,
  upper(trim(grant_id))                                            AS grant_id,
  cast(fiscal_year AS INT)                                         AS fiscal_year,
  period,
  coalesce(cast(period_date AS DATE),
           cast(concat(period, '-01') AS DATE))                    AS period_date,
  budget_line,
  appropriation,
  cast(budgeted AS DOUBLE)                                         AS budgeted,
  cast(obligated AS DOUBLE)                                        AS obligated,
  cast(expended AS DOUBLE)                                         AS expended,
  cost_center,
  source_system,
  classification
FROM STREAM(onr_itss_poc.da_platform.bronze_financial);

-- COMMAND ----------

CREATE OR REFRESH STREAMING TABLE onr_itss_poc.da_platform.silver_vendors (
  subscription_id  STRING,
  vendor_name      STRING,
  dataset_name     STRING,
  license_type     STRING,
  seats            INT,
  start_date       DATE,
  renewal_date     DATE,
  annual_cost      DOUBLE,
  status           STRING,
  usage_pct        DOUBLE,
  quality_sla      DOUBLE,
  feeds_gold_table STRING,
  days_to_renewal  INT,
  classification   STRING,
  source_system    STRING,
  gap_status       STRING,
  CONSTRAINT valid_sub EXPECT (subscription_id IS NOT NULL) ON VIOLATION DROP ROW
)
COMMENT 'Cleansed subscriptions + gap status. Prompt e.'
AS
SELECT DISTINCT
  subscription_id,
  vendor_name,
  dataset_name,
  license_type,
  cast(seats AS INT)                     AS seats,
  cast(start_date AS DATE)               AS start_date,
  cast(renewal_date AS DATE)             AS renewal_date,
  cast(annual_cost AS DOUBLE)            AS annual_cost,
  status,
  cast(usage_pct AS DOUBLE)              AS usage_pct,
  cast(quality_sla AS DOUBLE)            AS quality_sla,
  feeds_gold_table,
  cast(datediff(cast(renewal_date AS DATE), current_date()) AS INT) AS days_to_renewal,
  classification,
  source_system,
  CASE
    WHEN upper(trim(coalesce(status, ''))) IN ('LAPSED', 'EXPIRED', 'CANCELLED', 'CANCELED') THEN 'DATA_GAP'
    WHEN datediff(cast(renewal_date AS DATE), current_date()) < 0  THEN 'DATA_GAP'
    WHEN datediff(cast(renewal_date AS DATE), current_date()) <= 30 THEN 'RENEWAL_DUE'
    WHEN cast(usage_pct AS DOUBLE) > 0.95                           THEN 'LICENSE_PRESSURE'
    WHEN upper(trim(coalesce(status, ''))) IN ('ACTIVE', 'CURRENT') THEN 'HEALTHY'
    ELSE 'WATCH'
  END                                     AS gap_status
FROM STREAM(onr_itss_poc.da_platform.bronze_vendors);

-- COMMAND ----------
-- MAGIC %md
-- MAGIC ## Gold — materialized views (Elements 4–6)
-- MAGIC
-- MAGIC Materialized views over the streaming tables refresh **automatically and
-- MAGIC incrementally** as upstream tables update. `REFRESH MATERIALIZED VIEW <name>`
-- MAGIC forces a refresh; Catalog Explorer shows the serverless-managed Lakeflow
-- MAGIC pipeline behind each view (no pipeline for you to create).

-- COMMAND ----------

CREATE MATERIALIZED VIEW IF NOT EXISTS onr_itss_poc.da_platform.gold_financial_execution
COMMENT 'Budget vs spend + risk. Elements 5/6.'
AS
WITH fin_agg AS (
  SELECT
    grant_id,
    sum(budgeted)       AS budgeted,
    sum(obligated)      AS obligated,
    sum(expended)       AS expended,
    min(period_date)    AS first_period,
    max(period_date)    AS last_period
  FROM onr_itss_poc.da_platform.silver_financial
  GROUP BY grant_id
),
proj AS (
  SELECT
    f.grant_id,
    g.project_name,
    g.onr_code,
    g.tech_area,
    g.award_amount,
    f.budgeted,
    f.obligated,
    f.expended,
    greatest(1.0, months_between(f.last_period, f.first_period) + 1.0)               AS months_elapsed,
    f.expended / greatest(1.0, months_between(f.last_period, f.first_period) + 1.0)  AS monthly_burn,
    greatest(0.0, months_between(g.end_date, current_date()))                        AS remaining_months,
    f.expended
      + (f.expended / greatest(1.0, months_between(f.last_period, f.first_period) + 1.0))
      * greatest(0.0, months_between(g.end_date, current_date()))                    AS projected_total,
    CASE
      WHEN g.award_amount IS NULL OR g.award_amount <= 0 THEN 'UNKNOWN'
      WHEN (f.expended
              + (f.expended / greatest(1.0, months_between(f.last_period, f.first_period) + 1.0))
              * greatest(0.0, months_between(g.end_date, current_date())))
           / g.award_amount > 1.05 THEN 'OVERRUN'
      WHEN greatest(0.0, months_between(g.end_date, current_date())) < 3
       AND (f.expended
              + (f.expended / greatest(1.0, months_between(f.last_period, f.first_period) + 1.0))
              * greatest(0.0, months_between(g.end_date, current_date())))
           / g.award_amount < 0.80 THEN 'UNDER_EXEC'
      WHEN (f.expended
              + (f.expended / greatest(1.0, months_between(f.last_period, f.first_period) + 1.0))
              * greatest(0.0, months_between(g.end_date, current_date())))
           / g.award_amount >= 0.95 THEN 'AT_RISK'
      ELSE 'ON_TRACK'
    END                                                                             AS risk_class
  FROM fin_agg f
  LEFT JOIN onr_itss_poc.da_platform.silver_grants g
    ON f.grant_id = g.grant_id
)
SELECT
  grant_id,
  project_name,
  onr_code,
  tech_area,
  award_amount,
  budgeted,
  obligated,
  expended,
  monthly_burn,
  remaining_months,
  projected_total,
  risk_class,
  concat('TRD-', risk_class, '-BURN') AS trend_id,
  monthly_burn                       AS predicted_velocity,
  current_timestamp()                AS as_of_ts
FROM proj;

-- COMMAND ----------

CREATE MATERIALIZED VIEW IF NOT EXISTS onr_itss_poc.da_platform.gold_predictive_velocity
COMMENT 'SQL-native risk/velocity model. Element 5.'
AS
WITH model AS (
  SELECT
    e.grant_id,
    e.project_name,
    e.onr_code,
    e.award_amount,
    e.risk_class,
    e.monthly_burn,
    e.projected_total,
    e.remaining_months,
    greatest(1.0, months_between(g.end_date, g.start_date) + 1.0) AS duration_months,
    e.monthly_burn
      / nullif(e.award_amount, 0.0)
      * greatest(1.0, months_between(g.end_date, g.start_date) + 1.0) AS velocity_ratio
  FROM onr_itss_poc.da_platform.gold_financial_execution e
  LEFT JOIN onr_itss_poc.da_platform.silver_grants g
    ON e.grant_id = g.grant_id
)
SELECT
  grant_id,
  project_name,
  onr_code,
  award_amount,
  risk_class,
  CASE
    WHEN award_amount IS NULL OR award_amount <= 0 THEN 'UNKNOWN'
    WHEN projected_total / award_amount > 1.05                THEN 'OVERRUN'
    WHEN remaining_months < 3 AND projected_total / award_amount < 0.80 THEN 'UNDER_EXEC'
    WHEN projected_total / award_amount >= 0.95               THEN 'AT_RISK'
    ELSE 'ON_TRACK'
  END                                                  AS model_risk_class,
  monthly_burn                                         AS predicted_velocity,
  CASE
    WHEN award_amount IS NULL OR award_amount <= 0 THEN 'TRD-UNKNOWN'
    ELSE concat('TRD-', risk_class, '-',
      CASE
        WHEN velocity_ratio > 1.2 THEN 'ACCEL'
        WHEN velocity_ratio < 0.8 THEN 'DECEL'
        ELSE 'FLAT'
      END)
  END                                                  AS trend_id,
  current_timestamp()                                  AS as_of_ts
FROM model;

-- COMMAND ----------

CREATE MATERIALIZED VIEW IF NOT EXISTS onr_itss_poc.da_platform.gold_anomalies
COMMENT 'Flags + routing. Element 6.'
AS
SELECT
  grant_id,
  'FINANCIAL'                       AS anomaly_type,
  risk_class                        AS severity,
  concat('Grant ', grant_id, ' is ', risk_class) AS description,
  'financial_execution_lead'        AS route_to,
  current_timestamp()               AS detected_ts
FROM onr_itss_poc.da_platform.gold_financial_execution
WHERE risk_class IN ('OVERRUN', 'AT_RISK', 'UNDER_EXEC')
UNION ALL
SELECT
  subscription_id                   AS grant_id,
  'VENDOR'                          AS anomaly_type,
  gap_status                        AS severity,
  concat(vendor_name, ' ', gap_status) AS description,
  'data_vendor_manager'             AS route_to,
  current_timestamp()               AS detected_ts
FROM onr_itss_poc.da_platform.silver_vendors
WHERE gap_status IN ('DATA_GAP', 'RENEWAL_DUE');

-- COMMAND ----------

CREATE MATERIALIZED VIEW IF NOT EXISTS onr_itss_poc.da_platform.gold_vendors
COMMENT 'Subscription health. Prompt e.'
AS
SELECT
  *,
  current_timestamp() AS as_of_ts
FROM onr_itss_poc.da_platform.silver_vendors;

-- COMMAND ----------

CREATE MATERIALIZED VIEW IF NOT EXISTS onr_itss_poc.da_platform.gold_data_quality
COMMENT 'Health scores. Element 4.'
AS
WITH scores AS (
  SELECT
    'bronze_grants' AS dataset,
    count(*)        AS row_count,
    sum(CASE WHEN grant_id IS NULL THEN 1 ELSE 0 END) / count(*) AS null_rate
  FROM onr_itss_poc.da_platform.bronze_grants
  UNION ALL
  SELECT
    'silver_grants' AS dataset,
    count(*)        AS row_count,
    sum(CASE WHEN grant_id IS NULL THEN 1 ELSE 0 END) / count(*) AS null_rate
  FROM onr_itss_poc.da_platform.silver_grants
  UNION ALL
  SELECT
    'silver_financial' AS dataset,
    count(*)           AS row_count,
    sum(CASE WHEN transaction_id IS NULL THEN 1 ELSE 0 END) / count(*) AS null_rate
  FROM onr_itss_poc.da_platform.silver_financial
  UNION ALL
  SELECT
    'gold_financial_execution' AS dataset,
    count(*)                   AS row_count,
    sum(CASE WHEN grant_id IS NULL THEN 1 ELSE 0 END) / count(*) AS null_rate
  FROM onr_itss_poc.da_platform.gold_financial_execution
)
SELECT
  dataset,
  row_count,
  null_rate,
  (1.0 - null_rate) * 100                        AS health_score,
  CASE WHEN (1.0 - null_rate) * 100 >= 90
       THEN 'HEALTHY' ELSE 'WATCH' END           AS health_band,
  current_timestamp()                            AS computed_ts
FROM scores;

-- COMMAND ----------

CREATE MATERIALIZED VIEW IF NOT EXISTS onr_itss_poc.da_platform.gold_executive_kpis
COMMENT 'KPI strip. Element 6.'
AS
SELECT
  k.grant_count,
  k.total_awarded,
  k.total_expended,
  k.overrun_count,
  k.at_risk_count,
  k.avg_monthly_velocity,
  k.execution_rate,
  v.vendor_data_gaps,
  current_timestamp() AS as_of_ts
FROM (
  SELECT
    count(*)                                   AS grant_count,
    sum(award_amount)                          AS total_awarded,
    sum(expended)                              AS total_expended,
    sum(CASE WHEN risk_class = 'OVERRUN' THEN 1 ELSE 0 END)  AS overrun_count,
    sum(CASE WHEN risk_class = 'AT_RISK' THEN 1 ELSE 0 END)  AS at_risk_count,
    avg(predicted_velocity)                    AS avg_monthly_velocity,
    sum(expended) / sum(award_amount)          AS execution_rate
  FROM onr_itss_poc.da_platform.gold_financial_execution
) k
CROSS JOIN (
  SELECT
    sum(CASE WHEN gap_status = 'DATA_GAP' THEN 1 ELSE 0 END) AS vendor_data_gaps
  FROM onr_itss_poc.da_platform.gold_vendors
) v;

-- COMMAND ----------

CREATE MATERIALIZED VIEW IF NOT EXISTS onr_itss_poc.da_platform.gold_executive_summary
COMMENT 'Automated narrative. Element 6.'
AS
SELECT
  current_timestamp() AS generated_ts,
  concat(
    'Mock portfolio: ', cast(grant_count AS STRING),
    ' awards, $', format_number(total_awarded, 0),
    ' awarded, ', format_number(execution_rate * 100, 1),
    '% executed. Overruns: ', cast(overrun_count AS STRING),
    '. Vendor data-gaps: ', cast(vendor_data_gaps AS STRING), '.'
  ) AS summary_text
FROM onr_itss_poc.da_platform.gold_executive_kpis;

-- COMMAND ----------
-- MAGIC %md
-- MAGIC ## Verify (optional, quick)
-- MAGIC
-- MAGIC ```sql
-- MAGIC SHOW TABLES IN onr_itss_poc.da_platform;
-- MAGIC DESCRIBE EXTENDED onr_itss_poc.da_platform.bronze_grants;  -- evolved schema
-- MAGIC ```
-- MAGIC
-- MAGIC **Updates:** `ALTER STREAMING TABLE ... REFRESH` pulls new landing files;
-- MAGIC `REFRESH MATERIALIZED VIEW ...` forces a gold refresh (normally automatic).
-- MAGIC **Rebuild/DR:** just re-run this notebook — every statement is `CREATE OR REFRESH`.

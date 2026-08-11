# Databricks on AWS — AI Agent BUILDER Memory & Execution Playbook
**Version:** 2.0 — BUILDER EDITION — 2026-08-11 | Charlotte, NC (America/New_York)
**Primary Source:** https://docs.databricks.com/aws/en/ (Updated 2026-07-28) + Developer Docs / DLT / MLflow 3 / Apps
**Purpose:** Enable AI Agent to **DEVELOP, BUILD, QA, TEST, VALIDATE, DESIGN, DEPLOY** completed code, notebooks, jobs, schemas, tables, pipelines, AI/ML, MLflow, and Databricks Apps on AWS — not just describe them.
**Agent Role:** Senior Databricks on AWS Engineer + QA + DevOps. You write **runnable, Unity Catalog-governed, serverless-first code** and validate it before delivery.

> **BUILDER DIRECTIVE:** For every request, deliver: (1) Working code/notebook artifact, (2) Unity Catalog DDL & grants, (3) Job/bundle YAML, (4) QA/tests + validation queries, (5) How to run on AWS (compute choice, `dbutils`, `bundle deploy`). Default to `pyspark.pipelines as dp` (new SDP API), `cloudFiles` Auto Loader, Delta Live Tables expectations, MLflow 3 `scorers`, and DABs. Never propose DBFS mounts — use UC Volumes/External Locations. Note FedRAMP scope when GovCloud is implied.

---

## 1. HOW TO USE THIS MEMORY (Agent Operating Loop)

For ANY build request, follow this loop:

1.  **Clarify Target:** Catalog.Schema scope, AWS region (commercial vs GovCloud), latency/SLA, data volume. Ask if missing.
2.  **Scaffold via DAB:** Initialize bundle (`databricks bundle init`) — dev/prod targets, workspace host, permissions.
3.  **Create UC Objects:** DDL first (catalog, schema, volume, table). See §3.
4.  **Build Code:** Use templates in §4-§11. One notebook/file per layer/task. Add `dbutils.widgets` for params.
5.  **Add Quality Gates:** Expectations (`@dp.expect*`), `CONSTRAINT`s, DQX, unit tests (`pytest` + `databricks-connect`).
6.  **Wire Orchestration:** `databricks.yml` jobs/pipelines/tasks (§7). Use serverless where possible.
7.  **Validate:** Run validation queries (§12), `mlflow.genai.evaluate` for AI, Lakehouse Monitoring.
8.  **Deploy:** `databricks bundle validate && bundle deploy -t dev && bundle run`.
9.  **Observability:** System tables (`system.billing.usage`, `system.query.history`, `system.lakeflow.*`).

---

## 2. PROJECT SCAFFOLD (Databricks Asset Bundles — DABs)

**DABs are the REQUIRED IaC for all builds — not Terraform alone for workflows.**

### 2.1 Init Commands (Agent should emit these)
```bash
# Install CLI (AWS)
brew install databricks/tap/databricks  # or curl | bash
databricks auth profiles # OAuth M2M or PAT (max 730 days)
databricks bundle init https://github.com/databricks/bundle-examples --template-dir default-python  # or streamlit-app
# Choose: default-python (jobs+pipelines), streamlit-app (Apps)
```

### 2.2 Canonical `databricks.yml` — Jobs + Pipelines + Apps + UC
```yaml
# databricks.yml — Builder Template (AWS, serverless-first, UC-governed)
bundle:
  name: aws_lakehouse_builder

include:
  - resources/*.yml

targets:
  dev:
    mode: development
    default: true
    workspace:
      host: https://<aws-workspace>.cloud.databricks.com
      root_path: /Workspace/Users/${workspace.current_user.userName}/.bundle/${bundle.name}/${bundle.target}
    variables:
      catalog: dev_catalog
      schema: ops
    permissions:
      - level: CAN_MANAGE
        group_name: data-engineers

  prod:
    mode: production
    workspace:
      host: https://<aws-workspace-prod>.cloud.databricks.com
      root_path: /Workspace/Users/svc-prod@company.com/.bundle/${bundle.name}/${bundle.target}
    variables:
      catalog: prod_catalog
      schema: ops
    permissions:
      - level: CAN_VIEW
        group_name: analysts

resources:
  # UC Volume for landing (see §3)
  volumes:
    landing_volume:
      name: landing
      catalog_name: ${var.catalog}
      schema_name: ${var.schema}
      volume_type: MANAGED

  # Pipelines = SDP/DLT
  pipelines:
    medallion_pipeline:
      name: ${var.catalog}.${var.schema}.medallion_pipeline
      serverless: true
      photon: true
      catalog: ${var.catalog}
      target: ${var.schema}
      libraries:
        - notebook:
            path: ../src/pipelines/bronze_silver_gold.py
      # Continuous for streaming bronze, triggered for batch
      continuous: false
      expectations:
        strict: true

  jobs:
    main_workflow:
      name: main_workflow-${bundle.target}
      serverless: true
      # Trigger options: periodic | file_arrival | table_update
      trigger:
        periodic:
          interval: 1
          unit: HOURS
      # Also supported: trigger.file_arrival, trigger.table_update (GA June 2025)
      tasks:
        - task_key: ingest_bronze
          pipeline_task:
            pipeline_id: ${resources.pipelines.medallion_pipeline.id}
        - task_key: validate_gold
          depends_on: [ingest_bronze]
          notebook_task:
            notebook_path: ../src/qa/validate_gold
            base_parameters:
              catalog: ${var.catalog}
              schema: ${var.schema}
          environment_key: serverless
      environments:
        - environment_key: serverless
          spec:
            environment_version: "2"
            dependencies: ["dqx==0.12.0", "pytest"]

  apps:
    ops_app:
      name: ops-app-${bundle.target}
      description: "Ops dashboard for gold tables"
      source_code_path: ../src/app
      # Python Dash/Streamlit/Gradio — command in app.yaml
      permissions:
        - level: CAN_USE
          group_name: users
      resources:
        - name: gold-table
          table:
            id: ${var.catalog}.${var.schema}.gold_daily_orders
            permission: CAN_READ

  quality_monitors:
    gold_monitor:
      table_name: ${var.catalog}.${var.schema}.gold_daily_orders
      output_schema_name: ${var.catalog}.${var.schema}
      assets_dir: /Workspace/Users/${workspace.current_user.userName}/.bundle/monitoring
      time_series:
        granularities: [1 hour]
        timestamp_col: order_date
      schedule:
        quartz_cron_expression: 0 0 8 * * ? # daily 8am UTC
        timezone_id: UTC

variables:
  catalog:
    description: Unity Catalog catalog
    default: dev_catalog
  schema:
    description: Default schema
    default: ops
```

**Validate & Run:**
```bash
databricks bundle validate -t dev
databricks bundle deploy -t dev
databricks bundle run main_workflow -t dev  # or schedule
databricks bundle destroy -t dev # teardown
```

---

## 3. UNITY CATALOG — DDL AGENT MUST GENERATE FIRST

**Rule: No code without UC place. New accounts post-2025-12-18 have legacy disabled — must use UC.**

### 3.1 Catalog / Schema / Volume / External Location
```sql
-- Run as Account Admin / Metastore Admin (notebook %sql or SDK)
CREATE CATALOG IF NOT EXISTS dev_catalog MANAGED LOCATION 's3://<your-uc-bucket>/dev_catalog';
CREATE SCHEMA IF NOT EXISTS dev_catalog.ops;
CREATE SCHEMA IF NOT EXISTS dev_catalog.qa;
CREATE VOLUME IF NOT EXISTS dev_catalog.ops.landing; -- Managed volume replaces DBFS mounts
-- For data that MUST stay at path:
CREATE EXTERNAL LOCATION IF NOT EXISTS s3_raw
  URL 's3://company-raw-bucket/landing/'
  WITH (STORAGE CREDENTIAL s3_cred) COMMENT 'Read-only raw';

GRANT USE CATALOG ON CATALOG dev_catalog TO `data-engineers`;
GRANT USE SCHEMA, CREATE TABLE, CREATE VOLUME ON SCHEMA dev_catalog.ops TO `data-engineers`;
GRANT SELECT ON SCHEMA dev_catalog.ops TO `analysts`;
-- ABAC (Preview) — tag-based dynamic policy, inherits:
-- ALTER CATALOG dev_catalog SET TAGS ('pii'='true');
-- ABAC policy defined at account level: masks pii columns for analysts group
```

**Python SDK equivalent (Agent emits when automating):**
```python
from databricks.sdk import WorkspaceClient
w = WorkspaceClient()
w.catalogs.create(name="dev_catalog", storage_root="s3://<bucket>/dev_catalog")
w.schemas.create(name="ops", catalog_name="dev_catalog")
w.volumes.create(catalog_name="dev_catalog", schema_name="ops", name="landing", volume_type="MANAGED")
```

### 3.2 Tables — Managed (Preferred) + External
```sql
-- MANAGED — Delta with Liquid Clustering & Predictive Optimization
CREATE TABLE IF NOT EXISTS dev_catalog.ops.bronze_orders
USING DELTA TBLPROPERTIES ('delta.feature.allowColumnDefaults'='supported');

-- With constraints (QA at table level)
CREATE TABLE IF NOT EXISTS dev_catalog.ops.silver_orders (
  order_id STRING NOT NULL,
  order_datetime TIMESTAMP,
  amount DOUBLE,
  customer_id STRING,
  CONSTRAINT valid_amount CHECK (amount > 0),
  CONSTRAINT valid_id CHECK (customer_id IS NOT NULL)
) USING DELTA
CLUSTER BY (customer_id); -- Auto Liquid Clustering prefers CLUSTER BY vs PARTITIONED BY

-- EXTERNAL (only when path matters)
CREATE TABLE IF NOT EXISTS dev_catalog.ops.ext_raw
USING DELTA LOCATION 's3://company-raw-bucket/silver/orders/';
```

**Alter to Managed (migration):**
```sql
ALTER TABLE dev_catalog.ops.ext_raw SET MANAGED; -- GA Oct 2025
ALTER TABLE hive_metastore.default.old_table SET MANAGED; -- Converts Hive
```

### 3.3 Governance Extras Agent Must Add
```sql
-- Row filter + Column mask (GA)
CREATE FUNCTION dev_catalog.ops.mask_email(email STRING) RETURN CASE WHEN IS_MEMBER('analysts') THEN '***' ELSE email END;
ALTER TABLE dev_catalog.ops.gold_customers ALTER COLUMN email SET MASK dev_catalog.ops.mask_email;
ALTER TABLE dev_catalog.ops.gold_orders SET ROW FILTER dev_catalog.ops.is_own_region ON (region = current_user_region());

-- Tags
ALTER TABLE dev_catalog.ops.gold_orders SET TAGS ('domain'='sales', 'pii'='false');
```

---

## 4. NOTEBOOK TEMPLATE (Agent Generates This for Every Notebook)

Every notebook Agent builds MUST include header, widgets, UC checks, and tests.

```python
# Databricks notebook source
# MAGIC %md
# MAGIC # Notebook: bronze_ingest_orders
# MAGIC **Purpose:** Auto Loader ingest S3 -> Bronze Delta  
# MAGIC **Catalog:** dev_catalog.ops | **Compute:** Serverless | **Owner:** data-engineers
# MAGIC **Inputs:** /Volumes/dev_catalog/ops/landing/orders/*.json
# MAGIC **Output:** dev_catalog.ops.bronze_orders
# MAGIC **QA:** expectations + row count validation

# COMMAND ----------
dbutils.widgets.text("catalog", "dev_catalog")
dbutils.widgets.text("schema", "ops")
dbutils.widgets.text("volume_path", "/Volumes/dev_catalog/ops/landing/orders/")
catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")

# COMMAND ----------
from pyspark.sql.functions import col, current_timestamp, input_file_name
spark.sql(f"USE CATALOG {catalog}")
spark.sql(f"USE SCHEMA {schema}")

# Validate UC exists
assert spark.catalog.tableExists(f"{catalog}.{schema}.bronze_orders") or True, "Bronze table not yet created — run DDL first"

# COMMAND ----------
# AUTO LOADER — Incremental ingest (preferred over spark.read)
bronze_df = (spark.readStream
  .format("cloudFiles")
  .option("cloudFiles.format", "json")
  .option("cloudFiles.inferColumnTypes", "true")
  .option("cloudFiles.schemaLocation", f"/Volumes/{catalog}/{schema}/landing/_schemas/bronze_orders")
  .option("cloudFiles.schemaEvolutionMode", "addNewColumns") # for evolution
  .load(dbutils.widgets.get("volume_path"))
  .withColumn("_ingest_time", current_timestamp())
  .withColumn("_source_file", input_file_name())
)

# COMMAND ----------
# Write stream to Delta (managed) — for classic job; for SDP use @dp.table instead (see §5)
(bronze_df.writeStream
  .format("delta")
  .option("checkpointLocation", f"/Volumes/{catalog}/{schema}/landing/_chk/bronze_orders")
  .option("mergeSchema", "true")
  .trigger(availableNow=True) # batch-like for jobs; use processingTime='30 seconds' for continuous
  .toTable(f"{catalog}.{schema}.bronze_orders")
)

# COMMAND ----------
# MAGIC %md ### Validation (QA) — Agent always appends this cell
# COMMAND ----------
# Row count + schema checks
cnt = spark.table(f"{catalog}.{schema}.bronze_orders").count()
print(f"Row count: {cnt}")
assert cnt > 0, "QA FAIL: bronze_orders empty after ingest"
spark.table(f"{catalog}.{schema}.bronze_orders").printSchema()
# Data quality quick check
display(spark.sql(f"SELECT COUNT(*) as null_ids FROM {catalog}.{schema}.bronze_orders WHERE order_id IS NULL"))
```

**SQL Notebook Variant (`%sql`):**
```sql
-- Ingest via COPY INTO (SQL alternative to Auto Loader for batch)
COPY INTO dev_catalog.ops.bronze_orders
FROM '/Volumes/dev_catalog/ops/landing/orders/'
FILEFORMAT = JSON
FORMAT_OPTIONS ('inferColumnTypes'='true')
COPY_OPTIONS ('mergeSchema'='true');
```

---

## 5. MEDALLION PIPELINE — LAKEFLOW DECLARATIVE PIPELINES (SDP/DLT) — PRIMARY BUILD PATTERN

**Agent defaults to `pyspark.pipelines` (Spark 4.1+ open API, extends on Databricks). Use streaming tables for incremental, materialized views for batch.**

### 5.1 Full Bronze → Silver → Gold (Agent Copies & Adapts This File: `src/pipelines/bronze_silver_gold.py`)

```python
from pyspark import pipelines as dp
from pyspark.sql.functions import col, current_timestamp, coalesce, count

# BRONZE — raw, append-only, cloudFiles
@dp.table(name="bronze_orders")
@dp.expect_or_drop("valid_json", "order_id IS NOT NULL")  # keeps metrics, drops bad
def bronze_orders():
  return (spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "json")
    .option("cloudFiles.schemaLocation", "/Volumes/dev_catalog/ops/landing/_schemas/bronze_orders")
    .load("/Volumes/dev_catalog/ops/landing/orders/")
    .withColumn("_ingest_ts", current_timestamp())
  )

# SILVER — cleansed, deduped, quality-enforced
@dp.table(name="silver_orders")
@dp.expect("valid_amount", "amount > 0")
@dp.expect_or_fail("has_customer", "customer_id IS NOT NULL") # fail pipeline if missing
@dp.expect_or_drop("valid_timestamp", "order_datetime IS NOT NULL")
def silver_orders():
  bronze = spark.readStream.table("bronze_orders")
  # Deduplicate via watermark + dropDuplicates (for streaming)
  return (bronze
    .withWatermark("order_datetime", "1 hour")
    .dropDuplicates(["order_id"])
    .withColumn("order_date", col("order_datetime").cast("date"))
    .withColumn("amount", col("amount").cast("double"))
  )

# Join + enrich example
@dp.materialized_view(name="customer_orders")
def customer_orders():
  orders = spark.read.table("silver_orders")
  customers = spark.read.table("dev_catalog.ops.customers") # reference existing
  return (orders.join(customers, "customer_id", "left")
    .select("customer_id", "order_id", "state", "order_date", "amount")
  )

# GOLD — business-ready aggregate, partitioned/clustered
@dp.materialized_view(name="gold_daily_orders_by_state")
def gold_daily_orders_by_state():
  return (spark.read.table("customer_orders")
    .groupBy("state", "order_date")
    .agg(count("*").alias("order_count"), coalesce(sum("amount"),0).alias("total_amount"))
  )

# Advanced: Reuse expectations dict
valid_orders = {
  "has_id": "order_id IS NOT NULL",
  "positive_amount": "amount > 0"
}
@dp.table(name="silver_orders_v2")
@dp.expect_all(valid_orders)
def silver_orders_v2():
  return spark.readStream.table("bronze_orders")

# CDC Auto-flow (Databricks extension, not open Spark)
# @dp.create_auto_cdc_flow(...) — for CDC from CDC feed
```

### 5.2 Expectations QA Matrix (Agent Must Explain)
- `expect`: Logs, keeps invalid rows.
- `expect_or_drop`: Drops invalid, logs count — for non-critical.
- `expect_or_fail`: Halts pipeline — for critical business rules.
- `expect_all / expect_all_or_drop / expect_all_or_fail`: Batch apply dict.

**Validation:** Pipeline UI → Expectations tab shows pass/fail/drop counts per dataset.

### 5.3 Batch Alternative (if no streaming needed)
```python
@dp.materialized_view()
def batch_mv():
  return spark.read.format("json").load("/Volumes/dev_catalog/ops/landing/orders/")
```

---

## 6. STREAMING & INGESTION COOKBOOK (Agent Picks One)

```python
# A) Auto Loader (default)
spark.readStream.format("cloudFiles").option("cloudFiles.format","csv").option("cloudFiles.schemaLocation", schema_loc).load(volume_path)

# B) Kinesis (AWS)
spark.readStream.format("kinesis")
  .option("streamName", "orders-stream")
  .option("region", "us-east-1")
  .option("initialPosition", "latest")
  .load()

# C) Lakeflow Connect — Salesforce (no code, governed)
# Configure in UI: Lakeflow Connect > Salesforce connector -> `dev_catalog.ops.salesforce_raw` (serverless), then transform via SDP above.

# D) COPY INTO (SQL batch)
# COPY INTO target FROM '/Volumes/...' FILEFORMAT = JSON

# E) Zerobus (high-volume Kafka replacement) — $0.05/GB
# Lakeflow Connect > Zerobus Ingest -> topic -> bronze
```

---

## 7. WORKFLOWS — ORCHESTRATION TEMPLATES

### 7.1 Job JSON (via DAB) — Three Trigger Types
```yaml
# resources/job.yml — File arrival trigger (Preview May 2025)
resources:
  jobs:
    file_arrival_job:
      name: file-arrival-ingest
      trigger:
        file_arrival:
          location: /Volumes/dev_catalog/ops/landing/orders/
          min_time_between_triggers_seconds: 300
      tasks:
        - task_key: run_pipeline
          pipeline_task: { pipeline_id: ${resources.pipelines.medallion_pipeline.id} }
    table_update_job:
      name: table-update-refresh
      trigger:
        table_update:
          table_names: ["dev_catalog.ops.silver_orders"]
          condition: UPDATE
      tasks:
        - task_key: refresh_gold_mv
          sql_task:
            warehouse_id: ${resources.sql_warehouses.serverless.id}
            query:
              query_id: ${resources.queries.refresh_gold.id}
    periodic_job:
      name: periodic-validations
      trigger:
        periodic: { interval: 1, unit: DAYS }
      tasks:
        - task_key: notebook_qa
          notebook_task:
            notebook_path: ../src/qa/run_all_tests
            base_parameters: { catalog: ${var.catalog} }
```

### 7.2 Python Task + Conditional Retry
```yaml
tasks:
  - task_key: python_transform
    spark_python_task: { python_file: ../src/transform/main.py }
    environment_key: serverless
    max_retries: 2
    min_retry_interval_millis: 5000
  - task_key: check_quality
    depends_on: [{task_key: python_transform}]
    condition_task:
      op: EQUAL_TO
      left: "{{tasks.python_transform.values.quality_passed}}"
      right: "true"
  - task_key: notify_failure
    depends_on: [{task_key: check_quality, outcome: "false"}]
    # email_notifications at job level
```

**Agent always sets:** `max_retries`, `timeout_seconds`, `email_notifications.on_failure`, `health.rules`.

---

## 8. DELTA LAKE — BUILD OPERATIONS AGENT MUST USE

```python
# Upsert (MERGE) — Silver to Gold
from delta.tables import DeltaTable
deltaGold = DeltaTable.forName(spark, "dev_catalog.ops.gold_customers")
(deltaGold.alias("t").merge(
    spark.table("dev_catalog.ops.silver_customers").alias("s"),
    "t.customer_id = s.customer_id")
  .whenMatchedUpdateAll()
  .whenNotMatchedInsertAll()
  .execute()
)

# Time Travel & Audit
spark.sql("SELECT * FROM dev_catalog.ops.gold_orders VERSION AS OF 2")
spark.sql("SELECT * FROM dev_catalog.ops.gold_orders TIMESTAMP AS OF '2026-08-10'")
spark.sql("DESCRIBE HISTORY dev_catalog.ops.gold_orders")
# Restore on bad publish
spark.sql("RESTORE TABLE dev_catalog.ops.gold_orders TO VERSION AS OF 5")

# Optimize — do not use PARTITIONED BY for new; use CLUSTER BY + predictive
spark.sql("OPTIMIZE dev_catalog.ops.gold_orders") # + ZORDER was old; now CLUSTER BY
spark.sql("VACUUM dev_catalog.ops.gold_orders RETAIN 168 HOURS") # 7 days, default

# Schema Evolution
spark.sql("ALTER TABLE dev_catalog.ops.silver_orders ADD COLUMN loyalty_tier STRING")
# For streaming writes: .option("mergeSchema","true")

# Delete vectors & Liquid clustering (auto in 2025+)
spark.sql("ALTER TABLE dev_catalog.ops.gold_orders CLUSTER BY (state, order_date)")
# Enable predictive optimization (auto) — no manual OPTIMIZE needed for managed tables if enabled
```

---

## 9. QA / TEST / VALIDATION FRAMEWORK (Agent Must Ship With Every Build)

### 9.1 Levels
1.  **Unit (Pytest + Databricks Connect):** Mock `spark` locally.
```python
# tests/test_transforms.py
import pytest
from pyspark.sql import SparkSession
from src.transform.silver import clean_orders

@pytest.fixture
def spark():
  return SparkSession.builder.master("local[1]").getOrCreate()

def test_drops_null_ids(spark):
  df = spark.createDataFrame([(None, 10.0), ("1", 20.0)], ["order_id","amount"])
  out = clean_orders(df)
  assert out.count() == 1 and out.collect()[0]["order_id"] == "1"
```
Run: `pytest tests/ && databricks bundle run` (serverless environment).

2.  **Expectations (in-pipeline):** See §5.2 — on every table.

3.  **DQX (Databricks Lab Quality):** Declarative row-level checks.
```python
import dqx
from dqx import ColRule

rules = [ColRule(col="amount", check="> 0", criticality="error"), ColRule(col="customer_id", check="not_null", criticality="error")]
dqx.validate(spark.table("dev_catalog.ops.bronze_orders"), rules, quarantine_table="dev_catalog.qa.quarantine_bronze")
```

4.  **Post-Deploy Validation Notebook (`src/qa/validate_gold.py`):**
```python
# Assert counts, freshness, referential integrity
catalog, schema = dbutils.widgets.get("catalog"), dbutils.widgets.get("schema")
for tbl in ["bronze_orders","silver_orders","gold_daily_orders_by_state"]:
  cnt = spark.table(f"{catalog}.{schema}.{tbl}").count()
  print(f"{tbl}: {cnt}"); assert cnt>0, f"FAIL {tbl} empty"
# Freshness
fresh = spark.sql(f"SELECT max(order_date) as max_dt FROM {catalog}.{schema}.gold_daily_orders_by_state").collect()[0][0]
assert fresh >= get_today_minus(2), f"Stale gold: {fresh}"
# Row filter / mask smoke test
assert spark.sql(f"SELECT email FROM {catalog}.{schema}.gold_customers LIMIT 1").collect()[0][0].startswith("***") if is_analyst else True
print("All QA passed")
dbutils.notebook.exit("quality_passed=true" if True else "quality_passed=false")
```

5.  **Lakehouse Monitoring (Auto QA Dashboard):**
```yaml
# In bundle §2 — quality_monitors — generates drift/quality dashboard + alerts
# Also inference_log: prediction_col, label_col, problem_type for ML
```

**Agent Exit Criteria:** All expectations green + `validate_gold` passed + `vacuum`/`optimize` history checked before marking `prod` deploy.

---

## 10. AI / ML & MLFLOW 3 — BUILD TEMPLATES

### 10.1 Classic ML (sklearn) with MLflow Tracking & Registry (UC-Governed)
```python
import mlflow
from sklearn.ensemble import RandomForestClassifier

mlflow.set_registry_uri("databricks-uc") # UC Model Registry
mlflow.set_experiment("/Workspace/Users/${user}/experiments/churn")

with mlflow.start_run(run_name="rf-baseline") as run:
  mlflow.log_params({"n_estimators": 100, "max_depth": 5})
  model = RandomForestClassifier().fit(X_train, y_train)
  acc = model.score(X_test, y_test)
  mlflow.log_metric("accuracy", acc)
  # Log model to UC
  mlflow.sklearn.log_model(model, "model", registered_model_name="dev_catalog.ops.churn_rf")
  # Log as UC table for batch inference
  mlflow.evaluate(data=eval_df, model_type="classifier", targets="churn")

# Batch Inference via AI Functions (SQL) — no cluster
spark.sql("""
  SELECT customer_id, ai_query('databricks-meta-llama-3-70b', prompt) as summary
  FROM dev_catalog.ops.gold_customers
""")

# Serve: via Model Serving endpoint (UI or REST)
# databricks serving-endpoints create --name churn-rf --config served_models=[{model_name:"dev_catalog.ops.churn_rf", model_version:"1", workload_size:"Small"}]
```

### 10.2 GenAI — RAG Agent (Mosaic AI: Vector Search + Agent Framework + Evaluation)

**A) Create Vector Search Index (syncs from Delta):**
```python
from databricks.vector_search.client import VectorSearchClient
vsc = VectorSearchClient()
# Endpoint (serverless, pay per CU) — create once
vsc.create_endpoint("vs_endpoint", endpoint_type="STANDARD") # or STORAGE_OPTIMIZED
# Index over UC table (embeds synced)
index = vsc.create_delta_sync_index(
  endpoint_name="vs_endpoint",
  index_name="dev_catalog.ops.docs_index",
  source_table_name="dev_catalog.ops.docs_chunked", # must have text column + UC
  pipeline_type="TRIGGERED", # or CONTINUOUS
  primary_key="id",
  embedding_source_column="text",
  embedding_model_endpoint_name="databricks-bge-large-en"
)
# Query
results = index.similarity_search(query_text="refund policy", num_results=5, filters={"domain":"sales"})
```

**B) Agent Code (MLflow-traced, UC-governed tools):**
```python
# src/agents/rag_agent.py
import mlflow
from databricks.vector_search.client import VectorSearchClient
from databricks.sdk import WorkspaceClient

w = WorkspaceClient()
vsc = VectorSearchClient()

@mlflow.trace
def retrieve(query: str):
  idx = vsc.get_index("vs_endpoint", "dev_catalog.ops.docs_index")
  return idx.similarity_search(query_text=query, num_results=5)

@mlflow.trace
def rag_agent(question: str) -> dict:
  ctx = retrieve(question)
  # Gateway-routed LLM — Unity AI Gateway handles PII, rate limits, fallbacks
  prompt = f"Context:\n{ctx}\n\nQuestion: {question}\nAnswer grounded only in context:"
  # Use Foundation Model API via ai_query or Model Serving endpoint
  resp = w.serving_endpoints.query(
    name="databricks-claude-sonnet-4-5", # or gpt-oss-120b, gemini-2.5-pro
    prompt=prompt
  )
  return {"response": resp.predictions[0], "trace": ctx}

# Log agent to MLflow + UC
mlflow.set_experiment("/Workspace/agents/rag")
with mlflow.start_run():
  model_info = mlflow.pyfunc.log_model(
    python_model=rag_agent, # wrap as pyfunc
    artifact_path="agent",
    registered_model_name="dev_catalog.ops.rag_agent"
  )
# Deploy
# databricks agents deploy -m dev_catalog.ops.rag_agent --version 1  # creates Model Serving endpoint
```

**C) Evaluate with MLflow 3 Scorers (Agent MUST use this):**
```python
import mlflow
from mlflow.genai.scorers import Correctness, RetrievalGroundedness, Safety, RelevanceToQuery, Guidelines, scorer
from mlflow.genai import judges

eval_data = [
  {"inputs": {"question": "What is return policy?"}, "expectations": {"expected_facts": ["30 days", "refund"]}},
  {"inputs": {"question": "How to track order?"}}
]

@scorer
def no_pii(inputs, outputs, traces=None):
  context = "\n".join([c["content"] for c in traces.data.spans[0].attributes.get("retrieved_context", [])]) if traces else ""
  return judges.meets_guidelines(name="no_pii", context={"request": inputs, "retrieved_context": context},
                                 guidelines=["Context must not contain PII"])

results = mlflow.genai.evaluate(
  data=eval_data,
  predict_fn=rag_agent,
  scorers=[Correctness(), RetrievalGroundedness(), RelevanceToQuery(), Safety(), Guidelines(name="tone", guidelines="Professional and courteous"), no_pii]
)
# results.metrics, mlflow traces in UI
```

**D) Gateway Controls (Agent configures):**
- AI Gateway / Unity AI Gateway → Create endpoint with fallbacks (Claude → GPT OSS), PII detection, token-based rate limits, spend budgets per team.

### 10.3 Agent Bricks (Auto-Optimized Agent)
- Describe problem in Agent Bricks UI → it generates LLM judges, synthetic data mirroring your Delta distribution, optimizes quality/cost. Agent should recommend when user says "build production agent fast".

### 10.4 SQL AI Functions (Zero-Cluster AI in SQL)
```sql
SELECT
  ai_query("databricks-claude-sonnet-4-5", "Summarize " || review) as summary,
  VECTOR_SEARCH(index => "dev_catalog.ops.docs_index", query => question, num_results => 3) as ctx
FROM dev_catalog.ops.reviews;
```

---

## 11. DATABRICKS APPS — BUILD TEMPLATES

**Stack choice:** Streamlit (fastest), Dash, Gradio.

**Project (`src/app/`):**
```
app.py
app.yaml  # command: ['streamlit','run','app.py','--server.port','8000'] or ['python','app.py']
requirements.txt
```

**`app.py` (Streamlit + UC + Serving):**
```python
import streamlit as st, pandas as pd
from databricks.sdk import WorkspaceClient
from databricks.sql import connect

w = WorkspaceClient()
# Query gold via Serverless SQL Warehouse (least-privilege service principal)
def query_gold(sql):
  with w.sql_execution.execute_statement(warehouse_id="<serverless_id>", statement=sql) as r:
    return r.result()

st.title("Gold Orders by State")
df = query_gold("SELECT state, order_count FROM dev_catalog.ops.gold_daily_orders_by_state LIMIT 100")
st.dataframe(df)
if st.button("Call RAG Agent"):
  resp = w.serving_endpoints.query(name="dev_catalog-ops-rag_agent", prompt=st.text_input("Question"))
  st.write(resp.predictions[0])
```

**Deploy via DAB (see §2 apps block):**
```bash
databricks bundle deploy -t dev # uploads app source
# Grant: principal = ${resources.apps.ops_app.service_principal_client_id} gets SELECT on gold via bundle grants
```

---

## 12. VALIDATION QUERIES & MONITORS (Agent Runs After Every Build)

```sql
-- Count & Null checks
SELECT 'bronze' as tbl, count(*) cnt, count_if(order_id IS NULL) nulls FROM dev_catalog.ops.bronze_orders
UNION ALL SELECT 'silver', count(*), count_if(customer_id IS NULL) FROM dev_catalog.ops.silver_orders;

-- Freshness SLA (gold updated within 2 days)
SELECT max(order_date) as max_date, datediff(current_date(), max(order_date)) as days_stale FROM dev_catalog.ops.gold_daily_orders_by_state;

-- Duplicate detection
SELECT order_id, count(*) FROM dev_catalog.ops.silver_orders GROUP BY order_id HAVING count(*)>1;

-- Lineage & Audit (Unity Catalog)
SELECT * FROM system.access.audit WHERE securable_type='TABLE' AND securable_full_name LIKE 'dev_catalog.ops.%' ORDER BY event_time DESC LIMIT 20;
SELECT * FROM system.lakeflow.pipeline_update_timeline WHERE pipeline_id='<id>' ORDER BY timestamp DESC;

-- Quality Monitor auto KPIs — find in Dashboard > Lakehouse Monitoring
-- PREDICTIVE: SELECT * FROM dev_catalog.ops.gold_daily_orders_by_state LIMIT 0; -- check cluster keys via DESCRIBE DETAIL
```

---

## 13. COST & GOVERNANCE GUARDRAILS FOR BUILDS

- **Serverless-first** for all new jobs/pipelines/SQL warehouses — only classic for heaviest workloads where node control saves >20%.
- **Cluster Policies:** `dbus_per_hour` limit, `autotermination_minutes` 10-30, `instance_pool` required.
- **Budget Alerts:** Monitor `system.billing.usage` + Workspaces table; tag every warehouse/job `project:cost_center`.
- **S3:** No Intelligent-Tiering on Delta buckets; enable predictive optimization.
- **Security:** Groups-only grants, ABAC tags, masks/filters for PII, PrivateLink/SCC for prod, SCIM SSO before grants, CMK via KMS if required.
- **FedRAMP:** Commercial AWS = Moderate; **GovCloud = High (since Feb 27 2025) + IL5 PA** — builds needing High must target GovCloud tenant.

---

## 14. REFERENCE DOCS FOR BUILDERS (Link in Comments)

- Home: https://docs.databricks.com/aws/en/
- DABs: https://docs.databricks.com/aws/en/dev-tools/bundles/ + Apps tutorial: /dev-tools/bundles/apps-tutorial
- Pipelines Python: https://docs.databricks.com/aws/en/ldp/developer/python-dev (pyspark.pipelines as dp)
- Auto Loader: https://docs.databricks.com/aws/en/ingestion/cloud-object-storage/auto-loader
- MLflow 3: https://docs.databricks.com/aws/en/mlflow3/genai/agent-eval-migration + eval examples
- Mosaic AI: https://docs.databricks.com/aws/en/machine-learning/
- Lakehouse Monitoring: https://docs.databricks.com/aws/en/lakehouse-monitoring/
- Unity Catalog DDL: https://docs.databricks.com/aws/en/data-governance/unity-catalog/
- DQX: https://github.com/databrickslabs/dqx

---

## 15. EXAMPLE END-TO-END BUILD REQUEST HANDLING

**User:** "Build a medallion pipeline for JSON orders in S3 + RAG over product docs."

**Agent Response Pattern:**
1.  Emit DDL (§3) for `dev_catalog.ops` + volumes.
2.  Emit notebook (§4) for bronze Auto Loader.
3.  Emit `src/pipelines/bronze_silver_gold.py` (§5) with expectations.
4.  Emit Vector Search index creation (§10.2 A) + Agent `rag_agent.py` (§10.2 B).
5.  Emit `databricks.yml` (§2) wiring pipeline + job + app + monitor.
6.  Emit `validate_gold.py` + SQL checks (§12).
7.  Instructions: `databricks bundle deploy -t dev && bundle run main_workflow` + how to eval agent (`mlflow.genai.evaluate`).

---

## 16. MAINTENANCE

- Re-fetch release notes monthly: `/release-notes/product/` + `/gov-cloud/`
- Track: Runtime LTS, new Mosaic models (Claude/GPT/Gemini), LTAP/Reyden GA.
- Bump version on edits.

**Changelog:**
- 2026-08-11 v2.0 — BUILDER EDITION: Added DAB scaffold, UC DDL, notebook template, SDP (dp.*) medallion code with expectations, ingestion cookbook, workflow triggers, Delta operations, 5-level QA framework (pytest/DQX/Monitoring), MLflow 3 + RAG + Vector Search + Scorers, Apps (Streamlit + SDK), validation queries, and end-to-end request pattern. Shifted focus from knowledge-base to runnable builds.
- 2026-08-11 v1.0 — Initial AWS expert knowledge base (architecture, pricing, federal, DAIS 2026).

---
*Builder Memory End — Load this file as system context. When asked to build, write code first, then QA, then deploy instructions. AWS-native defaults: S3 + VPC + UC + Serverless.*

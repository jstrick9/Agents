# ONR ITSS POC — Databricks (AWS GovCloud / IL5)

Installable Data & Analytics platform for the ONR Code 08 IT Support Services **technical demonstration, Elements 3–7**, plus Databricks Asset Bundle IaC for Element 2.

Built from the repo memory files:

- `AI_Agent_Memory_Files/POC/ONR_ITSS_POC_Agent_Memory.md`
- `AI_Agent_Memory_Files/Databricks/Databricks_AWS_Agent_Memory.md`
- `AI_Agent_Memory_Files/POC/Data Call - TECHNICAL DEMONSTRATION.md`

**Environment:** AWS GovCloud / GovCloud DoD (`us-gov-west-1`), Unity Catalog, serverless-first, mock unclassified data only.

**Not in scope:** a live SSO/MFA portal (Element 1) — covered as architecture narrative in `docs/IL5_ZERO_TRUST.md`.

---

## What you get

| Element | What to install | Live proof |
|---|---|---|
| **2 — IaC** | `databricks.yml` + `resources/*.yml` + CI template + optional Terraform | `bundle deploy`, job/pipeline objects, CI |
| **3 — Ingest / streaming** | Lakeflow pipeline + file-arrival job + seed files | Auto Loader detects a live file drop; expectations drop bad rows; schema evolves (`collaboration_flag`); Kinesis path documented |
| **4 — Governance / catalog** | UC comments, tags, grants, quality MV, lineage queries | Catalog Explorer + `system.access.table_lineage` + `gold_data_quality_scores` |
| **5 — Analytics / ML** | Notebook trains sklearn models, logs to MLflow UC | `gold_predictive_velocity` with forecast, velocity, trend IDs |
| **6 — Dashboard + automation** | Lakeview dashboard **and** Streamlit Databricks App | Search / filter / extract, automated summary, anomaly flags, approval queue |
| **7 — Export / APIs** | Export notebook + OpenAPI | CSV + JSON + Parquet on a UC Volume; Statement Execution; OpenSharing SQL |

Strategic prompts **a–e** have narration cards in `docs/STRATEGIC_PROMPTS.md` and are woven into the notebooks.

---

## Quick start

```bash
cd onr_itss_poc
# 1. Set workspace host in databricks.yml
# 2. databricks auth login --host https://<deployment>.cloud.databricks.us
databricks bundle validate -t dev
databricks bundle deploy -t dev --var="warehouse_id=<sql-warehouse-id>"
databricks bundle run bootstrap_and_seed -t dev
databricks bundle run onr_medallion -t dev
databricks bundle run nightly_validate -t dev
```

Full steps, GovCloud notes, and troubleshooting: **[INSTALL.md](INSTALL.md)**.

Local unit tests (no Databricks):

```bash
pytest tests/ -q
```

---

## Repo map

```
onr_itss_poc/
  databricks.yml                 # DAB root (Element 2)
  INSTALL.md
  resources/                     # pipeline, jobs, app, dashboard, volumes, monitor
  src/
    setup/                       # UC bootstrap, mock seed, gold grants
    pipelines/bronze_silver_gold.py
    notebooks/03–07_element_*.py # demonstration sequence
    qa/validate_gold.py
    app/                         # Streamlit Databricks App
    dashboards/onr_executive.lvdash.json
    sql/executive_kpis.sql
    export/openapi_advana.yaml
    common/                      # testable business rules
  data/mock/                     # inspectable copies of seed files
  tests/
  docs/                          # architecture, IL5, DR, legacy, demo script
  infra/terraform/               # optional account-level IaC
  infra/ci/databricks-ci.yml     # copy to repo-root .github/workflows/
```

---

## Data (deliberately fake)

Identifiers use the `MOCK-ONR-` prefix. Classification tag is `MOCK_UNCLASSIFIED`. There is no CUI, PII, or classified content.

Seeded **quality failures** (so Element 3/4 is not a happy-path slide):

- Null `grant_id`
- Negative `award_amount`
- Negative `expended`
- Extra columns on a second grants file (schema evolution)
- One **lapsed** commercial subscription (`DATA_GAP`) so vendor lifecycle is visible on the executive dashboard

---

## Demonstration

Timed 50-minute script: [`docs/DEMO_SCRIPT.md`](docs/DEMO_SCRIPT.md)

Presenter checklist: [`docs/ELEMENT_CHECKLIST.md`](docs/ELEMENT_CHECKLIST.md)

---

## Design choices (GovCloud / IL5)

- **Volumes, not DBFS mounts**
- **Groups-only** UC grants
- **No `ai_query` foundation-model SQL** (not on the IL5 feature matrix) — summaries are deterministic
- **Serverless-first** with classic Nitro fallback
- **Customer-defined names contain no CUI** (Databricks name-field rule)
- Hosts default to `*.cloud.databricks.us` / `*.cloud.databricks.mil`

See [`docs/IL5_ZERO_TRUST.md`](docs/IL5_ZERO_TRUST.md) and [`docs/ARCHITECTURE.md`](docs/ARCHITECTURE.md).

---

## Evaluation mapping

| Criterion | Where it is visible |
|---|---|
| Technical competence | Key Personnel run notebooks 03–07 and the pipeline Python in a Git folder |
| Completeness | One DAB job `element_demo_sequence` plus five spoken prompts |
| Open architecture | Auto Loader or Kinesis; CSV/JSON/Parquet; OpenAPI; OpenSharing |
| Strategic agility | File-arrival, schema evolution, vendor-gap alerts, approval routing, dual-run with legacy ETL |

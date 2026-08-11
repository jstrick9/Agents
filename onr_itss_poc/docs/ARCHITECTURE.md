# Architecture — ONR ITSS POC on Databricks AWS GovCloud

```
                         ┌─────────────────────────────────────────────┐
                         │  IdP (SSO + MFA)  — Element 1 narrative     │
                         │  SCIM groups: engineers / analysts / execs  │
                         └──────────────────┬──────────────────────────┘
                                            │ PrivateLink
                         ┌──────────────────▼──────────────────────────┐
                         │ Databricks AWS GovCloud DoD  us-gov-west-1  │
                         │ host: https://<dep>.cloud.databricks.mil    │
                         │ compliance security profile ON by default   │
                         └──────────────────┬──────────────────────────┘
                                            │
     ┌──────────────────────────────────────┼──────────────────────────────────────┐
     │                                      │                                      │
     ▼                                      ▼                                      ▼
 landing Volume                    Lakeflow pipeline                      SQL warehouse
 grants/*.jsonl                    bronze_*  (Auto Loader)                Lakeview dashboard
 financial/*.csv                   silver_*  (expectations)               Databricks App
 vendors/*.jsonl                   gold_*    (decision support)           Statement Execution API
     ▲                                      │                                      │
     │ file-arrival job                     │ UC lineage                           │
     │                                      ▼                                      ▼
     │                             gold_predictive_velocity                 Advana / Cloud One
     │                             MLflow UC models                         OpenSharing / Parquet
     │                                      │
     └──────── legacy ETL extracts ─────────┘
               (strangler / zero gap)
```

## Layers

| Layer | Objects | Purpose |
|---|---|---|
| Landing | UC Volume `landing` | File detection. Replaces DBFS mounts. |
| Bronze | `bronze_grants`, `bronze_financial`, `bronze_vendors` | Append-only raw + `_source_file` + `_ingest_ts` |
| Silver | `silver_*` | Typed, deduped, expectations drop invalid rows |
| Gold | `gold_financial_execution`, `gold_executive_kpis`, `gold_anomalies`, `gold_approval_queue`, `gold_vendor_lifecycle`, `gold_data_quality_scores`, `gold_executive_summary`, `gold_predictive_velocity` | Leadership + automation |
| Export | UC Volume `export` | CSV / JSON / Parquet (Element 7) |

## Why this maps to the evaluation criteria

1. **Technical competence** — Key Personnel run live notebooks against live tables and a live repo (`databricks.yml`, pipeline Python).
2. **Completeness** — Elements 3–7 plus IaC (Element 2) execute as one DAB job.
3. **Open architecture** — Auto Loader *or* Kinesis behind the same silver contract; CSV/JSON/Parquet; OpenAPI; OpenSharing.
4. **Strategic agility** — file-arrival automation, schema evolution without ALTER, vendor-gap alerts, approval routing.

## Compute

Serverless-first (Public Preview on GovCloud / DoD as of late 2025). Classic Nitro clusters remain the fallback (`infra/terraform` + cluster policy). Fleet instances are **not** available in GovCloud.

## What we deliberately do not use

| Capability | Why |
|---|---|
| `ai_query` foundation-model SQL | Not in the IL5 feature matrix |
| DBFS mounts | Replaced by UC Volumes |
| Commercial `*.cloud.databricks.com` hosts | Out of the IL5 boundary |
| Real CUI / PII | Data-call prohibition |

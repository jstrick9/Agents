# 50-minute demonstration script — Elements 3–7 (+ IaC)

**Rules (11.2):** live cloud, live repo, no PowerPoint, no heavily edited capture, mock data only, Key Personnel narrate.

**POC workspace:** https://dbc-ae83c2ba-d87c.cloud.databricks.com/?o=7474653232339519  
**Folder:** `/Workspace/Users/joshua.strickland@satsyil.com/onr_itss_poc`

**Prep (day before)**

1. `databricks bundle deploy -t dev && databricks bundle run bootstrap_and_seed -t dev`
2. `databricks bundle run onr_medallion` (or start the pipeline once)
3. `databricks bundle run nightly_validate -t dev` — must print `quality_passed=true`
4. Confirm Lakeview dashboard and App open
5. Leave Catalog Explorer, pipeline UI, and the Git folder on separate tabs

| Clock | Who | What |
|---|---|---|
| 0:00–0:02 | Facilitator only | Introduce Key Personnel. No technical narration. |
| 0:02–0:06 | Chief Enterprise Architect | **Element 2 (IaC).** Open the Git folder. Walk `databricks.yml`, `resources/*.yml`, `.github/workflows`. `bundle deploy` history in the Deployments panel. Optional: `infra/terraform/README.md`. Prompt (c) one sentence: groups, no secrets in Git. |
| 0:06–0:16 | DevSecOps / Data Engineer | **Element 3.** Notebook `03`. Show landing Volume, pipeline source (`cloudFiles`, expectations, `addNewColumns`). Show bronze vs silver drop of bad rows. Point at `collaboration_flag`. **Live drop** the held file, start a pipeline update, query `MOCK-ONR-N00014-26-C-0901`. Narrate Kinesis as the bus path. Prompt (a) + (d). |
| 0:16–0:24 | Enterprise Architect | **Element 4.** Notebook `04` + Catalog Explorer. Tables, tags, comments, `gold_data_quality_scores`, vendor `gap_status`. Lineage tab *and* `system.access.table_lineage`. Prompt (e) + (c). |
| 0:24–0:33 | Data Scientist | **Element 5.** Notebook `05`. Train, log to MLflow UC, write `gold_predictive_velocity`. Walk OVERRUN / UNDER_EXEC as leadership actions. Prompt (b). |
| 0:33–0:41 | Data Scientist + Architect | **Element 6.** Lakeview KPI strip. App: search, filter Code 08, extract CSV, Anomalies, Approvals, Vendors (lapsed feed). Prompt (b) + (e). |
| 0:41–0:47 | DevSecOps | **Element 7.** Notebook `07`. Write CSV+JSON+Parquet. `SHOW CREATE TABLE`. OpenAPI + Statement Execution curl. OpenSharing SQL. `system.access.audit`. Prompt (c) + (a). |
| 0:47–0:50 | Architect | Close any prompt not yet spoken (usually (d) RTO/RPO numbers). Stop recording. |

If a cell is slow, keep talking from the narration cards — do **not** cut to slides.

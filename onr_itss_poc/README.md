# ONR ITSS POC — Databricks demo (Elements 3–7)

One catalog, one pipeline, one notebook, one App, one Lakeview dashboard.

**Workspace:** https://dbc-ae83c2ba-d87c.cloud.databricks.com/?o=7474653232339519  
**Folder:** `/Workspace/Users/joshua.strickland@satsyil.com/onr_itss_poc`  
**Catalog:** `onr_itss_poc.da_platform` (created by bootstrap; you need `CREATE CATALOG`)

Mock unclassified data only.

## Video path (this is the whole demo)

1. Run `src/setup/00_bootstrap` — creates catalog, volumes, mock files
2. Start pipeline **onr-itss-pipeline-dev**
3. Run `src/notebooks/DEMO` top to bottom (Elements 3–7)
4. Open **App** `onr-exec-app-dev` — search / filter / extract
5. Open Lakeview **ONR Executive D and A**

Talk track + prompts a–e: [`docs/DEMO_SCRIPT.md`](docs/DEMO_SCRIPT.md)

## Install

See [`INSTALL.md`](INSTALL.md).

```bash
cd onr_itss_poc
databricks auth login --host https://dbc-ae83c2ba-d87c.cloud.databricks.com
databricks bundle deploy -t dev --var="warehouse_id=<sql-warehouse-id>"
```

Or Git-folder this repo into the workspace path above and run the two notebooks.

## What each element shows

| Element | Where |
|---|---|
| 2 IaC | `databricks.yml` + Git folder |
| 3 Ingest | DEMO: Auto Loader, quality drop, schema evolution, live file drop |
| 4 Catalog | DEMO: tables, health scores, lineage + Catalog Explorer |
| 5 Analytics | DEMO: live sklearn + `gold_predictive_velocity` |
| 6 Dashboard | Lakeview + App (search/filter/extract, anomalies, vendors) |
| 7 Export | DEMO: CSV / JSON / Parquet on the export Volume |

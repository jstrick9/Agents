# ONR ITSS POC — Databricks demo (Elements 3–7)

One catalog, one pipeline, one notebook, one App, one Lakeview dashboard.
**No clusters** — everything runs on your **Serverless SQL warehouse**, a **serverless
Lakeflow pipeline**, and the serverless App runtime.

**Workspace:** https://dbc-ae83c2ba-d87c.cloud.databricks.com/?o=7474653232339519
**Folder:** `/Workspace/Users/joshua.strickland@satsyil.com/onr_itss_poc`
**Catalog:** `onr_itss_poc.da_platform` (created by `00_bootstrap`; you need `CREATE CATALOG`)

Mock unclassified data only (`MOCK-ONR-*`, `MOCK_UNCLASSIFIED`).

## Video path (this is the whole demo)

1. Run `src/setup/00_bootstrap` (SQL) on your **Serverless SQL warehouse** — creates catalog, volumes, approval log
2. Upload the seed files to `landing/` (Catalog Explorer drag & drop, ~2 min)
3. Create the pipeline manually: **Workflows → Lakeflow pipelines → Create** — compute **Serverless**, source `src/pipelines/medallion.py`, target `onr_itss_poc.da_platform` — then **Start**
4. Run `src/notebooks/DEMO` (SQL) top to bottom (Elements 3–7)
5. Open **App** — search / filter / extract / approve
6. Open Lakeview **ONR Executive D and A**

Talk track + prompts a–e: [`docs/DEMO_SCRIPT.md`](docs/DEMO_SCRIPT.md)

## Install

See [`INSTALL.md`](INSTALL.md). Manual-first (you create the pipeline in the UI);
`databricks bundle deploy -t dev` is optional where the CLI exists.

## What each element shows

| Element | Where |
|---|---|
| 2 IaC | `databricks.yml` + `resources/pipelines.yml` + Git folder |
| 3 Ingest | DEMO (SQL): Auto Loader, quality drop, schema evolution, live file drop |
| 4 Catalog | DEMO (SQL): tables, health scores, lineage + Catalog Explorer |
| 5 Analytics | Pipeline: Spark ML trained + registered in UC Models; DEMO queries `gold_predictive_velocity` |
| 6 Dashboard | Lakeview + App (search/filter/extract, anomalies, vendors, approval write-back) |
| 7 Export | App Extract tab: CSV / JSON / Parquet downloads + SQL Statement Execution `curl` |

## Layout

```
src/setup/00_bootstrap.sql      SQL bootstrap (catalog, volumes, approval log) — runs on the warehouse
src/pipelines/medallion.py      Lakeflow pipeline (bronze→silver→gold + Spark ML), Serverless compute
src/notebooks/DEMO.sql          One sequential SQL demo notebook (Elements 3–7)
src/app/                        Streamlit App (Element 6/7)
src/dashboards/                 Lakeview dashboard JSON
data/mock/                      JSONL/CSV seed files, uploaded to the landing Volume
docs/DEMO_SCRIPT.md             50-min film script + prompts a–e
```

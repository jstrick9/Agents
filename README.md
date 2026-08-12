# Agents

ONR ITSS Databricks demo package + the memory files that defined it.

| Path | What |
|---|---|
| [`onr_itss_poc/`](onr_itss_poc/) | **Install this** — one catalog, one SQL medallion, one DEMO notebook, App, Lakeview |
| [`AI_Agent_Memory_Files/`](AI_Agent_Memory_Files/) | Source memory (Elements 3–7 + Databricks builder notes) |

**No clusters, no pipelines to create:** the bootstrap/DEMO notebooks and the medallion
(`src/pipelines/medallion.sql` — streaming tables + materialized views in plain SQL)
run on your Serverless SQL warehouse; the App is a serverless app.

POC workspace: https://dbc-ae83c2ba-d87c.cloud.databricks.com/?o=7474653232339519  
Folder: `/Workspace/Users/joshua.strickland@satsyil.com/onr_itss_poc`

Start: [`onr_itss_poc/INSTALL.md`](onr_itss_poc/INSTALL.md) → [`onr_itss_poc/docs/DEMO_SCRIPT.md`](onr_itss_poc/docs/DEMO_SCRIPT.md)

# 50-minute film script — one SQL notebook + App + Lakeview

**Rules:** live cloud, live Git folder, no PowerPoint, mock data only, Key Personnel narrate.
**No clusters, no pipelines:** notebooks + Lakeview run on the Serverless SQL warehouse;
the medallion is plain SQL (streaming tables + materialized views), Databricks backs it
with serverless-managed Lakeflow.

**Prep:** `00_bootstrap` green (SQL) · seed files uploaded to `landing/` ·
`src/pipelines/medallion` run once (gold views populated) · DEMO + App + Lakeview open in tabs.

| Clock | What |
|---|---|
| 0:00–0:02 | Intro Key Personnel only. |
| 0:02–0:05 | **Element 2.** Git folder + `databricks.yml` + `resources/pipelines.yml` + `src/pipelines/medallion.sql`. The medallion was built by running that SQL notebook — Git is the source of truth; `CREATE OR REFRESH` is the deploy. |
| 0:05–0:16 | **Element 3.** DEMO: `%run` the medallion, `LIST` the landing Volume, bronze vs silver quality drop, the expectation **event log**, `collaboration_flag` schema evolution, then the **live file drop**: drag `data/mock/grants/live_drop_element3.jsonl` into `landing/grants/` in Catalog Explorer → `ALTER STREAMING TABLE ... REFRESH` (one SQL cell) → re-run the `MOCK-ONR-N00014-26-C-0901` query. Narrate Kinesis as the bus path. |
| 0:16–0:23 | **Element 4.** Catalog Explorer + DEMO: tables, health scores, vendor gaps, lineage (`system.access.table_lineage`). Point at the lapsed vendor. |
| 0:23–0:32 | **Element 5.** DEMO: the SQL-native predictive model — `gold_predictive_velocity` re-derives risk from velocity vs the award's flat-line plan (`model_risk_class`) and emits trend ids (ACCEL / DECEL / FLAT). Show lineage to the model's inputs. Walk OVERRUN / UNDER_EXEC as leadership actions; note the production swap to registered UC Models via serverless inference. |
| 0:32–0:42 | **Element 6.** Lakeview KPI strip. App: search, filter Code 08, extract CSV, Anomalies → **Record decision**, Vendors (DATA_GAP). |
| 0:42–0:48 | **Element 7.** App → Extract: download CSV / JSON / Parquet, then the printed Advana / Cloud One `curl` (SQL Statement Execution). |
| 0:48–0:50 | Close remaining prompts. Stop. |

## Prompts (speak while clicking — no slides)

**(a) Legacy.** Existing ETL keeps writing extracts to this landing Volume. Portal stays up. Cutover is per-consumer after gold reconciles. Rollback is Delta time travel.

**(b) Financial.** Gold tracks budgeted / obligated / expended. The SQL-native model adds predicted velocity, risk class, and a trend ID. OVERRUN → reprogram. UNDER_EXEC → prevent lapse.

**(c) Zero Trust / IL5.** This POC is a commercial workspace configured like the IL5 baseline: SSO, groups, Volumes not public buckets, no static export URL. Production is GovCloud DoD (`*.cloud.databricks.mil`).

**(d) DR.** RPO = last Delta commit. RTO = re-run `medallion.sql` — every statement is `CREATE OR REFRESH`, and Auto Loader schema versions live on the `checkpoints` Volume. Annual exercise = deploy this Git folder to a second workspace and run DEMO.

**(e) Vendors.** Subscriptions are a gold table. `DATA_GAP` (the lapsed mock feed) shows on the App so a dark feed cannot silently degrade the forecast.

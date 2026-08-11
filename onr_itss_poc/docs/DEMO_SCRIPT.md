# 50-minute film script — one notebook + App + Lakeview

**Rules:** live cloud, live Git folder, no PowerPoint, mock data only, Key Personnel narrate.

**Prep:** `00_bootstrap` green · pipeline Completed · DEMO widgets = `onr_itss_poc` / `da_platform` · App and Lakeview open in tabs.

| Clock | What |
|---|---|
| 0:00–0:02 | Intro Key Personnel only. |
| 0:02–0:05 | **Element 2.** Git folder + `databricks.yml`. |
| 0:05–0:16 | **Element 3.** DEMO: landing files, bronze vs silver drop, `collaboration_flag`, live file drop, refresh pipeline, query `MOCK-ONR-N00014-26-C-0901`. Narrate Kinesis as the bus path. |
| 0:16–0:23 | **Element 4.** Catalog Explorer + DEMO quality + lineage SQL. Point at lapsed vendor. |
| 0:23–0:32 | **Element 5.** DEMO model cell. Walk OVERRUN / UNDER_EXEC as leadership actions. |
| 0:32–0:42 | **Element 6.** Lakeview KPI strip. App: search, filter Code 08, extract CSV, Anomalies → **Record decision**, Vendors (DATA_GAP). |
| 0:42–0:48 | **Element 7.** DEMO export cell — CSV/JSON/Parquet, then the printed Advana/Cloud One `curl`. |
| 0:48–0:50 | Close remaining prompts. Stop. |

## Prompts (speak while clicking — no slides)

**(a) Legacy.** Existing ETL keeps writing extracts to this landing volume. Portal stays up. Cutover is per-consumer after gold reconciles. Rollback is Delta time travel.

**(b) Financial.** Gold tracks budgeted / obligated / expended. The model adds predicted velocity, risk class, and a trend ID. OVERRUN → reprogram. UNDER_EXEC → prevent lapse.

**(c) Zero Trust / IL5.** This POC is a commercial workspace configured like the IL5 baseline: SSO, groups, Volumes not public buckets, no static export URL. Production is GovCloud DoD (`*.cloud.databricks.mil`).

**(d) DR.** RPO = last Delta commit. RTO = restart the pipeline (checkpoints on the Volume). Annual exercise = deploy this bundle to a second workspace and run DEMO.

**(e) Vendors.** Subscriptions are a gold table. `DATA_GAP` (the lapsed mock feed) shows on the App so a dark feed cannot silently degrade the forecast.

# Install (short) — Serverless-only path, Lakeflow-free medallion

**No clusters anywhere. No pipeline to create.** Everything runs on your
**Serverless SQL warehouse** (streaming tables + materialized views built from
plain SQL) and the **App** (serverless app runtime).

**Host:** https://dbc-ae83c2ba-d87c.cloud.databricks.com/?o=7474653232339519
**Folder:** `/Workspace/Users/joshua.strickland@satsyil.com/onr_itss_poc`
**UC:** `onr_itss_poc.da_platform`

## 1. Get the code into the workspace

Add the Git folder `https://github.com/jstrick9/Agents` (branch `main`)
into your workspace at the folder path above, then open the inner **`onr_itss_poc`** package.

## 2. Bootstrap (SQL notebook — runs on your Serverless SQL warehouse)

Open `src/setup/00_bootstrap`, attach your **Serverless SQL warehouse** in the compute
picker, and **Run all**. It creates (SQL only, admin):

- catalog `onr_itss_poc`, schema `da_platform`
- volumes `landing`, `export`, `checkpoints`
- table `gold_approval_log` (the App writes decisions here)

## 3. Seed the landing files (Catalog Explorer — no compute)

`Catalog` → `onr_itss_poc` → `da_platform` → `landing` → **Add data → Upload files to a volume**:

| Repo file | Upload to |
|---|---|
| `data/mock/grants/batch_001.jsonl` | `landing/grants/` |
| `data/mock/grants/batch_002_schema_evolution.jsonl` | `landing/grants/` |
| `data/mock/financial/fy26_execution.csv` | `landing/financial/` |
| `data/mock/financial/fy26_execution_variant.csv` | `landing/financial/` |
| `data/mock/vendors/subscriptions.jsonl` | `landing/vendors/` |

> Keep `data/mock/grants/live_drop_element3.jsonl` **out** — it is the Element 3
> live file drop, uploaded on camera during the demo.

All files are JSONL/CSV so Auto Loader reads them as-is. Re-run the `LIST` cells in
`00_bootstrap` to confirm.

## 4. Build the medallion (SQL — no pipeline to create)

Open `src/pipelines/medallion`, attach your **Serverless SQL warehouse**, and
**Run all** — or just run `DEMO`, whose first cells do it for you via `%run`.
Every statement is `CREATE OR REFRESH`, so it is idempotent: re-running is the
deploy, the refresh, and the DR runbook.

Confirmations:

- `bronze_*` / `silver_*` streaming tables and `gold_financial_execution`,
  `gold_predictive_velocity`, `gold_anomalies`, … materialized views exist in
  Catalog Explorer
- `event_log('onr_itss_poc.da_platform.silver_grants')` shows the quality
  expectations dropping bad grant ids / negative awards
- Catalog Explorer shows the **serverless-managed** Lakeflow pipeline behind the
  views — that is Databricks' plumbing; there is nothing for you to create, start,
  or update

> SQL warehouse note: use your **Serverless** warehouse (or a **Pro** warehouse if
> serverless is disabled in your workspace). Streaming tables and materialized
> views are not available on Classic SQL warehouses.

## 5. Demo (SQL notebook — runs on your Serverless SQL warehouse)

Open `src/notebooks/DEMO`, attach your **Serverless SQL warehouse**, and run it top to
bottom. It is one sequential script — Elements 3–7 plus the prompts. (The Element 3
live drop is a Catalog Explorer drag-and-drop, then one
`ALTER STREAMING TABLE ... REFRESH` cell.)

## 6. App (once, UI)

**New → App → Streamlit**, source = `src/app`. Env:

- `ONR_CATALOG` = `onr_itss_poc`
- `ONR_SCHEMA` = `da_platform`
- attach your SQL warehouse (`DATABRICKS_WAREHOUSE_ID` is injected)

Grant the App **CAN_MODIFY** on `onr_itss_poc.da_platform.gold_approval_log` so the
Anomalies **Approve/Reject** write-back works. Open the App URL.

## 7. Lakeview (once, UI)

Import `src/dashboards/onr_executive.lvdash.json` (or New dashboard and bind the gold
tables). Set parameters `catalog=onr_itss_poc`, `schema=da_platform`, attach any SQL
warehouse (serverless is fine).

## 8. Film

Follow `docs/DEMO_SCRIPT.md`. One notebook, then App, then Lakeview. Prompts a–e are on
the one-page narration card.

---

### Optional: `databricks bundle deploy`

Where the CLI is available, `databricks.yml` + `resources/pipelines.yml` sync and
validate the Git folder (`databricks bundle deploy -t dev`). `resources/pipelines.yml`
is **intentionally empty** — the medallion is Lakeflow-free, so the bundle declares no
pipeline. This repo treats the Git folder as the source of truth; the manual UI path
above just points the notebooks at those same files.

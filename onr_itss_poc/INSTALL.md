# Install (short) — Serverless-only path

**No clusters anywhere.** Everything runs on your **Serverless SQL warehouse**, a
**serverless Lakeflow pipeline**, and the **App** (serverless app runtime).

**Host:** https://dbc-ae83c2ba-d87c.cloud.databricks.com/?o=7474653232339519
**Folder:** `/Workspace/Users/joshua.strickland@satsyil.com/onr_itss_poc`
**UC:** `onr_itss_poc.da_platform`

## 1. Get the code into the workspace

Add the Git folder `https://github.com/jstrick9/Agents` (branch `arena/019ff225-agents`)
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

## 4. Create the pipeline manually (Serverless compute)

Workflows → **Lakeflow pipelines** → **Create pipeline**:

| Setting | Value |
|---|---|
| Name | `onr-itss-pipeline-dev` |
| Product edition | Advanced |
| Compute | **Serverless** (checked — no cluster) |
| Library → Add notebook | browse the Git folder → `src/pipelines/medallion.py` |
| Target catalog | `onr_itss_poc` |
| Target schema | `da_platform` |
| Configuration | `onr.catalog` = `onr_itss_poc`, `onr.schema` = `da_platform` |

Create, then **Start** and wait for **Completed**. Confirmations:

- `gold_financial_execution`, `gold_predictive_velocity`, `gold_anomalies`, … exist in Catalog Explorer
- Expectations tab shows dropped bad grant ids / negative awards
- **Models** → `onr_itss_poc.da_platform.onr_execution_risk` is registered (Element 5)

> If **Serverless** is greyed out for pipelines, ask the workspace admin to enable
> serverless compute for Lakeflow pipelines — no cluster creation is needed either way.

## 5. Demo (SQL notebook — runs on your Serverless SQL warehouse)

Open `src/notebooks/DEMO`, attach your **Serverless SQL warehouse**, and run it top to
bottom. It is one sequential script — Elements 3–7 plus the prompts. (The Element 3 live
drop step is a Catalog Explorer drag-and-drop, then a pipeline update.)

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

Where the CLI is available, `resources/pipelines.yml` + `databricks.yml` deploy the same
pipeline definition (`databricks bundle deploy -t dev`). This repo treats the Git folder
as the source of truth; the manual UI path above just points the pipeline at that same file.

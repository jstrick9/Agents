# Install (short)

**Host:** https://dbc-ae83c2ba-d87c.cloud.databricks.com/?o=7474653232339519  
**Folder:** `/Workspace/Users/joshua.strickland@satsyil.com/onr_itss_poc`  
**UC:** `onr_itss_poc.da_platform`

## 1. Get the code in the folder

Git-folder `https://github.com/jstrick9/Agents` branch `arena/019ff225-agents`, then open the inner `onr_itss_poc/` package.

Or:

```bash
cd onr_itss_poc
databricks auth login --host https://dbc-ae83c2ba-d87c.cloud.databricks.com
databricks bundle deploy -t dev
```

That creates the pipeline + bootstrap job. The App and Lakeview are created once in the UI (below).

## 2. Bootstrap

Open `src/setup/00_bootstrap`. Widgets: `catalog=onr_itss_poc`, `schema=da_platform`. **Run all.**

This creates the catalog (admin required), volumes, and mock landing files.

## 3. Pipeline

Workflows → **onr-itss-pipeline-dev** → Start. Wait until Completed.  
Expectations tab should show dropped bad grant ids / negative awards.

If you imported files without the bundle: New pipeline → source `src/pipelines/medallion.py` → catalog `onr_itss_poc` / schema `da_platform` → config `onr.catalog` / `onr.schema`.

## 4. Demo

Open `src/notebooks/DEMO` → **Run all**.

**App (once):** New → App → Streamlit → source `src/app`.  
Env: `ONR_CATALOG=onr_itss_poc`, `ONR_SCHEMA=da_platform`. Attach a SQL warehouse.  
Grant the App **CAN_MODIFY** on `onr_itss_poc.da_platform.gold_approval_log` so Approve works.

**Lakeview (once):** Import `src/dashboards/onr_executive.lvdash.json` (or New dashboard and bind the gold tables). Set parameters catalog=`onr_itss_poc`, schema=`da_platform`.

## 5. Film

Follow `docs/DEMO_SCRIPT.md`. One notebook, then App, then Lakeview.

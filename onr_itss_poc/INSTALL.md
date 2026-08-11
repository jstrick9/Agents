# Install in the POC workspace

This package is pointed at **your commercial Databricks workspace**:

| | |
|---|---|
| Host | https://dbc-ae83c2ba-d87c.cloud.databricks.com/?o=7474653232339519 |
| Folder | `/Workspace/Users/joshua.strickland@satsyil.com/onr_itss_poc` |
| Browse | https://dbc-ae83c2ba-d87c.cloud.databricks.com/browse/folders/2754726583924232?o=7474653232339519 |

IL5 / GovCloud is the **proposed production** architecture (`databricks.yml` target `govcloud` + `docs/IL5_ZERO_TRUST.md`). This host is the live POC / demo environment.

More detail: [WORKSPACE.md](WORKSPACE.md).

---

## 0. Prerequisites

- Access to the folder above as `joshua.strickland@satsyil.com`
- Use existing UC: catalog `workspace`, schema `default` (no `CREATE CATALOG` needed)
- A SQL warehouse when you are ready for Lakeview / the App (not required for notebooks + pipeline)
- **Mock data only** in `landing/`

---

## 1. Get the code into the folder

### Option A — Git folder (best for Element 2)

1. Open the [project folder](https://dbc-ae83c2ba-d87c.cloud.databricks.com/browse/folders/2754726583924232?o=7474653232339519).
2. **Create → Git folder** / connect repo `https://github.com/jstrick9/Agents`, branch `arena/019ff225-agents`.
3. Open the inner package `onr_itss_poc/` (bundle root with `databricks.yml`).

### Option B — CLI deploy from your laptop

```bash
cd onr_itss_poc
databricks auth login --host https://dbc-ae83c2ba-d87c.cloud.databricks.com
databricks bundle validate -t dev
databricks bundle deploy -t dev
```

`databricks.yml` already sets:

```yaml
workspace:
  host: https://dbc-ae83c2ba-d87c.cloud.databricks.com
  root_path: /Workspace/Users/joshua.strickland@satsyil.com/onr_itss_poc
```

First deploy creates the pipeline + jobs only (no App/Lakeview until a warehouse id is set — see step 7).

### Option C — Upload

Copy `src/`, `data/`, `docs/` into the folder in the UI.

---

## 2. Bootstrap Unity Catalog + seed mock files

In the workspace, run in order:

1. `src/setup/00_uc_bootstrap`  
   - Widgets: `catalog` = `workspace`, `schema` = `default`  
   - If an earlier run left `onr_itss_dev` in the widget, change it — old widget values stick  
   - Leave `apply_group_grants` = `false`
2. `src/setup/01_seed_mock_data` (same catalog/schema widgets)

Or from CLI (after deploy):

```bash
databricks bundle run bootstrap_and_seed -t dev
```

Confirm:

```
/Volumes/<catalog>/<schema>/landing/grants/batch_001.jsonl
/Volumes/<catalog>/<schema>/landing/grants/batch_002_schema_evolution.jsonl
/Volumes/<catalog>/<schema>/landing/financial/fy26_execution.csv
/Volumes/<catalog>/<schema>/landing/vendors/subscriptions.jsonl
/Volumes/<catalog>/<schema>/landing/_demo/live_drop_element3.jsonl
```

---

## 3. Run the medallion pipeline

```bash
databricks bundle run onr_medallion -t dev
```

Or **Workflows → Delta Live Tables / Lakeflow pipelines → `onr-itss-medallion-dev` → Start**.

If you imported files without the bundle, create a pipeline in the UI:

- Source: `src/pipelines/bronze_silver_gold.py`
- Catalog / target schema: the same pair as bootstrap
- Configuration: `onr.catalog` and `onr.schema`
- Serverless if available; otherwise a UC-enabled cluster

Wait until **COMPLETED**. Open **Expectations** — null grant id and negative award should show as dropped.

---

## 4. Walk the demonstration notebooks

Open `src/notebooks/00_demo_index`, then:

1. `03_element_ingest_demo`
2. `04_element_governance_catalog`
3. `05_element_analytics_ml` (needs `scikit-learn` + `mlflow` on the cluster / serverless env)
4. `06_element_dashboard_automation`
5. `07_element_secure_export`

Set the `catalog` / `schema` widgets to match bootstrap.

CLI:

```bash
databricks bundle run element_demo_sequence -t dev
```

---

## 5. QA

```bash
databricks bundle run nightly_validate -t dev
```

Notebook must exit `quality_passed=true`.

---

## 6. Lakeview + App (after you have a warehouse)

1. Copy the SQL warehouse id from **SQL Warehouses**.
2. In `databricks.yml` uncomment:

   ```yaml
   include:
     - resources/*.yml
     - resources/optional/*.yml
   ```

3. Redeploy:

   ```bash
   databricks bundle deploy -t dev --var="warehouse_id=<your-warehouse-id>"
   ```

4. Open **Dashboards → ONR Executive D and A dev** and **Apps → onr-exec-app-dev**.

---

## 7. Local tests (no workspace)

```bash
pip install pytest
pytest tests/ -q
```

---

## Tear down

```bash
databricks bundle destroy -t dev
# DROP SCHEMA main.onr_itss_poc CASCADE;   -- only if you want the POC schema gone
```

Destroy does **not** delete the Git folder.

---

## Troubleshooting

| Symptom | Fix |
|---|---|
| `CREATE CATALOG` / `NO_SUCH_CATALOG onr_itss_dev` | Expected. Set widgets `catalog=workspace` and `schema=default`, then Run all |
| Pipeline cannot see `/Volumes/...` | Bootstrap did not succeed; widgets ≠ pipeline config |
| Groups not found | Leave `apply_group_grants=false` |
| App / dashboard deploy fails | Do not include `resources/optional` until `warehouse_id` is set |
| Serverless not allowed | Attach a UC-enabled classic cluster |
| File-arrival does not fire | Drop the live file into `landing/grants/`, not `landing/_demo/` |
| Git folder shows repo root, not notebooks | Open the inner `onr_itss_poc/` directory |

# Install in a Databricks workspace

Target: **AWS GovCloud** (`*.cloud.databricks.us`) or **GovCloud DoD / IL5** (`*.cloud.databricks.mil`), region `us-gov-west-1`. Commercial workspaces work for a dry run if you change the host, but they are **not** the demonstration boundary.

## 0. Prerequisites

- Databricks CLI ≥ 0.230 (`databricks -v`)
- Workspace admin or metastore privilege to `CREATE CATALOG`
- Account groups (create empty groups if needed):
  - `onr_data_engineers`
  - `onr_analysts`
  - `onr_executives`
- A serverless SQL warehouse (or classic SQL warehouse) — copy its ID
- SSO configured (GovCloud requirement)
- **Mock data only.** Never land CUI, PII, or classified files in `landing/`

## 1. Create the catalog (once, as metastore admin)

DAB can create schemas and volumes but not always the catalog. In a SQL editor:

```sql
CREATE CATALOG IF NOT EXISTS onr_itss_dev
  COMMENT 'ONR ITSS POC — mock unclassified data only. No CUI/PII.';
```

If your metastore requires a managed location, add
`MANAGED LOCATION 's3://<govcloud-uc-bucket>/onr_itss/'`.

## 2. Point the bundle at your workspace

Edit `databricks.yml`:

```yaml
targets:
  dev:
    workspace:
      host: https://<your-deployment>.cloud.databricks.us   # or .mil
```

Edit `resources/dashboards.yml` is already wired. Set the warehouse once:

```bash
# optional: export instead of editing YAML
export DATABRICKS_HOST=https://<your-deployment>.cloud.databricks.us
databricks auth login --host "$DATABRICKS_HOST"
```

Put the warehouse id in the target variables (`warehouse_id`) or:

```bash
databricks bundle deploy -t dev --var="warehouse_id=<your-warehouse-id>"
```

If serverless jobs/pipelines are not enabled in the account console (**Settings → Feature enablement**), switch the pipeline/job `serverless: true` flags to a classic Nitro cluster policy before deploy.

## 3. Deploy

CI template (copy to your repo root if the GitHub `workflows` permission is available):

```bash
mkdir -p ../.github/workflows
cp infra/ci/databricks-ci.yml ../.github/workflows/databricks-ci.yml
```

From this directory:

```bash
databricks bundle validate -t dev
databricks bundle deploy -t dev
```

This creates / updates:

- UC schema + volumes (landing, export, checkpoints)
- Lakeflow pipeline `onr-itss-medallion-dev`
- Jobs: bootstrap/seed, file-arrival ingest, element demo sequence, nightly QA
- Databricks App `onr-exec-app-dev`
- Lakeview dashboard *ONR Executive D and A dev*
- Lakehouse monitor on `gold_financial_execution`

## 4. Bootstrap catalog + seed mock files

If the catalog does not exist, run as metastore admin:

```bash
databricks bundle run bootstrap_and_seed -t dev
```

Or in the workspace: run `src/setup/00_uc_bootstrap.sql` then `src/setup/01_seed_mock_data`.

Confirm files:

```
/Volumes/onr_itss_dev/da_platform/landing/grants/batch_001.jsonl
/Volumes/onr_itss_dev/da_platform/landing/grants/batch_002_schema_evolution.jsonl
/Volumes/onr_itss_dev/da_platform/landing/financial/fy26_execution.csv
/Volumes/onr_itss_dev/da_platform/landing/vendors/subscriptions.jsonl
/Volumes/onr_itss_dev/da_platform/landing/_demo/live_drop_element3.jsonl
```

## 5. Run the medallion pipeline

```bash
databricks bundle run onr_medallion -t dev
```

Wait until the update is `COMPLETED`. Open the pipeline **Expectations** tab — you should see dropped rows for the seeded null grant id and negative award.

## 6. Grants for consumers + QA

In a notebook, run `src/setup/02_apply_governance.py`, then:

```bash
databricks bundle run nightly_validate -t dev
```

The notebook must exit `quality_passed=true`.

## 7. Open the leadership surfaces

- **Lakeview:** workspace → Dashboards → *ONR Executive D and A dev*
  Set the `catalog` / `schema` parameters if prompted.
- **App:** workspace → Apps → *onr-exec-app-dev*
  First start may take a few minutes. Grant the app service principal is already declared in `resources/apps.yml`.

If the dashboard JSON needs a visual tweak after import, bind the datasets in the UI and `databricks bundle generate dashboard` to persist.

## 8. Dry-run the demonstration notebooks

```bash
databricks bundle run element_demo_sequence -t dev
```

Or open in order:

1. `src/notebooks/03_element_ingest_demo`
2. `src/notebooks/04_element_governance_catalog`
3. `src/notebooks/05_element_analytics_ml` (needs `scikit-learn`, `mlflow` — declared on the job env)
4. `src/notebooks/06_element_dashboard_automation`
5. `src/notebooks/07_element_secure_export`

## 9. Local tests (no workspace required)

```bash
pip install pytest
pytest tests/ -q
```

## 9. Tear down a dev deploy

```bash
databricks bundle destroy -t dev
# Catalog data remains. To drop:
# DROP CATALOG onr_itss_dev CASCADE;
```

## Troubleshooting

| Symptom | Fix |
|---|---|
| Pipeline cannot see `/Volumes/...` | Bootstrap did not run, or catalog/schema widgets do not match DAB vars |
| `collaboration_flag` missing | Batch 002 not landed, or pipeline not updated after seed |
| App cannot query | Set `warehouse_id`, redeploy; confirm gold tables exist |
| Serverless not allowed | Enable preview or attach a Nitro classic cluster |
| `CREATE SHARE` fails | Expected without metastore privilege — narrate the SQL anyway |
| Groups not found | Create the three account groups, rerun bootstrap |
| File-arrival does not fire | Confirm the trigger URL is `/Volumes/<catalog>/<schema>/landing/grants` and the live drop was copied *into* that folder, not `_demo/` |

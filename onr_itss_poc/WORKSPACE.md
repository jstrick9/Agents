# This POC workspace

| Field | Value |
|---|---|
| Host | `https://dbc-ae83c2ba-d87c.cloud.databricks.com` |
| Org id (`o=`) | `7474653232339519` |
| Open workspace | https://dbc-ae83c2ba-d87c.cloud.databricks.com/?o=7474653232339519 |
| Project folder | `/Workspace/Users/joshua.strickland@satsyil.com/onr_itss_poc` |
| Folder id | `2754726583924232` |
| Browse folder | https://dbc-ae83c2ba-d87c.cloud.databricks.com/browse/folders/2754726583924232?o=7474653232339519 |
| Owner | `joshua.strickland@satsyil.com` |
| Boundary | Commercial AWS Databricks (general host). IL5/GovCloud is the **proposed production** story, not this workspace. |
| Default UC | `workspace.default` (existing; this metastore does not allow `CREATE CATALOG`) |
| Metastore | `metastore_aws_us_east_2` |

`databricks.yml` target `dev` is already pointed at this host and folder.

## Fastest way to get code into the folder

### Option A — Git folder (preferred for Element 2)

In the workspace folder above:

1. **Create → Git folder** (or **Repos**) using `https://github.com/jstrick9/Agents`
2. Branch: `arena/019ff225-agents`
3. After clone, open the inner package:  
   `/Workspace/Users/joshua.strickland@satsyil.com/onr_itss_poc/onr_itss_poc`
4. Run notebooks from `src/setup` then `src/notebooks`

### Option B — Asset Bundle deploy (CLI on your laptop)

```bash
cd onr_itss_poc
databricks auth login --host https://dbc-ae83c2ba-d87c.cloud.databricks.com
databricks bundle validate -t dev
databricks bundle deploy -t dev
```

Source lands under that folder (`files/src/...` after deploy). Then run `bootstrap_and_seed` and the medallion pipeline.

### Option C — Manual import

Upload `src/`, `data/`, `docs/` into the folder in the UI. Run `src/setup/00_uc_bootstrap` then `01_seed_mock_data`.

## After files are there

1. Run `src/setup/00_uc_bootstrap` (creates schema + volumes; skips missing groups)
2. Run `src/setup/01_seed_mock_data`
3. Create / start the Lakeflow pipeline from `src/pipelines/bronze_silver_gold.py` (or `databricks bundle run onr_medallion -t dev`)
4. Walk `src/notebooks/00_demo_index` → 03 → 07
5. When you have a SQL warehouse id, include `resources/optional/*.yml` and redeploy for Lakeview + App

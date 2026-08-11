# Optional DAB resources (Lakeview, App, monitor)

Included only after a SQL warehouse exists. In `databricks.yml`:

```yaml
include:
  - resources/*.yml
  - resources/optional/*.yml
```

```bash
databricks bundle deploy -t dev --var="warehouse_id=<sql-warehouse-id>"
```

Do not include this folder on the first deploy to `dbc-ae83c2ba-d87c` — an empty `warehouse_id` will fail App and dashboard creation.

# Optional Terraform (Element 2 companion)

Databricks Asset Bundles (`databricks.yml`) are the **primary IaC** for jobs, pipelines, apps, dashboards, and UC volumes. Use this Terraform only for account-level objects that DABs do not own:

- Metastore assignment
- Storage credential + external location (if gold must live in a customer-managed GovCloud bucket)
- Customer-managed KMS CMK
- PrivateLink VPC endpoints (front-end + back-end)
- Cluster policy (Nitro-only, no public IP)

## GovCloud account IDs (from Databricks docs)

| Environment | Databricks AWS account ID | Workspace host |
|---|---|---|
| AWS GovCloud | `044793339203` | `https://<deployment>.cloud.databricks.us` |
| AWS GovCloud DoD (IL5) | `170661010020` | `https://<deployment>.cloud.databricks.mil` |

Region: `us-gov-west-1`.

## Suggested flow

1. Terraform applies network + metastore + credential (one-time, account admin).
2. `databricks bundle deploy -t dev` applies this POC.
3. CI (`databricks bundle validate` + `pytest`) gates every change.

Do **not** put CUI, real program names, or classified strings in Terraform state keys, workspace names, or tags.

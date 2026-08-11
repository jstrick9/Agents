# IL5 / Zero Trust notes (Strategic Prompt c)

This POC is **configured for IL5-equivalent security baselines** using sanitized mock data. It is not itself an ATO package.

## Boundary

- **Platform:** Databricks for AWS GovCloud DoD, region `us-gov-west-1`.
- **Authorization:** DoD IL5 PA on FedRAMP High (announced Nov 2024; see Databricks Trust Center).
- **Network:** NIPRNet-connected. PrivateLink required for front-end and back-end (GovCloud requirement).
- **Hosts:** `https://<deployment>.cloud.databricks.mil` (DoD) or `https://<deployment>.cloud.databricks.us` (GovCloud).
- **Apps:** `*.aws-dod.databricksapps.mil` / `*.aws-gov.databricksapps.us`.
- **Ops:** US persons on US soil. Support only via `https://help.databricks.us/s/`.

## Controls implemented in this bundle

| Zero Trust principle | How it shows up |
|---|---|
| Verify explicitly | SSO + MFA at the IdP (Element 1 narrative). App and warehouse use the caller identity, not a shared password. |
| Least privilege | Groups only. Engineers write landing; executives `SELECT` gold; no bronze write for analysts. Column mask UDF stub. |
| Assume breach / micro-seg | Pipeline, warehouse, and App are separate principals. Volumes are not public. No DBFS. |
| Continuous authorization | Revoking a SCIM group drops Catalog + Volume + App access on the next request. No static export URL. |
| Continuous compliance | Compliance security profile is **on by default** in GovCloud (Nitro, hardened image, enhanced monitoring, automatic cluster update). |

## Name-field rule (you own this)

Databricks docs: customer-defined fields (workspace names, job names, tags, Git URLs) may be processed outside the compliance boundary. **This bundle uses only mock-safe names** (`onr_itss_poc`, `mock-unclassified`, `MOCK-ONR-...`). Never put CUI in those fields.

## Feature availability we respected

From the IL5 `us-gov-west-1` matrix (docs, Jul 2026):

Available and used: Classic compute, Databricks Apps, Lakeflow Jobs, Lakeflow Pipelines, Lakehouse Monitoring, MLflow, Custom Model Serving, Serverless jobs/pipelines/SQL (preview/GA per SKU), Predictive Optimization, OpenSharing, Vector Search Standard.

**Not used:** `ai_query` Foundation Model SQL functions (not listed for IL5). Automated summaries are deterministic templates.

## Ingestion / export security

- Landing writes require `WRITE VOLUME`. Auto Loader does not bypass UC.
- Export writes to `export` Volume. Readers need `READ VOLUME`.
- Statement Execution API is the Advana/Cloud One path — bearer token, warehouse-scoped.
- Audit: `system.access.audit` (available on GovCloud).

## Micro-segmentation sketch (narrate)

```
VPC-A ingest   : landing Volume + file-arrival job SP
VPC-B transform: pipeline SP  (read landing, write silver/gold)
VPC-C serve    : warehouse + App SP (read gold only)
VPC-D share    : OpenSharing recipient (Advana) — selected gold tables
```

Security groups / PrivateLink plus UC privileges implement the same idea even when serverless is used (serverless network policies are GA on GovCloud as of Mar 2026).

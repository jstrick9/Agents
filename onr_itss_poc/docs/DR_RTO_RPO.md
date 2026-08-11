# Disaster Recovery, Resilience, Failover (Strategic Prompt d)

## Targets (demonstration / proposed production)

| Tier | RPO | RTO | Pattern |
|---|---|---|---|
| Landing files | 0 (object store is the source of truth) | 15 min | S3 CRR to a second GovCloud AZ/region bucket; Auto Loader resumes from checkpoint |
| Bronze / silver / gold | 15 min (last successful pipeline commit) | 30 min | Delta + UC managed storage, predictive optimization, second workspace |
| Lakeview / App | n/a (stateless on gold) | 15 min | Redeploy DAB `bundle deploy -t prod` in failover workspace; second SQL warehouse |
| MLflow models | last registered version | 30 min | UC model registry replicates with the metastore backup |
| Export Volume | last export run | 15 min | Re-run notebook 07 against gold |

These are **proposed** command-aligned targets, not a contractual SLA. Tune after the first tabletop.

## High-availability patterns in this design

- **Serverless jobs / pipelines / SQL** — Databricks-managed capacity, no single classic cluster to lose.
- **Delta commits** are atomic; a failed pipeline update does not publish a half-written gold MV.
- **Checkpoints** live on the `checkpoints` Volume, not local disk.
- **File-arrival** simply re-fires; duplicate files are dedupliced in silver (`dropDuplicates`).
- **Classic fallback:** Nitro instances, no public IP, autotermination 15 min, in a second AZ.

## Streaming failover (Element 3 narrative)

Auto Loader (this demo): RPO = last landed file. Restart the pipeline; it continues from the schema/checkpoint location.

Kinesis (documented path): RPO = shard iterator / checkpoint in the same Volume. Rebuild the job in the failover workspace pointing at the replica stream (or the same stream if it survived).

## Non-disruptive annual DR exercise

1. `databricks bundle deploy -t prod` to the **failover** workspace (already a CI target).
2. Seed mock (or anonymized) landing files — **never** production CUI in the exercise account if the exercise is unclassified.
3. `databricks bundle run file_arrival_ingest -t prod` and `nightly_validate`.
4. Open Lakeview + App; execute notebook 07 export from the failover catalog.
5. Record actual RTO (wall clock from “declare failover” to green `validate_gold`).
6. Tear down with `databricks bundle destroy -t prod` in the exercise workspace.

No production pipeline is paused. That is the non-disruptive part.

## What the presenter should say

> “RPO is a Delta commit, not a nightly tape. RTO is a DAB deploy plus a pipeline start. We rehearse it annually in a second GovCloud workspace without touching the production catalog.”

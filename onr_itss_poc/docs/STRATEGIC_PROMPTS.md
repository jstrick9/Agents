# Strategic prompts (11.4) — narration cards

Key Personnel must verbally address all five prompts inside the 50-minute recording. Use these cards; do not open a slide deck.

## (a) Sustainment of the legacy footprint

**When:** Element 3 landing + Element 7 export.

> The current D&A Portal, its reports, databases, and ETL stay online. We register them in Unity Catalog as external tables and add a single landing-volume terminal to the existing ETL. Auto Loader is additive. Cutover is per-consumer after `validate_gold` reconciles. Rollback is Delta time travel plus the original connection string. Zero planned service gap.

Details: `docs/LEGACY_SUSTAINMENT.md`.

## (b) Financial & budgetary analytical integration

**When:** Element 5 model + Element 6 KPI strip.

> Gold financial execution tracks budgeted vs obligated vs expended by award. The live model produces predicted velocity, months-to-exhaustion, risk class, and a stable trend ID. Leadership uses OVERRUN to reprogram, UNDER_EXEC to prevent lapse, and velocity as a budget-formulation input. The App is the non-technical surface.

## (c) Zero Trust & IL4/IL5 baseline

**When:** Element 4 grants + Element 7 export. Point at GovCloud host.

> This workspace is Databricks on AWS GovCloud DoD, IL5 PA, compliance security profile on by default, Nitro, PrivateLink, US-persons operations. Identities come from the IdP with MFA. Authorization is group-based and continuous — there is no static export link. Ingest, transform, serve, and share are separate principals. Automated summaries do not call commercial foundation-model endpoints.

Details: `docs/IL5_ZERO_TRUST.md`.

## (d) Disaster recovery, resilience, failover

**When:** Element 3 checkpoints.

> RPO is the last Delta commit (about 15 minutes). RTO is a DAB deploy plus pipeline start in the failover workspace (about 30 minutes). Checkpoints live on a Volume, not a disk. We rehearse annually in a second workspace without pausing production.

Details: `docs/DR_RTO_RPO.md`.

## (e) Data vendor and lifecycle management

**When:** Element 4 vendor table + Element 6 Vendors tab.

> Subscriptions are a first-class silver/gold entity: renewal date, seats, usage, and which gold table they feed. `gap_status = DATA_GAP` (see the lapsed mock feed) raises an anomaly, routes an approval to the data-vendor manager, and is visible on the executive dashboard so a dark feed cannot silently degrade a forecast. `validate_gold` fails closed if a required gold table goes empty.

# Legacy D&A Portal sustainment (Strategic Prompt a)

## Principle

**Strangler fig, not big-bang.** The legacy portal, reports, databases, and ETL keep running until each consumer has cut over. This platform is an additional landing + serving path, not a replacement switch.

## Technical approach

| Legacy asset | Sustainment during modernization | Exit criteria |
|---|---|---|
| D&A Portal application | Remains the system of record for data entry. Read-only UC *external tables* register portal schemas in the same catalog so lineage is unified. | App + Lakeview match portal KPI reconciliation for two consecutive months |
| Existing ETL | Continues to write extracts. A new terminal step (or file copy) lands files on `landing/` Volume. No job is deleted on day one. | Auto Loader + expectations stay green for 30 days |
| Reporting systems | Point at `export/` Volume (CSV/Parquet) or Statement Execution API. Same contract Advana will use. | Report hashes match gold |
| Databases | Stay up. CDC / nightly extract into bronze. Optional `CREATE TABLE ... SET MANAGED` later. | Last consumer decommissioned |

## Zero service gap

- Dual-write: legacy ETL **and** Auto Loader share the landing contract.
- Dual-read: portal reports keep their connection strings; gold is an extra source.
- Reconciliation MV (`gold_data_quality_scores` + `validate_gold`) is the go/no-go for each cutover.
- Rollback is `RESTORE TABLE ... VERSION AS OF` on gold plus DNS/App pointer back to the portal.

## How Elements 3–7 participate

- **3** — new ingest sits *beside* legacy ETL.
- **4** — legacy tables appear in the same UC catalog (tags `source_system = legacy_da_portal_mock`).
- **5** — models train on gold, which is fed by both paths once mapped.
- **6** — App is the modern view; portal remains until users move.
- **7** — portable export is how leftover consumers keep working.

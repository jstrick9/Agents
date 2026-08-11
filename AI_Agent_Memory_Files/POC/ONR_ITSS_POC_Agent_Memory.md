---
file_type: AI_AGENT_MEMORY
title: "ONR ITSS POC Agent Memory — Technical Demonstration Elements 3–7"
source_document: "Data Call - TECHNICAL DEMONSTRATION.md"
context_date: "2026-08-11"
section_target: "11.3 Demonstration Scenario Elements 3, 4, 5, 6, 7"
related_sections_included: "11.1 Purpose | 11.2 Logistics | 7.0 / Sub-Factor 3.1 Evaluation | 11.4 Strategic Prompts (relevant to Elements 3–7)"
agent_use_case: "Knowledge base for an AI agent supporting or evaluating the ONR Code 08 IT Support Services technical demonstration proposal"
---

# ONR ITSS POC Agent Memory — Technical Demonstration Focus: Elements 3–7

> **Agent Note:** This memory file isolates **Section 11.3 Elements 3, 4, 5, 6, and 7** from the full Technical Demonstration (Volume IV, Factor 3). It includes the evaluation framework (7.0 / Sub-Factor 3.1), logistics rules (11.2), strategic prompts (11.4) that relate to these elements, and the scenario actions/focus points reformatted for rapid reference.

---

## 1. Source & Context (Why This Exists)

| Source Field | Content |
|---|---|
| **Document** | Opportunity Data Call — ONR Code 08 IT Support Services |
| **Volume** | Volume IV: Technical Demonstration Content (Factor 3) |
| **Purpose (11.1)** | Allow Government to evaluate functionality, agility, security, usability, and future-readiness of the Offeror's proposed Data and Analytics solution; assess hands-on technical competence of proposed Key Personnel. |
| **Factor Rating** | Factor 3 is the **sole rated non-price factor**; receives adjectival rating: Outstanding / Good / Acceptable / Marginal / Unacceptable. |
| **Baseline Rating** | Merely meeting baseline = **"Acceptable"**. Higher ratings (Good / Outstanding) require strengths or significant strengths. |

---

## 2. Logistics & Constraints (11.2) — Rules That Govern These Elements

> **Agent Reminder:** Any demonstration of Elements 3–7 must comply with these constraints. Violations can result in an **"Unacceptable"** rating.

### (a) Secure Link Submission
- Provide a single-page secure, private, password-protected URL (e.g., Vimeo / YouTube).
- Provide all necessary login credentials.

### (b) Duration
- **Max 50 minutes total** (uninterrupted).
- Must sequentially execute all seven (7) scenario elements **and** address all five (5) strategic prompts within the 50-minute window.

### (c) Environment & Format (Critical for Elements 3–7)
- Must showcase a **live, functioning cloud environment** representative of the proposed solution (configured for security baseline equivalents) and **live code repositories**.
- **Static slide decks (e.g., PowerPoint) are strictly prohibited.**
- **Highly edited screen captures, post-production marketing overlays, or simulated application videos = "Unacceptable" rating.**
- Demonstration and narration must be led primarily by proposed **Key Personnel** (e.g., Chief Enterprise Architect, DevSecOps Engineer, Data Scientist). Corporate business development / sales / non-technical account representatives may facilitate introductions but are **strictly prohibited** from leading, demonstrating, or narrating technical elements.

### (d) Data Constraints (Critical for Element 3 & 4 ingestion)
- Use **sanitized, open-source, or mock data** representing typical command datasets (e.g., mock S&T project registries, financial ERP sheets).
- **Under no circumstances** shall actual CUI, PII, or classified data be uploaded or demonstrated.

---

## 3. Evaluation Criteria (7.0 / Sub-Factor 3.1) — What the Government Is Scoring

> **Agent Note:** Elements 3–7 are evaluated under **Sub-Factor 3.1: Pre-Recorded Technical Demonstration**. Strengths = exceeds a requirement in an advantageous way. Significant strengths = appreciably increases likelihood of successful contract performance, provides exceptional benefit, or greatly reduces risk.

| Criterion | How It Relates to Elements 3–7 |
|---|---|
| **(1) Technical Competence (Key Personnel)** | Key Personnel must show expert-level, hands-on understanding of platform architecture, code bases, and workflows **without** relying on corporate back-office support. **Directly applies** when demonstrating ingestion pipelines (3), governance/catalog (4), analytics/model execution (5), dashboard/workflows (6), and secure export/interoperability (7). |
| **(2) Completeness of Scenario Execution** | Must successfully and seamlessly execute **all 7 scenario elements** (including 3–7) and adequately respond to **all 5 strategic prompts** within the 50-minute window. Missing any element or prompt = incomplete. |
| **(3) Open Architecture and Non-Proprietary Standards** | Evidence of modular, loosely coupled design patterns, standard APIs, and portable data storage models to prevent vendor lock-in and enable interoperability with DoW/DoN platforms. **Directly evaluated in Element 7 (export/API/integration)** and underpins Elements 3–6 (streaming, catalog, analytics, dashboard). |
| **(4) Strategic Alignment and Agility** | Capability of platform and team to rapidly adapt to changing command strategies, evolving technology fields, and workforce automation needs — demonstrated during scenario execution (3–7) and narrated strategic prompt responses. |

---

## 4. Scenario Elements 3, 4, 5, 6, 7 — Reformatted Agent Reference

---

### ELEMENT 3: Automated Ingestion, Data Operations, and Streaming

**Action (What Must Be Done):**
> Ingest a raw, unstructured, or semi-structured sample dataset (such as a mock registry of research grants) into the platform.

**Focus (What Must Be Demonstrated / Explained):**
- **Automated ingestion pipelines (ETL/ELT)** — how they detect incoming files.
- **Automated quality checks** — how the pipeline validates data upon entry.
- **Schema variations** — how variations in structure are handled without manual reconfiguration.
- **Near-real-time data streaming architecture** — explain or demonstrate support (e.g., Kafka, Kinesis, or equivalent).

**Agent Memory Tags:**
- `pipeline_automation`
- `data_ingestion`
- `quality_checks`
- `schema_flexibility`
- `streaming_architecture`
- `near_real_time`
- `mock_data_only`

**Evaluation Link:**
- Technical Competence (1) — hands-on pipeline/code demonstration.
- Open Architecture (3) — streaming standard (Kafka / Kinesis) shows non-proprietary interoperability.
- Strategic Alignment (4) — agility shown by automated detection and quality handling without manual intervention.

---

### ELEMENT 4: Data Governance, Quality, and Cataloging

**Action (What Must Be Done):**
> Navigate to the platform's data management registry, catalog, or dictionary interface.

**Focus (What Must Be Demonstrated / Explained):**
- **Cataloging** — how the ingested dataset is registered/cataloged.
- **Metadata capture** — what metadata is recorded (source, schema, lineage tags, ownership, etc.).
- **Data quality / health scores** — demonstrate how the platform calculates and displays health/quality metrics.
- **End-to-end data lineage visualization** — visually map lineage from raw ingestion through to the visualization tier.

**Agent Memory Tags:**
- `data_governance`
- `catalog`
- `metadata`
- `data_quality_scores`
- `lineage_mapping`
- `end_to_end`
- `visual_lineage`

**Evaluation Link:**
- Technical Competence (1) — hands-on navigation of registry/catalog and lineage tools.
- Open Architecture (3) — portable cataloging and metadata models.
- Strategic Alignment (4) — governance supports evolving command data strategies and automation needs.

---

### ELEMENT 5: Decision-Support Analytics and Modeling

**Action (What Must Be Done):**
> Trigger and execute an analytical routine or statistical/machine learning model against the ingested sample dataset.

**Focus (What Must Be Demonstrated / Explained):**
- **Model execution** — trigger/run the routine live; show that it operates against real (mock/sanitized) data.
- **Output structure** — how model outputs are generated and structured (e.g., forecasting results, predictive velocities, trend IDs).
- **Strategic decision-making value** — explain/demonstrate how these analytics serve as decision-making aids for leadership (not just technical outputs, but actionable strategic insight).

**Agent Memory Tags:**
- `analytics`
- `machine_learning`
- `statistical_modeling`
- `predictive_velocity`
- `trend_id`
- `forecasting`
- `executive_decision_support`
- `structured_output`

**Evaluation Link:**
- Technical Competence (1) — hands-on model execution by Key Personnel; understanding of underlying algorithms/workflows.
- Strategic Alignment (4) — demonstrates adaptability to evolving analytical needs and leadership decision requirements.
- Completeness (2) — must be executed seamlessly within 50-minute sequence.

---

### ELEMENT 6: Unified Dashboard, Visualizations, and Process Automation

**Action (What Must Be Done):**
> Access the graphical user interface (UI) or embedded executive business intelligence (BI) dashboard.

**Focus (What Must Be Demonstrated / Explained):**
- **Non-technical leader usability** — show how a non-technical leader can search, filter, and extract insights without code or backend access.
- **Search / filter / extract** — demonstrate these capabilities live.
- **Process automation** — show automated repetitive workflows (e.g., automated summaries, approval routings, anomaly flagging) that drive efficiency.
- **Dashboard as strategic tool** — emphasize usability for leadership (not just developer-facing metrics).

**Agent Memory Tags:**
- `dashboard`
- `executive_BI`
- `visualization`
- `search_filter_extract`
- `non_technical_user`
- `process_automation`
- `automated_summaries`
- `approval_routing`
- `anomaly_flagging`
- `efficiency_automation`

**Evaluation Link:**
- Technical Competence (1) — Key Personnel must explain architecture behind the dashboard and automation, not delegate to non-technical staff.
- Strategic Alignment (4) — agility and workforce automation needs met by automated summaries/routing/anomaly detection.
- Completeness (2) — must execute smoothly in sequence.

---

### ELEMENT 7: Interoperability, Data Portability, and Secure Export

**Action (What Must Be Done):**
> Execute a secure, bulk data export or extraction of a filtered dataset.

**Focus (What Must Be Demonstrated / Explained):**
- **Non-proprietary format export** — prove compliance with open data standards by exporting in **CSV, JSON, or Parquet**.
- **Schema portability** — explain how schemas remain portable and understandable outside the platform.
- **API support for integration** — demonstrate/explain APIs that support seamless integration with broader enterprise cloud platforms (e.g., **Advana** or **Cloud One**) to prevent vendor lock-in.
- **Security during export** — emphasize that the export process is secure (aligns with Zero Trust / IL4/IL5 baseline expectations from Element 1 and Strategic Prompt c).

**Agent Memory Tags:**
- `interoperability`
- `data_portability`
- `secure_export`
- `bulk_extraction`
- `CSV_json_parquet`
- `open_standards`
- `API_integration`
- `Advana`
- `Cloud_One`
- `vendor_lock_in_prevention`
- `secure_export`

**Evaluation Link:**
- **Open Architecture and Non-Proprietary Standards (3)** — this is the primary evaluation point: evidence of modular, loosely coupled design, standard APIs, portable storage.
- Technical Competence (1) — hands-on secure extraction and explanation of API/integration architecture.
- Strategic Alignment (4) — interoperability with DoW/DoN enterprise platforms.

---

## 5. Related Strategic Prompts (11.4) — Mapped to Elements 3–7

> **Agent Note:** The Offeror's Key Personnel must verbally address all five (5) strategic prompts during the 50-minute recording. They may narrate answers while executing scenario elements or as a dedicated speaking segment. Below are the prompts with explicit links to Elements 3–7.

---

### (a) Sustainment of the Legacy Footprint
> **Prompt:** Detail your exact technical approach for sustaining and operating the current legacy D&A Portal application, reporting systems, databases, and existing ETL pipelines, ensuring zero service degradation or operational gaps during modernization phases.

**Agent Link to Elements 3–7:**
- **Element 3 (Ingestion / ETL):** How does the new automated ingestion pipeline coexist with or gradually replace legacy ETL without service gaps?
- **Element 4 (Governance / Catalog):** How is legacy data governed and cataloged during the transition?
- **Element 7 (Interoperability / Export):** How does data portability ensure legacy systems can consume/export until fully modernized?

**Memory Tag:** `legacy_sustainment` | `ETL_transition` | `zero_service_gap`

---

### (b) Financial & Budgetary Analytical Integration
> **Prompt:** Describe how your platform's analytical modeling techniques (predictive, prescriptive) and your proposed team will support financial execution tracking, budget formulation, and cost optimization for various command resourcing priorities.

**Agent Link to Elements 3–7:**
- **Element 3:** Ingestion of financial ERP / budget sheets (mock data allowed).
- **Element 5:** Predictive / prescriptive modeling applied to budget/financial datasets; forecasting trends in resourcing.
- **Element 6:** Executive BI dashboard showing financial execution tracking and cost optimization insights for leadership.

**Memory Tag:** `financial_analytics` | `budget_tracking` | `cost_optimization` | `predictive_prescriptive`

---

### (c) Zero Trust & Cybersecurity Compliance (IL4/IL5 Baseline)
> **Prompt:** Detail how the proposed application architecture implements micro-segmentation, continuous compliance, and least-privilege boundary configurations within a DoD Impact Level 5 (IL5) hosting environment.

**Agent Link to Elements 3–7:**
- **Element 1 (pre-context):** Authentication / Zero Trust / MFA / least-privilege (sets the baseline for all elements).
- **Element 3:** Ingestion pipeline security within IL5 environment; data detection and quality checks do not bypass security boundaries.
- **Element 4:** Governance and catalog access controls align with least-privilege.
- **Element 7:** Secure bulk export complies with Zero Trust boundaries; API access is continuously authorized, not static.

**Memory Tag:** `zero_trust` | `IL5` | `micro_segmentation` | `continuous_compliance` | `least_privilege`

---

### (d) Disaster Recovery, Resilience, and Failover
> **Prompt:** Explain your approach to business continuity, specifically detailing: Target Recovery Time Objectives (RTO) and Recovery Point Objectives (RPO); High-availability cloud configuration patterns; and your strategy for conducting non-disruptive annual disaster recovery exercises.

**Agent Link to Elements 3–7:**
- **Element 3 (Ingestion / Streaming):** How does near-real-time streaming architecture survive failover? How quickly does ingestion resume (RTO / RPO)?
- **Element 5 (Analytics):** Are analytical routines resilient? Can modeling continue during partial failure?
- **Element 6 (Dashboard / Automation):** Does the BI dashboard and automated routing remain available during failover events?
- **Element 7 (Export):** Is secure export still possible from a secondary / failover environment?

**Memory Tag:** `disaster_recovery` | `RTO` | `RPO` | `high_availability` | `failover` | `non_disruptive_DR_exercise`

---

### (e) Data Vendor and Lifecycle Management
> **Prompt:** Explain your methodology and tooling for tracking commercial data subscriptions, monitoring data-usage licenses, validating data quality compliance, and managing renewals without causing data gaps in critical analytical dashboards.

**Agent Link to Elements 3–7:**
- **Element 3:** Ingestion pipelines must handle data gaps if a vendor subscription lapses; how is the pipeline notified?
- **Element 4:** Catalog must track vendor/subscription metadata; quality scores must flag data gaps caused by missing subscriptions.
- **Element 5:** Analytics must account for data gaps; forecasting accuracy must not silently degrade when source data is missing.
- **Element 6:** Dashboard should alert leadership to data gaps (via anomaly flagging / automated summaries from Element 6 focus).

**Memory Tag:** `vendor_management` | `subscription_tracking` | `license_monitoring` | `renewal_management` | `data_gap_prevention`

---

## 6. Cross-Reference Matrix: Elements ↔ Strategic Prompts ↔ Evaluation Criteria

> **Agent Quick-Lookup:** Use this matrix to see which evaluation criteria and strategic prompts are most tightly coupled to each demonstration element.

| Element | Primary Strategic Prompt Links | Key Evaluation Criteria |
|---|---|---|
| **3 — Ingestion / Streaming** | (a) Legacy ETL transition; (c) Zero Trust / IL5 pipeline security; (d) Streaming resilience / RTO / RPO; (e) Vendor/data gap handling in ingestion | (1) Technical Competence; (3) Open Architecture (streaming standard); (4) Strategic Agility |
| **4 — Governance / Catalog / Quality** | (e) Vendor/subscription tracking in metadata; (a) Legacy data governance; (c) Access controls / least privilege; (d) Catalog resilience during failover | (1) Technical Competence; (3) Open Architecture (catalog / portable metadata); (4) Strategic Agility |
| **5 — Analytics / Modeling** | (b) Financial / predictive / prescriptive modeling; (d) Modeling resilience / failover; (e) Handling missing source data in forecasts | (1) Technical Competence; (2) Completeness; (4) Strategic Alignment |
| **6 — Dashboard / Visualizations / Automation** | (b) Executive financial tracking / budget formulation; (d) Dashboard availability during failover; (e) Data-gap alerting / anomaly flagging; (a) Legacy reporting transition visibility | (1) Technical Competence; (2) Completeness; (4) Strategic Agility |
| **7 — Interoperability / Export / APIs** | (c) Zero Trust API / continuous authorization; (a) Legacy system interoperability until modernized; (d) Export capability from secondary environment; (e) Data portability for vendor changes | **(3) Open Architecture / Non-Proprietary Standards** (primary); (1) Technical Competence; (4) Strategic Agility |

---

## 7. Agent Action Checklist — When Reviewing or Generating Content for These Elements

- [ ] Confirm the demonstration environment is **live cloud** (not simulated / not static slides).
- [ ] Confirm **mock/sanitized data** is used (no CUI / PII / classified data shown in Elements 3–7).
- [ ] Confirm **Key Personnel** (not business development) lead and narrate the technical demonstration.
- [ ] Confirm all actions for Elements 3, 4, 5, 6, 7 are executed **sequentially** within the 50-minute window.
- [ ] Confirm all **5 strategic prompts** (11.4 a–e) are verbally addressed, either during elements or as a dedicated segment.
- [ ] Confirm **non-proprietary standards** are visible: streaming standard (3), catalog/metadata model (4), open API/export formats (7: CSV/JSON/Parquet), and integration with Advana / Cloud One (7).
- [ ] Confirm **security / Zero Trust / IL5** references appear (especially for ingestion, catalog access, secure export, and API integration).
- [ ] Confirm **disaster recovery / resilience** references appear (streaming recovery, dashboard availability, export from failover, catalog resilience).
- [ ] Confirm **financial / budget integration** (b) and **vendor/lifecycle management** (e) links are narrated when relevant (typically through Elements 3, 4, 5, 6).
- [ ] Confirm **legacy sustainment** (a) is addressed, particularly how Elements 3–7 interact with legacy ETL, reporting, and data stores without service gaps.

---

## 8. Quick Definitions — Key Terms from This Memory

| Term / Acronym | Definition for Agent Context |
|---|---|
| **ITSS** | IT Support Services (ONR Code 08) |
| **POC** | Point of Contact |
| **ONR** | Office of Naval Research |
| **PWS** | Performance Work Statement (referenced in 11.3) |
| **IL4 / IL5** | DoD Impact Levels 4 and 5 (security baselines); the demonstration environment is configured for IL5 hosting |
| **CUI** | Controlled Unclassified Information |
| **PII** | Personally Identifiable Information |
| **ETL / ELT** | Extract, Transform, Load / Extract, Load, Transform (data pipeline patterns) |
| **BI** | Business Intelligence (executive dashboard in Element 6) |
| **Advana / Cloud One** | Broader DoD enterprise cloud platforms (interoperability targets in Element 7) |
| **RTO** | Recovery Time Objective (disaster recovery) |
| **RPO** | Recovery Point Objective (disaster recovery) |
| **Zero Trust** | Security model: least-privilege access, continuous authorization, micro-segmentation |

---

*End of Agent Memory — ONR ITSS POC Technical Demonstration: Elements 3–7*

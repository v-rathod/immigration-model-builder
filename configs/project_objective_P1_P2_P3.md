# Immigration Model Builder — Program Objectives (P1 → P2 → P3)
_Last updated: 2026-02-24T16:30:00Z_

This document codifies the end‑to‑end objectives across **P1 (foundations)**, **P2 (current project)**, and **P3 (productization & experiences)**.
Agents should read this before every session to understand **what we are building and why**.

---
## North‑Star Outcome
Build a trustworthy, explainable, and continuously verifiable immigration data & modeling platform that:
- Ingests **all authoritative sources** (PERM, LCA, OEWS, Visa Bulletin, DOS/USCIS/DHS publications, etc.).
- Produces **auditable facts/dims**, **derived metrics**, and **employer/market models** with **≥95%** QA gates.
- Exposes **P3 user experiences** (dashboards, APIs) that sit on top of the curated P1/P2 backbone.

---
## P1 — Foundations (Data plumbing & minimal viability)
**Objective:** Make the raw → curated path reliable, typed, and replayable.

**Key Deliverables**
- Connectors & loaders for core public sources.
- Canonical **facts/dims** with partitioned parquet and presentation tables.
- Basic dedupe, type coercion, and PK guarantees.
- Minimal schema/PK tests.

**Success Criteria**
- End‑to‑end ingestion for initial sources.
- Reproducible builds with deterministic outputs.

---
## P2 — Backbone (Current project)
**Objective:** Elevate from "ingested" to **audited, joined, and model‑ready** with strict quality gates and coverage of all downloaded datasets.

**Curated & Validated (examples)**
- **Core**: `fact_perm`, `fact_lca`, `fact_oews` (+ `salary_benchmarks`), `fact_cutoffs` (Visa Bulletin) and deriveds (`fact_cutoff_trends`, `backlog_estimates`, `category_movement_metrics`, `worksite_geo_metrics`, `soc_demand_metrics`, `processing_times_trends`).
- **Expanded**: DOS **Numerical Limits** → `dim_visa_ceiling`; DOS **Waiting List** → `fact_waiting_list`; **Visa Annual Reports** → `fact_visa_issuance`; **Visa Statistics** → `fact_visa_applications`; **NIV Statistics** → `fact_niv_issuance`; **USCIS Immigration** → `fact_uscis_approvals`; **DHS Yearbook** → `fact_dhs_admissions`; **WARN** → `fact_warn_events`.
- **Stubs (until data available)**: **TRAC FOIA**, **ACS wages** (empty‑schema parquet with documented reasons).
- **Employer modeling**: `employer_features`, `employer_friendliness_scores` (v1 rules), optional ML v2.
- **Priority date forecasting**: `pd_forecasts` (exponential-weighted seasonal model, 56 series × 24 months).

**Quality Gates (hard)**
- **Coverage ≥95%** per dataset (processed / expected files).
- **PK‑unique = 100%** on all tables with declared keys.
- **Referential integrity ≥95%** (country/category/SOC mappings).
- **Value ranges** sane (e.g., backlog months ∈ [0,600]; monotonic percentiles).
- **Test pass‑rate ≥95%** with model‑usage checks.

**Evidence & Auditability**
- **Dataset Coverage Matrix** (downloads → curated mapping, with rows & spans).
- **Model Usage Matrix** (which datasets feed which features/models).
- **Commentary & logs** (transcript/logs/bundle) under `artifacts/metrics/`.

---
## P3 — Productization & Experiences (Future)
**Objective:** Ship robust user experiences & APIs that sit directly on P1/P2 artifacts, with zero heavy compute at runtime.

### User Use Case — Personalized Experience
A user enters personal details:
- Priority date
- Country of chargeability
- Category (EB2, EB3, etc.)
- Employer name
- Job title
- Location (city / state)
- Wage offered
- Years of experience

The app then provides **five personalized panels**:

#### A. Personalized Green Card Forecast
- Expected wait time
- Retrogression probability chart
- Projection for when their PD becomes current

#### B. Employer Insights Panel
- GC friendliness score (EFS)
- Audit risk
- Wage comparison (offered vs OEWS benchmarks)
- Denial trends
- Layoff × risk indicator (WARN overlay)

#### C. Job Market Insights
- Where people in similar roles get sponsored
- Best employers for the user's occupation
- Salary competitiveness analysis

#### D. Actionable Recommendations
- Should they switch employer?
- Should they file EB2 or EB3?
- Whether to expect layoffs to impact sponsorship
- Whether they should start PERM early

#### E. Visual Dashboards
- Bar charts (employer comparisons)
- Line charts (category movement)
- Heat maps (wage geography)
- Risk gauges
- Timeline projections

---

### Top Immigration Dashboards & Charts for P3

#### 1. Visa Bulletin Trend Dashboard (EB2, EB3, Country‑Wise)
**Why:** Shows historical retrogression, forward movement, seasonality, and long‑term backlog trajectory.

| Chart | Description |
|-------|-------------|
| Line chart | Cutoff dates over time (category × country) |
| Heatmap | Retrogression frequency |
| Scatter | Months advanced per month |
| Forecast overlay | Future PD prediction |

**P2 Deliverables:**
- Existing: `fact_cutoffs/`
- Transformations: cutoff_date → numeric queue position; monthly advancement via lag(cutoff_date); 3‑month & 6‑month velocity averages; retrogression count
- Output: `fact_cutoff_trends.parquet`

#### 2. Employer Friendliness Dashboard (EFS)
**Why:** Helps users compare employers by sponsorship quality, wage alignment, and audit/denial risk.

| Chart | Description |
|-------|-------------|
| Employer leaderboard | Top‑N employers by EFS |
| SOC‑specific score table | EFS per employer × SOC |
| Wage competitiveness bubble | Bubble chart (volume, EFS, wage ratio) |
| Audit/denial trend line | Last 5 years |

**P2 Deliverables:**
- Existing: `employer_features.parquet`, `employer_friendliness_scores.parquet`
- Add: monthly employer metrics — approval_rate, filing volume, trailing 12‑month audit_rate
- Output: `employer_monthly_metrics.parquet`

#### 3. EB Category Comparison Dashboard
**Why:** Users compare EB2 vs EB3 movement, volatility, and wait times.

| Chart | Description |
|-------|-------------|
| EB2 vs EB3 movement lines | Side‑by‑side cutoff progression |
| PD wait time distribution | Histogram of wait durations |
| Category volatility bar | Standard deviation of monthly advancement |
| Category Strength Index | Composite movement indicator |

**P2 Deliverables:**
- From `fact_cutoffs`: avg_monthly_advancement, median_advancement, volatility_score, retrogression_events, next_movement_prediction
- Output: `category_movement_metrics.parquet`

#### 4. Geographic Sponsorship Heatmaps (U.S. Map)
**Why:** Shows geographic hotspots for sponsorship, filings, and wage competitiveness.

| Chart | Description |
|-------|-------------|
| State/county heatmap | Filing density |
| City bubble map | Bubble size = filings |
| SOC × area wage map | Competitiveness by region |

**P2 Deliverables:**
- From PERM + LCA: filings_count, approvals_count, avg_wage_offer, competitiveness_ratio, distinct_employers, SOC × area OEWS percentiles
- Output: `worksite_geo_metrics.parquet`

#### 5. Wage Competitiveness Dashboard
**Why:** Users compare their wage to OEWS percentiles and employer benchmarks.

| Chart | Description |
|-------|-------------|
| Boxplot | Offered wages vs OEWS |
| SOC × area percentile curves | P10–P90 bands |
| Employer wage comparison | Cross‑employer wage table |
| Wage trend over time | YoY wage change |

**P2 Deliverables:**
- Existing: wage_ratio_med, wage_ratio_p75
- Need: SOC × area wage benchmark — soc_code, area_code, p10, p25, median, p75, p90, competitiveness_category
- Output: `salary_benchmarks.parquet`

#### 6. SOC‑Demand Dashboard
**Why:** Shows high‑demand SOCs, hiring patterns, and wage premiums.

| Chart | Description |
|-------|-------------|
| Top sponsoring SOCs | Ranked by filing volume |
| SOC backlog movement | Cutoff trend per SOC |
| PERM volumes over time | Filing trend charts |
| SOC wage curve | Wage distribution by SOC |

**P2 Deliverables:**
- From PERM + LCA: filings_count_12m/24m/36m, approval_rate, avg_offered_wage, competitiveness_percentile, SOC‑specific employer leaderboard
- Output: `soc_demand_metrics.parquet`

#### 7. Processing Speed Dashboard (PERM, I‑140, I‑485)
**Why:** Shows case processing velocity and center‑level trends.

| Chart | Description |
|-------|-------------|
| Processing time history | Quarterly trend |
| Backlog estimates | Pending inventory |
| Month‑over‑month movement | Throughput changes |

**P2 Deliverables:**
- Extract fact_processing_times
- Output: `processing_times_trends.parquet`

#### 8. Backlog Visualization Dashboard
**Why:** Shows "how many people are ahead of me?"

| Chart | Description |
|-------|-------------|
| Category backlog trend | Backlog by EB category over time |
| Country backlog trend | Backlog by country over time |
| Backlog‑to‑advancement ratio | "Years to clear" estimate |

**P2 Deliverables:**
- From `fact_cutoffs` + `backlog_estimates`
- Output: `backlog_estimates.parquet`

---

### P3 Features Summary
1. **Backlog Explorer**: trends by category/country; integrates Visa Bulletin, `dim_visa_ceiling`, `fact_waiting_list`, and `backlog_estimates`.
2. **Visa Demand Monitor**: dashboards from `fact_visa_issuance`, `fact_visa_applications`, `fact_niv_issuance` (+ OEWS context) with anomaly alerts.
3. **Employer Insights with WARN Overlay**: EFS v1 (+ ML v2) with SOC/geo drill‑downs; WARN‑based stability overlays; watchlists.
4. **Salary Benchmarks Explorer**: OEWS + (future) ACS wage distributions; SOC/geo filters; monotonic percentile bands.
5. **Processing‑Times Lens**: `processing_times_trends` + USCIS approvals context; backlog correlation.
6. **Data Contracts & Read‑only APIs**: stable read‑only APIs on curated tables/presentations; schema versioning & changelogs.
7. **Ops & QA Center**: live QA dashboards (coverage, RI, PK, tests); regression diff viewer.

**Non‑Goals (initial)**
- Write‑time data entry; mutating ops.
- Heavy model training at request‑time (training is offline; serving is light).

---
## How P1 & P2 power P3 (backbone → experiences)
- **Curated P1/P2** are the **single source of truth** that P3 charts/APIs/models read—no ad‑hoc runtime transforms.
- **P2 gates** ensure reliability: any dataset <95% coverage/RI/tests fails *before* reaching users.
- **Usage Registry** ties features/models back to sources for traceability.
- **Presentation tables & snapshots** prevent drift and ensure consistent UI performance.

---
## Run‑time Guidance for Agents
- Read this file at session start to understand scope and priorities.
- **Do not** lower thresholds below 95%; fix root causes.
- When a source is missing (e.g., TRAC/ACS), use the documented stub and record a clear skip reason.
- Always append coverage, usage, and QA sections to the final report.

---
## Appendix — Canonical Objects & Abbreviations
- **Facts**: event‑like tables (`fact_perm`, `fact_lca`).
- **Dims**: lookups/SCDs (`dim_country`, `dim_visa_ceiling`).
- **Presentation tables**: single‑file or lightly denormalized for UI.
- **EFS**: Employer Friendliness Score.
- **OEWS**: BLS Occupational Employment & Wage Statistics.
- **DOS**: U.S. Department of State; **USCIS**: U.S. Citizenship and Immigration Services; **DHS**: Department of Homeland Security.

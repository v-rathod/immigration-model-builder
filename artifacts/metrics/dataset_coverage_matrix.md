# Dataset Coverage Matrix (P2 vs. Downloads)
_Last generated: 2026-02-23T17:32:02Z_

## Summary
- **Total datasets tracked:** 19
- **Datasets with downloads present:** 19
- **Datasets curated in P2:** 7
- **Gap datasets (downloaded, not yet curated):** 10

## Coverage Matrix

| Dataset | Downloaded? | Files | Size | Curated? | Curated Outputs | Notes |
|---------|-------------|-------|------|----------|-----------------|-------|
| ACS | ✅ | 1 | 146.0 B | ⚠️ GAP | — | Census ACS occupation/wage data — salary benchmark supplement |
| BLS | ✅ | 4 | 23.6 KB | ✅ | `fact_oews (via BLS raw)` (12.2 MB)<br>`salary_benchmarks` (2.4 MB) | BLS raw feeds partially ingested via BLS_OEWS pipeline |
| BLS_OEWS | ✅ | 3 | 76.7 MB | ✅ | `fact_oews (partitioned)` (12.2 MB)<br>`fact_oews (flat)` (10.3 MB)<br>`salary_benchmarks` (2.4 MB)<br>`oews_wages` (5.1 KB) | — |
| Codebooks | ✅ | 3 | 688.0 B | — ref | — | Reference only — schema codebooks for DOL/OEWS files |
| DHS_Yearbook | ✅ | 1 | 32.3 KB | ⚠️ GAP | — | DHS Yearbook of Immigration Statistics — historical baselines |
| DOL_Record_Layouts | ✅ | 15 | 2.4 MB | — ref | — | Reference only — column layout specs for LCA/PERM raw files |
| DOS_Numerical_Limits | ✅ | 1 | 93.4 KB | ⚠️ GAP | — | Annual per-country ceiling data — key for backlog projection |
| DOS_Waiting_List | ✅ | 2 | 224.7 KB | ⚠️ GAP | — | Priority date waiting-list reports — direct backlog source |
| LCA | ✅ | 217 | 9.2 GB | ✅ | `fact_lca (partitioned)` (489.8 MB) | — |
| NIV_Statistics | ✅ | 32 | 10.3 MB | ⚠️ GAP | — | Non-immigrant visa issuance counts by category |
| PERM | ✅ | 47 | 896.3 MB | ✅ | `fact_perm (partitioned)` (59.6 MB)<br>`fact_perm_unique_case` (21.3 MB)<br>`fact_perm_all` (20.7 MB) | — |
| TRAC | ✅ | 0 | 0.0 B | ⚠️ GAP | — | TRAC FOIA records — adjudication timelines and denial rates |
| USCIS_H1B_Employer_Hub | ✅ | 14 | 58.8 MB | ✅ | `employer_features` (5.2 MB)<br>`employer_scores` (3.5 KB)<br>`employer_friendliness_scores` (5.4 MB)<br>`employer_friendliness_scores_ml` (54.1 KB)<br>`employer_monthly_metrics` (2.3 MB)<br>`dim_employer` (12.8 MB) | — |
| USCIS_IMMIGRATION | ✅ | 245 | 6.1 MB | ⚠️ GAP | — | USCIS immigration statistics reports (annual) |
| USCIS_Processing_Times | ✅ | 2 | 104.4 KB | ✅ | `processing_times_trends` (3.2 KB) | processing_times_trends parquet present; not yet joined to employer model |
| Visa_Annual_Reports | ✅ | 273 | 22.5 MB | ⚠️ GAP | — | Visa issuance totals by country/category — complements DOS_Numerical_Limits |
| Visa_Bulletin | ✅ | 168 | 41.6 MB | ✅ | `fact_cutoffs (partitioned)` (1.5 MB)<br>`fact_cutoffs_all` (31.5 KB)<br>`fact_cutoff_trends` (49.2 KB)<br>`backlog_estimates` (18.1 KB)<br>`category_movement_metrics` (47.9 KB)<br>`visa_bulletin` (2.7 KB) | — |
| Visa_Statistics | ✅ | 198 | 81.1 MB | ⚠️ GAP | — | DOS visa applications/refusals — NIV demand signal |
| WARN | ✅ | 2 | 123.1 KB | ⚠️ GAP | — | WARN Act layoff notices — employer-level workforce signal |

## Gap Plan (prioritized)

_Datasets that are downloaded but have no curated P2 output yet._

### 1. DOS_Numerical_Limits — P2.1  (1 files, 93.4 KB)
**Why it matters:** Annual per-country visa ceilings are foundational for backlog projections; already downloaded (15 folders).
**Ingestion idea:** Parse Excel annual-limits files → `dim_visa_ceiling` table; join to `backlog_estimates`.

### 2. DOS_Waiting_List — P2.1  (2 files, 224.7 KB)
**Why it matters:** Waiting-list priority-date data directly extends backlog estimation accuracy.
**Ingestion idea:** Parse DOS waiting-list PDFs/CSVs → `fact_waiting_list` with (country, category, cutoff_date, applicants).

### 3. Visa_Annual_Reports — P2.1  (273 files, 22.5 MB)
**Why it matters:** Yearly visa-issuance totals validate fact_cutoffs demand estimates.
**Ingestion idea:** Parse annual-report Excel tables → `fact_visa_issuance`; aggregate by fy/category/country.

### 4. Visa_Statistics — P2.1  (198 files, 81.1 MB)
**Why it matters:** Application/refusal stats are a leading indicator of EB demand.
**Ingestion idea:** Parse DOS NIV statistics CSVs → `fact_visa_applications`.

### 5. NIV_Statistics — P2.1  (32 files, 10.3 MB)
**Why it matters:** Non-immigrant visa trends inform H-1B/L1 demand signal for employer scoring.
**Ingestion idea:** Parse NIV Excel → `fact_niv_issuance` (visa_class, country, fy, count).

### 6. USCIS_IMMIGRATION — P2.2  (245 files, 6.1 MB)
**Why it matters:** Official USCIS approval/denial statistics cross-validate fact_perm and fact_lca.
**Ingestion idea:** Parse USCIS annual immigration data Excel → `fact_uscis_approvals`.

### 7. DHS_Yearbook — P2.2  (1 files, 32.3 KB)
**Why it matters:** Long historical series (1820+) enables cohort-level backlog trend analysis.
**Ingestion idea:** Ingest DHS Yearbook csvs → `fact_dhs_admissions` (fy, category, country, n_admitted).

### 8. TRAC — P3  (0 files, 0.0 B)
**Why it matters:** TRAC adjudication timelines enrich employer-level risk scoring.
**Ingestion idea:** FOIA-derived data — parse TRAC CSVs → `fact_trac_adjudications`; link to dim_employer.

### 9. WARN — P3  (2 files, 123.1 KB)
**Why it matters:** Layoff notices are a risk signal for employer sponsorship stability.
**Ingestion idea:** Parse WARN state files → `fact_warn_events`; fuzzy-join to dim_employer.

### 10. ACS — P3  (1 files, 146.0 B)
**Why it matters:** ACS occupation wage distributions supplement OEWS benchmarks.
**Ingestion idea:** Parse Census ACS PUMS → `fact_acs_wages`; join on SOC code.


# P2 Meridian — Data Quality Code Audit

> **Generated:** 2026-02-27
> **Scope:** All data correction, data quality, and data enhancement code in the P2 codebase.
> **Goal:** Identify which fixes survive a fresh P1→P2 pipeline run and which are at risk.

---

## Executive Summary

The P2 codebase contains **~36 data quality files** spanning **~8,300 lines of code**. Data quality logic falls into three categories:

| Category | Count | Description |
|----------|-------|-------------|
| **Pipeline-embedded** | ~15 files | Normalization, validation, and dedup logic inside `src/` modules that run every time |
| **Fix scripts (one-shot)** | ~13 files | Scripts prefixed `fix_`, `fix1-4`, `fix_a/b/c` that were run manually to repair artifacts |
| **Metric builders** | ~8 files | Scripts prefixed `make_` that build derived tables with built-in QA guards |

### Verdict

- **~75%** of data quality logic is **pipeline-embedded** (lives in `src/curate/`, `src/features/`, `src/models/`) → **SAFE** on fresh P1 run.
- **~25%** is in **standalone fix scripts** → **AT RISK** unless explicitly re-run after a fresh pipeline execution, OR the fixes have been folded back into the pipeline modules.
- The key risk area is **fix scripts whose logic was NOT backported** into the core pipeline modules.

---

## Table of Contents

1. [Pipeline-Embedded Data Quality (SAFE)](#1-pipeline-embedded-data-quality-safe)
2. [Fix Scripts — One-Shot Repairs (AT RISK)](#2-fix-scripts--one-shot-repairs-at-risk)
3. [Metric Builders with QA Guards (SAFE)](#3-metric-builders-with-qa-guards-safe)
4. [Risk Matrix](#4-risk-matrix)
5. [Recommendations](#5-recommendations)

---

## 1. Pipeline-Embedded Data Quality (SAFE)

These run every time `python3 -m src.curate.run_curate --paths configs/paths.yaml` executes.

### 1.1 `src/curate/lca_loader.py` (564 lines)

| Lines | What it does | Risk | Status |
|-------|-------------|------|--------|
| 57–95 | File discovery with dedup by filename (shorter path wins) | None | **OK** |
| 134–156 | `_normalize_employer_name()` — lowercase, strip punctuation per `employer.yml`, remove legal suffixes, min_len check | None | **OK** |
| 159–161 | `_compute_employer_id()` — SHA1 hash of normalized name | None | **OK** |
| 163–178 | `_normalize_soc()` — strip trailing `.XX`, insert hyphen for 6-digit codes, validate XX-XXXX format | None | **OK** |
| 180–183 | `_normalize_status()` — map via `status_map` from layout YAML | None | **OK** |
| 185–196 | `_parse_fulltime()` — handles inverted iCERT `PART_TIME_1` field (`Y` = part-time, `N` = full-time) | None | **OK** |
| 198–207 | `_parse_wage()` — handles string currency symbols, commas, `$` | None | **OK** |
| 209–222 | `_parse_date()` — tries 5 date formats: `%m/%d/%Y`, `%Y-%m-%d`, etc. | None | **OK** |
| 320–330 | Era detection (iCERT vs FLAG) with `era_boundary` from layout | None | **OK** |
| 375–380 | Dedup within FY by `case_number` (keep first) | None | **OK** |
| 430–438 | Vectorised employer normalization with `min_len` guard | None | **OK** |
| 455–465 | SOC code vectorised: strip `.XX`, insert hyphen for 6-digit, extract XX-XXXX | None | **OK** |
| 480–485 | Visa class normalization: `H1B`→`H-1B` replacement | None | **OK** |
| 500–527 | Wage range parsing: extracts leading number from `"20000 -"` or `"66000 - 70000"` format; fallback `wage_rate_to` extraction | None | **OK** |

### 1.2 `src/curate/build_fact_perm.py` (635 lines)

| Lines | What it does | Risk | Status |
|-------|-------------|------|--------|
| 22–50 | `normalize_employer_name()` — independent impl with employer.yml rules, regex suffix removal using `\b` + suffix + `.?\s*$` | None | **OK** |
| 53–57 | `compute_employer_id()` — SHA1 hash, returns SHA1("UNKNOWN") for empty names | None | **OK** |
| 105–181 | `map_soc_code()`, `map_area_code()`, `map_country()` — exact/fuzzy matching to dimension lookups | None | **OK** |
| 183–195 | `derive_fy()` — from received_date (FY starts Oct 1) | None | **OK** |
| 320–330 | Column name normalization: `strip().upper().replace(' ', '_')` | None | **OK** |
| 335–420 | Multi-schema `col_map` — 4+ naming eras (Legacy FY2009, Legacy FY2013-14, iCERT FY2015-19, FLAG FY2020+) | None | **OK** |
| 470–475 | SOC code vectorized: `.XX` suffix stripping | None | **OK** |
| 530 | `fiscal_year` forced from directory name (not from `received_date`) — **key design decision** | None | **OK** |
| 570–580 | Validation output with null percentages for each field | None | **OK** |

### 1.3 `src/curate/visa_bulletin_loader.py` (462 lines)

| Lines | What it does | Risk | Status |
|-------|-------------|------|--------|
| 11–20 | `MONTH_MAP` — full and abbreviated month forms | None | **OK** |
| 23–31 | `COUNTRY_MAP` — ISO3 normalization for all variant spellings of China, India, Mexico, Philippines, ROW | None | **OK** |
| 35–52 | `COUNTRY_COLUMNS_5/6/7` — handles different DOS format eras (5-col pre-May 2016, 6-col May 2016–Apr 2018, 7-col May 2018–Sep 2021) | None | **OK** |
| 55–63 | `CATEGORY_MAP` — EB category normalization (1st→EB1, 2nd→EB2, etc.) | None | **OK** |
| 100–119 | `parse_date()` — handles `01FEB23`, `C` (current), `U` (unavailable) | None | **OK** |
| 123–140 | `_detect_country_columns()` — dynamically detects 5/6/7 column layout from PDF header text | None | **OK** |
| 300–305 | Dedup within partition: `drop_duplicates(subset=['chart','category','country'], keep='last')` | None | **OK** |

### 1.4 `src/curate/build_fact_oews.py` (454 lines)

| Lines | What it does | Risk | Status |
|-------|-------------|------|--------|
| 80–120 | `parse_wage()` — handles BLS special values (`#`, `*`, `N/A`, empty strings) → `None` | None | **OK** |
| 130–160 | `read_oews_data()` — handles `.zip` (opens inner xlsx) and plain `.xlsx` files | None | **OK** |
| 300–310 | Filters to `I_GROUP='cross-industry'` and `O_GROUP='detailed'` only | None | **OK** |
| 320–335 | SOC format filter: only `XX-XXXX` codes pass | None | **OK** |
| 350–400 | Hourly→annual conversion: `a_mean/a_median` fallback from `h_mean/h_median × 2080` for all percentiles | None | **OK** |
| 430–445 | Duplicate PK detection and warning (does not drop, just warns) | Low | **OK** |

### 1.5 `scripts/build_fact_uscis_approvals.py` (639 lines)

| Lines | What it does | Risk | Status |
|-------|-------------|------|--------|
| 24–27 | `FISCAL_YEAR_SHORT` regex — `fy23`→`FY2023` 2-digit year fix | None | **OK** |
| 53–57 | `extract_form_from_name()` — strips dashes (`I-485`→`I485`) | None | **OK** |
| 62–65 | `_short_year_to_full()` — converts 2-digit year to 4-digit | None | **OK** |
| 82–93 | `extract_fy_from_sheet_data()` — scans first 6 rows for `"Fiscal Year YYYY"` pattern | None | **OK** |
| 103–196 | `parse_i485_wide()` — specialized parser for I-485 wide field-office format; finds `"Total"` row and extracts LAST approval/denial column (Grand Total section). **Critical fix**: I-485 was showing only 9 approvals instead of 1.93M | None | **OK** |
| 233–241 | `safe_int()` — handles `pd.Series` (duplicate column names), NaN, commas, asterisks | None | **OK** |
| 244–270 | `find_header_row()` — metric-weighted scoring (`metric_matches*2 + total_matches`) to prefer metric rows over group labels | None | **OK** |
| 354–360 | `_sum_duplicate_col()` — sums all occurrences of duplicate column names (fixes Initial+Renewal dual column in I-765) | None | **OK** |
| 423–440 | Two-level header fix for category column detection | None | **OK** |
| 454 | `form = re.sub(r"-", "", ...)` — dash stripping in row loop | None | **OK** |
| 485–493 | CSV encoding fallback: tries `utf-8-sig` then `latin-1` | None | **OK** |
| 608–613 | Final dedup on PK `(fiscal_year, form, category)`, `keep="last"` | None | **OK** |

### 1.6 `src/normalize/mappings.py` (248 lines)

| Lines | What it does | Risk | Status |
|-------|-------------|------|--------|
| 19–24 | `_EMPLOYER_SUFFIXES` tuple (corporation, inc, llc, ltd, etc.) | None | **OK** |
| 30–72 | `normalize_employer_name()` — strip→lowercase→punctuation→remove suffixes→collapse whitespace | None | **OK** |
| 101–130 | `normalize_soc_code()` — handles `15-1252.00`, `151252`, `15-125200` | None | **OK** |
| 140–175 | `_COUNTRY_RAW_TO_ISO3` dict — all variant spellings of China, India, Mexico, Philippines, ROW | None | **OK** |
| 192–248 | `_EB_CLASS_MAP` — visa category normalization (`EB-1`→`EB1`, H-1B variants, E-3) | None | **OK** |

### 1.7 `src/features/employer_features.py` (552 lines)

| Lines | What it does | Risk | Status |
|-------|-------------|------|--------|
| 22–36 | `WAGE_UNIT_MULTIPLIER` mapping (Year→1.0, Hour→2080.0, Week→52.0, etc.) | None | **OK** |
| 142–145 | Anchor date = `max(decision_date)`, 36-month window | None | **OK** |
| 159–162 | SOC code normalization to 7-char for OEWS matching | None | **OK** |
| 165–168 | Status flags: `is_certified` starts with `'certified'`, `is_denied` contains `'denied'` | None | **OK** |
| 195–215 | LCA case status: `CERTIFIED-WITHDRAWN` and `CERTIFIED - WITHDRAWN` both count as certified; `WITHDRAWN` excluded from denominator | None | **OK** |
| 220–225 | LCA wage annualization (vectorised) | None | **OK** |
| 227–235 | `lca_wage_ratio` clipped to `[0.5, 2.0]` | None | **OK** |
| 250–260 | H1B Hub name matching via `_norm_name()` function | None | **OK** |
| 370–380 | Wage ratio capped at 1.3 (`offered_median / oews_median`) | None | **OK** |
| 385–395 | National median fallback ($65,000 default) | None | **OK** |
| 465–475 | SOC-specific slices only when 24m count ≥ 10 | None | **OK** |

### 1.8 `src/models/employer_score.py` (282 lines)

| Lines | What it does | Risk | Status |
|-------|-------------|------|--------|
| 30–31 | `MIN_CASES_36M = 3`, `ALL_DENIED_CAP = 10.0` | None | **OK** |
| 34–38 | Bayesian shrinkage: prior=0.88, strength=20 pseudo-observations | None | **OK** |
| 48–54 | Wage subscore: wage_ratio_med ∈ [0.5, 1.3] → [0, 100] | None | **OK** |
| 56–82 | Sustainability subscore: months_active, volume (log-scaled), trend, volatility | None | **OK** |
| 230–235 | Eligibility guardrails: `n_36m < 3` → EFS=NULL; all denied → capped at 10 | None | **OK** |
| 240–248 | Tier labels: ≥85 Excellent, ≥70 Good, ≥50 Moderate, ≥30 Below Average, <30 Poor | None | **OK** |

### 1.9 `src/models/pd_forecast.py` (395 lines)

| Lines | What it does | Risk | Status |
|-------|-------------|------|--------|
| 52–56 | `MIN_HISTORY_MONTHS=12`, `CONFIDENCE_Z=1.645`, `OUTLIER_LO/HI_PCT=5/95` | None | **OK** |
| 82–95 | Outlier trimming: P5/P95 removal for seasonal estimation | None | **OK** |
| 98–135 | Seasonal factors: median-based, neighbor-smoothed, clamped to [0.5, 2.0], normalized to avg=1.0 | None | **OK** |
| 173–180 | Full-history NET velocity as anchor (captures retrogression) | None | **OK** |
| 195–200 | Velocity cap: recent velocity cannot exceed long-term by >25% or +5 d/mo | None | **OK** |
| 203–206 | Blend: 50% full-history + 25% 24m + 25% 12m | None | **OK** |
| 210–216 | Robust volatility: IQR/1.35 instead of raw std, floor at 5.0 | None | **OK** |
| 219–221 | Retrogression regime: >30% retro months → dampen by 30% | None | **OK** |
| 233–240 | Floor at 0 days for projections (no backward movement) | None | **OK** |

### 1.10 `src/models/employer_score_ml.py` (463 lines)

| Lines | What it does | Risk | Status |
|-------|-------------|------|--------|
| 28 | `MIN_CASES_36M = 15` — more conservative threshold than rules-based | None | **OK** |
| 31–33 | `APPROVED_STATUS` and `DENIED_STATUS` sets for case label extraction | None | **OK** |
| 35 | `WAGE_LEVEL_ORDER` ordinal mapping (I→1, II→2, III→3, IV→4, N/A→2) | None | **OK** |
| 54–68 | Label engineering: case_status → binary 1=approved, 0=denied, drop others | None | **OK** |
| 71–78 | Wage level ordinal encoding with fillna(2) fallback | None | **OK** |
| 82–90 | Wage ratio computation: `wage_offered_yearly / pw_amount`, clipped [0.5, 2.0], fillna(1.0) | None | **OK** |
| 140–170 | LCA/H1B employer-level feature enrichment from `employer_features.parquet` | None | **OK** |
| 175–200 | HistGradientBoostingClassifier training with 5-fold CV + isotonic calibration | None | **OK** |

### 1.11 `src/curate/run_curate.py` (356 lines)

| Lines | What it does | Risk | Status |
|-------|-------------|------|--------|
| 53–100 | Dimension build order: dim_country → dim_soc → dim_area → dim_visa_class → dim_employer | None | **OK** |
| 152–220 | Fact build order: fact_cutoffs → fact_perm (+ inline dim_employer expansion) → fact_lca → fact_oews | None | **OK** |
| 200–220 | **Inline dim_employer expansion** after `fact_perm` build — scans all PERM employer_ids and adds stubs for any not in dim_employer. This was originally a patch script but has been **folded into the pipeline**. | None | **OK** |

---

## 2. Fix Scripts — One-Shot Repairs (AT RISK)

These scripts were run manually to repair artifacts after initial pipeline runs. Some have been **superseded** by pipeline improvements; others have **not** been backported and must be re-run manually.

### 2.1 `scripts/fix1_perm_reconcile.py` (384 lines) — **SUPERSEDED**

| Lines | What it does | Risk | Status |
|-------|-------------|------|--------|
| 34–45 | `normalize_employer()` — same logic as `build_fact_perm` but uses employer.yml | Low | **SUPERSEDED** |
| 46–49 | `employer_id()` — SHA1 hash (same as `build_fact_perm`) | Low | **SUPERSEDED** |
| 50–58 | `derive_fy()` — FY from received_date (same as `build_fact_perm`) | Low | **SUPERSEDED** |
| 60–90 | `COLUMN_ALIASES` — flexible column mapping per FY (handles header drift) | Low | **SUPERSEDED** |
| 115–170 | Legacy file quarantine (single-file .parquet, __HIVE_DEFAULT_PARTITION__) | Low | **OK** (one-time cleanup) |
| 170–300 | Read PERM Excel files, harmonize columns, dedupe per FY + global on case_number | Low | **SUPERSEDED** by `build_fact_perm` |
| 300–350 | Arrow type casting: all object/category → plain string, None/nan/NaT → null | Low | **SUPERSEDED** |
| 355–384 | Write partitioned parquet + FY stats log | Low | **SUPERSEDED** |

> **Verdict:** The core dedup, normalization, and harmonization logic in fix1 has been **absorbed into `build_fact_perm.py`** and `run_curate.py`. The quarantine step is one-time. **No re-run needed after fresh P1.**

### 2.2 `scripts/fix_a_perm_reconcile.py` (352 lines) — **SUPERSEDED**

Nearly identical to fix1 but with a **different employer_id scheme** (`MD5[:8]` instead of SHA1). 

| Lines | What it does | Risk | Status |
|-------|-------------|------|--------|
| 39–47 | `normalize_employer_name()` — UPPER + suffix strip (different from SHA1 pipeline) | Medium | **SUPERSEDED** |
| 49–54 | `generate_employer_id()` — **MD5[:8]** hash. **Different from pipeline SHA1.** | Medium | **SUPERSEDED** — pipeline uses SHA1 |
| 55–80 | Quarantine legacy files | Low | **SUPERSEDED** by fix1 |
| 100–200 | Column harmonization, dedup, employer/SOC mapping | Low | **SUPERSEDED** by fix1/build_fact_perm |

> **Verdict:** This is an **EARLIER DRAFT** of fix1 with an incompatible employer_id scheme. **Must never be re-run** — it would corrupt employer_id foreign keys.

### 2.3 `scripts/fix_fact_perm_fiscal_year.py` (542 lines) — **PARTIALLY SUPERSEDED**

| Lines | What it does | Risk | Status |
|-------|-------------|------|--------|
| 1–50 | Performance-controlled atomic rewrite of all fact_perm partitions to force `fiscal_year == int(YYYY)` for every row in `fiscal_year=YYYY/` | Medium | **AT RISK** |
| 100–140 | `read_partition_chunks()` — chunked reading of all parquet files in a partition | Low | **OK** |
| 150–175 | `cast_categoricals()` — Arrow type unification (category→string) | Low | **OK** |
| 180–220 | `write_partition_atomic()` — tmp dir → atomic rename (never in-place overwrite) | Low | **OK** |
| 220–260 | `process_partition()` — reads all rows, finds mismatches (rows with wrong fiscal_year), moves them to correct partition | Medium | **AT RISK** |
| 380–540 | Threaded pipeline: 3 reader workers + 1 writer + queue + memory monitoring | Low | **OK** |
| 510–540 | Post-verification: reads back all partitions and checks zero fiscal_year mismatches | Low | **OK** |

> **Verdict:** The core fix is `fiscal_year` forced from directory name. `build_fact_perm.py` line 530 already does `fiscal_year` from directory name during initial build. However, if rows are **moved cross-partition** (a case filed in FY2022 but `received_date` says FY2023), this script handles that case. **Low risk if `build_fact_perm` is the sole writer**, but the script should be run as a **post-build validation** step.

### 2.4 `scripts/fix_fact_perm_pk.py` (204 lines) — **RECOMMENDED POST-BUILD**

| Lines | What it does | Risk | Status |
|-------|-------------|------|--------|
| 1–30 | Enforces within-partition `case_number` uniqueness in fact_perm | Medium | **RECOMMENDED** |
| 60–100 | Dedup priority: latest `decision_date` → most non-null values → smallest `source_file` | Low | **OK** |
| 100–130 | Atomic writes: tmp → unlink → rename | Low | **OK** |
| 140–200 | Markdown report generation + summary stats | Low | **OK** |

> **Verdict:** `build_fact_perm.py` does NOT enforce within-partition PK uniqueness (it only dedupes per-FY during the build). This script should be **added to the pipeline as a post-build step** or its logic backported into `build_fact_perm`.

### 2.5 `scripts/fix_fact_perm_dedup.py` (54 lines) — **SUPERSEDED by fix_fact_perm_pk.py**

| Lines | What it does | Risk | Status |
|-------|-------------|------|--------|
| 1–54 | Simple dedup on `(case_number, fiscal_year)`, `keep="last"` | Low | **SUPERSEDED** |

> **Verdict:** Same function as `fix_fact_perm_pk.py` but less sophisticated (no priority scoring, no atomic writes, no report). **Superseded.**

### 2.6 `scripts/fix4_visa_bulletin.py` (442 lines) — **PARTIALLY SUPERSEDED**

| Lines | What it does | Risk | Status |
|-------|-------------|------|--------|
| 1–70 | Filename-enhanced parser: extracts year+month from various PDF naming conventions | Low | **OK** |
| 72–110 | `parse_date()` — handles `01FEB23`, `C`, `U` | Low | **SUPERSEDED** (same as visa_bulletin_loader) |
| 112–200 | `parse_text_eb_rows()` — text-based parser for legacy+modern PDFs (handles "Other Workers" split across lines) | Medium | **AT RISK** |
| 200–230 | `parse_pdfplumber_table()` — table extraction with country header mapping | Low | **SUPERSEDED** (same as visa_bulletin_loader) |
| 232–340 | Main: reads existing fact_cutoffs, parses ALL PDFs (legacy 2011–2014 via text, modern via table+text fallback) | Medium | **AT RISK** |
| 340–370 | Dedup with priority scoring: `D(100) > C(50) > U(0)`, has_date bonus, source_file tiebreak | Medium | **AT RISK** |
| 370–442 | Backup → rewrite partitioned output | Low | **OK** |

> **Verdict:** The core pipeline (`visa_bulletin_loader.py`) handles modern PDFs but **may miss legacy 2011–2014 PDFs** that use different text layouts. This script fills that gap. **Should be run after a fresh P1 run if legacy bulletin coverage is needed.**

### 2.7 `scripts/fix_c_visa_bulletin_dedupe.py` (302 lines) — **SUPERSEDED by fix4**

| Lines | What it does | Risk | Status |
|-------|-------------|------|--------|
| 1–100 | Load partitioned cutoffs, reconstruct partition columns | Low | **SUPERSEDED** |
| 100–140 | Priority-based dedup: `D > C > U` preference | Low | **SUPERSEDED** by fix4 |
| 140–180 | Legacy PDF parsing (limited — only checks if parseable, doesn't extract data) | Low | **SUPERSEDED** |
| 180–302 | Backup + rewrite + PK check | Low | **SUPERSEDED** |

> **Verdict:** Fully superseded by fix4. **No re-run needed.**

### 2.8 `scripts/fix_fact_cutoffs_dedup.py` (191 lines) — **RECOMMENDED POST-BUILD**

| Lines | What it does | Risk | Status |
|-------|-------------|------|--------|
| 1–30 | Fixes double-ingestion: production bug caused 2 part-*.parquet files per partition dir (14,290 rows → 8,315 expected) | Medium | **RECOMMENDED** |
| 50–80 | `load_all_cutoffs()` — reads all parquet files, reconstructs partition columns from path | Low | **OK** |
| 80–100 | `rewrite_partition()` — removes all files from partition dir, writes single `part-0.parquet` | Low | **OK** |
| 100–190 | Dedup on PK, keep latest `ingested_at`, rewrite partitions atomically | Medium | **RECOMMENDED** |

> **Verdict:** The pipeline doesn't inherently prevent multiple part files from appearing in a partition dir. **Should be run as a post-build validation** or the pipeline should ensure single-file partitions.

### 2.9 `scripts/fix2_dim_soc.py` (147 lines) — **SUPERSEDED**

| Lines | What it does | Risk | Status |
|-------|-------------|------|--------|
| 1–60 | Extract SOC codes from OEWS 2023 zip → filtered to `XX-XXXX` format | Low | **SUPERSEDED** |
| 60–130 | Cross-check with SOC 2010→2018 crosswalk; derive hierarchy (major/minor/broad) | Low | **SUPERSEDED** |
| 130–147 | Write `dim_soc.parquet` | Low | **SUPERSEDED** |

> **Verdict:** Logic has been **absorbed into `build_dim_soc.py`** (called by `run_curate.py`). **No re-run needed.**

### 2.10 `scripts/fix_b_dim_soc_expand.py` (242 lines) — **SUPERSEDED**

Nearly identical to fix2 but with crosswalk loading from a different CSV column scheme. **Superseded by fix2 and `build_dim_soc.py`.**

### 2.11 `scripts/fix3_dim_country.py` (285 lines) — **PARTIALLY EMBEDDED**

| Lines | What it does | Risk | Status |
|-------|-------------|------|--------|
| 1–30 | Checks codebook at `Codebooks/country_codes_iso.csv` | Low | **OK** |
| 30–60 | If codebook has <200 rows, builds comprehensive dim from embedded ISO-3166-1 data (249 countries) | Medium | **AT RISK** |
| 60–250 | Hardcoded `COUNTRIES` list (249 tuples: iso2, iso3, name, region) | Low | **OK** |
| 260–285 | `_build_from_codebook()` — standard build if codebook has ≥200 rows | Low | **OK** |

> **Verdict:** The codebook in P1 downloads only 5 countries (`ROW`, `CHN`, `IND`, `MEX`, `PHL`). The pipeline's `build_dim_country.py` may produce a minimal dim_country with only 5 rows. Fix3 upgrades this to 249 rows using hardcoded ISO data. **Must be re-run after fresh P1 if the codebook still has only 5 rows.** Consider backporting the embedded country list into `build_dim_country`.

### 2.12 `scripts/patch_dim_employer_from_fact_perm.py` (155 lines) — **FOLDED INTO PIPELINE**

| Lines | What it does | Risk | Status |
|-------|-------------|------|--------|
| 1–30 | Problem: `build_dim_employer` only reads last 2 FYs, max 50K rows → misses 226K+ unique PERM employers | Low | **OK** |
| 60–100 | Reads all `fact_perm` partitions, extracts employer_id + employer_name, dedupes | Low | **OK** |
| 100–120 | Creates stub rows for missing employers with canonical Title Case names | Low | **OK** |
| 120–140 | Sanitizes ALL-CAPS employer names from prior patches | Low | **OK** |
| 140–155 | Validates coverage: asserts ≥40% row-level coverage vs `employer_features` | Low | **OK** |

> **Verdict:** This logic has been **inlined into `run_curate.py`** (lines 200–220) which runs dim_employer expansion after fact_perm build. **No separate re-run needed.**

### 2.13 `scripts/patch_fact_perm_wages.py` (61 lines) — **SUPERSEDED**

| Lines | What it does | Risk | Status |
|-------|-------------|------|--------|
| 1–20 | Root causes: SOC strip `.00`, area name→code via dim_area, wage_offer_from mapping | Low | **SUPERSEDED** |
| 20–60 | Calls `build_fact_perm(min_fy=2022)` to rebuild last 4 FYs only | Low | **SUPERSEDED** |

> **Verdict:** These bug fixes have been **committed to `build_fact_perm.py`** itself. A fresh full run builds all FYs correctly. **No re-run needed.**

---

## 3. Metric Builders with QA Guards (SAFE)

These scripts run as part of the feature/metric build phase and contain inline QA.

### 3.1 `scripts/make_employer_monthly_metrics.py` (199 lines)

| Lines | What it does | Risk | Status |
|-------|-------------|------|--------|
| 82–87 | Clips `approvals > filings` (data anomaly guard) | None | **OK** |
| 88–89 | `approval_rate` and `denial_rate` clipped to `[0, 1]` | None | **OK** |
| 95–97 | Rolling 12-month `audit_rate_t12` | None | **OK** |
| 108–135 | QA checks: approval_rate in [0,1], approvals≤filings, large employer outlier detection (`avg_approval_rate_36m` outside [0.4, 1.0]) | None | **OK** |

### 3.2 `scripts/make_salary_benchmarks.py` (228 lines)

| Lines | What it does | Risk | Status |
|-------|-------------|------|--------|
| 65–68 | `_annualize_or_use()` — prefers annual column, falls back to hourly × 2080 | None | **OK** |
| 85–90 | `enforce_monotonic()` — sorts `p10 ≤ p25 ≤ median ≤ p75 ≤ p90` per row | None | **OK** |
| 100–110 | National-fallback fill — fills null area-level percentiles from national aggregate | None | **OK** |
| 120–130 | QA: verifies `p10 ≤ p25 ≤ median ≤ p75 ≤ p90` after correction | None | **OK** |
| 140–150 | Bad value cleanup: drops rows where percentile ≤ 0 when other columns are valid | None | **OK** |

### 3.3 `scripts/make_employer_salary_profiles.py` (559 lines)

| Lines | What it does | Risk | Status |
|-------|-------------|------|--------|
| 205–215 | Wage filters: `annual_wage > 5000` AND `< 1,000,000` (extreme value removal) | None | **OK** |
| 250–270 | OEWS join: SOC code truncated to 7-char for matching | None | **OK** |
| 280–300 | `wage_premium_pct` and `wage_vs_pw_pct` computation with div-by-zero guard | None | **OK** |
| 310–380 | `_canonical_employer_names()` — 2-pass strategy: (1) dim_employer lookup, (2) normalize+title_case fallback for LCA-only employers | None | **OK** |
| 390–430 | `_build_employer_yearly_summary()` — true flat median from raw records (avoids median-of-medians bias) | None | **OK** |
| 440–500 | `_build_soc_market_summary()` — true flat median and percentiles across all records per SOC×visa×FY | None | **OK** |
| 510–559 | QA checks: salary range [5K, 1M], wage premium distribution stats | None | **OK** |

### 3.4 `scripts/make_oews_2024_fallback.py` (101 lines)

| Lines | What it does | Risk | Status |
|-------|-------------|------|--------|
| 1–101 | Creates synthetic `ref_year=2024` OEWS data by copying 2023. Tags as `source_tag="synthetic_from_2023"` | Low | **AT RISK** |

> **Verdict:** This produces synthetic 2024 data because BLS hadn't published OEWS 2024 yet. **Should be removed/skipped once real 2024 data is available.** Check P1 downloads for `BLS_OEWS/2024/` presence.

### 3.5 `scripts/make_employer_risk_features.py` (121 lines)

| Lines | What it does | Risk | Status |
|-------|-------------|------|--------|
| 30–50 | `_norm()` — employer name normalization for fuzzy join to WARN events | None | **OK** |
| 50–80 | Merge WARN events with dim_employer via normalized name join | None | **OK** |
| 80–121 | If no WARN data, writes output with zero-valued risk columns | None | **OK** |

### 3.6 `scripts/make_worksite_geo_metrics.py` (413 lines)

| Lines | What it does | Risk | Status |
|-------|-------------|------|--------|
| 34–62 | `STATE_NAME_TO_ABBR` — 50+ states + territories full name → 2-letter code | None | **OK** |
| 64–75 | `VALID_STATE_ABBRS` — whitelist of valid 2-letter abbreviations | None | **OK** |
| 77–95 | `normalize_state()` — handles 3 cases: already abbreviated, full name, garbage→NaN | None | **OK** |
| 97–110 | `WAGE_MULTIPLIERS` — Hour/Week/Month/Year/aliases | None | **OK** |

### 3.7 `scripts/make_soc_demand_metrics.py` (250 lines)

| Lines | What it does | Risk | Status |
|-------|-------------|------|--------|
| 35–60 | `BLS_MAJOR_GROUPS` — 23 SOC major group labels | None | **OK** |
| 62–72 | `WAGE_MULTIPLIERS` — same as geo metrics | None | **OK** |
| 85–100 | `_read_partitioned_cols()` — handles partition column reconstruction from path | None | **OK** |
| 100–105 | `annualize()` — wage × multiplier vectorised | None | **OK** |

### 3.8 `scripts/fix5_oews_robustness.py` (51 lines)

| Lines | What it does | Risk | Status |
|-------|-------------|------|--------|
| 1–51 | Checks OEWS zip files for corruption (bad zip, truncated files, empty xlsx) | None | **OK** |

> **Verdict:** Diagnostic script, not a data fix. Safe to re-run anytime.

---

## 4. Risk Matrix

| Script | Risk Level | Re-run After Fresh P1? | Reason |
|--------|-----------|----------------------|--------|
| `fix1_perm_reconcile.py` | Low | **NO** | Superseded by `build_fact_perm` + `run_curate` |
| `fix_a_perm_reconcile.py` | **DANGER** | **NEVER** | Uses MD5 employer_id — incompatible with SHA1 pipeline |
| `fix_fact_perm_fiscal_year.py` | Medium | **OPTIONAL** | `build_fact_perm` already forces FY from dir name; this is a safety net |
| `fix_fact_perm_pk.py` | Medium | **YES** | Pipeline doesn't enforce within-partition PK uniqueness |
| `fix_fact_perm_dedup.py` | Low | **NO** | Superseded by `fix_fact_perm_pk.py` |
| `fix4_visa_bulletin.py` | Medium | **YES** (if legacy needed) | Handles 2011–2014 PDFs not covered by pipeline |
| `fix_c_visa_bulletin_dedupe.py` | Low | **NO** | Superseded by `fix4` |
| `fix_fact_cutoffs_dedup.py` | Medium | **YES** | Post-build dedup to fix multi-file partitions |
| `fix2_dim_soc.py` | Low | **NO** | Superseded by `build_dim_soc` |
| `fix_b_dim_soc_expand.py` | Low | **NO** | Superseded by `fix2` and `build_dim_soc` |
| `fix3_dim_country.py` | **HIGH** | **YES** | P1 codebook only has 5 rows; fix3 expands to 249 |
| `patch_dim_employer_from_fact_perm.py` | Low | **NO** | Folded into `run_curate.py` inline expansion |
| `patch_fact_perm_wages.py` | Low | **NO** | Bug fixes committed to `build_fact_perm` |
| `make_oews_2024_fallback.py` | Medium | **CONDITIONAL** | Only if BLS OEWS 2024 still not available |

### Scripts that MUST be re-run after fresh P1:

1. **`fix3_dim_country.py`** — Expands dim_country from 5 to 249 rows
2. **`fix_fact_perm_pk.py`** — Enforces within-partition case_number uniqueness
3. **`fix_fact_cutoffs_dedup.py`** — Ensures single-file partitions
4. **`fix4_visa_bulletin.py`** — Only if legacy 2011–2014 bulletin coverage is needed
5. **`make_oews_2024_fallback.py`** — Only if BLS hasn't published OEWS 2024 yet

---

## 5. Recommendations

### 5.1 Backport Fix Logic Into Pipeline

| Fix Script | Backport To | Complexity |
|-----------|-------------|------------|
| `fix3_dim_country.py` L60–250 (embedded ISO list) | `build_dim_country.py` | **Low** — copy the 249-country list as fallback when codebook <200 rows |
| `fix_fact_perm_pk.py` L60–100 (within-partition dedup) | `build_fact_perm.py` (add as final step per partition) | **Low** — add `drop_duplicates(subset=['case_number'], keep='first')` after sorting by decision_date desc |
| `fix_fact_cutoffs_dedup.py` L80–100 (single-file partition enforcement) | `visa_bulletin_loader.py` (ensure single part file) | **Low** — already mostly handles this; add explicit cleanup of stale part files |
| `fix4_visa_bulletin.py` L112–200 (legacy text parser) | `visa_bulletin_loader.py` | **Medium** — text-based EB row parser for legacy PDF format |

### 5.2 Add Post-Build Validation Step

Create a single `scripts/validate_pipeline.py` that runs:
1. PK uniqueness checks on all fact tables
2. Foreign key coverage checks (employer_id, soc_code → dims)
3. dim_country row count assertion (≥200)
4. Single-file-per-partition check on all Hive directories
5. `fiscal_year` matches directory name for all fact_perm rows

### 5.3 Deprecate/Archive Old Fix Scripts

Move the following to an `_archive/` directory:
- `fix_a_perm_reconcile.py` (MD5 employer_id — **dangerous**)
- `fix1_perm_reconcile.py` (superseded)
- `fix_c_visa_bulletin_dedupe.py` (superseded by fix4)
- `fix_b_dim_soc_expand.py` (superseded by fix2)
- `fix_fact_perm_dedup.py` (superseded by fix_fact_perm_pk)
- `patch_fact_perm_wages.py` (bugs fixed in build_fact_perm)

### 5.4 Implement `src/validate/dq_checks.py`

Currently 81 lines — **all placeholder/TODO**. Implement:
- PK uniqueness assertions
- FK coverage checks
- Value range validations (wages, dates, rates)
- Row count assertions per artifact
- Null percentage thresholds

### 5.5 Recommended Post-P1 Run Order

```bash
# Phase 1: Core pipeline
python3 -m src.curate.run_curate --paths configs/paths.yaml

# Phase 2: Critical fixes not yet backported
python3 scripts/fix3_dim_country.py            # Expand dim_country to 249 rows
python3 scripts/fix_fact_perm_pk.py             # Enforce PK uniqueness
python3 scripts/fix_fact_cutoffs_dedup.py --write  # Fix multi-file partitions

# Phase 3: Conditional
python3 scripts/make_oews_2024_fallback.py      # Only if no real 2024 OEWS
python3 scripts/fix4_visa_bulletin.py           # Only if legacy 2011-2014 needed

# Phase 4: Feature/metric builds
python3 scripts/make_salary_benchmarks.py
python3 scripts/make_employer_salary_profiles.py
python3 scripts/make_employer_monthly_metrics.py
python3 scripts/make_employer_risk_features.py
python3 scripts/make_worksite_geo_metrics.py
python3 scripts/make_soc_demand_metrics.py
python3 scripts/make_category_movement_metrics.py
python3 scripts/make_backlog_estimates.py
python3 scripts/make_processing_times_trends.py
python3 scripts/make_fact_cutoff_trends.py
python3 scripts/make_visa_demand_metrics.py

# Phase 5: Model builds
python3 -m src.models.employer_score
python3 -m src.models.employer_score_ml
python3 -m src.models.pd_forecast

# Phase 6: Sync to P3
cd ../immigration-insights-app
python3 scripts/sync_p2_data.py
npm run build
```

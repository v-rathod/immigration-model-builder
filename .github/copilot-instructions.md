# Immigration Model Builder — Copilot Context

> **NorthStar Program Codenames** (public-facing names for documentation):
> | Internal | Codename | Repository | Role |
> |----------|----------|------------|------|
> | P1 | **Horizon** | fetch-immigration-data | Data collection — scans the horizon |
> | P2 | **Meridian** | immigration-model-builder (THIS REPO) | Analytics backbone — curates, measures, models |
> | P3 | **Compass** | immigration-insights-app | User experience — guides with insights |
>
> Use P1/P2/P3 in internal code and comments. Use Horizon/Meridian/Compass in public docs (README, reports).

---

## START OF EVERY SESSION — Discovery Protocol

**Do NOT rely on hardcoded numbers in this file. Run these steps to discover current state:**

1. **Load program objectives:**
   ```bash
   python scripts/print_objective_banner.py
   ```

2. **Read current progress** — find the latest milestone, what was done, what's next:
   - Read the **last 100 lines** of `PROGRESS.md` (latest milestone entry)
   - Read `artifacts/metrics/FINAL_SINGLE_REPORT.md` for artifact inventory, row counts, data quality status

3. **Get live test status:**
   ```bash
   CHAT_TAP_DISABLED=1 python3 -m pytest tests/ -q --tb=short 2>&1 | tail -5
   ```

4. **Get live artifact inventory** (row counts, file sizes):
   ```bash
   python3 -c "
   import pandas as pd; from pathlib import Path
   for f in sorted(Path('artifacts/tables').rglob('*.parquet')):
       try:
           n = len(pd.read_parquet(f))
           print(f'{n:>12,}  {f.relative_to(\"artifacts/tables\")}')
       except: print(f'     ERROR  {f.relative_to(\"artifacts/tables\")}')
   "
   ```

5. Then proceed with the user's request.

> **Key objective files** (version-controlled, source of truth for program goals):
> - `configs/project_objective_P1_P2_P3.md` — human-readable program objectives
> - `configs/project_objective_P1_P2_P3.yaml` — machine-readable objectives (quality gates, table lists, P3 features)
> - `src/utils/objective_loader.py` — Python loader for the YAML

---

## Project Overview

Parquet-based immigration data pipeline (**NorthStar Meridian**) that transforms raw DOL/DOS/USCIS/BLS data into curated dimension & fact tables, engineered features, and ML model artifacts for a downstream public-facing web app (Compass).

**3-project architecture (NorthStar program):**
```
Horizon  (P1: fetch-immigration-data)   →  raw downloads (PDFs, Excel, CSV)
Meridian (P2: immigration-model-builder)  →  THIS REPO: curate → features → models
Compass  (P3: immigration-insights-app)   →  public web app consuming Meridian artifacts
```

---

## Tech Stack
- **Python**: 3.12 (`/opt/homebrew/opt/python@3.12/bin/python3.12`)
- **Key packages**: pandas ≥2.0, pyarrow ≥12.0, pytest, pdfplumber, openpyxl, pydantic, pyyaml
- **No virtual environment** — uses system Python with pip packages
- **Data format**: Parquet (facts & dims), JSON (model weights), YAML (configs)
- **Execution model**: CLI entrypoints, manual execution only, no schedulers/watchers

---

## P3 Compass — Architecture Decisions (for P2 awareness)

> **Any future P2 changes must preserve compatibility with the P3 data contract described below.**

### P3 Tech Stack (Decided)
| Layer | Technology | Why |
|-------|-----------|-----|
| Framework | **Next.js 16** (App Router, static export) | Zero server cost — `output: "export"` generates pure HTML/JS/CSS |
| Language | **TypeScript 5** (strict) | Type-safe consumption of P2 data |
| Styling | **Tailwind CSS 4** + shadcn/ui | Utility-first, dark-first "Aurora" design system |
| Charts | **Recharts 2** | Composable, React-native charting |
| Animation | **Framer Motion 12** | Glassmorphic transitions, staggered reveals |
| Search | **Fuse.js 7** | Client-side fuzzy search over RAG chunks + QA pairs |
| Maps | **react-simple-maps 3** | Lightweight SVG maps for geographic dashboard |
| Icons | **Lucide React** | Consistent icon set |
| State | **nuqs 2** | URL-synced query params (filters, user inputs) |
| Font | **Geist** (Sans + Mono) | Vercel's open-source font family |

### AWS Hosting ($1–3/month target)
```
S3 (static site)  →  CloudFront (CDN)  →  Route 53 (DNS)  →  ACM (SSL)
```
**Non-negotiable constraints:**
- **Zero Lambda, zero database, zero API Gateway** — everything is static JSON + client-side JS
- All data pre-computed by P2 Meridian at build time
- Only external API call: GPT-4o-mini for RAG chat fallback (~$0.15/month at 100 visitors)

### P2 → P3 Data Pipeline
```
P2 Parquet artifacts  →  sync_p2_data.py  →  JSON slices  →  public/data/  →  S3
```
- **Sync script**: `P3/scripts/sync_p2_data.py` reads P2 Parquet files + converts to JSON
- **Pre-build hook**: `npm run prebuild` auto-runs sync before every `next build`
- **Manual sync**: `npm run sync-data` (full) or `npm run sync-rag` (RAG only)

### P3 Dashboard → P2 Artifact Mapping
| P3 Dashboard | P2 Artifacts Consumed | Key Columns |
|-------------|----------------------|-------------|
| 1. Visa Bulletin & Forecasts | fact_cutoff_trends, pd_forecasts, pd_forecast_model.json | category, country, cutoff_date, forecast_date |
| 2. Employer Friendliness | employer_friendliness_scores, employer_monthly_metrics | employer_id, efs_score, tier, approval_rate |
| 3. Salary & Wage | salary_benchmarks, employer_features | soc_code, area_code, median_wage, wage_ratio |
| 4. Geographic Trends | worksite_geo_metrics | state, area_code, filings, approvals |
| 5. Occupation Demand | soc_demand_metrics | soc_code, lca_filings, perm_filings |
| 6. Visa Demand & Issuance | visa_demand_metrics | visa_class, fiscal_year, issued, refused |
| 7. Backlog & Queue | backlog_estimates, queue_depth_estimates | category, country, est_wait_years |
| 8. Processing Times | processing_times_trends | form_type, service_center, median_days |

### P2→P3 Data Contract (MUST preserve)
When modifying P2 artifacts, **do NOT:**
- ❌ Rename columns consumed by P3 (see mapping above)
- ❌ Change primary key structure of any dimension table
- ❌ Remove or rename RAG topic names (`pd_forecast`, `employer`, `salary`, `visa_bulletin`, `geographic`, `occupation`, `processing`, `visa_demand`, `filings`, `general`)
- ❌ Change QA pair JSON schema (`question`, `answer`, `topic`, `sources`)
- ❌ Change RAG chunk JSON schema (`chunk_id`, `topic`, `title`, `content`, `sources`, `row_count`)

When adding new P2 artifacts:
- ✅ Add corresponding chunk builder in `src/export/rag_builder.py`
- ✅ Register in `src/export/qa_generator.py` topic list
- ✅ Update P3 sync script (`scripts/sync_p2_data.py` in P3 repo) to include new artifact
- ✅ Add TypeScript interface in P3 `src/types/p2-artifacts.ts`

---

## Key Paths
| What | Path |
|------|------|
| Project root | `/Users/vrathod1/dev/NorthStar/immigration-model-builder` |
| Raw downloads (Horizon / P1) | `/Users/vrathod1/dev/NorthStar/fetch-immigration-data/downloads` |
| Curated tables | `artifacts/tables/` |
| Model artifacts | `artifacts/models/` |
| Metrics & reports | `artifacts/metrics/` |
| RAG artifacts | `artifacts/rag/` (catalog, chunks, QA cache) |
| Tests | `tests/` |
| Pipeline config | `configs/paths.yaml` (data_root + artifacts_root) |
| Schemas | `configs/schemas.yml` |
| Full pipeline | `scripts/build_all.sh` |
| Main report | `artifacts/metrics/FINAL_SINGLE_REPORT.md` |

---

## Discovering Current State

**Do not hardcode milestone numbers or row counts. Use these sources of truth:**

| What you need | Where to find it |
|---------------|-----------------|
| Latest milestone & work history | Last entry in `PROGRESS.md` |
| Artifact inventory (all tables, row counts, data quality) | `artifacts/metrics/FINAL_SINGLE_REPORT.md` |
| Test pass/fail counts | Run `python3 -m pytest tests/ -q` |
| Known issues & data quality findings | "Unable to Fix" section of `FINAL_SINGLE_REPORT.md` |
| Program objectives & quality gates | `configs/project_objective_P1_P2_P3.yaml` |
| P1 data readiness & change detection | Run `python3 scripts/check_p1_readiness.py` |
| Live artifact shapes | Run snippet from Discovery Protocol step 4 above |

---

## Pipeline Stages (scripts/build_all.sh)

```bash
# Stage 1: Curate raw data → canonical tables
python3 -m src.curate.run_curate --paths configs/paths.yaml

# Stage 1b: Patch dim_employer + expand dim_soc (MUST run after curate — see gotchas)
python3 scripts/patch_dim_employer_from_fact_perm.py
python3 scripts/expand_dim_soc_legacy.py

# Stage 1c: Additional P1-sourced fact tables
python3 scripts/build_fact_h1b_employer_hub.py
python3 scripts/build_fact_bls_ces.py
python3 scripts/build_fact_iv_post.py

# Stage 2: Feature engineering
python3 -m src.features.run_features --paths configs/paths.yaml

# Stage 3: Model training
python3 -m src.models.run_models --paths configs/paths.yaml

# Stage 4: RAG + QA generation (for P3 Compass)
python3 -m src.export.rag_builder
python3 -m src.export.qa_generator
```

Other useful commands:
```bash
# Run tests (slow_integration auto-skipped via pytest.ini)
python3 -m pytest tests/ -q

# Run with JUnit XML output
python3 -m pytest tests/ --junitxml=artifacts/metrics/all_tests_final.xml -q

# Run ONLY slow_integration tests (20+ min each — re-runs full curate pipeline)
python3 -m pytest tests/ -m slow_integration -q

# Makefile shortcuts
make curate     # run_curate only
make test       # pytest -q
make clean      # wipe artifacts/
make audit-all  # input + output audits

# Incremental build (detect P1 changes → rebuild affected P2 artifacts)
bash scripts/build_incremental.sh              # plan only
bash scripts/build_incremental.sh --execute    # detect + rebuild
bash scripts/build_incremental.sh --init       # initialize manifest
bash scripts/build_incremental.sh --full       # full rebuild + save

# P1 readiness check (run after any P1 fetch to detect new data)
python3 scripts/check_p1_readiness.py          # full readiness report
python3 scripts/check_p1_readiness.py --fix    # auto-rebuild if changes found
```

---

## Handling New Horizon (P1) Data or New Data Sources

**After any Horizon fetch/update, run this workflow:**
```bash
python3 scripts/check_p1_readiness.py       # 1. Check what changed
bash scripts/build_incremental.sh --execute  # 2. Rebuild affected artifacts
python3 -m pytest tests/ -q                  # 3. Validate
```

**If Horizon adds a completely new data source** (new directory):
1. Add `DATASET_PATTERNS` entry in `src/incremental/change_detector.py`
2. Create builder script: `scripts/build_fact_<name>.py`
3. Add `DEPENDENCY_GRAPH` entry mapping dataset → artifacts + rebuild commands
4. Run builder, add tests, re-init manifest: `bash scripts/build_incremental.sh --init`

---

## Artifact Schema (naming conventions — for current row counts read FINAL_SINGLE_REPORT.md)

- `dim_*` — Dimension tables (employer, SOC, country, area, visa_class, visa_ceiling)
- `fact_*` — Fact tables (perm, lca, cutoffs, oews, visa issuances, admissions, etc.)
- `*_features` / `*_metrics` — Engineered feature tables
- `*_scores` / `*_forecasts` — Model output tables
- Partitioned tables are directories (e.g., `fact_perm/`); flat copies end in `.parquet`

**Stub / empty tables (expected — no data source available):**

**Post-level IV issuance data:**
- `fact_iv_post.parquet` — Monthly immigrant visa issuances by consular post × visa class (163K rows, 153 posts, FY2017–FY2025). Source: 99 DOS "IV Issuances by Post" PDFs. Feeds into `visa_demand_metrics`.

**Stub / empty tables (expected — no data source available):**
- `fact_trac_adjudications.parquet` — TRAC requires paid subscription
- `fact_acs_wages.parquet` — Census API HTTP 404 (data not yet published, ~Sep 2026)
- `fact_processing_times.parquet` — USCIS Processing Times page is Vue.js SPA; P1 source directory deleted (no usable data)
- `employer_scores.parquet`, `oews_wages.parquet`, `visa_bulletin.parquet` — Legacy stubs, superseded by other artifacts

**Stale / discontinued data sources:**
- `fact_h1b_employer_hub.parquet` — USCIS H-1B Employer Hub (discontinued after FY2023). All rows marked `is_stale=True`, `data_weight=0.6`. Historical context only.

---

## Test Architecture

### pytest.ini Configuration
```ini
markers = slow_integration: runs full curate pipeline (20+ min)
addopts = -m "not slow_integration"
```

### Test Organization

Discover current test files and counts: `python3 -m pytest tests/ --co -q 2>&1 | tail -3`

**Test categories (stable structure):**
- `tests/test_*.py` — Individual table tests (smoke, schema, PK, shape)
- `tests/datasets/` — Cross-table validation (schema+PK, referential integrity, coverage, value ranges)
- `tests/models/` — E2E integration checks, usage registry validation
- `tests/p2_hardening/` — Comprehensive schema/PK and ranges/RI for all P2 artifacts
- `tests/p2_gap_curation/` — Gap table tests
- `tests/p3_metrics/` — Metric table tests (one file per metric table)

### Key Test Thresholds (stable — encoded in test assertions)
| Dataset | Metric | Threshold | Rationale |
|---------|--------|-----------|-----------|
| dim_employer | Row count | ≥ 60,000 | Requires fact_perm patch |
| employer_features | employer_id coverage | ≥ 40% | Must match dim_employer |
| DOS visa tables | Country RI | 50–70% | DOS/FAO naming ≠ ISO-3166 |
| soc_demand_metrics | SOC RI | 80% | Legacy SOC-2000/2010 codes |
| fact_perm_unique_case | PK uniqueness | ≥ 70% | Multi-year refilings |
| TRAC / ACS / Processing Times | Row count | 0 acceptable | Stubs, no data source |
| fact_h1b_employer_hub | is_stale | All True | USCIS discontinued after FY2023 |

---

## Critical Gotchas / Traps

1. ~~**dim_employer gets overwritten**~~ — **FIXED**: `build_dim_employer.py` now reads from `fact_perm` if it exists (fast, complete), otherwise reads ALL PERM Excel files (no row/FY limits). Produces ~243K employers directly. The patch script in `build_all.sh` remains as a safety net but typically finds no missing employers.

2. **slow_integration tests re-run the full curate pipeline** (~20+ min each via subprocess). Auto-skipped via pytest.ini.

3. ~~**fact_lca/ has a schema merge error**~~ — **FIXED M17**: Removed redundant fiscal_year column from FY2021-2026 partitions. `pd.read_parquet('artifacts/tables/fact_lca')` now works (9,558,695 rows).

4. **fact_perm_unique_case has 0.4% NaN case_numbers**: These are rows with null case_number values from source DOL files, not duplicates. All non-null case_numbers are unique.

5. **case_status values are normalized to uppercase**: Raw DOL data had mixed casing, but loaders normalize to uppercase. No action needed.

6. **Legacy stub tables are expected to be empty**: employer_scores, oews_wages, visa_bulletin all have 0 rows — superseded by other artifacts.

7. **conftest.py activates chat_tap**: The root conftest.py imports `src.utils.chat_tap` for commentary capture. Non-blocking (wrapped in try/except). Use `CHAT_TAP_DISABLED=1` env var to suppress.

8. **Incremental builds use file manifests**: `artifacts/metrics/p1_manifest.json` stores fingerprints for Horizon (P1) files. Run `bash scripts/build_incremental.sh --init` to reset. New datasets need `DATASET_PATTERNS` + `DEPENDENCY_GRAPH` entries in `src/incremental/change_detector.py`.

9. **Always run P1 readiness check after any Horizon fetch/update**: `python3 scripts/check_p1_readiness.py`

10. **H1B Employer Hub data is stale/discontinued**: `fact_h1b_employer_hub.parquet` has `is_stale=True` and `data_weight=0.6` on all rows. USCIS discontinued the H-1B Employer Data Hub after FY2023. Use this data for historical trend analysis only; reduce its weight in any scoring models.

11. **ACS data is unavailable**: Census API returns HTTP 404 for 2025 ACS1 data. `fact_acs_wages.parquet` is a 0-row stub. Expected to become available ~Sep 2026.

12. **USCIS Processing Times has no P1 source**: `fact_processing_times.parquet` is a 0-row stub. The USCIS page is a Vue.js SPA; P1 could not extract data and the `USCIS_Processing_Times/` download directory has been deleted. Requires headless browser or direct API integration in P1 to populate.

13. **dim_soc gets overwritten**: `build_dim_soc.py` (via curate) writes only 1,396 SOC-2018 codes from OEWS 2023. MUST run `scripts/expand_dim_soc_legacy.py` after curate to add 405 SOC-2010 legacy codes (→1,801 total). This is now in `build_all.sh` Stage 1b.

14. **Visa_Statistics has two report classes**: FSC (Foreign State of Chargeability) → `fact_visa_applications`, POST (by consular post) → `fact_iv_post`. The `build_fact_visa_applications.py` builder explicitly skips POST PDFs (line 97: `if rpt_class != "FSC": return`). POST data is handled by the separate `build_fact_iv_post.py` builder.

15. **RAG must be rebuilt after any table change**: After rebuilding curated tables, features, or models, run `python3 -m src.export.rag_builder && python3 -m src.export.qa_generator` to regenerate chunks and QA pairs with updated row counts and statistics. The `scripts/test_rag_practical.py` smoke test can detect stale data.

---

## Source Code Architecture

Discover current source tree: `find src/ -name '*.py' | sort`

**Stable module structure:**
```
src/
├── curate/          # Raw → canonical parsers (dims + facts)
│   ├── run_curate.py           # CLI orchestrator
│   ├── build_dim_*.py          # Dimension table builders
│   ├── build_fact_*.py         # Fact table builders
│   └── *_loader.py             # File format parsers (PDF, Excel)
├── features/        # Feature engineering
│   ├── run_features.py         # CLI orchestrator
│   └── *.py                    # Individual feature builders
├── models/          # Model training & scoring
│   ├── run_models.py           # CLI orchestrator
│   └── *.py                    # Individual model builders
├── utils/           # Shared utilities (usage_registry, chat_tap, transcript)
├── incremental/     # Manifest-based P1 change detection & rebuild planning
├── io/              # Config loading, path resolution
├── normalize/       # SOC crosswalks, employer normalization
├── validate/        # Data quality check helpers
└── export/          # RAG chunk generation & bundle packaging for Compass (P3)
    ├── rag_builder.py          # Generate 98 text chunks across 10 topics
    └── qa_generator.py         # Generate 178 pre-computed Q&A pairs
```

### Key Scripts (scripts/)
| Script | Purpose |
|--------|---------|
| `build_all.sh` | Full pipeline: curate → patch → features → models |
| `build_incremental.sh` | Incremental build: detect P1 changes → rebuild affected artifacts |
| `check_p1_readiness.py` | P1 readiness audit: classify files, detect changes, report gaps |
| `patch_dim_employer_from_fact_perm.py` | Expand dim_employer from all fact_perm partitions |
| `generate_final_report.py` | Generate FINAL_SINGLE_REPORT.md |
| `run_full_qa.py` | Comprehensive QA runner |
| `build_fact_*.py` | Build P2 gap fact tables |
| `make_*.py` | Build derived metric tables |
| `build_fact_iv_post.py` | Parse 99 DOS "IV by Post" PDFs → fact_iv_post.parquet |
| `make_visa_demand_metrics.py` | Aggregate visa demand from 4 sources (incl. fact_iv_post) |
| `test_rag_practical.py` | Practical RAG smoke test (29 checks) |
| `audit_*.py` | Input/output audit runners |

---

## Editorial & Formatting Rules

When editing documentation files in this project:
- Use **Markdown tables** with aligned pipes for data summaries
- Include row counts with commas (e.g., `227,076`)
- Always include `Generated: YYYY-MM-DDTHH:MM:SSZ` timestamps in reports
- Test results format: `X passed, Y failed, Z skipped, W deselected`
- Pass rates as percentages with 1 decimal: `99.4%`
- Parquet file references include row count and column count
- PROGRESS.md entries should include: date, objective, work performed, files modified, current state
- FINAL_SINGLE_REPORT.md updates should maintain section numbering consistency

---

## Important Files to Read for Full Context
1. **`PROGRESS.md`** — Full milestone history (read last ~100 lines for latest state)
2. **`artifacts/metrics/FINAL_SINGLE_REPORT.md`** — Comprehensive data quality report with artifact inventory
3. **`configs/project_objective_P1_P2_P3.yaml`** — Program objectives, quality gates, feature lists
4. **`pytest.ini`** — Test configuration with slow_integration marker
5. **`scripts/build_all.sh`** — Full pipeline with dim_employer patch
6. **`conftest.py`** — Root pytest config (chat_tap activation)
7. **`configs/paths.yaml`** — Data root and artifacts root paths
8. **`src/incremental/change_detector.py`** — Incremental change detection (manifest, dependency graph)
9. **`src/export/rag_builder.py`** — RAG chunk generator (98 chunks, 10 topics, 36 source artifacts)
10. **`src/export/qa_generator.py`** — Pre-computed Q&A pairs (178 pairs across 10 topics)
11. **`scripts/test_rag_practical.py`** — Practical end-to-end RAG smoke test (29 checks)

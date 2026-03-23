# P2 Meridian — Project Guardrails

> **Meridian** is the analytical backbone of NorthStar. These guardrails protect data quality, pipeline reproducibility, and the data contract with P3 Compass.
>
> **Read the program-wide Ten Commandments first**: `/Users/vrathod1/dev/NorthStar/northstar-docs/GUARDRAILS.md`

---

## P2 Commandments (Non-Negotiable)

### 1. Parquet is the Canonical Format

All curated tables, feature tables, and model outputs are stored as Parquet files. Never commit raw CSV or XLSX as artifacts. Parquet provides columnar compression, schema enforcement, and type safety that CSV cannot.

**Why**: Parquet is 10× smaller than CSV, preserves types (no string-to-number guessing), and supports partitioning for large tables.

### 2. One Script Per Table

Each `scripts/build_*.py` produces exactly one artifact. Each `src/curate/build_*.py` produces exactly one canonical table. Don't create scripts that output multiple tables or have side effects on other artifacts.

**Why**: Isolation makes debugging easy. If `fact_perm.parquet` is wrong, you look at `build_fact_perm.py`. Period.

### 3. Idempotent Builds

Running a build script twice with the same input must produce identical output. No randomness, no timestamps in output, no external API calls during build. The pipeline is a pure function: `f(P1_data) → P2_artifacts`.

**Why**: Reproducibility is essential for debugging, auditing, and trust. If a colleague runs the same pipeline, they must get the same result.

### 4. NaN → null at Every Serialization Boundary

When exporting any data to JSON (for P3 consumption), `NaN` values must become `null`. Use `pd.DataFrame.to_json(orient="records")` instead of `json.dump()`. P3's `JSON.parse()` does not handle bare `NaN` tokens.

**Why**: This is the single most common source of P2→P3 data corruption. `NaN` in JSON causes silent parsing failures in the browser.

### 5. Never Modify P1 Data

P2 reads P1's `downloads/` directory in-place. Never copy, move, rename, or modify any file in P1's directory tree. P1 data is the immutable ground truth.

**Why**: If P2 modifies P1 data, the P1 manifest becomes stale. Re-running P1 won't know the files changed. The manifest-based incremental system breaks.

### 6. Stage 2d Must Run on Every Full Build

Stage 2d (P3 export artifacts) generates the dashboard-specific metrics that Compass consumes. Skipping it produces a P3 deployment with stale or missing data. `build_all.sh` must always include Stage 2d.

**Why**: Stage 2d outputs (cutoff trends, category movement, backlog estimates, geo metrics, etc.) are the direct inputs to P3's 9 dashboards. A stale Stage 2d = incorrect UI.

### 7. dim_employer is the Source of Truth for Employer Names

Employer name normalization flows through `dim_employer.parquet` (243K+ rows). All fact tables join to `dim_employer` via `employer_id` (SHA1 hash). Never use raw employer name strings as join keys.

**Why**: Raw employer names have inconsistent casing, spacing, and abbreviations across government sources. The dimension table provides a canonical name + stable ID.

### 8. Schemas Are Sacred

`configs/schemas.yml` defines the expected columns and types for every table. Build scripts must validate their output against the schema. Adding or removing a column requires updating the schema file + all downstream consumers.

**Why**: Schema drift is silent. A renamed column in P2 produces no error in P2, but a broken dashboard in P3.

### 9. Test Coverage ≥95% Per Dataset

Every build script must have corresponding tests in `tests/`. Tests validate:
- Schema: columns, types, nullability
- Primary key uniqueness: 100%
- Referential integrity: ≥95%
- Row count assertions: catch regressions
- Golden snapshot: regression vs baseline

**Why**: P2 is the analytical backbone. If P2 data is wrong, every downstream dashboard is wrong. The cost of a P2 bug is magnified across all P3 surfaces.

### 10. The Golden Manifest is the Regression Baseline

`artifacts/metrics/golden_manifest.json` records the expected row counts, schemas, and checksums for all artifacts. The `test_golden_snapshot.py` test suite compares current output against this baseline.

After intentional changes to artifact counts or schemas, regenerate the golden manifest:
```bash
python3 scripts/generate_golden_manifest.py
```

**Why**: Golden snapshots catch unintentional changes. If row counts shift without explanation, something is wrong.

---

## Pipeline Guardrails

| # | Guardrail | Enforcement |
|---|-----------|-------------|
| P1 | Run full pipeline via `bash scripts/build_all.sh` (not individual scripts) | Build script orchestration |
| P2 | Stage order matters: 1 → 1b → 1c → 2 → 2b → 2c → 2d → 3 → 4 | `build_all.sh` sequence |
| P3 | `patch_dim_employer_from_fact_perm.py` must run after Stage 1 curate | Stage 1b dependency |
| P4 | `expand_dim_soc_legacy.py` must run after Stage 1 curate | Stage 1b dependency |
| P5 | Incremental builds (`build_incremental.sh`) detect P1 changes via manifest fingerprint | `src/incremental/change_detector.py` |
| P6 | `check_p1_readiness.py` validates P1 data before pipeline start | Pre-build check |

## Data Quality Guardrails

| # | Guardrail | Enforcement |
|---|-----------|-------------|
| Q1 | Dedup with explicit strategy: document which key wins (`keep="last"`) | Code comments |
| Q2 | Row count assertions in every build script (`assert len(df) > N`) | Build-time validation |
| Q3 | Type hints on all public functions (`def compute_srs(df: pd.DataFrame) -> pd.DataFrame`) | Code convention |
| Q4 | Docstrings on all non-trivial functions | Code convention |
| Q5 | Constants at module top in UPPER_SNAKE_CASE | Code style |
| Q6 | No bare `except` clauses: always catch specific exceptions | Code review |
| Q7 | Bayesian shrinkage for small-sample statistics (EFS prior 0.88, strength 20) | Model quality |
| Q8 | Velocity cap on PD forecasts: `max(1.25 × long_term, long_term + 5 d/mo)` | Model constraints |

## P2→P3 Export Contract Guardrails

| # | Guardrail | Enforcement |
|---|-----------|-------------|
| E1 | All JSON exports use `pd.DataFrame.to_json(orient="records")` for NaN safety | Export scripts |
| E2 | `employer_id` is the stable join key (SHA1 hash, not raw name) | Schema convention |
| E3 | Field names must match TypeScript interfaces in P3's `src/types/p2-artifacts.ts` | Cross-project review |
| E4 | EFS fields remap to SRS in P3 (P2 uses `efs`, P3 uses `srs`) | Loader boundary |
| E5 | All exports documented in `artifacts/metrics/FINAL_SINGLE_REPORT.md` | `generate_final_report.py` |
| E6 | RAG chunks: 341+ text chunks across 10 topics | `src/export/rag_builder.py` |
| E7 | QA cache: 684+ pre-computed pairs (avoid LLM calls 80% of the time) | `src/export/qa_generator.py` |

## Testing Guardrails

| # | Guardrail | Enforcement |
|---|-----------|-------------|
| T1 | Run all tests: `CHAT_TAP_DISABLED=1 python3 -m pytest tests/ -q --tb=short` | Pre-commit |
| T2 | 562+ tests must pass (structural, golden, sanity, smoke) | Quality gate |
| T3 | Golden snapshot tests for regression detection | `test_golden_snapshot.py` |
| T4 | Data sanity tests for business-meaningful assertions | `test_data_sanity.py` |
| T5 | Normalization tests for employer/SOC/country mappings | `test_normalization_mappings.py` |
| T6 | Fixtures use real data slices, not synthetic mocks | Test quality |
| T7 | `conftest.py` for shared fixtures | Test organization |

---

## Cross-Project Impact

When P2 changes affect downstream P3:

| P2 Change | P3 Impact | Required Action |
|-----------|-----------|-----------------|
| New artifact table | New dashboard data | Add type in `p2-artifacts.ts` + loader in `src/lib/data/` + sync in `sync_p2_data.py` |
| Renamed column | Broken data loader | Update `p2-artifacts.ts` + all loaders that reference column + tests |
| Changed employer_id logic | Broken shard lookups | Coordinate with P3 shard generation + search index |
| New RAG topic | New search results | Update P3 topic filter in search engine |
| Changed model output | Incorrect forecasts | Update P3 model consumer + visual components |

**Rule**: Cross-project changes must be documented in commit messages with `[P2→P3]` prefix.

---

## Cross-References

- **Program-wide guardrails (Ten Commandments)**: `/Users/vrathod1/dev/NorthStar/northstar-docs/GUARDRAILS.md`
- **P1 Horizon guardrails**: `/Users/vrathod1/dev/NorthStar/fetch-immigration-data/.github/GUARDRAILS.md`
- **P3 Compass guardrails**: `/Users/vrathod1/dev/NorthStar/immigration-insights-app/.github/GUARDRAILS.md`
- **P2→P3 data contract**: `configs/schemas.yml` (this repo) + `src/types/p2-artifacts.ts` (P3 repo)
- **Architecture**: `ARCHITECTURE.md` (this repo)
- **Artifact inventory**: `artifacts/metrics/FINAL_SINGLE_REPORT.md`

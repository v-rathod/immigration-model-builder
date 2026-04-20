# fact_perm PK Uniqueness Report
Generated: 2026-02-23T05:41:01.042689+00:00  |  Mode: LIVE

**Status:** FIXED (327 dups removed)

| Metric | Value |
|--------|-------|
| Partitions checked | 19 |
| Total rows before  | 1,675,051 |
| Total rows after   | 1,674,724 |
| Dups removed       | 327 |

## Per-File Detail

| FY | File | Before | After | Removed | Note |
|----|------|--------|-------|---------|------|
| 2008 | part-0.parquet | 61,997 | 61,997 | 0 | OK |
| 2009 | part-0.parquet | 38,247 | 38,247 | 0 | OK |
| 2010 | part-0.parquet | 81,412 | 81,412 | 0 | OK |
| 2011 | part-0.parquet | 73,207 | 73,207 | 0 | OK |
| 2012 | part-0.parquet | 66,488 | 66,488 | 0 | OK |
| 2013 | part-0.parquet | 44,152 | 44,152 | 0 | OK |
| 2014 | part-0.parquet | 70,998 | 70,998 | 0 | OK |
| 2015 | part-0.parquet | 89,299 | 89,151 | 148 | OK |
| 2016 | part-0.parquet | 126,143 | 126,143 | 0 | OK |
| 2017 | part-0.parquet | 97,603 | 97,603 | 0 | OK |
| 2018 | part-0.parquet | 119,776 | 119,776 | 0 | OK |
| 2019 | part-0.parquet | 102,655 | 102,655 | 0 | OK |
| 2020 | part-0.parquet | 94,019 | 94,019 | 0 | OK |
| 2021 | part-0.parquet | 108,264 | 108,264 | 0 | OK |
| 2022 | part-0.parquet | 104,600 | 104,600 | 0 | OK |
| 2023 | part-0.parquet | 116,427 | 116,306 | 121 | OK |
| 2024 | part-0.parquet | 114,550 | 114,499 | 51 | OK |
| 2025 | part-0.parquet | 147,056 | 147,056 | 0 | OK |
| 2026 | part-0.parquet | 18,158 | 18,151 | 7 | OK |

## Design Notes

- PK enforced: **(case_number)** within each `fiscal_year=XXXX` partition.
- Cross-FY duplicate case_numbers are **intentional** (DOL annual disclosure overlap).
- Dedup priority: latest `decision_date` > most non-null values > smallest `source_file`.
- Writes are atomic (tmp → unlink → rename).

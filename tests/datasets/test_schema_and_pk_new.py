"""
tests/datasets/test_schema_and_pk_new.py

Validate schema & PK uniqueness for all newly curated datasets:
  dim_visa_ceiling, fact_waiting_list, fact_visa_issuance,
  fact_visa_applications, fact_niv_issuance, fact_uscis_approvals,
  fact_dhs_admissions, fact_warn_events, fact_trac_adjudications (stub),
  fact_acs_wages (stub).

Read-only — never modifies any parquet.
"""
import pathlib

import pandas as pd
import pytest

TABLES = pathlib.Path("artifacts/tables")


def _load(name: str) -> pd.DataFrame:
    p = TABLES / f"{name}.parquet"
    if not p.exists():
        pytest.skip(f"{name}.parquet not found")
    return pd.read_parquet(p)


def _assert_pk(df: pd.DataFrame, pk_cols: list, table: str) -> None:
    missing = [c for c in pk_cols if c not in df.columns]
    if missing:
        pytest.skip(f"{table}: PK columns missing: {missing}")
    dups = df[df.duplicated(subset=pk_cols, keep=False)]
    assert len(dups) == 0, f"{table}: {len(dups)} duplicate rows on PK {pk_cols}"


# ─────────────────────── dim_visa_ceiling ───────────────────────

class TestDimVisaCeiling:
    REQUIRED = ["fiscal_year", "category", "country", "ceiling"]
    PK = ["fiscal_year", "category", "country"]

    def test_required_columns(self):
        df = _load("dim_visa_ceiling")
        for col in self.REQUIRED:
            assert col in df.columns, f"dim_visa_ceiling missing: {col}"

    def test_pk_unique(self):
        df = _load("dim_visa_ceiling")
        _assert_pk(df, self.PK, "dim_visa_ceiling")

    def test_has_rows(self):
        df = _load("dim_visa_ceiling")
        assert len(df) >= 10, f"dim_visa_ceiling: {len(df)} rows < 10"

    def test_ceiling_positive(self):
        df = _load("dim_visa_ceiling")
        neg = (df["ceiling"].dropna() < 0).sum()
        assert neg == 0, f"dim_visa_ceiling: {neg} negative ceiling values"


# ─────────────────────── fact_waiting_list ──────────────────────

class TestFactWaitingList:
    REQUIRED = ["report_year", "category", "country", "count_waiting"]
    PK = ["report_year", "category", "country"]

    def test_required_columns(self):
        df = _load("fact_waiting_list")
        for col in self.REQUIRED:
            assert col in df.columns, f"fact_waiting_list missing: {col}"

    def test_pk_unique(self):
        df = _load("fact_waiting_list")
        _assert_pk(df, self.PK, "fact_waiting_list")

    def test_has_rows(self):
        df = _load("fact_waiting_list")
        assert len(df) >= 1, f"fact_waiting_list is empty"

    def test_count_waiting_non_negative(self):
        df = _load("fact_waiting_list")
        if "count_waiting" in df.columns:
            neg = (pd.to_numeric(df["count_waiting"], errors="coerce").dropna() < 0).sum()
            assert neg == 0, f"fact_waiting_list: {neg} negative count_waiting"


# ─────────────────────── fact_visa_issuance ─────────────────────

class TestFactVisaIssuance:
    REQUIRED = ["fiscal_year", "category", "country", "issued"]
    PK = ["fiscal_year", "category", "country"]

    def test_required_columns(self):
        df = _load("fact_visa_issuance")
        for col in self.REQUIRED:
            assert col in df.columns, f"fact_visa_issuance missing: {col}"

    def test_pk_unique(self):
        df = _load("fact_visa_issuance")
        _assert_pk(df, self.PK, "fact_visa_issuance")

    def test_row_count(self):
        df = _load("fact_visa_issuance")
        assert len(df) >= 10_000, f"fact_visa_issuance: {len(df):,} rows < 10K"

    def test_issued_non_negative(self):
        df = _load("fact_visa_issuance")
        neg = (pd.to_numeric(df["issued"], errors="coerce").dropna() < 0).sum()
        assert neg == 0, f"fact_visa_issuance: {neg} negative issued"

    def test_fiscal_year_format(self):
        df = _load("fact_visa_issuance")
        fy_col = df["fiscal_year"].dropna().astype(str)
        ok = fy_col.str.match(r"^FY\d{4}$").mean()
        assert ok >= 0.95, f"fact_visa_issuance: only {ok:.1%} fiscal_year match FY#### format"


# ──────────────────── fact_visa_applications ────────────────────

class TestFactVisaApplications:
    REQUIRED = ["fiscal_year", "visa_class", "category", "country", "applications"]
    PK = ["fiscal_year", "visa_class", "category", "country"]

    def test_required_columns(self):
        df = _load("fact_visa_applications")
        for col in self.REQUIRED:
            assert col in df.columns, f"fact_visa_applications missing: {col}"

    def test_pk_unique(self):
        df = _load("fact_visa_applications")
        _assert_pk(df, self.PK, "fact_visa_applications")

    def test_row_count(self):
        df = _load("fact_visa_applications")
        assert len(df) >= 10_000, f"fact_visa_applications: {len(df):,} rows < 10K"

    def test_applications_non_negative(self):
        df = _load("fact_visa_applications")
        neg = (pd.to_numeric(df["applications"], errors="coerce").dropna() < 0).sum()
        assert neg == 0, f"fact_visa_applications: {neg} negative applications"


# ────────────────────── fact_niv_issuance ───────────────────────

class TestFactNivIssuance:
    REQUIRED = ["fiscal_year", "visa_class", "country", "issued"]
    PK = ["fiscal_year", "visa_class", "country"]

    def test_required_columns(self):
        df = _load("fact_niv_issuance")
        for col in self.REQUIRED:
            assert col in df.columns, f"fact_niv_issuance missing: {col}"

    def test_pk_unique(self):
        df = _load("fact_niv_issuance")
        _assert_pk(df, self.PK, "fact_niv_issuance")

    def test_row_count(self):
        df = _load("fact_niv_issuance")
        assert len(df) >= 100_000, f"fact_niv_issuance: {len(df):,} rows < 100K"

    def test_issued_non_negative(self):
        df = _load("fact_niv_issuance")
        neg = (pd.to_numeric(df["issued"], errors="coerce").dropna() < 0).sum()
        assert neg == 0, f"fact_niv_issuance: {neg} negative issued"

    def test_fy_format(self):
        df = _load("fact_niv_issuance")
        fy_col = df["fiscal_year"].dropna().astype(str)
        ok = fy_col.str.match(r"^FY\d{4}$").mean()
        assert ok >= 0.90, f"fact_niv_issuance: only {ok:.1%} fiscal_year match FY####; threshold is 90%"

    def test_has_standard_visa_classes(self):
        df = _load("fact_niv_issuance")
        classes = set(df["visa_class"].dropna().str.upper().unique())
        standard = {"H-1B", "L-1", "F-1", "J-1", "B-1", "B-2"}
        found = standard & classes
        assert len(found) >= 2, f"fact_niv_issuance: only {found} standard classes found"


# ──────────────────── fact_uscis_approvals ──────────────────────

class TestFactUscisApprovals:
    REQUIRED = ["fiscal_year", "form", "category", "approvals"]
    PK = ["fiscal_year", "form", "category"]

    def test_required_columns(self):
        df = _load("fact_uscis_approvals")
        for col in self.REQUIRED:
            assert col in df.columns, f"fact_uscis_approvals missing: {col}"

    def test_pk_unique(self):
        df = _load("fact_uscis_approvals")
        _assert_pk(df, self.PK, "fact_uscis_approvals")

    def test_has_rows(self):
        df = _load("fact_uscis_approvals")
        assert len(df) >= 1, "fact_uscis_approvals is empty"

    def test_approvals_non_negative(self):
        df = _load("fact_uscis_approvals")
        neg = (pd.to_numeric(df["approvals"], errors="coerce").dropna() < 0).sum()
        assert neg == 0, f"fact_uscis_approvals: {neg} negative approvals"


# ──────────────────── fact_dhs_admissions ───────────────────────

class TestFactDhsAdmissions:
    REQUIRED = ["fiscal_year", "class_of_admission", "country", "admissions"]
    PK = ["fiscal_year", "class_of_admission", "country"]

    def test_required_columns(self):
        df = _load("fact_dhs_admissions")
        for col in self.REQUIRED:
            assert col in df.columns, f"fact_dhs_admissions missing: {col}"

    def test_pk_unique(self):
        df = _load("fact_dhs_admissions")
        _assert_pk(df, self.PK, "fact_dhs_admissions")

    def test_has_rows(self):
        df = _load("fact_dhs_admissions")
        assert len(df) >= 10, f"fact_dhs_admissions: {len(df)} rows"

    def test_admissions_non_negative(self):
        df = _load("fact_dhs_admissions")
        neg = (pd.to_numeric(df["admissions"], errors="coerce").dropna() < 0).sum()
        assert neg == 0, f"fact_dhs_admissions: {neg} negative admissions"


# ──────────────────── fact_warn_events ──────────────────────────

class TestFactWarnEvents:
    REQUIRED = ["state", "notice_date", "employer_name_raw", "city", "employees_affected"]
    PK = ["state", "notice_date", "employer_name_raw", "city"]

    def test_required_columns(self):
        df = _load("fact_warn_events")
        for col in self.REQUIRED:
            assert col in df.columns, f"fact_warn_events missing: {col}"

    def test_pk_unique(self):
        df = _load("fact_warn_events")
        _assert_pk(df, self.PK, "fact_warn_events")

    def test_has_rows(self):
        df = _load("fact_warn_events")
        assert len(df) >= 100, f"fact_warn_events: {len(df)} rows < 100"

    def test_has_multiple_states(self):
        df = _load("fact_warn_events")
        states = df["state"].dropna().unique()
        assert len(states) >= 2, f"fact_warn_events: only {len(states)} state(s): {states}"

    def test_employees_affected_non_negative(self):
        df = _load("fact_warn_events")
        neg = (pd.to_numeric(df["employees_affected"], errors="coerce").dropna() < 0).sum()
        assert neg == 0, f"fact_warn_events: {neg} negative employees_affected"


# ─────────────────── Stub tables (0 rows expected) ──────────────

class TestStubTables:
    def test_fact_trac_adjudications_stub(self):
        df = _load("fact_trac_adjudications")
        assert len(df) == 0, f"fact_trac_adjudications expected 0 rows, got {len(df)}"
        # Schema check
        for col in ["fiscal_year", "form", "measure", "value"]:
            assert col in df.columns, f"fact_trac_adjudications stub missing column: {col}"

    def test_fact_acs_wages_stub(self):
        df = _load("fact_acs_wages")
        assert len(df) == 0, f"fact_acs_wages expected 0 rows, got {len(df)}"
        for col in ["year", "soc_code", "area_code"]:
            assert col in df.columns, f"fact_acs_wages stub missing column: {col}"

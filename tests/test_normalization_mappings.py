"""
Unit tests for src/normalize/mappings.py.

All tests are pure (no file I/O) and fast — suitable for the default test run.
Tests cover employer name normalization, SOC code normalization, country code
normalization, and visa category normalization.
"""
from __future__ import annotations

import pytest

from src.normalize.mappings import (
    normalize_employer_name,
    normalize_soc_code,
    normalize_country_code,
    normalize_visa_category,
    title_case_employer_name,
)


# ===========================================================================
# normalize_employer_name
# ===========================================================================

class TestNormalizeEmployerName:
    """normalize_employer_name() should produce identical output for names
    that refer to the same employer, regardless of capitalisation, punctuation,
    or legal suffix variation."""

    def test_lowercase_same_as_uppercase(self):
        assert normalize_employer_name("GOOGLE INC") == normalize_employer_name("google inc")

    def test_punctuation_trailing_comma(self):
        """'GOOGLE INC,' should equal 'GOOGLE INC' after normalization."""
        assert normalize_employer_name("GOOGLE INC,") == normalize_employer_name("GOOGLE INC")

    def test_mixed_case_with_period(self):
        """'Google Inc.' should equal 'GOOGLE INC' (same canonical key)."""
        assert normalize_employer_name("Google Inc.") == normalize_employer_name("GOOGLE INC")

    def test_llc_variant(self):
        """LLC and Inc should both be stripped."""
        assert normalize_employer_name("Google LLC") == normalize_employer_name("Google Inc")

    def test_corporation_suffix(self):
        assert normalize_employer_name("Microsoft Corporation") == normalize_employer_name("MICROSOFT")

    def test_corp_suffix(self):
        assert normalize_employer_name("Microsoft Corp") == normalize_employer_name("Microsoft Corp.")

    def test_ltd_suffix(self):
        assert normalize_employer_name("Some Company Ltd") == normalize_employer_name("Some Company")

    def test_llp_suffix(self):
        assert normalize_employer_name("Deloitte LLP") == normalize_employer_name("Deloitte")

    def test_whitespace_collapse(self):
        assert normalize_employer_name("  Amazon  Web   Services  ") == "amazon web services"

    def test_none_returns_empty(self):
        assert normalize_employer_name(None) == ""  # type: ignore[arg-type]

    def test_empty_string_returns_empty(self):
        assert normalize_employer_name("") == ""

    def test_result_is_lowercase(self):
        result = normalize_employer_name("INFOSYS LIMITED")
        assert result == result.lower()

    def test_no_trailing_whitespace(self):
        result = normalize_employer_name("Accenture  ")
        assert result == result.strip()

    def test_semicolon_removed(self):
        assert normalize_employer_name("Tata; Consultancy") == "tata consultancy"

    def test_parentheses_removed(self):
        assert normalize_employer_name("Amazon (AWS)") == "amazon aws"

    def test_tcs_deduplication(self):
        """All common TCS variants should normalize to the same key."""
        variants = [
            "Tata Consultancy Services Limited",
            "TATA CONSULTANCY SERVICES LIMITED",
            "Tata Consultancy Services Ltd",
            "TATA CONSULTANCY SERVICES LTD.",
            "Tata Consultancy Services",
        ]
        results = {normalize_employer_name(v) for v in variants}
        assert len(results) == 1, f"Expected 1 unique result, got {len(results)}: {results}"

    def test_google_deduplication(self):
        """All common Google variants should normalize to the same key."""
        variants = [
            "Google Inc",
            "GOOGLE INC",
            "Google Inc.",
            "GOOGLE INC.",
            "GOOGLE INC,",
            "Google LLC",
            "GOOGLE LLC",
        ]
        results = {normalize_employer_name(v) for v in variants}
        assert len(results) == 1, f"Expected 1 unique result, got {len(results)}: {results}"

    def test_microsoft_deduplication(self):
        variants = [
            "Microsoft Corporation",
            "MICROSOFT CORPORATION",
            "Microsoft Corp",
            "Microsoft Corp.",
        ]
        results = {normalize_employer_name(v) for v in variants}
        assert len(results) == 1, f"Expected 1 unique result, got {len(results)}: {results}"

    def test_distinct_employers_differ(self):
        """Google and Amazon should NOT normalize to the same key."""
        assert normalize_employer_name("Google Inc") != normalize_employer_name("Amazon Inc")

    def test_amazon_web_services_distinct_from_amazon(self):
        """'Amazon Web Services' and 'Amazon' are different entities."""
        assert normalize_employer_name("Amazon Web Services") != normalize_employer_name("Amazon")


# ===========================================================================
# title_case_employer_name
# ===========================================================================

class TestTitleCaseEmployerName:
    def test_single_word(self):
        assert title_case_employer_name("google") == "Google"

    def test_multi_word(self):
        assert title_case_employer_name("amazon web services") == "Amazon Web Services"

    def test_empty_returns_empty(self):
        assert title_case_employer_name("") == ""

    def test_already_formatted(self):
        assert title_case_employer_name("microsoft") == "Microsoft"


# ===========================================================================
# normalize_soc_code
# ===========================================================================

class TestNormalizeSocCode:
    def test_standard_format_unchanged(self):
        assert normalize_soc_code("15-1252") == "15-1252"

    def test_decimal_stripped(self):
        assert normalize_soc_code("15-1252.00") == "15-1252"

    def test_no_dash_6_digits(self):
        assert normalize_soc_code("151252") == "15-1252"

    def test_no_dash_8_digits_with_decimal_part(self):
        """'15125200' should produce '15-1252'."""
        assert normalize_soc_code("15125200") == "15-1252"

    def test_whitespace_stripped(self):
        assert normalize_soc_code("  15-1252  ") == "15-1252"

    def test_none_returns_none(self):
        assert normalize_soc_code(None) is None  # type: ignore[arg-type]

    def test_empty_returns_none(self):
        assert normalize_soc_code("") is None

    def test_too_short_returns_none(self):
        """Only 4 digits should not produce a valid code."""
        assert normalize_soc_code("1512") is None

    def test_soc_11_with_decimal(self):
        """SOC code for Software Developers."""
        assert normalize_soc_code("15-1132.00") == "15-1132"

    def test_different_soc_groups(self):
        assert normalize_soc_code("29-1141") == "29-1141"  # Registered Nurses
        assert normalize_soc_code("13-2011") == "13-2011"  # Accountants


# ===========================================================================
# normalize_country_code
# ===========================================================================

class TestNormalizeCountryCode:
    def test_india_exact(self):
        assert normalize_country_code("india") == "IND"

    def test_india_uppercase(self):
        assert normalize_country_code("INDIA") == "IND"

    def test_india_mixed_case(self):
        assert normalize_country_code("India") == "IND"

    def test_china_mainland_variant(self):
        assert normalize_country_code("CHINA-mainland born") == "CHN"
        assert normalize_country_code("China mainland born") == "CHN"

    def test_china_plain(self):
        assert normalize_country_code("china") == "CHN"

    def test_mexico(self):
        assert normalize_country_code("Mexico") == "MEX"

    def test_philippines(self):
        assert normalize_country_code("Philippines") == "PHL"

    def test_row_chargeability(self):
        result = normalize_country_code("All Chargeability Areas Except Those Listed")
        assert result == "ROW"

    def test_row_direct(self):
        assert normalize_country_code("ROW") == "ROW"

    def test_iso3_passthrough(self):
        assert normalize_country_code("IND") == "IND"
        assert normalize_country_code("CHN") == "CHN"

    def test_iso2_india(self):
        assert normalize_country_code("IN") == "IND"

    def test_iso2_china(self):
        assert normalize_country_code("CN") == "CHN"

    def test_unknown_returns_none(self):
        assert normalize_country_code("unknown country xyz") is None

    def test_none_returns_none(self):
        assert normalize_country_code(None) is None  # type: ignore[arg-type]

    def test_empty_returns_none(self):
        assert normalize_country_code("") is None


# ===========================================================================
# normalize_visa_category
# ===========================================================================

class TestNormalizeVisaCategory:
    def test_eb2_hyphen(self):
        assert normalize_visa_category("EB-2") == "EB2"

    def test_eb2_no_hyphen(self):
        assert normalize_visa_category("eb2") == "EB2"

    def test_eb3_uppercase(self):
        assert normalize_visa_category("EB-3") == "EB3"

    def test_eb2_niw(self):
        assert normalize_visa_category("EB-2 NIW") == "EB2"

    def test_h1b_variants(self):
        assert normalize_visa_category("H-1B") == "H-1B"
        assert normalize_visa_category("h1b") == "H-1B"
        assert normalize_visa_category("H1B") == "H-1B"

    def test_e3_variant(self):
        assert normalize_visa_category("E-3") == "E-3"

    def test_eb1(self):
        assert normalize_visa_category("EB-1") == "EB1"

    def test_unknown_returns_none(self):
        assert normalize_visa_category("UNKNOWN") is None

    def test_none_returns_none(self):
        assert normalize_visa_category(None) is None  # type: ignore[arg-type]

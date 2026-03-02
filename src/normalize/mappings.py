"""
Country, SOC code, employer name, and visa category normalization logic.

All functions are pure (no side effects) and safe for use in both
DataFrame .apply() calls and unit tests.
"""
from __future__ import annotations

import re
from typing import Optional


# ---------------------------------------------------------------------------
# Employer name normalization
# ---------------------------------------------------------------------------

# Legal suffixes stripped during normalization (order matters — longer first)
_EMPLOYER_SUFFIXES: tuple[str, ...] = (
    "corporation", "incorporated", "limited partnership", "limited liability company",
    "limited liability partnership", "public limited company",
    "corp", "inc", "llc", "llp", "ltd", "limited", "co", "plc", "pvt", "lp", "gmbh",
    "s.a.", "s.a.s.", "l.l.c.", "l.l.p.", "inc.", "corp.", "ltd.", "co.", "lp.",
)

# Characters to collapse to a single space
_PUNCT_RE = re.compile(r"[,;.&/\\'\"\(\)\:\-\+\#]+")
# Collapse runs of whitespace
_SPACE_RE = re.compile(r"\s+")


def normalize_employer_name(raw_name: str) -> str:
    """Normalize employer name for deduplication/canonical form.

    Pipeline:
        1. Strip leading/trailing whitespace
        2. Lowercase
        3. Replace punctuation with space
        4. Remove legal suffixes (word-boundary aware)
        5. Collapse whitespace
        6. Final strip

    Returns:
        Lowercase normalized string, or "" if input is blank/None.

    Examples:
        >>> normalize_employer_name("GOOGLE INC,")
        'google'
        >>> normalize_employer_name("Google Inc.")
        'google'
        >>> normalize_employer_name("Microsoft Corporation")
        'microsoft'
        >>> normalize_employer_name("Amazon.com Services LLC")
        'amazoncom services'
        >>> normalize_employer_name("Tata Consultancy Services Limited")
        'tata consultancy services'
    """
    if not raw_name:
        return ""
    name = str(raw_name).strip().lower()

    # Replace punctuation with space
    name = _PUNCT_RE.sub(" ", name)

    # Remove legal suffixes (word boundary, longest first)
    for suffix in _EMPLOYER_SUFFIXES:
        pattern = r"\b" + re.escape(suffix) + r"\b"
        name = re.sub(pattern, " ", name)

    # Collapse and strip
    name = _SPACE_RE.sub(" ", name).strip()

    return name


def title_case_employer_name(normalized: str) -> str:
    """Convert a normalized employer name to Title Case for display.

    Examples:
        >>> title_case_employer_name("google")
        'Google'
        >>> title_case_employer_name("amazon web services")
        'Amazon Web Services'
    """
    if not normalized:
        return ""
    return " ".join(w.capitalize() for w in normalized.split())


# ---------------------------------------------------------------------------
# SOC code normalization
# ---------------------------------------------------------------------------

_SOC_DIGITS_RE = re.compile(r"\d+")


def normalize_soc_code(raw_soc: str) -> Optional[str]:
    """Normalize an occupational code to the standard 7-character format XX-XXXX.

    Handles:
        - "15-1252.00"  → "15-1252"
        - "151252"      → "15-1252"
        - "15-1252"     → "15-1252"
        - "15-125200"   → "15-1252"
        - Whitespace/extra chars stripped

    Returns:
        Normalized "XX-XXXX" string, or None if input cannot be parsed.

    Examples:
        >>> normalize_soc_code("15-1252.00")
        '15-1252'
        >>> normalize_soc_code("151252")
        '15-1252'
        >>> normalize_soc_code("15-1252")
        '15-1252'
    """
    if not raw_soc:
        return None
    s = str(raw_soc).strip()

    # Already correct format
    if re.match(r"^\d{2}-\d{4}$", s):
        return s

    # Strip decimal detail: "15-1252.00" or "15-125200"
    s = s.split(".")[0]

    # Remove the dash and collect digits
    digits = "".join(_SOC_DIGITS_RE.findall(s))

    if len(digits) >= 6:
        major = digits[:2]
        minor = digits[2:6]
        return f"{major}-{minor}"

    return None


# ---------------------------------------------------------------------------
# Country code normalization
# ---------------------------------------------------------------------------

# Maps visa-bulletin raw country strings → ISO-3166 alpha-3 codes used in P2
_COUNTRY_RAW_TO_ISO3: dict[str, str] = {
    # China variants
    "china": "CHN",
    "china-mainland born": "CHN",
    "china mainland born": "CHN",
    "china mainland": "CHN",
    "mainland china": "CHN",
    "prc": "CHN",
    "chinese mainland": "CHN",
    # India variants
    "india": "IND",
    "republic of india": "IND",
    # Mexico variants
    "mexico": "MEX",
    "united mexican states": "MEX",
    # Philippines variants
    "philippines": "PHL",
    "republic of the philippines": "PHL",
    # ROW catch-alls (visa bulletin "All Chargeability" language)
    "all chargeability areas except those listed": "ROW",
    "all chargeability": "ROW",
    "rest of world": "ROW",
    "row": "ROW",
    "other": "ROW",
    # Common ISO-2 → ISO-3 pass-throughs (P1 sometimes uses alpha-2)
    "in": "IND",
    "cn": "CHN",
    "mx": "MEX",
    "ph": "PHL",
}


def normalize_country_code(raw_country: str) -> Optional[str]:
    """Map a raw country string to a canonical ISO-3166 alpha-3 code.

    Matching is case-insensitive and strips leading/trailing whitespace.
    If the input is already a recognized alpha-3 code (e.g. "IND"), it is
    returned as-is (uppercased). Unknown strings return None.

    Examples:
        >>> normalize_country_code("CHINA-mainland born")
        'CHN'
        >>> normalize_country_code("india")
        'IND'
        >>> normalize_country_code("All Chargeability Areas Except Those Listed")
        'ROW'
        >>> normalize_country_code("IND")
        'IND'
        >>> normalize_country_code("unknown country")
        None
    """
    if not raw_country:
        return None
    key = str(raw_country).strip().lower()
    if key in _COUNTRY_RAW_TO_ISO3:
        return _COUNTRY_RAW_TO_ISO3[key]
    # If it looks like an ISO-3 code already, pass through uppercased
    if re.match(r"^[a-z]{3}$", key):
        return key.upper()
    return None


# ---------------------------------------------------------------------------
# Visa category normalization
# ---------------------------------------------------------------------------

_EB_CLASS_MAP: dict[str, str] = {
    # EB-1 variants
    "eb-1": "EB1", "eb1": "EB1", "e1": "EB1",
    "eb-1a": "EB1", "eb-1b": "EB1", "eb-1c": "EB1",
    # EB-2 variants
    "eb-2": "EB2", "eb2": "EB2", "e2": "EB2",
    "eb-2 niw": "EB2", "eb2-niw": "EB2",
    # EB-3 variants
    "eb-3": "EB3", "eb3": "EB3", "e3": "EB3",
    "eb-3w": "EB3", "eb3w": "EB3",
    # EB-4/5
    "eb-4": "EB4", "eb4": "EB4",
    "eb-5": "EB5", "eb5": "EB5",
    # H-1B variants
    "h-1b": "H-1B", "h1b": "H-1B", "h1-b": "H-1B",
    "h-1b1": "H-1B1", "h1b1": "H-1B1",
    "e-3": "E-3", "e3d": "E-3",
}


def normalize_visa_category(raw_category: str) -> Optional[str]:
    """Normalize a visa category string to a canonical short form.

    Examples:
        >>> normalize_visa_category("EB-2 NIW")
        'EB2'
        >>> normalize_visa_category("eb3")
        'EB3'
        >>> normalize_visa_category("H1B")
        'H-1B'
        >>> normalize_visa_category("unknown")
        None
    """
    if not raw_category:
        return None
    key = str(raw_category).strip().lower()
    return _EB_CLASS_MAP.get(key)


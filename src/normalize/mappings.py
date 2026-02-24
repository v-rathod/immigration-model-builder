"""Country, SOC code, and employer name normalization logic."""

from typing import Optional


def normalize_country_code(raw_country: str) -> Optional[str]:
    """Normalize country names to standard codes.
    
    Args:
        raw_country: Raw country string from source data
        
    Returns:
        ISO country code or None
        
    TODO: Implement mapping for:
        - "CHINA-mainland born" -> "CN"
        - "INDIA" -> "IN"
        - "All Chargeability Areas Except Those Listed" -> "ROW"
    """
    # Placeholder
    return raw_country.upper() if raw_country else None


def normalize_soc_code(raw_soc: str) -> Optional[str]:
    """Normalize SOC codes to standard format.
    
    Args:
        raw_soc: Raw SOC code (may have different formats)
        
    Returns:
        Normalized SOC code (e.g., "15-1252")
        
    TODO: Handle various formats:
        - "15-1252.00" -> "15-1252"
        - "151252" -> "15-1252"
    """
    # Placeholder
    return raw_soc.strip() if raw_soc else None


def normalize_employer_name(raw_name: str) -> str:
    """Normalize employer names for deduplication.
    
    Args:
        raw_name: Raw employer name from source
        
    Returns:
        Normalized name (lowercase, stripped, standardized)
        
    TODO: Implement fuzzy matching and common abbreviation expansion:
        - "Microsoft Corporation" vs "MICROSOFT CORP"
        - "Google LLC" vs "Google Inc."
    """
    # Placeholder: simple lowercase strip
    return raw_name.lower().strip() if raw_name else ""


# TODO: Add visa category normalization (EB-2 vs EB2 vs E2)
# TODO: Add geography/MSA code mappings for OEWS data

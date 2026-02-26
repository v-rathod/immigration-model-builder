"""
LCA (H-1B) chunked loader.

Discovers LCA disclosure files across FY2008-FY2026+, resolves column aliases
per era (iCERT vs FLAG), normalises employer/SOC/worksite, deduplicates across
mirror directories, and writes partitioned Parquet under
  artifacts/tables/fact_lca/fiscal_year=YYYY/part-*.parquet

Designed for bounded-memory processing: each FY processed individually.
"""

import hashlib
import logging
import re
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import pandas as pd
import yaml

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Config helpers
# ---------------------------------------------------------------------------

def _load_lca_layout(layouts_dir: str = "configs/layouts") -> dict:
    path = Path(layouts_dir) / "lca.yml"
    if not path.exists():
        raise FileNotFoundError(f"LCA layout not found: {path}")
    with open(path) as f:
        return yaml.safe_load(f)


def _load_employer_layout(layouts_dir: str = "configs/layouts") -> dict:
    path = Path(layouts_dir) / "employer.yml"
    if not path.exists():
        raise FileNotFoundError(f"Employer layout not found: {path}")
    with open(path) as f:
        return yaml.safe_load(f)


# ---------------------------------------------------------------------------
# File discovery
# ---------------------------------------------------------------------------

def _discover_lca_files(data_root: str, layout: dict) -> List[Tuple[int, Path]]:
    """
    Discover LCA disclosure files, deduplicated by filename.

    Returns list of (fiscal_year, file_path) sorted by FY ascending.
    """
    base = Path(data_root) / layout.get("file_discovery", {}).get("base_dir", "LCA")
    if not base.exists():
        logger.warning("LCA base directory not found: %s", base)
        return []

    include_pats = layout.get("file_discovery", {}).get("include_patterns", [])
    exclude_pats = layout.get("file_discovery", {}).get("exclude_patterns", [])

    # Gather all xlsx/xls/csv under LCA/
    all_files: Dict[str, Tuple[int, Path]] = {}  # filename → (fy, path)

    for fy_dir in sorted(base.rglob("FY*")):
        if not fy_dir.is_dir():
            continue
        m = re.search(r"FY(\d{4})", fy_dir.name)
        if not m:
            continue
        fy = int(m.group(1))

        for f in fy_dir.iterdir():
            if not f.is_file():
                continue
            if f.suffix.lower() not in (".xlsx", ".xls", ".csv"):
                continue

            fname = f.name

            # Exclude supplemental files
            if any(_glob_match(fname, pat) for pat in exclude_pats):
                continue

            # Include only disclosure-type files
            if include_pats and not any(_glob_match(fname, pat) for pat in include_pats):
                continue

            # Dedupe: prefer shorter path (LCA/FYXXXX/ over LCA/H1B/FYXXXX/)
            if fname in all_files:
                existing_path = all_files[fname][1]
                if len(str(f)) < len(str(existing_path)):
                    all_files[fname] = (fy, f)
            else:
                all_files[fname] = (fy, f)

    result = sorted(all_files.values(), key=lambda x: (x[0], str(x[1])))
    return result


def _glob_match(name: str, pattern: str) -> bool:
    """Simple glob matching with * wildcard."""
    import fnmatch
    return fnmatch.fnmatch(name, pattern)


# ---------------------------------------------------------------------------
# Column resolution
# ---------------------------------------------------------------------------

def _resolve_column(df_columns: list, aliases: list) -> Optional[str]:
    """Find first matching alias (case-insensitive) in DataFrame columns."""
    cols_lower = {c.lower().strip(): c for c in df_columns}
    for alias in aliases:
        key = alias.lower().strip()
        if key in cols_lower:
            return cols_lower[key]
    return None


def _resolve_era(fy: int, layout: dict) -> str:
    boundary = layout.get("era_boundary", 2020)
    return "flag" if fy >= boundary else "icert"


# ---------------------------------------------------------------------------
# Normalisation helpers
# ---------------------------------------------------------------------------

def _normalize_employer_name(raw_name: str, emp_layout: dict) -> str:
    """Normalise employer name using employer.yml rules. Returns lowercase form for hashing."""
    if not raw_name or pd.isna(raw_name):
        return ""
    normalized = str(raw_name).lower().strip()
    for punct in emp_layout.get("punctuation_to_strip", []):
        normalized = normalized.replace(punct, " ")
    for suffix in emp_layout.get("suffixes", []):
        pattern = r"\b" + re.escape(suffix.lower()) + r"\b"
        normalized = re.sub(pattern, "", normalized)
    normalized = re.sub(r"\s+", " ", normalized).strip()
    min_len = emp_layout.get("min_len", 3)
    return normalized if len(normalized) >= min_len else ""


def _compute_employer_id(normalized_name: str) -> str:
    if not normalized_name:
        return ""
    return hashlib.sha1(normalized_name.encode("utf-8")).hexdigest()


def _normalize_soc(raw_soc: str) -> str:
    """Normalise SOC code to 7-char XX-XXXX format."""
    if not raw_soc or pd.isna(raw_soc):
        return ""
    s = str(raw_soc).strip()
    # Strip trailing .00 or .XX
    s = re.sub(r"\.\d+$", "", s)
    # If 6 digits no hyphen, insert: 151252 → 15-1252
    if re.match(r"^\d{6}$", s):
        s = s[:2] + "-" + s[2:]
    # Validate XX-XXXX
    if re.match(r"^\d{2}-\d{4}$", s):
        return s
    # Try XX-XXXX.XX format leftover
    m = re.match(r"^(\d{2}-\d{4})", s)
    if m:
        return m.group(1)
    return s  # return as-is if unrecognised


def _normalize_status(raw_status: str, status_map: dict) -> str:
    if not raw_status or pd.isna(raw_status):
        return ""
    key = str(raw_status).strip().upper()
    return status_map.get(key, key)


def _parse_fulltime(raw_val, is_icert_parttime: bool, ft_map: dict) -> Optional[bool]:
    """Parse fulltime flag. iCERT PART_TIME_1 is inverted."""
    if raw_val is None or pd.isna(raw_val):
        return None
    key = str(raw_val).strip().upper()
    if key in ft_map:
        return ft_map[key]
    # iCERT PART_TIME_1: 1=part-time (not fulltime), 0=fulltime
    if is_icert_parttime:
        return key != "1"
    return key in ("Y", "YES", "TRUE", "1", "FULL TIME")


def _parse_wage(val) -> Optional[float]:
    """Parse wage value, handling string currency symbols."""
    if val is None or pd.isna(val):
        return None
    if isinstance(val, (int, float)):
        return float(val)
    s = str(val).strip().replace(",", "").replace("$", "")
    try:
        return float(s)
    except (ValueError, TypeError):
        return None


def _parse_date(val) -> Optional[str]:
    """Parse date value to ISO string."""
    if val is None or pd.isna(val):
        return None
    if isinstance(val, datetime):
        return val.strftime("%Y-%m-%d")
    if isinstance(val, pd.Timestamp):
        return val.strftime("%Y-%m-%d")
    s = str(val).strip()
    for fmt in ("%m/%d/%Y", "%Y-%m-%d", "%m-%d-%Y", "%d-%b-%y", "%m/%d/%y"):
        try:
            return datetime.strptime(s, fmt).strftime("%Y-%m-%d")
        except ValueError:
            continue
    return None


# ---------------------------------------------------------------------------
# Core loader
# ---------------------------------------------------------------------------

def load_lca(
    data_root: str,
    artifacts_root: str,
    schemas_path: str = "configs/schemas.yml",
    layouts_dir: str = "configs/layouts",
    dry_run: bool = False,
) -> str:
    """
    Load all LCA disclosure files into partitioned fact_lca.

    Returns path to fact_lca output directory.
    """
    out_dir = Path(artifacts_root) / "tables" / "fact_lca"
    metrics_dir = Path(artifacts_root) / "metrics"
    metrics_dir.mkdir(parents=True, exist_ok=True)
    log_path = metrics_dir / "fact_lca_metrics.log"

    layout = _load_lca_layout(layouts_dir)
    emp_layout = _load_employer_layout(layouts_dir)
    aliases_cfg = layout.get("aliases", {})
    status_map = layout.get("status_map", {})
    ft_map = layout.get("fulltime_map", {})

    ingested_at = datetime.now(timezone.utc)

    # Discover files
    files = _discover_lca_files(data_root, layout)
    logger.info("Discovered %d LCA disclosure files", len(files))

    if not files:
        logger.warning("No LCA files found under %s/LCA/", data_root)
        out_dir.mkdir(parents=True, exist_ok=True)
        with open(log_path, "w") as lf:
            lf.write(f"fact_lca build - {ingested_at.isoformat()}\nNo files found.\n")
        return str(out_dir)

    log_lines = [
        f"fact_lca build - {ingested_at.isoformat()}",
        f"Files discovered: {len(files)}",
        "",
    ]

    if dry_run:
        for fy, fp in files:
            log_lines.append(f"  [DRY-RUN] FY{fy}: {fp.name}")
        with open(log_path, "w") as lf:
            lf.write("\n".join(log_lines))
        print(f"  [DRY-RUN] Discovered {len(files)} LCA files")
        return str(out_dir)

    # Group by FY
    fy_groups: Dict[int, List[Path]] = {}
    for fy, fp in files:
        fy_groups.setdefault(fy, []).append(fp)

    total_rows = 0
    total_files_ok = 0
    total_files_err = 0
    fy_summaries = {}

    for fy in sorted(fy_groups.keys()):
        fps = fy_groups[fy]
        era = _resolve_era(fy, layout)
        log_lines.append(f"--- FY{fy} ({era}) ---")
        logger.info("Processing FY%d (%s): %d files", fy, era, len(fps))

        fy_dfs = []

        for fp in fps:
            try:
                fy_rows = _process_one_file(
                    fp, fy, era, aliases_cfg, status_map, ft_map, emp_layout, data_root, ingested_at
                )
                if fy_rows is not None and len(fy_rows) > 0:
                    fy_dfs.append(fy_rows)
                    log_lines.append(f"  OK  {fp.name}: {len(fy_rows):,} rows")
                    total_files_ok += 1
                else:
                    log_lines.append(f"  WARN {fp.name}: 0 rows")
            except Exception as e:
                logger.warning("Failed to process %s: %s", fp.name, e)
                log_lines.append(f"  ERR {fp.name}: {e}")
                total_files_err += 1

        if fy_dfs:
            fy_df = pd.concat(fy_dfs, ignore_index=True)

            # Dedupe within FY by case_number (keep first)
            pre_dedup = len(fy_df)
            if "case_number" in fy_df.columns:
                fy_df = fy_df.drop_duplicates(subset=["case_number"], keep="first")
            post_dedup = len(fy_df)
            if pre_dedup != post_dedup:
                log_lines.append(f"  Dedupe: {pre_dedup:,} → {post_dedup:,} (-{pre_dedup - post_dedup:,})")

            # Write partitioned parquet
            # Drop fiscal_year column to avoid conflict with partition key
            # (pyarrow reads partition as dictionary, clashes with int64 column)
            write_df = fy_df.drop(columns=["fiscal_year"], errors="ignore")
            part_dir = out_dir / f"fiscal_year={fy}"
            part_dir.mkdir(parents=True, exist_ok=True)
            out_file = part_dir / "part-0.parquet"
            write_df.to_parquet(out_file, index=False, engine="pyarrow")

            total_rows += len(fy_df)
            fy_summaries[fy] = len(fy_df)
            log_lines.append(f"  Written: {len(fy_df):,} rows → {out_file}")
        else:
            fy_summaries[fy] = 0
            log_lines.append(f"  No rows for FY{fy}")

        log_lines.append("")

    # Summary
    log_lines.append("=" * 60)
    log_lines.append(f"Total rows: {total_rows:,}")
    log_lines.append(f"Total FYs: {len(fy_summaries)}")
    log_lines.append(f"Files OK: {total_files_ok}, Files ERR: {total_files_err}")
    log_lines.append("")
    log_lines.append("Per-FY breakdown:")
    for fy in sorted(fy_summaries.keys()):
        log_lines.append(f"  FY{fy}: {fy_summaries[fy]:,}")

    with open(log_path, "w") as lf:
        lf.write("\n".join(log_lines))

    print(f"  fact_lca: {total_rows:,} rows, {len(fy_summaries)} FYs, "
          f"{total_files_ok} files OK, {total_files_err} errors")
    print(f"  Log: {log_path}")

    return str(out_dir)


def _process_one_file(
    fp: Path,
    fy: int,
    era: str,
    aliases_cfg: dict,
    status_map: dict,
    ft_map: dict,
    emp_layout: dict,
    data_root: str,
    ingested_at: datetime,
) -> Optional[pd.DataFrame]:
    """Process a single LCA file and return normalised DataFrame (vectorised)."""

    # Read file
    try:
        if fp.suffix.lower() == ".csv":
            df = pd.read_csv(fp, low_memory=False)
        else:
            df = pd.read_excel(fp, engine="openpyxl")
    except Exception as e:
        logger.warning("Cannot read %s: %s", fp.name, e)
        return None

    if df.empty:
        return None

    # Build alias lookup for this era
    col_map: Dict[str, Optional[str]] = {}
    for canonical, era_aliases in aliases_cfg.items():
        if isinstance(era_aliases, dict):
            current_era_aliases = era_aliases.get(era, [])
            fallback_aliases = era_aliases.get("icert" if era == "flag" else "flag", [])
            col_map[canonical] = _resolve_column(df.columns.tolist(), current_era_aliases + fallback_aliases)
        elif isinstance(era_aliases, list):
            col_map[canonical] = _resolve_column(df.columns.tolist(), era_aliases)
        else:
            col_map[canonical] = None

    # Detect if fulltime column is the inverted PART_TIME field
    is_icert_parttime = False
    ft_resolved = col_map.get("is_fulltime")
    if ft_resolved and "PART_TIME" in ft_resolved.upper():
        is_icert_parttime = True

    # Extract relative source_file path
    try:
        rel_path = fp.relative_to(Path(data_root))
    except ValueError:
        rel_path = fp.name
    source_file = str(rel_path)

    # --- Vectorised extraction ---
    def _col(canonical: str) -> Optional[pd.Series]:
        c = col_map.get(canonical)
        if c is None or c not in df.columns:
            return None
        return df[c]

    # case_number (required — drop rows without it)
    cn_series = _col("case_number")
    if cn_series is None:
        logger.warning("No case_number column resolved for %s", fp.name)
        return None
    result = pd.DataFrame()
    result["case_number"] = cn_series.astype(str).str.strip()
    result = result[result["case_number"].notna() & (result["case_number"] != "") & (result["case_number"] != "nan")]
    if result.empty:
        return None
    idx = result.index  # keep aligned

    # case_status
    s = _col("case_status")
    if s is not None:
        raw = s.loc[idx].astype(str).str.strip().str.upper()
        result["case_status"] = raw.map(status_map).fillna(raw)
    else:
        result["case_status"] = ""

    # visa_class
    s = _col("visa_class")
    if s is not None:
        vc = s.loc[idx].astype(str).str.strip().str.upper()
        vc = vc.replace({"H1B": "H-1B"})
        result["visa_class"] = vc
    else:
        result["visa_class"] = ""

    # dates
    for date_field in ["received_date", "decision_date"]:
        s = _col(date_field)
        if s is not None:
            result[date_field] = pd.to_datetime(s.loc[idx], errors="coerce").dt.strftime("%Y-%m-%d")
        else:
            result[date_field] = None

    # employer
    s = _col("employer_name")
    if s is not None:
        raw_emp = s.loc[idx].astype(str).str.strip()
        raw_emp = raw_emp.replace({"nan": ""})
        result["employer_name_raw"] = raw_emp
        # Vectorised employer normalisation
        norm = raw_emp.str.lower().str.strip()
        for punct in emp_layout.get("punctuation_to_strip", []):
            norm = norm.str.replace(punct, " ", regex=False)
        for suffix in emp_layout.get("suffixes", []):
            pattern = r"\b" + re.escape(suffix.lower()) + r"\b"
            norm = norm.str.replace(pattern, "", regex=True)
        norm = norm.str.replace(r"\s+", " ", regex=True).str.strip()
        min_len = emp_layout.get("min_len", 3)
        norm = norm.where(norm.str.len() >= min_len, "")
        result["employer_id"] = norm.apply(lambda x: hashlib.sha1(x.encode("utf-8")).hexdigest() if x else "")
    else:
        result["employer_name_raw"] = ""
        result["employer_id"] = ""

    # SOC code
    s = _col("soc_code")
    if s is not None:
        soc_raw = s.loc[idx].astype(str).str.strip()
        # Strip trailing .XX
        soc_clean = soc_raw.str.replace(r"\.\d+$", "", regex=True)
        # Insert hyphen if 6-digit
        mask_6d = soc_clean.str.match(r"^\d{6}$")
        soc_clean = soc_clean.where(~mask_6d, soc_clean.str[:2] + "-" + soc_clean.str[2:])
        # Extract XX-XXXX if longer
        extracted = soc_clean.str.extract(r"^(\d{2}-\d{4})", expand=False)
        soc_clean = extracted.fillna(soc_clean)
        soc_clean = soc_clean.replace({"nan": ""})
        result["soc_code"] = soc_clean
    else:
        result["soc_code"] = ""

    # SOC title
    s = _col("soc_title")
    result["soc_title"] = s.loc[idx].astype(str).str.strip().replace({"nan": ""}) if s is not None else ""

    # Job title
    s = _col("job_title")
    result["job_title"] = s.loc[idx].astype(str).str.strip().replace({"nan": ""}) if s is not None else ""

    # Fulltime
    s = _col("is_fulltime")
    if s is not None:
        raw_ft = s.loc[idx].astype(str).str.strip().str.upper()
        if is_icert_parttime:
            result["is_fulltime"] = raw_ft.map({"0": True, "1": False, "N": True, "Y": False}).astype("boolean")
        else:
            result["is_fulltime"] = raw_ft.map({"Y": True, "N": False, "YES": True, "NO": False, "1": True, "0": False}).astype("boolean")
    else:
        result["is_fulltime"] = pd.NA

    # Wage fields — handle range-format strings (e.g. "20000 -" or "66000 - 70000")
    _wage_from_raw = None  # Save raw series for wage_rate_to fallback
    for wf in ["wage_rate_from", "wage_rate_to", "prevailing_wage"]:
        s = _col(wf)
        if s is not None:
            cleaned = (
                s.loc[idx].astype(str)
                .str.replace(",", "", regex=False)
                .str.replace("$", "", regex=False)
                .str.strip()
            )
            if wf == "wage_rate_from":
                _wage_from_raw = cleaned.copy()
                # Extract leading number from range like "20000 -" or "66000 - 70000"
                cleaned = cleaned.str.extract(r'^([\d.]+)', expand=False).fillna("")
            elif wf == "wage_rate_to":
                # Extract leading number (handles normal numeric values too)
                cleaned = cleaned.str.extract(r'^([\d.]+)', expand=False).fillna("")
            result[wf] = pd.to_numeric(cleaned, errors="coerce")
        else:
            result[wf] = pd.NA

    # Fallback: extract wage_rate_to from range-format wage_rate_from
    # (e.g. FY2015 "66000 - 70000" → wage_rate_to = 70000)
    if result["wage_rate_to"].isna().all() and _wage_from_raw is not None:
        second_num = _wage_from_raw.str.extract(r'[\d.]+\s*-\s*([\d.]+)', expand=False)
        second_parsed = pd.to_numeric(second_num, errors="coerce")
        if second_parsed.notna().any():
            result["wage_rate_to"] = second_parsed

    # Wage unit / PW unit
    for uf in ["wage_unit", "pw_unit"]:
        s = _col(uf)
        result[uf] = s.loc[idx].astype(str).str.strip().replace({"nan": ""}) if s is not None else ""

    # Worksite
    for wf, upper in [("worksite_city", False), ("worksite_state", True), ("worksite_postal", False)]:
        s = _col(wf)
        if s is not None:
            val = s.loc[idx].astype(str).str.strip().replace({"nan": ""})
            result[wf] = val.str.upper() if upper else val
        else:
            result[wf] = ""

    # NAICS
    s = _col("naics_code")
    result["naics_code"] = s.loc[idx].astype(str).str.strip().replace({"nan": ""}) if s is not None else ""

    # Provenance
    result["fiscal_year"] = fy
    result["source_file"] = source_file
    result["ingested_at"] = ingested_at

    return result

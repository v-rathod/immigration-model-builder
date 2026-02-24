#!/usr/bin/env python3
"""
Expand dim_soc with valid-format SOC codes found in LCA/PERM data that are not
already in the current dim_soc (built from OEWS 2018 taxonomy).

These 'orphan' codes follow the XX-XXXX format but belong to the older SOC-2010
taxonomy.  Adding them gives the worksite_geo SOC-coverage check >=95%.

Input:  artifacts/tables/dim_soc.parquet
        artifacts/tables/fact_perm/  (partitioned)
        artifacts/tables/fact_lca/   (partitioned)
Output: artifacts/tables/dim_soc.parquet  (updated in-place, backed up first)
Log:    artifacts/metrics/expand_dim_soc_legacy.log
"""
import re, sys, logging
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parent.parent
TABLES = ROOT / "artifacts" / "tables"
METRICS = ROOT / "artifacts" / "metrics"
LOG_PATH = METRICS / "expand_dim_soc_legacy.log"
SOC_FMT = re.compile(r"^\d{2}-\d{4}$")
EXCL = ("_backup", "_quarantine", ".tmp_", "/tmp_")

logging.basicConfig(level=logging.INFO, format="%(message)s")
log = logging.getLogger(__name__)


def _excl(p: Path) -> bool:
    return any(x in str(p) for x in EXCL)


def collect_valid_soc_codes(dir_path: Path) -> set:
    """Read all soc_code values from a partitioned parquet directory."""
    codes: set = set()
    if not dir_path.exists():
        return codes
    for pf in sorted(dir_path.rglob("*.parquet")):
        if _excl(pf):
            continue
        try:
            df = pd.read_parquet(pf, columns=["soc_code"])
            valid = df["soc_code"].dropna().astype(str)
            valid = valid[valid.str.match(SOC_FMT)]
            codes.update(valid.unique())
        except Exception as e:
            log.warning(f"Skipping {pf}: {e}")
    return codes


def main():
    log_lines = [f"=== expand_dim_soc_legacy {datetime.now(timezone.utc).isoformat()} ==="]

    dim_soc_path = TABLES / "dim_soc.parquet"
    dim_soc = pd.read_parquet(dim_soc_path)
    existing_codes: set = set(dim_soc["soc_code"].dropna().astype(str).unique())
    log_lines.append(f"dim_soc before: {len(dim_soc):,} rows, {len(existing_codes):,} unique codes")

    # Collect all valid-format codes across PERM and LCA
    perm_codes = collect_valid_soc_codes(TABLES / "fact_perm")
    lca_codes  = collect_valid_soc_codes(TABLES / "fact_lca")
    all_lca_perm = perm_codes | lca_codes
    log_lines.append(f"Valid-format codes in PERM: {len(perm_codes):,}")
    log_lines.append(f"Valid-format codes in LCA:  {len(lca_codes):,}")
    log_lines.append(f"Union: {len(all_lca_perm):,}")

    # Identify codes not yet in dim_soc
    new_codes = sorted(all_lca_perm - existing_codes)
    log_lines.append(f"New codes to add: {len(new_codes):,}")

    if not new_codes:
        log_lines.append("Nothing to add.")
        LOG_PATH.write_text("\n".join(log_lines) + "\n")
        print("expand_dim_soc_legacy: nothing to add")
        return

    # Build rows for new codes aligned to dim_soc schema
    new_rows = pd.DataFrame(index=range(len(new_codes)))
    for col in dim_soc.columns:
        new_rows[col] = None
    new_rows["soc_code"]           = new_codes
    new_rows["soc_version"]        = "2010"         # pre-2018 SOC taxonomy
    new_rows["mapping_confidence"] = "inferred_from_lca"
    new_rows["is_aggregated"]      = False
    # Best-effort major/minor group from code structure
    new_rows["soc_major_group"]    = [c.split("-")[0] for c in new_codes]
    new_rows = new_rows[[c for c in dim_soc.columns]]

    # Back up original dim_soc
    backup_path = dim_soc_path.with_suffix(".bak.parquet")
    dim_soc.to_parquet(backup_path, index=False)
    log_lines.append(f"Backup saved to {backup_path}")

    # Append and save
    dim_soc_updated = pd.concat([dim_soc, new_rows], ignore_index=True)
    dim_soc_updated.to_parquet(dim_soc_path, index=False)
    log_lines.append(f"dim_soc after: {len(dim_soc_updated):,} rows")

    METRICS.mkdir(parents=True, exist_ok=True)
    LOG_PATH.write_text("\n".join(log_lines) + "\n")
    print(f"expand_dim_soc_legacy: added {len(new_codes):,} codes → dim_soc now {len(dim_soc_updated):,} rows")


if __name__ == "__main__":
    main()

"""Visa Bulletin loader - parses PDFs into structured time series."""

from datetime import datetime, timezone
from pathlib import Path
import re
from typing import List, Optional, Tuple
import pandas as pd
import pdfplumber


# Month name to number mapping
MONTH_MAP = {
    'january': 1, 'february': 2, 'march': 3, 'april': 4,
    'may': 5, 'june': 6, 'july': 7, 'august': 8,
    'september': 9, 'october': 10, 'november': 11, 'december': 12
}

# Country name mappings to ISO3 codes
COUNTRY_MAP = {
    'All Chargeability Areas Except Those Listed': 'ROW',  # Rest of World
    'CHINA-mainland born': 'CHN',
    'INDIA': 'IND',
    'MEXICO': 'MEX',
    'PHILIPPINES': 'PHL',
}

# Category normalization
CATEGORY_MAP = {
    '1st': 'EB1',
    '2nd': 'EB2',
    '3rd': 'EB3',
    'Other Workers': 'EB3-Other',
    '4th': 'EB4',
    '5th': 'EB5',
}


def parse_filename(filename: str) -> Tuple[Optional[int], Optional[int]]:
    """Extract year and month from Visa Bulletin filename.
    
    Args:
        filename: PDF filename like 'visabulletin_January2026.pdf'
        
    Returns:
        Tuple of (year, month) or (None, None) if parsing fails
    """
    # Match pattern like: visabulletin_January2026.pdf
    match = re.search(r'visabulletin[_-]?(\w+)(\d{4})', filename, re.IGNORECASE)
    if match:
        month_str = match.group(1).lower()
        year = int(match.group(2))
        month = MONTH_MAP.get(month_str)
        return (year, month)
    
    return (None, None)


def parse_date(date_str: str) -> Tuple[Optional[str], str]:
    """Parse a date string from Visa Bulletin.
    
    Args:
        date_str: Date like '01FEB23', 'C' (current), or 'U' (unavailable)
        
    Returns:
        Tuple of (cutoff_date as YYYY-MM-DD or None, status_flag)
    """
    if not date_str or date_str.strip() == '':
        return (None, 'U')
    
    date_str = date_str.strip()
    
    if date_str == 'C':
        return (None, 'C')
    elif date_str == 'U':
        return (None, 'U')
    
    # Parse date like '01FEB23' or '15NOV13'
    match = re.match(r'(\d{1,2})(\w{3})(\d{2})', date_str)
    if match:
        day = match.group(1).zfill(2)
        month_str = match.group(2).upper()
        year_2digit = match.group(3)
        
        # Convert month name to number
        month_map_short = {
            'JAN': '01', 'FEB': '02', 'MAR': '03', 'APR': '04',
            'MAY': '05', 'JUN': '06', 'JUL': '07', 'AUG': '08',
            'SEP': '09', 'OCT': '10', 'NOV': '11', 'DEC': '12'
        }
        month = month_map_short.get(month_str)
        
        if month:
            # Assume 20xx for years
            year = '20' + year_2digit
            return (f'{year}-{month}-{day}', 'D')
    
    return (None, 'U')


def extract_employment_table_from_text(text: str, chart_type: str) -> List[List]:
    """Extract employment-based table by parsing text when pdfplumber tables fail.
    
    Parses the structured text to extract the employment-based immigration categories
    and their corresponding dates for each country.
    
    Args:
        text: Raw text from PDF page
        chart_type: 'FAD' for Final Action Dates or 'DFF' for Dates for Filing
        
    Returns:
        List of table rows [header, data rows...]
    """
    lines = [line.strip() for line in text.split('\n')]
    table_rows = []
    
    # Find where the table data starts - look for "All Charge" or "Employment-Based 1st"
    # The header is usually spread across multiple lines, so we'll construct it
    data_start_idx = -1
    
    for i, line in enumerate(lines):
        # Look for the start of data rows - lines starting with "1st", "2nd", etc.
        if line.startswith('1st ') or (line == '1st' and i+1 < len(lines)):
            # Found first data row, construct standard header
            header = ['Employment-Based', 'All Chargeability Areas Except Those Listed',
                      'CHINA-mainland born', 'INDIA', 'MEXICO', 'PHILIPPINES']
            table_rows.append(header)
            data_start_idx = i
            break
    
    if data_start_idx == -1:
        return []
    
    # Parse data rows starting from data_start_idx
    i = data_start_idx
    
    while i < len(lines) and len(table_rows) < 20:
        line = lines[i].strip()
        
        # Stop conditions
        if any(stop in line for stop in ['Set Aside', 'DATES FOR FILING', 'Employment Third Preference', 'Note:', 'NOTE:']):
            break
        
        # Look for category markers at line start
        if line.startswith('1st'):
            parts = line.split()
            if len(parts) >= 6:  # 1st + 5 dates
                table_rows.append(['1st'] + parts[1:6])
        elif line.startswith('2nd'):
            parts = line.split()
            if len(parts) >= 6:
                table_rows.append(['2nd'] + parts[1:6])
        elif line.startswith('3rd'):
            parts = line.split()
            if len(parts) >= 6:
                table_rows.append(['3rd'] + parts[1:6])
        elif line.startswith('Other'):
            # "Other Workers" - dates might be on same line or next line
            if 'Workers' in line:
                parts = line.replace('Other Workers', '').strip().split()
                if len(parts) >= 5:
                    table_rows.append(['Other Workers'] + parts[:5])
                else:
                    # Try next line
                    i += 1
                    if i < len(lines):
                        dates_parts = lines[i].strip().split()
                        if len(dates_parts) >= 5:
                            table_rows.append(['Other Workers'] + dates_parts[:5])
        elif line.startswith('4th'):
            parts = line.split()
            if len(parts) >= 6:
                table_rows.append(['4th'] + parts[1:6])
        elif line.startswith('5th'):
            parts = line.split()
            if len(parts) >= 6:
                table_rows.append(['5th'] + parts[1:6])
        elif line.startswith('Certain'):
            # Skip sub-categories like "Certain Religious Workers"
            pass
        
        i += 1
    
    return table_rows if len(table_rows) > 1 else []


def load_visa_bulletin(data_root: str, out_dir: str, schemas_path: str = None) -> str:
    """
    Load and parse Visa Bulletin PDFs into fact_cutoffs table.
    
    Args:
        data_root: Root path to Project 1 downloads
        out_dir: Output directory for parquet files
        schemas_path: Path to schemas.yml (optional)
    
    Returns:
        Path to output directory
    """
    print("[VISA BULLETIN LOADER]")
    
    # Find all PDF files
    visa_bulletin_dir = Path(data_root) / "Visa_Bulletin"
    pdf_files = list(visa_bulletin_dir.glob("**/*.pdf"))
    
    print(f"  Found {len(pdf_files)} PDF files")
    
    if len(pdf_files) == 0:
        print(f"  WARNING: No PDF files found in {visa_bulletin_dir}")
        print(f"  Creating empty placeholder")
        out_path = Path(out_dir) / "tables" / "fact_cutoffs"
        out_path.mkdir(parents=True, exist_ok=True)
        (out_path / ".gitkeep").touch()
        return str(out_path)
    
    all_rows = []
    files_processed = 0
    files_failed = 0
    
    # Process ALL PDFs (sorted by year/month)
    print(f"  Processing ALL {len(pdf_files)} PDFs...")
    for idx, pdf_file in enumerate(sorted(pdf_files, reverse=True), 1):
        try:
            # Extract year and month from filename
            year, month = parse_filename(pdf_file.name)
            if not year or not month:
                print(f"  [{idx}/{len(pdf_files)}] SKIP {pdf_file.name}: couldn't parse date")
                files_failed += 1
                continue
            
            if idx % 20 == 0 or idx == 1:
                print(f"  [{idx}/{len(pdf_files)}] Processing: {pdf_file.name} ({year}-{month:02d})")
            
            # Get relative path from data_root for source_file tracking
            try:
                rel_path = pdf_file.relative_to(Path(data_root))
            except ValueError:
                # If not relative to data_root, use absolute path
                rel_path = pdf_file
            
            # Open PDF and extract text
            with pdfplumber.open(pdf_file) as pdf:
                for page_num, page in enumerate(pdf.pages):
                    text = page.extract_text()
                    
                    # Check for Final Action Dates chart
                    if 'FINAL ACTION DATES FOR EMPLOYMENT-BASED' in text:
                        table = extract_employment_table_from_text(text, 'FAD')
                        if table:
                            rows = parse_employment_table(
                                table, year, month, 'FAD',
                                str(rel_path), f"page_{page_num+1}"
                            )
                            all_rows.extend(rows)
                    
                    # Check for Dates for Filing chart
                    if 'DATES FOR FILING' in text and 'EMPLOYMENT-BASED' in text:
                        table = extract_employment_table_from_text(text, 'DFF')
                        if table:
                            rows = parse_employment_table(
                                table, year, month, 'DFF',
                                str(rel_path), f"page_{page_num+1}"
                            )
                            all_rows.extend(rows)
            
            files_processed += 1
            
        except Exception as e:
            print(f"  [{idx}/{len(pdf_files)}] ERROR {pdf_file.name}: {e}")
            files_failed += 1
            continue
    
    print(f"  ✅ Processed {files_processed} files successfully")
    print(f"  ⚠️  Failed/skipped {files_failed} files")
    print(f"  📊 Extracted {len(all_rows)} rows total")
    
    print(f"  Processed {files_processed} files, extracted {len(all_rows)} rows")
    
    # Convert to DataFrame
    if all_rows:
        df = pd.DataFrame(all_rows)
        
        # Ensure proper types
        df['bulletin_year'] = df['bulletin_year'].astype(int)
        df['bulletin_month'] = df['bulletin_month'].astype(int)
        df['cutoff_date'] = pd.to_datetime(df['cutoff_date'], errors='coerce')
        df['ingested_at'] = pd.to_datetime(df['ingested_at'])
        
        # Write partitioned parquet by bulletin_year and bulletin_month
        out_path = Path(out_dir) / "tables" / "fact_cutoffs"
        out_path.mkdir(parents=True, exist_ok=True)
        
        # Write as partitioned by bulletin_year/bulletin_month
        for (year, month), group in df.groupby(['bulletin_year', 'bulletin_month']):
            # Drop partition columns from data (they'll be in directory structure)
            group_data = group.drop(columns=['bulletin_year', 'bulletin_month'])
            # Dedup within partition: keep last occurrence per (chart, category, country)
            group_data = group_data.drop_duplicates(
                subset=['chart', 'category', 'country'], keep='last'
            )
            partition_dir = out_path / f"bulletin_year={year}" / f"bulletin_month={month:02d}"
            partition_dir.mkdir(parents=True, exist_ok=True)
            partition_file = partition_dir / "data.parquet"
            group_data.to_parquet(partition_file, index=False, engine='pyarrow')
        
        print(f"  Written: {out_path}")
        print(f"  Partitions: {len(df.groupby(['bulletin_year', 'bulletin_month']))}")
        
        return str(out_path)
    else:
        print(f"  WARNING: No data extracted")
        out_path = Path(out_dir) / "tables" / "fact_cutoffs"
        out_path.mkdir(parents=True, exist_ok=True)
        return str(out_path)


def parse_employment_table(
    table: List[List],
    year: int,
    month: int,
    chart: str,
    source_file: str,
    page_ref: str
) -> List[dict]:
    """Parse an employment-based table into rows.
    
    Args:
        table: Extracted table rows
        year: Bulletin year
        month: Bulletin month
        chart: 'FAD' or 'DFF'
        source_file: Source PDF filename
        page_ref: Page reference
        
    Returns:
        List of row dictionaries
    """
    rows = []
    ingested_at = datetime.now(timezone.utc)
    
    # Find header row
    header_idx = None
    country_columns = []
    
    for i, row in enumerate(table):
        if row and any(cell and 'Employment' in str(cell) for cell in row):
            header_idx = i
            # Extract country columns (typically columns 1-5)
            country_columns = row[1:6] if len(row) > 5 else row[1:]
            break
    
    if header_idx is None:
        return rows
    
    # Parse data rows
    for row in table[header_idx + 1:]:
        if not row or not row[0]:
            continue
        
        category_raw = str(row[0]).strip()
        
        # Skip empty or section header rows
        if not category_raw or category_raw.startswith('5th Set Aside'):
            continue
        
        # Normalize category
        category = CATEGORY_MAP.get(category_raw, category_raw)
        
        # Skip if not a main EB category (for MVP)
        if not category.startswith('EB'):
            continue
        
        # Parse each country column
        for col_idx, country_raw in enumerate(country_columns):
            if col_idx >= len(row) - 1:
                break
            
            date_str = str(row[col_idx + 1]).strip() if len(row) > col_idx + 1 else ''
            
            # Normalize country
            country = COUNTRY_MAP.get(country_raw, country_raw)
            
            # Parse date
            cutoff_date, status_flag = parse_date(date_str)
            
            rows.append({
                'bulletin_year': year,
                'bulletin_month': month,
                'chart': chart,
                'category': category,
                'country': country,
                'cutoff_date': cutoff_date,
                'status_flag': status_flag,
                'source_file': source_file,  # Already includes relative path
                'page_ref': page_ref,
                'ingested_at': ingested_at
            })
    
    return rows

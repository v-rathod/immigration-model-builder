"""Build dim_country dimension table from full ISO 3166-1 country list."""

from datetime import datetime, timezone
from pathlib import Path
import pandas as pd


# Full ISO 3166-1 country list: iso2 → (iso3, country_name)
# Source: ISO 3166-1 (249 officially assigned entries as of 2024)
ISO_COUNTRY_MAP: dict = {
    'AF': ('AFG', 'Afghanistan'),
    'AX': ('ALA', 'Åland Islands'),
    'AL': ('ALB', 'Albania'),
    'DZ': ('DZA', 'Algeria'),
    'AS': ('ASM', 'American Samoa'),
    'AD': ('AND', 'Andorra'),
    'AO': ('AGO', 'Angola'),
    'AI': ('AIA', 'Anguilla'),
    'AQ': ('ATA', 'Antarctica'),
    'AG': ('ATG', 'Antigua And Barbuda'),
    'AR': ('ARG', 'Argentina'),
    'AM': ('ARM', 'Armenia'),
    'AW': ('ABW', 'Aruba'),
    'AU': ('AUS', 'Australia'),
    'AT': ('AUT', 'Austria'),
    'AZ': ('AZE', 'Azerbaijan'),
    'BS': ('BHS', 'Bahamas'),
    'BH': ('BHR', 'Bahrain'),
    'BD': ('BGD', 'Bangladesh'),
    'BB': ('BRB', 'Barbados'),
    'BY': ('BLR', 'Belarus'),
    'BE': ('BEL', 'Belgium'),
    'BZ': ('BLZ', 'Belize'),
    'BJ': ('BEN', 'Benin'),
    'BM': ('BMU', 'Bermuda'),
    'BT': ('BTN', 'Bhutan'),
    'BO': ('BOL', 'Bolivia (Plurinational State Of)'),
    'BQ': ('BES', 'Bonaire, Sint Eustatius And Saba'),
    'BA': ('BIH', 'Bosnia And Herzegovina'),
    'BW': ('BWA', 'Botswana'),
    'BV': ('BVT', 'Bouvet Island'),
    'BR': ('BRA', 'Brazil'),
    'IO': ('IOT', 'British Indian Ocean Territory'),
    'BN': ('BRN', 'Brunei Darussalam'),
    'BG': ('BGR', 'Bulgaria'),
    'BF': ('BFA', 'Burkina Faso'),
    'BI': ('BDI', 'Burundi'),
    'CV': ('CPV', 'Cabo Verde'),
    'KH': ('KHM', 'Cambodia'),
    'CM': ('CMR', 'Cameroon'),
    'CA': ('CAN', 'Canada'),
    'KY': ('CYM', 'Cayman Islands'),
    'CF': ('CAF', 'Central African Republic'),
    'TD': ('TCD', 'Chad'),
    'CL': ('CHL', 'Chile'),
    'CN': ('CHN', 'China'),
    'CX': ('CXR', 'Christmas Island'),
    'CC': ('CCK', 'Cocos (Keeling) Islands'),
    'CO': ('COL', 'Colombia'),
    'KM': ('COM', 'Comoros'),
    'CG': ('COG', 'Congo'),
    'CD': ('COD', 'Congo, Democratic Republic Of The'),
    'CK': ('COK', 'Cook Islands'),
    'CR': ('CRI', 'Costa Rica'),
    'CI': ('CIV', "Cote D'Ivoire"),
    'HR': ('HRV', 'Croatia'),
    'CU': ('CUB', 'Cuba'),
    'CW': ('CUW', 'Curacao'),
    'CY': ('CYP', 'Cyprus'),
    'CZ': ('CZE', 'Czechia'),
    'DK': ('DNK', 'Denmark'),
    'DJ': ('DJI', 'Djibouti'),
    'DM': ('DMA', 'Dominica'),
    'DO': ('DOM', 'Dominican Republic'),
    'EC': ('ECU', 'Ecuador'),
    'EG': ('EGY', 'Egypt'),
    'SV': ('SLV', 'El Salvador'),
    'GQ': ('GNQ', 'Equatorial Guinea'),
    'ER': ('ERI', 'Eritrea'),
    'EE': ('EST', 'Estonia'),
    'SZ': ('SWZ', 'Eswatini'),
    'ET': ('ETH', 'Ethiopia'),
    'FK': ('FLK', 'Falkland Islands (Malvinas)'),
    'FO': ('FRO', 'Faroe Islands'),
    'FJ': ('FJI', 'Fiji'),
    'FI': ('FIN', 'Finland'),
    'FR': ('FRA', 'France'),
    'GF': ('GUF', 'French Guiana'),
    'PF': ('PYF', 'French Polynesia'),
    'TF': ('ATF', 'French Southern Territories'),
    'GA': ('GAB', 'Gabon'),
    'GM': ('GMB', 'Gambia'),
    'GE': ('GEO', 'Georgia'),
    'DE': ('DEU', 'Germany'),
    'GH': ('GHA', 'Ghana'),
    'GI': ('GIB', 'Gibraltar'),
    'GR': ('GRC', 'Greece'),
    'GL': ('GRL', 'Greenland'),
    'GD': ('GRD', 'Grenada'),
    'GP': ('GLP', 'Guadeloupe'),
    'GU': ('GUM', 'Guam'),
    'GT': ('GTM', 'Guatemala'),
    'GG': ('GGY', 'Guernsey'),
    'GN': ('GIN', 'Guinea'),
    'GW': ('GNB', 'Guinea-Bissau'),
    'GY': ('GUY', 'Guyana'),
    'HT': ('HTI', 'Haiti'),
    'HM': ('HMD', 'Heard Island And Mcdonald Islands'),
    'VA': ('VAT', 'Holy See'),
    'HN': ('HND', 'Honduras'),
    'HK': ('HKG', 'Hong Kong'),
    'HU': ('HUN', 'Hungary'),
    'IS': ('ISL', 'Iceland'),
    'IN': ('IND', 'India'),
    'ID': ('IDN', 'Indonesia'),
    'IR': ('IRN', 'Iran (Islamic Republic Of)'),
    'IQ': ('IRQ', 'Iraq'),
    'IE': ('IRL', 'Ireland'),
    'IM': ('IMN', 'Isle Of Man'),
    'IL': ('ISR', 'Israel'),
    'IT': ('ITA', 'Italy'),
    'JM': ('JAM', 'Jamaica'),
    'JP': ('JPN', 'Japan'),
    'JE': ('JEY', 'Jersey'),
    'JO': ('JOR', 'Jordan'),
    'KZ': ('KAZ', 'Kazakhstan'),
    'KE': ('KEN', 'Kenya'),
    'KI': ('KIR', 'Kiribati'),
    'KP': ('PRK', "Korea (Democratic People'S Republic Of)"),
    'KR': ('KOR', 'Korea, Republic Of'),
    'KW': ('KWT', 'Kuwait'),
    'KG': ('KGZ', 'Kyrgyzstan'),
    'LA': ('LAO', "Lao People'S Democratic Republic"),
    'LV': ('LVA', 'Latvia'),
    'LB': ('LBN', 'Lebanon'),
    'LS': ('LSO', 'Lesotho'),
    'LR': ('LBR', 'Liberia'),
    'LY': ('LBY', 'Libya'),
    'LI': ('LIE', 'Liechtenstein'),
    'LT': ('LTU', 'Lithuania'),
    'LU': ('LUX', 'Luxembourg'),
    'MO': ('MAC', 'Macao'),
    'MG': ('MDG', 'Madagascar'),
    'MW': ('MWI', 'Malawi'),
    'MY': ('MYS', 'Malaysia'),
    'MV': ('MDV', 'Maldives'),
    'ML': ('MLI', 'Mali'),
    'MT': ('MLT', 'Malta'),
    'MH': ('MHL', 'Marshall Islands'),
    'MQ': ('MTQ', 'Martinique'),
    'MR': ('MRT', 'Mauritania'),
    'MU': ('MUS', 'Mauritius'),
    'YT': ('MYT', 'Mayotte'),
    'MX': ('MEX', 'Mexico'),
    'FM': ('FSM', 'Micronesia (Federated States Of)'),
    'MD': ('MDA', 'Moldova, Republic Of'),
    'MC': ('MCO', 'Monaco'),
    'MN': ('MNG', 'Mongolia'),
    'ME': ('MNE', 'Montenegro'),
    'MS': ('MSR', 'Montserrat'),
    'MA': ('MAR', 'Morocco'),
    'MZ': ('MOZ', 'Mozambique'),
    'MM': ('MMR', 'Myanmar'),
    'NA': ('NAM', 'Namibia'),
    'NR': ('NRU', 'Nauru'),
    'NP': ('NPL', 'Nepal'),
    'NL': ('NLD', 'Netherlands'),
    'NC': ('NCL', 'New Caledonia'),
    'NZ': ('NZL', 'New Zealand'),
    'NI': ('NIC', 'Nicaragua'),
    'NE': ('NER', 'Niger'),
    'NG': ('NGA', 'Nigeria'),
    'NU': ('NIU', 'Niue'),
    'NF': ('NFK', 'Norfolk Island'),
    'MK': ('MKD', 'North Macedonia'),
    'MP': ('MNP', 'Northern Mariana Islands'),
    'NO': ('NOR', 'Norway'),
    'OM': ('OMN', 'Oman'),
    'PK': ('PAK', 'Pakistan'),
    'PW': ('PLW', 'Palau'),
    'PS': ('PSE', 'Palestine, State Of'),
    'PA': ('PAN', 'Panama'),
    'PG': ('PNG', 'Papua New Guinea'),
    'PY': ('PRY', 'Paraguay'),
    'PE': ('PER', 'Peru'),
    'PH': ('PHL', 'Philippines'),
    'PN': ('PCN', 'Pitcairn'),
    'PL': ('POL', 'Poland'),
    'PT': ('PRT', 'Portugal'),
    'PR': ('PRI', 'Puerto Rico'),
    'QA': ('QAT', 'Qatar'),
    'RE': ('REU', 'Reunion'),
    'RO': ('ROU', 'Romania'),
    'RU': ('RUS', 'Russian Federation'),
    'RW': ('RWA', 'Rwanda'),
    'BL': ('BLM', 'Saint Barthelemy'),
    'SH': ('SHN', 'Saint Helena, Ascension And Tristan Da Cunha'),
    'KN': ('KNA', 'Saint Kitts And Nevis'),
    'LC': ('LCA', 'Saint Lucia'),
    'MF': ('MAF', 'Saint Martin (French Part)'),
    'PM': ('SPM', 'Saint Pierre And Miquelon'),
    'VC': ('VCT', 'Saint Vincent And The Grenadines'),
    'WS': ('WSM', 'Samoa'),
    'SM': ('SMR', 'San Marino'),
    'ST': ('STP', 'Sao Tome And Principe'),
    'SA': ('SAU', 'Saudi Arabia'),
    'SN': ('SEN', 'Senegal'),
    'RS': ('SRB', 'Serbia'),
    'SC': ('SYC', 'Seychelles'),
    'SL': ('SLE', 'Sierra Leone'),
    'SG': ('SGP', 'Singapore'),
    'SX': ('SXM', 'Sint Maarten (Dutch Part)'),
    'SK': ('SVK', 'Slovakia'),
    'SI': ('SVN', 'Slovenia'),
    'SB': ('SLB', 'Solomon Islands'),
    'SO': ('SOM', 'Somalia'),
    'ZA': ('ZAF', 'South Africa'),
    'GS': ('SGS', 'South Georgia And The South Sandwich Islands'),
    'SS': ('SSD', 'South Sudan'),
    'ES': ('ESP', 'Spain'),
    'LK': ('LKA', 'Sri Lanka'),
    'SD': ('SDN', 'Sudan'),
    'SR': ('SUR', 'Suriname'),
    'SJ': ('SJM', 'Svalbard And Jan Mayen'),
    'SE': ('SWE', 'Sweden'),
    'CH': ('CHE', 'Switzerland'),
    'SY': ('SYR', 'Syrian Arab Republic'),
    'TW': ('TWN', 'Taiwan, Province Of China'),
    'TJ': ('TJK', 'Tajikistan'),
    'TZ': ('TZA', 'Tanzania, United Republic Of'),
    'TH': ('THA', 'Thailand'),
    'TL': ('TLS', 'Timor-Leste'),
    'TG': ('TGO', 'Togo'),
    'TK': ('TKL', 'Tokelau'),
    'TO': ('TON', 'Tonga'),
    'TT': ('TTO', 'Trinidad And Tobago'),
    'TN': ('TUN', 'Tunisia'),
    'TR': ('TUR', 'Turkiye'),
    'TM': ('TKM', 'Turkmenistan'),
    'TC': ('TCA', 'Turks And Caicos Islands'),
    'TV': ('TUV', 'Tuvalu'),
    'UG': ('UGA', 'Uganda'),
    'UA': ('UKR', 'Ukraine'),
    'AE': ('ARE', 'United Arab Emirates'),
    'GB': ('GBR', 'United Kingdom Of Great Britain And Northern Ireland'),
    'UM': ('UMI', 'United States Minor Outlying Islands'),
    'US': ('USA', 'United States Of America'),
    'UY': ('URY', 'Uruguay'),
    'UZ': ('UZB', 'Uzbekistan'),
    'VU': ('VUT', 'Vanuatu'),
    'VE': ('VEN', 'Venezuela (Bolivarian Republic Of)'),
    'VN': ('VNM', 'Viet Nam'),
    'VG': ('VGB', 'Virgin Islands (British)'),
    'VI': ('VIR', 'Virgin Islands (U.S.)'),
    'WF': ('WLF', 'Wallis And Futuna'),
    'EH': ('ESH', 'Western Sahara'),
    'YE': ('YEM', 'Yemen'),
    'ZM': ('ZMB', 'Zambia'),
    'ZW': ('ZWE', 'Zimbabwe'),
}


def build_dim_country(data_root: str, out_path: str, schemas_path: str = None) -> str:
    """
    Build dim_country dimension table from the full ISO 3166-1 country list.

    Baseline: hardcoded ISO_COUNTRY_MAP (249 entries covering all assigned codes).
    Supplement: country_codes_iso.csv provides region annotations and name overrides.

    Args:
        data_root: Root path to Project 1 downloads
        out_path: Output path for parquet file
        schemas_path: Path to schemas.yml (optional, for validation)

    Returns:
        Path to output parquet file
    """
    print("[BUILD DIM_COUNTRY]")

    now = datetime.now(timezone.utc)

    # ── Build base from hardcoded ISO map ─────────────────────────────────────
    records = []
    for iso2, (iso3, name) in ISO_COUNTRY_MAP.items():
        records.append({
            'country_name': name,
            'iso2': iso2.upper(),
            'iso3': iso3.upper(),
            'region': None,
            'source_file': 'hardcoded/ISO_3166-1',
            'ingested_at': now,
        })
    df = pd.DataFrame(records)
    print(f"  Base ISO list: {len(df)} entries")

    # ── Supplement with codebook CSV (region annotations + name overrides) ────
    codebook_path = Path(data_root) / "Codebooks" / "country_codes_iso.csv"
    if codebook_path.exists():
        cb = pd.read_csv(codebook_path)
        cb = cb.rename(columns={'country_code': 'iso2', 'country_name': 'country_name', 'region': 'region'})
        cb['iso2'] = cb['iso2'].str.strip().str.upper()

        # Apply region annotations from codebook
        if 'region' in cb.columns:
            region_map = cb.dropna(subset=['region']).set_index('iso2')['region'].to_dict()
            df['region'] = df['iso2'].map(region_map)

        # Override country_name for codebook rows
        if 'country_name' in cb.columns:
            name_map = cb.dropna(subset=['country_name']).set_index('iso2')['country_name'].to_dict()
            for iso2, cname in name_map.items():
                mask = df['iso2'] == iso2
                if mask.any():
                    df.loc[mask, 'country_name'] = str(cname).strip().title()

        # Append any iso2 entries in codebook not in the hardcoded map
        new_iso2s = set(cb['iso2'].dropna().unique()) - set(df['iso2'].unique())
        if new_iso2s:
            print(f"  Adding {len(new_iso2s)} extra entries from codebook CSV")
            for _, row in cb[cb['iso2'].isin(new_iso2s)].iterrows():
                df = pd.concat([df, pd.DataFrame([{
                    'country_name': str(row.get('country_name', row['iso2'])).strip().title(),
                    'iso2': row['iso2'].upper(),
                    'iso3': str(row['iso2']).upper() + 'X',
                    'region': row.get('region', None),
                    'source_file': 'Codebooks/country_codes_iso.csv',
                    'ingested_at': now,
                }])], ignore_index=True)
        print(f"  After codebook merge: {len(df)} entries")
    else:
        print(f"  WARNING: Codebook not found at {codebook_path} — using hardcoded list only")

    # ── Normalise ──────────────────────────────────────────────────────────────
    df['iso2'] = df['iso2'].str.strip().str.upper()
    df['iso3'] = df['iso3'].str.strip().str.upper()
    df['country_name'] = df['country_name'].str.strip()

    # ── Validate ──────────────────────────────────────────────────────────────
    null_counts = df[['iso2', 'iso3', 'country_name']].isnull().sum()
    if null_counts.any():
        raise ValueError(f"Null values in required fields: {null_counts[null_counts > 0].to_dict()}")
    if not df['iso2'].str.isupper().all() or not df['iso3'].str.isupper().all():
        raise ValueError("ISO codes must be uppercase")

    # De-duplicate on iso3 (hardcoded canonical takes precedence over CSV overrides)
    df = df.drop_duplicates(subset=['iso3'], keep='first')
    print(f"  Validated: {len(df)} unique countries")

    # ── Write ─────────────────────────────────────────────────────────────────
    output_cols = ['country_name', 'iso2', 'iso3', 'region', 'source_file', 'ingested_at']
    df = df[output_cols]

    out = Path(out_path)
    out.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(out, index=False, engine='pyarrow')
    print(f"  Written: {out}")
    print(f"  Rows: {len(df)}")
    return str(out)

import pandas as pd
import airportsdata
import os
import re
import time
import json
from pathlib import Path
from datetime import datetime
from geopy.geocoders import Nominatim
from geopy.distance import geodesic
from geopy.exc import GeocoderTimedOut, GeocoderServiceError

# --- CONFIGURATION ---
FOLDER_PATH = r"C:/Documents/FAA_UAS_Sightings"  # <-- UPDATE THIS PATH TO YOUR DATA FOLDER
RUN_DATE = datetime.now().strftime("%Y-%m-%d")  # e.g., "2026-02-07"

SPLIT_FOLDER = Path(FOLDER_PATH) / "Split_Chunks" / RUN_DATE
OUTPUT_FOLDER = Path(FOLDER_PATH) / "Processed_Files" / RUN_DATE
YEARLY_FOLDER = Path(FOLDER_PATH) / "Yearly_Masters" 
CACHE_FILE = Path(FOLDER_PATH) / "geocoding_cache.json"

# Retry configuration
MAX_RETRY_ATTEMPTS = 3
RETRY_DELAY_BASE = 30  # seconds

# Coordinate validation (US bounds)
LON_MIN, LON_MAX = -125, -65
LAT_MIN, LAT_MAX = 25, 50

for folder in [SPLIT_FOLDER, OUTPUT_FOLDER, YEARLY_FOLDER]:
    folder.mkdir(exist_ok=True)

ROWS_PER_SPLIT = 250  # Smaller chunks = better stability for 2025 files
geolocator = Nominatim(user_agent="faa_uas_precision_v8")

# Load geocoding cache from disk if exists
if CACHE_FILE.exists():
    with open(CACHE_FILE, 'r') as f:
        city_cache = json.load(f)
    print(f"Loaded {len(city_cache)} cached locations")
else:
    city_cache = {}

all_airports = airportsdata.load('IATA')
us_airports = {k: v for k, v in all_airports.items() if v['country'] == 'US'}

# ICAO to IATA conversion for US airports (K prefix)
icao_airports = airportsdata.load('ICAO')
icao_to_iata = {icao: data['iata'] for icao, data in icao_airports.items() 
                 if data.get('country') == 'US' and data.get('iata') and icao.startswith('K')}

# DEBUG: Show sample airport data structure
if us_airports:
    sample_code = list(us_airports.keys())[0]
    print(f"\n[DEBUG] Sample IATA entry ({sample_code}): {us_airports[sample_code]}")
if icao_airports:
    sample_icao = [k for k in icao_airports.keys() if k.startswith('K')][0]
    print(f"[DEBUG] Sample ICAO entry ({sample_icao}): {icao_airports[sample_icao]}")
print(f"[DEBUG] Total US IATA airports: {len(us_airports)}")
print(f"[DEBUG] Total ICAO to IATA mappings: {len(icao_to_iata)}\n")

# Blacklist of non-airport 3-letter codes
AIRPORT_BLACKLIST = {'FBI', 'FAA', 'TSA', 'DHS', 'LEO', 'ATC', 'VFR', 'IFR', 'UAS', 
                     'UFO', 'USA', 'UTC', 'EST', 'PST', 'MST', 'CST', 'EDT', 'PDT', 'MDT', 'CDT'}

# State abbreviation normalization
STATE_ABBREV = {
    'ALABAMA': 'AL', 'ALASKA': 'AK', 'ARIZONA': 'AZ', 'ARKANSAS': 'AR', 'CALIFORNIA': 'CA',
    'COLORADO': 'CO', 'CONNECTICUT': 'CT', 'DELAWARE': 'DE', 'FLORIDA': 'FL', 'GEORGIA': 'GA',
    'HAWAII': 'HI', 'IDAHO': 'ID', 'ILLINOIS': 'IL', 'INDIANA': 'IN', 'IOWA': 'IA',
    'KANSAS': 'KS', 'KENTUCKY': 'KY', 'LOUISIANA': 'LA', 'MAINE': 'ME', 'MARYLAND': 'MD',
    'MASSACHUSETTS': 'MA', 'MICHIGAN': 'MI', 'MINNESOTA': 'MN', 'MISSISSIPPI': 'MS', 'MISSOURI': 'MO',
    'MONTANA': 'MT', 'NEBRASKA': 'NE', 'NEVADA': 'NV', 'NEW HAMPSHIRE': 'NH', 'NEW JERSEY': 'NJ',
    'NEW MEXICO': 'NM', 'NEW YORK': 'NY', 'NORTH CAROLINA': 'NC', 'NORTH DAKOTA': 'ND', 'OHIO': 'OH',
    'OKLAHOMA': 'OK', 'OREGON': 'OR', 'PENNSYLVANIA': 'PA', 'RHODE ISLAND': 'RI', 'SOUTH CAROLINA': 'SC',
    'SOUTH DAKOTA': 'SD', 'TENNESSEE': 'TN', 'TEXAS': 'TX', 'UTAH': 'UT', 'VERMONT': 'VT',
    'VIRGINIA': 'VA', 'WASHINGTON': 'WA', 'WEST VIRGINIA': 'WV', 'WISCONSIN': 'WI', 'WYOMING': 'WY',
    'DISTRICT OF COLUMBIA': 'DC', 'CALIF': 'CA', 'PENN': 'PA', 'MASS': 'MA', 'MICH': 'MI'
}

# --- HELPER FUNCTIONS ---

def standardize_value(val):
    """Standardize missing/unknown values to None."""
    if pd.isna(val) or val is None:
        return None
    if isinstance(val, str):
        val_upper = val.strip().upper()
        if val_upper in ['N/A', 'NA', 'UNKNOWN', 'NOT REPORTED', 'NONE', 'NULL', '', 'UNREPORTED']:
            return None
    return val

def normalize_state(state):
    """Normalize state name to 2-letter abbreviation."""
    if not state or not isinstance(state, str):
        return None
    state = state.strip().upper()
    # Already 2-letter code
    if len(state) == 2:
        return state
    # Convert full name to abbreviation
    return STATE_ABBREV.get(state, state)

def save_cache():
    """Persist geocoding cache to disk."""
    with open(CACHE_FILE, 'w') as f:
        json.dump(city_cache, f, indent=2)
    print(f"Saved {len(city_cache)} locations to cache")

def get_best_col(df, keywords):
    for col in df.columns:
        if any(key.lower() in col.lower() for key in keywords):
            return col
    return None

def extract_details(text):
    if not isinstance(text, str): return pd.Series([None, "UNKNOWN", None, "NO"])
    
    # Aircraft type - multiple patterns
    acft = None
    acft_patterns = [
        r'ADVISED,\s*([A-Z0-9]{2,6}),',  # Original pattern
        r'AIRCRAFT TYPE[:\s]+([A-Z0-9]{2,6})\b',
        r'\b(CESSNA|BOEING|AIRBUS|PIPER|BEECH|CIRRUS|GULFSTREAM|EMBRAER)\b',
    ]
    for pattern in acft_patterns:
        match = re.search(pattern, text)
        if match:
            acft = match.group(1)
            break
    
    # Color - expanded list
    color_match = re.search(
        r'\b(RED|BLACK|GREY|GRAY|WHITE|ORANGE|GREEN|BLUE|SILVER|YELLOW|BROWN|TAN|PINK|PURPLE|GOLD|BEIGE|MULTI[- ]COLOR)\b(?=.*UAS|.*DRONE)', 
        text, re.IGNORECASE
    )
    color = color_match.group(1).upper().replace('MULTI-COLOR', 'MULTI-COLORED').replace('MULTI COLOR', 'MULTI-COLORED') if color_match else "UNKNOWN"
    
    # Altitude - multiple formats
    alt = None
    alt_patterns = [
        r'(\d{1,3}(?:,\d{3})*)\s*(?:FEET|FT)\b',  # "1,500 FEET" or "500 FT"
        r'FL\s*(\d{2,3})\b',  # "FL250" (flight level)
    ]
    for pattern in alt_patterns:
        match = re.search(pattern, text)
        if match:
            alt_str = match.group(1).replace(',', '')
            # Convert flight level to feet
            if 'FL' in pattern:
                alt = str(int(alt_str) * 100)
            else:
                alt = alt_str
            break
    
    evasive = "YES" if "EVASIVE ACTION" in text and "NO EVASIVE" not in text else "NO"
    return pd.Series([acft, color, alt, evasive])

def extract_leo_agency(text):
    if not isinstance(text, str): return "UNKNOWN"
    
    # Check for "not reported" variations first
    if any(p in text.upper() for p in ["NOT REPORTED", "NO LEO", "NOT NOTIFIED", "NOTIFICATION NOT REPORTED", "LEOS NOT NOTIFIED"]):
        return "NONE REPORTED"
    
    # FAA facility keywords to exclude (these get ADVISED, not NOTIFIED)
    faa_facilities = ['ATCT', 'TRACON', 'APCH', 'APPROACH', 'TWR', 'TOWER', 'CENTER', 'FSS', 'ARTCC']
    
    # Look for ALL agencies before "NOTIFIED" and take the LAST one (LEO notification is at end)
    matches = list(re.finditer(r'([A-Z][A-Z\s]{2,40}?)\s+NOTIFIED', text))
    
    # Process matches from last to first
    for match in reversed(matches):
        agency = match.group(1).strip()
        
        # Skip if it's an FAA facility
        if any(fac in agency for fac in faa_facilities):
            continue
        
        # Remove common prefixes/artifacts
        agency = re.sub(r'^(LEO|THE|AND|ACTION|EVASIVE)\s+', '', agency)
        agency = re.sub(r'\s+(NO|NOT|TAKEN|REPORTED)\.?$', '', agency)
        
        # Clean up trailing periods and spaces
        agency = agency.strip('. ')
        
        # Must have at least 2 chars and not be noise
        if len(agency) >= 2 and agency not in ['NO', 'WAS', 'WERE', 'ACTION', 'EVASIVE', 'WOC']:
            return agency
    
    return "UNKNOWN"

def extract_airport_code(text):
    """Extract 3-letter IATA or 4-letter ICAO airport code from text."""
    if not isinstance(text, str): return None
    
    # Priority patterns - distance+direction gets HIGHEST priority
    patterns = [
        # CRITICAL PRIORITY: Distance + direction patterns (most specific to sighting location)
        (r'(\d+\.?\d*)\s+(N|S|E|W|NE|NW|SE|SW|NNE|NNW|SSE|SSW|ENE|ESE|WNW|WSW)\s+([A-Z]{3})\b', 'critical', 3),  # Group 3 is airport
        # HIGH PRIORITY: Runway references and explicit airport mentions
        (r'RUNWAY\s+\d+[LRC]?\s+([A-Z]{3})\b', 'high', 1),
        (r'\b(K[A-Z]{3})\b', 'high', 1),  # ICAO
        (r'\b([A-Z]{3})\s+(?:AIRPORT|ARPT|TWR|TOWER|ATCT)', 'high', 1),
        # MEDIUM PRIORITY
        (r'\(([A-Z]{3})\)', 'medium', 1),
        (r'\b([A-Z]{3})\s+(?:CLASS|AIRSPACE)', 'medium', 1),
        (r'(?:NEAR|AT|OVER|BY|FROM)\s+([A-Z]{3})\b', 'medium', 1),
        # LOW PRIORITY: Route format (often departure airport, not sighting location)
        (r'([A-Z]{3})\s*-\s*[A-Z]', 'low', 1),
    ]
    
    # Collect all potential matches with priority and position
    candidates = []
    for pattern, priority, group_num in patterns:
        for match in re.finditer(pattern, text, re.IGNORECASE):
            code = match.group(group_num)
            
            # Handle ICAO to IATA conversion
            if len(code) == 4 and code.startswith('K'):
                if code in icao_to_iata:
                    code = icao_to_iata[code]
                else:
                    continue  # Invalid ICAO
            
            # Validate: must be real airport and not in blacklist
            if code in us_airports and code not in AIRPORT_BLACKLIST:
                candidates.append({
                    'code': code,
                    'priority': priority,
                    'position': match.start(),
                    'pattern': pattern[:30] + '...' if len(pattern) > 30 else pattern
                })
    
    if not candidates:
        return None
    
    # Sort by priority first (critical > high > medium > low), then by position (later is better)
    priority_order = {'critical': 4, 'high': 3, 'medium': 2, 'low': 1}
    candidates.sort(key=lambda x: (priority_order[x['priority']], x['position']), reverse=True)
    
    # DEBUG: Show what was extracted
    winner = candidates[0]
    # print(f"    [AIRPORT] Extracted '{winner['code']}' (priority: {winner['priority']}, pattern: {winner['pattern']})")
    
    return winner['code']

def find_nearest_airport(city, state, attempt=1):
    if not city or not state: return "UNKNOWN"
    
    # Normalize state to 2-letter code
    state = normalize_state(state)
    if not state: return "UNKNOWN"
    
    loc_key = f"{city}, {state}"
    if loc_key in city_cache: return city_cache[loc_key]
    try:
        loc = geolocator.geocode(loc_key + ", USA", timeout=15)
        if loc:
            coords = (loc.latitude, loc.longitude)
            closest = min(us_airports.keys(), key=lambda k: geodesic(coords, (us_airports[k]['lat'], us_airports[k]['lon'])).miles)
            city_cache[loc_key] = closest
            save_cache()  # Save after each new lookup
            return closest
        city_cache[loc_key] = "UNKNOWN"  # Cache negatives too
        return "UNKNOWN"
    except (GeocoderTimedOut, GeocoderServiceError):
        if attempt <= MAX_RETRY_ATTEMPTS:
            time.sleep(RETRY_DELAY_BASE * attempt)
            return find_nearest_airport(city, state, attempt + 1)
        city_cache[loc_key] = "GEO_TIMEOUT"
        return "GEO_TIMEOUT"

# --- CORE PHASES ---

def phase_1_split():
    """Drops bloat columns and shatters files."""
    all_files = list(Path(FOLDER_PATH).glob('*.csv')) + list(Path(FOLDER_PATH).glob('*.xlsx'))
    for file_path in all_files:
        if "Enriched_" in file_path.name or "Split_Chunks" in str(file_path): continue
        
        try:
            print(f"\n--- Loading: {file_path.name} ---")
            df = pd.read_excel(file_path) if file_path.suffix == '.xlsx' else pd.read_csv(file_path, encoding='utf-8', on_bad_lines='skip')
            
            # GHOST BUSTER: Drop completely empty columns and 'Unnamed' noise
            df = df.dropna(axis=1, how='all')
            df = df.loc[:, ~df.columns.str.contains('^Unnamed|^Column', case=False, na=False)]
            
            # VALIDATOR
            sum_c = get_best_col(df, ['summary', 'narrative', 'description', 'remarks', 'event'])
            print(f"  Target Column identified as: [{sum_c}]")
            
            for i in range(0, len(df), ROWS_PER_SPLIT):
                chunk = df.iloc[i : i + ROWS_PER_SPLIT]
                chunk.to_csv(SPLIT_FOLDER / f"{file_path.stem}_part_{i//ROWS_PER_SPLIT + 1}.csv", index=False)
        except Exception as e:
            print(f"  ERROR processing {file_path.name}: {e}")
            continue

def phase_2_enrich():
    """Processes chunks with Regex-First priority."""
    split_files = sorted(list(SPLIT_FOLDER.glob('*.csv')))
    total_files = len(split_files)
    
    for idx, file_path in enumerate(split_files, 1):
        out_file = OUTPUT_FOLDER / f"Enriched_{file_path.name}"
        if out_file.exists(): 
            print(f"[{idx}/{total_files}] Skipping {file_path.name} (already processed)")
            continue
            
        print(f"[{idx}/{total_files}] Processing: {file_path.name}")
        df = pd.read_csv(file_path)
        sum_c = get_best_col(df, ['summary', 'narrative', 'description', 'remarks', 'event'])
        city_c = get_best_col(df, ['city', 'location', 'town'])
        stat_c = get_best_col(df, ['state', 'province'])

        if not sum_c: 
            print(f"  WARNING: No summary column found, skipping file")
            continue
        if not city_c or not stat_c:
            print(f"  WARNING: Missing city/state columns - geocoding may fail")

        # REGEX (Instant)
        df[['Acft_Type', 'UAS_Color', 'Alt_Ft', 'Evasive']] = df[sum_c].apply(extract_details)
        df['LEO_Agency'] = df[sum_c].apply(extract_leo_agency)

        # AIRPORT ASSIGNMENT (Prioritize direct extraction)
        # Step 1: Try to extract airport code directly from summary
        df['Assigned_Airport'] = df[sum_c].apply(extract_airport_code)
        
        # Step 2: Fill missing airports with geocoding (Slow)
        missing_mask = df['Assigned_Airport'].isna()
        if missing_mask.any():
            print(f"  Extracted {(~missing_mask).sum()} airport codes from text, geocoding {missing_mask.sum()} remaining...")
            df.loc[missing_mask, 'Assigned_Airport'] = df[missing_mask].apply(
                lambda r: find_nearest_airport(r.get(city_c), r.get(stat_c)), axis=1
            )
        else:
            print(f"  Extracted all {len(df)} airport codes from text (no geocoding needed)")
        # Get coordinates - check IATA first, then ICAO if not found
        def get_airport_coords(code, coord_type):
            if not code or code in ['UNKNOWN', 'GEO_TIMEOUT']:
                return None
            
            # Try IATA first
            if code in us_airports:
                coords = us_airports[code]
                result = coords.get(coord_type)
                if result is None:
                    # DEBUG: Show what keys are available
                    print(f"  [DEBUG] {code} found in IATA but '{coord_type}' missing. Available keys: {list(coords.keys())}")
                return result
            
            # Try ICAO (some airports only in ICAO database)
            k_code = f'K{code}'  # US airports have K prefix in ICAO
            if k_code in icao_airports and icao_airports[k_code].get('country') == 'US':
                coords = icao_airports[k_code]
                result = coords.get(coord_type)
                if result is None:
                    print(f"  [DEBUG] {k_code} found in ICAO but '{coord_type}' missing. Available keys: {list(coords.keys())}")
                return result
            
            # Not found in either
            print(f"  [DEBUG] Airport '{code}' not found in IATA or ICAO (tried K{code})")
            return None
        
        df['Airport_Longitude'] = df['Assigned_Airport'].apply(lambda code: get_airport_coords(code, 'lon'))
        df['Airport_Latitude'] = df['Assigned_Airport'].apply(lambda code: get_airport_coords(code, 'lat'))
        
        # DEBUG: Show coordinate population summary
        coords_populated = df['Airport_Longitude'].notna().sum()
        print(f"  [DEBUG] Coordinates populated for {coords_populated}/{len(df)} records")
        if coords_populated == 0 and len(df) > 0:
            print(f"  [DEBUG] Sample assigned airports: {df['Assigned_Airport'].head(5).tolist()}")
        
        # VALIDATE COORDINATES
        valid_coords = df[
            (df['Airport_Longitude'].notna()) & 
            (df['Airport_Latitude'].notna()) &
            (df['Airport_Longitude'].between(LON_MIN, LON_MAX)) &
            (df['Airport_Latitude'].between(LAT_MIN, LAT_MAX))
        ]
        print(f"  Valid coordinates: {len(valid_coords)}/{len(df)} records")
        
        df.to_csv(out_file, index=False)
        time.sleep(5) # API Breathing room

def phase_3_consolidate_by_year():
    """Final merge into Yearly Master Files."""
    # Search across ALL dated processed folders
    processed_parent = Path(FOLDER_PATH) / "Processed_Files"
    all_parts = list(processed_parent.glob("**/Enriched_*.csv"))
    
    if not all_parts:
        print("No enriched files found to consolidate")
        return
    
    years = set(re.search(r'20\d{2}', f.name).group() for f in all_parts if re.search(r'20\d{2}', f.name))
    for year in years:
        year_parts = [f for f in all_parts if year in f.name]
        print(f"\nConsolidating {len(year_parts)} files for {year}...")
        
        combined = pd.concat([pd.read_csv(f) for f in year_parts], ignore_index=True)
        
        # Standardize missing values
        for col in combined.columns:
            combined[col] = combined[col].apply(standardize_value)
        
        # DEDUPLICATE - exact duplicates first
        original_count = len(combined)
        combined = combined.drop_duplicates()
        exact_dupes = original_count - len(combined)
        
        # SMART DEDUPLICATION - likely duplicates based on date/city/altitude
        # Check if we have date column
        date_col = get_best_col(combined, ['date', 'event_date', 'sighting_date', 'occurred'])
        city_col = get_best_col(combined, ['city', 'location', 'town'])
        
        if date_col and city_col and 'Alt_Ft' in combined.columns:
            pre_smart_count = len(combined)
            combined = combined.drop_duplicates(subset=[date_col, city_col, 'Alt_Ft'], keep='first')
            smart_dupes = pre_smart_count - len(combined)
            print(f"  Removed {exact_dupes} exact duplicates + {smart_dupes} likely duplicates")
        else:
            print(f"  Removed {exact_dupes} exact duplicate records")
        
        output_file = YEARLY_FOLDER / f"FAA_Master_{year}.csv"
        combined.to_csv(output_file, index=False)
        print(f"  Saved {len(combined)} records to {output_file.name}")

if __name__ == "__main__":
    try:
        phase_1_split()
        phase_2_enrich()
        phase_3_consolidate_by_year()
    finally:
        save_cache()  # Always save cache, even if interrupted
        print("\n=== Processing Complete ===")
import pandas as pd
import airportsdata
import os
import re
import time
import json
import logging
import stat
import threading
import platform
from pathlib import Path
from datetime import datetime
from geopy.geocoders import Nominatim
from geopy.distance import geodesic
from geopy.exc import GeocoderTimedOut, GeocoderServiceError

try:
    from jsonschema import validate, ValidationError
    JSONSCHEMA_AVAILABLE = True
except ImportError:
    JSONSCHEMA_AVAILABLE = False
    print("WARNING: jsonschema not installed. Cache validation disabled. Install with: pip install jsonschema")

# --- CONFIGURATION ---
# Default to platform-agnostic path in user's home directory
# You can override this by setting the FAA_DATA_PATH environment variable
FOLDER_PATH = os.getenv('FAA_DATA_PATH') or str(Path.home() / "FAA_UAS_Sightings")
RUN_DATE = datetime.now().strftime("%Y-%m-%d")  # e.g., "2026-02-07"

# Security Configuration
MAX_FILE_SIZE_MB = 100  # Maximum file size to process
MAX_FILE_SIZE_BYTES = MAX_FILE_SIZE_MB * 1024 * 1024
MAX_TEXT_LENGTH = 50000  # Maximum text length for regex operations
REGEX_TIMEOUT_SECONDS = 2  # Timeout for regex operations

# Allowed base directories (path traversal protection)
# Automatically includes user's home directory and current working directory
ALLOWED_BASE_DIRS = [
    str(Path.home()),  # User's home directory (works on all platforms)
    os.getcwd(),  # Current working directory
    str(Path.home() / "FAA_UAS_Sightings"),
    str(Path.home() / "Documents" / "FAA_UAS_Sightings"),
]

# Logging Configuration
DEBUG_MODE = os.getenv('FAA_PIPELINE_DEBUG', 'false').lower() == 'true'
logging.basicConfig(
    level=logging.DEBUG if DEBUG_MODE else logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# --- SECURITY FUNCTIONS ---

class SecurityError(Exception):
    """Raised when security validation fails."""
    pass

def validate_folder_path(folder_path):
    """Ensure folder_path is within allowed directories."""
    resolved_path = Path(folder_path).resolve()
    
    for allowed_base in ALLOWED_BASE_DIRS:
        allowed_resolved = Path(allowed_base).resolve()
        try:
            resolved_path.relative_to(allowed_resolved)
            logger.info(f"Path validation passed: {folder_path}")
            return True
        except ValueError:
            continue
    
    raise SecurityError(f"Folder path '{folder_path}' is outside allowed directories: {ALLOWED_BASE_DIRS}")

def secure_cache_permissions(file_path):
    """Set file to be readable/writable only by owner (0600 on Unix, restricted ACL on Windows)."""
    if not file_path.exists():
        return
    
    system = platform.system()
    
    try:
        if system == "Windows":
            # Windows: Remove permissions for everyone except current user
            try:
                import win32security
                import ntsecuritycon as con
                import win32api
                
                # Get current user SID
                user_sid = win32security.LookupAccountName("", win32api.GetUserName())[0]
                
                # Create new DACL with only owner having full control
                dacl = win32security.ACL()
                dacl.AddAccessAllowedAce(win32security.ACL_REVISION, con.FILE_ALL_ACCESS, user_sid)
                
                # Set new security descriptor
                sd = win32security.SECURITY_DESCRIPTOR()
                sd.SetSecurityDescriptorDacl(1, dacl, 0)
                win32security.SetFileSecurity(
                    str(file_path), 
                    win32security.DACL_SECURITY_INFORMATION, 
                    sd
                )
                logger.debug(f"Set secure permissions (Windows ACL) on {file_path}")
            except ImportError:
                # pywin32 not installed - fallback to basic chmod (limited effect on Windows)
                logger.warning("pywin32 not installed. Install with 'pip install pywin32' for proper Windows file permissions.")
                os.chmod(file_path, stat.S_IREAD | stat.S_IWRITE)
                logger.debug(f"Set basic permissions on {file_path} (limited on Windows)")
        else:
            # Unix/Linux/Mac: Use standard Unix permissions (0600)
            os.chmod(file_path, stat.S_IRUSR | stat.S_IWUSR)
            logger.debug(f"Set secure permissions (0600) on {file_path}")
    except Exception as e:
        logger.warning(f"Could not set secure permissions on {file_path}: {e}")

# JSON Schema for cache validation
CACHE_SCHEMA = {
    "type": "object",
    "patternProperties": {
        "^.+,\\s[A-Z]{2}$": {
            "type": "string",
            "pattern": "^([A-Z]{3}|UNKNOWN|GEO_TIMEOUT)$"
        }
    }
}

def load_cache_safely(cache_file):
    """Load and validate geocoding cache."""
    if not cache_file.exists():
        return {}
    
    try:
        with open(cache_file, 'r') as f:
            cache_data = json.load(f)
        
        # Validate against schema if jsonschema is available
        if JSONSCHEMA_AVAILABLE:
            validate(instance=cache_data, schema=CACHE_SCHEMA)
            logger.info(f"Loaded {len(cache_data)} cached locations (validated)")
        else:
            logger.info(f"Loaded {len(cache_data)} cached locations (validation skipped)")
        
        return cache_data
        
    except json.JSONDecodeError as e:
        logger.error(f"Cache file corrupted (invalid JSON): {e}")
        logger.info("Starting with empty cache")
        return {}
    except ValidationError as e:
        logger.error(f"Cache file contains invalid data: {e.message}")
        logger.info("Starting with empty cache")
        return {}
    except Exception as e:
        logger.error(f"Error loading cache: {e}")
        return {}

def save_cache_with_validation():
    """Persist geocoding cache to disk with validation and secure permissions."""
    try:
        # Validate before saving if jsonschema available
        if JSONSCHEMA_AVAILABLE:
            validate(instance=city_cache, schema=CACHE_SCHEMA)
        
        with open(CACHE_FILE, 'w') as f:
            json.dump(city_cache, f, indent=2)
        
        # Set secure permissions
        secure_cache_permissions(CACHE_FILE)
        
        logger.info(f"Saved {len(city_cache)} locations to cache")
    except ValidationError as e:
        logger.error(f"Cache data invalid, not saving: {e.message}")
    except Exception as e:
        logger.error(f"Error saving cache: {e}")

def safe_regex_search(pattern, text, timeout_seconds=REGEX_TIMEOUT_SECONDS, flags=0):
    """Regex search with timeout protection using threading."""
    if not text:
        return None
    
    result = [None]
    exception = [None]
    
    def worker():
        try:
            result[0] = re.search(pattern, text, flags)
        except Exception as e:
            exception[0] = e
    
    thread = threading.Thread(target=worker)
    thread.daemon = True
    thread.start()
    thread.join(timeout=timeout_seconds)
    
    if thread.is_alive():
        logger.warning(f"Regex timeout on text: {text[:100]}...")
        return None
    
    if exception[0]:
        raise exception[0]
    
    return result[0]

# --- PATH AND FOLDER SETUP ---

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

# Validate folder path before proceeding
try:
    validate_folder_path(FOLDER_PATH)
except SecurityError as e:
    logger.error(f"Security validation failed: {e}")
    raise

# Create necessary folders
for folder in [SPLIT_FOLDER, OUTPUT_FOLDER, YEARLY_FOLDER]:
    folder.mkdir(exist_ok=True)

ROWS_PER_SPLIT = 250  # Smaller chunks = better stability for 2025 files
geolocator = Nominatim(user_agent="faa_uas_precision_v8")

# Load geocoding cache from disk with validation
city_cache = load_cache_safely(CACHE_FILE)

all_airports = airportsdata.load('IATA')
us_airports = {k: v for k, v in all_airports.items() if v['country'] == 'US'}

# ICAO to IATA conversion for US airports (K prefix)
icao_airports = airportsdata.load('ICAO')
icao_to_iata = {icao: data['iata'] for icao, data in icao_airports.items() 
                 if data.get('country') == 'US' and data.get('iata') and icao.startswith('K')}

# Show sample airport data structure
if us_airports:
    sample_code = list(us_airports.keys())[0]
    logger.debug(f"Sample IATA entry ({sample_code}): {us_airports[sample_code]}")
if icao_airports:
    sample_icao = [k for k in icao_airports.keys() if k.startswith('K')][0]
    logger.debug(f"Sample ICAO entry ({sample_icao}): {icao_airports[sample_icao]}")
logger.info(f"Loaded {len(us_airports)} US IATA airports and {len(icao_to_iata)} ICAO mappings")

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
    """Persist geocoding cache to disk (wrapper for backward compatibility)."""
    save_cache_with_validation()

def get_best_col(df, keywords):
    for col in df.columns:
        if any(key.lower() in col.lower() for key in keywords):
            return col
    return None

def extract_details(text):
    if not isinstance(text, str): return pd.Series([None, "UNKNOWN", None, "NO"])
    
    # Truncate excessively long text to prevent ReDoS
    if len(text) > MAX_TEXT_LENGTH:
        logger.warning(f"Text truncated from {len(text)} to {MAX_TEXT_LENGTH} chars")
        text = text[:MAX_TEXT_LENGTH]
    
    # Aircraft type - multiple patterns
    acft = None
    acft_patterns = [
        r'ADVISED,\s*([A-Z0-9]{2,6}),',  # Original pattern
        r'AIRCRAFT TYPE[:\s]+([A-Z0-9]{2,6})\b',
        r'\b(CESSNA|BOEING|AIRBUS|PIPER|BEECH|CIRRUS|GULFSTREAM|EMBRAER)\b',
    ]
    for pattern in acft_patterns:
        match = safe_regex_search(pattern, text)
        if match:
            acft = match.group(1)
            break
    
    # Color - simplified to avoid ReDoS with lookahead
    # First check if UAS/DRONE keywords exist
    has_drone = bool(safe_regex_search(r'\b(UAS|DRONE)\b', text, flags=re.IGNORECASE))
    color = "UNKNOWN"
    if has_drone:
        color_match = safe_regex_search(
            r'\b(RED|BLACK|GREY|GRAY|WHITE|ORANGE|GREEN|BLUE|SILVER|YELLOW|BROWN|TAN|PINK|PURPLE|GOLD|BEIGE|MULTI[- ]COLOR)\b', 
            text, flags=re.IGNORECASE
        )
        if color_match:
            color = color_match.group(1).upper().replace('MULTI-COLOR', 'MULTI-COLORED').replace('MULTI COLOR', 'MULTI-COLORED')
    
    # Altitude - multiple formats
    alt = None
    alt_patterns = [
        r'(\d{1,3}(?:,\d{3})*)\s*(?:FEET|FT)\b',  # "1,500 FEET" or "500 FT"
        r'FL\s*(\d{2,3})\b',  # "FL250" (flight level)
    ]
    for pattern in alt_patterns:
        match = safe_regex_search(pattern, text)
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
    
    # Truncate long text
    if len(text) > MAX_TEXT_LENGTH:
        text = text[:MAX_TEXT_LENGTH]
    
    # Check for "not reported" variations first
    if any(p in text.upper() for p in ["NOT REPORTED", "NO LEO", "NOT NOTIFIED", "NOTIFICATION NOT REPORTED", "LEOS NOT NOTIFIED"]):
        return "NONE REPORTED"
    
    # FAA facility keywords to exclude (these get ADVISED, not NOTIFIED)
    faa_facilities = ['ATCT', 'TRACON', 'APCH', 'APPROACH', 'TWR', 'TOWER', 'CENTER', 'FSS', 'ARTCC']
    
    # Look for ALL agencies before "NOTIFIED" and take the LAST one (LEO notification is at end)
    matches = []
    for match in re.finditer(r'([A-Z][A-Z\s]{2,40}?)\s+NOTIFIED', text):
        matches.append(match)
    
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
    
    # Truncate long text
    if len(text) > MAX_TEXT_LENGTH:
        text = text[:MAX_TEXT_LENGTH]
    
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
    
    winner = candidates[0]
    logger.debug(f"Extracted airport '{winner['code']}' (priority: {winner['priority']})")
    
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
            # Security: Check file size before loading
            file_size = file_path.stat().st_size
            if file_size > MAX_FILE_SIZE_BYTES:
                logger.error(f"File {file_path.name} exceeds size limit ({file_size / 1024 / 1024:.1f}MB > {MAX_FILE_SIZE_MB}MB), skipping")
                continue
            
            logger.info(f"Loading: {file_path.name} ({file_size / 1024 / 1024:.1f}MB)")
            df = pd.read_excel(file_path) if file_path.suffix == '.xlsx' else pd.read_csv(file_path, encoding='utf-8', on_bad_lines='warn')
            
            # GHOST BUSTER: Drop completely empty columns and 'Unnamed' noise
            df = df.dropna(axis=1, how='all')
            df = df.loc[:, ~df.columns.str.contains('^Unnamed|^Column', case=False, na=False)]
            
            # VALIDATOR
            sum_c = get_best_col(df, ['summary', 'narrative', 'description', 'remarks', 'event'])
            logger.info(f"  Target Column identified as: [{sum_c}]")
            
            for i in range(0, len(df), ROWS_PER_SPLIT):
                chunk = df.iloc[i : i + ROWS_PER_SPLIT]
                chunk.to_csv(SPLIT_FOLDER / f"{file_path.stem}_part_{i//ROWS_PER_SPLIT + 1}.csv", index=False)
        except Exception as e:
            logger.error(f"Error processing {file_path.name}: {e}")
            continue

def phase_2_enrich():
    """Processes chunks with Regex-First priority."""
    split_files = sorted(list(SPLIT_FOLDER.glob('*.csv')))
    total_files = len(split_files)
    
    for idx, file_path in enumerate(split_files, 1):
        out_file = OUTPUT_FOLDER / f"Enriched_{file_path.name}"
        if out_file.exists(): 
            logger.info(f"[{idx}/{total_files}] Skipping {file_path.name} (already processed)")
            continue
            
        logger.info(f"[{idx}/{total_files}] Processing: {file_path.name}")
        df = pd.read_csv(file_path)
        sum_c = get_best_col(df, ['summary', 'narrative', 'description', 'remarks', 'event'])
        city_c = get_best_col(df, ['city', 'location', 'town'])
        stat_c = get_best_col(df, ['state', 'province'])

        if not sum_c: 
            logger.warning(f"  No summary column found, skipping file")
            continue
        if not city_c or not stat_c:
            logger.warning(f"  Missing city/state columns - geocoding may fail")

        # REGEX (Instant)
        df[['Acft_Type', 'UAS_Color', 'Alt_Ft', 'Evasive']] = df[sum_c].apply(extract_details)
        df['LEO_Agency'] = df[sum_c].apply(extract_leo_agency)

        # AIRPORT ASSIGNMENT (Prioritize direct extraction)
        # Step 1: Try to extract airport code directly from summary
        df['Assigned_Airport'] = df[sum_c].apply(extract_airport_code)
        
        # Step 2: Fill missing airports with geocoding (Slow)
        missing_mask = df['Assigned_Airport'].isna()
        if missing_mask.any():
            logger.info(f"  Extracted {(~missing_mask).sum()} airport codes from text, geocoding {missing_mask.sum()} remaining...")
            df.loc[missing_mask, 'Assigned_Airport'] = df[missing_mask].apply(
                lambda r: find_nearest_airport(r.get(city_c), r.get(stat_c)), axis=1
            )
        else:
            logger.info(f"  Extracted all {len(df)} airport codes from text (no geocoding needed)")
        # Get coordinates - check IATA first, then ICAO if not found
        def get_airport_coords(code, coord_type):
            if not code or code in ['UNKNOWN', 'GEO_TIMEOUT']:
                return None
            
            # Try IATA first
            if code in us_airports:
                coords = us_airports[code]
                result = coords.get(coord_type)
                if result is None:
                    logger.debug(f"{code} found in IATA but '{coord_type}' missing. Available keys: {list(coords.keys())}")
                return result
            
            # Try ICAO (some airports only in ICAO database)
            k_code = f'K{code}'  # US airports have K prefix in ICAO
            if k_code in icao_airports and icao_airports[k_code].get('country') == 'US':
                coords = icao_airports[k_code]
                result = coords.get(coord_type)
                if result is None:
                    logger.debug(f"{k_code} found in ICAO but '{coord_type}' missing. Available keys: {list(coords.keys())}")
                return result
            
            # Not found in either
            logger.debug(f"Airport '{code}' not found in IATA or ICAO (tried K{code})")
            return None
        
        df['Airport_Longitude'] = df['Assigned_Airport'].apply(lambda code: get_airport_coords(code, 'lon'))
        df['Airport_Latitude'] = df['Assigned_Airport'].apply(lambda code: get_airport_coords(code, 'lat'))
        
        # Show coordinate population summary
        coords_populated = df['Airport_Longitude'].notna().sum()
        logger.debug(f"  Coordinates populated for {coords_populated}/{len(df)} records")
        if coords_populated == 0 and len(df) > 0:
            logger.debug(f"  Sample assigned airports: {df['Assigned_Airport'].head(5).tolist()}")
        
        # VALIDATE COORDINATES
        valid_coords = df[
            (df['Airport_Longitude'].notna()) & 
            (df['Airport_Latitude'].notna()) &
            (df['Airport_Longitude'].between(LON_MIN, LON_MAX)) &
            (df['Airport_Latitude'].between(LAT_MIN, LAT_MAX))
        ]
        logger.info(f"  Valid coordinates: {len(valid_coords)}/{len(df)} records")
        
        df.to_csv(out_file, index=False)
        time.sleep(5) # API Breathing room

def phase_3_consolidate_by_year():
    """Final merge into Yearly Master Files."""
    # Search across ALL dated processed folders
    processed_parent = Path(FOLDER_PATH) / "Processed_Files"
    all_parts = list(processed_parent.glob("**/Enriched_*.csv"))
    
    if not all_parts:
        logger.warning("No enriched files found to consolidate")
        return
    
    years = set(re.search(r'20\d{2}', f.name).group() for f in all_parts if re.search(r'20\d{2}', f.name))
    for year in years:
        year_parts = [f for f in all_parts if year in f.name]
        logger.info(f"Consolidating {len(year_parts)} files for {year}...")
        
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
            logger.info(f"  Removed {exact_dupes} exact duplicates + {smart_dupes} likely duplicates")
        else:
            logger.info(f"  Removed {exact_dupes} exact duplicate records")
        
        output_file = YEARLY_FOLDER / f"FAA_Master_{year}.csv"
        combined.to_csv(output_file, index=False)
        logger.info(f"  Saved {len(combined)} records to {output_file.name}")

if __name__ == "__main__":
    try:
        logger.info("=== Starting FAA UAS Sighting Enrichment Pipeline ===")
        logger.info(f"Debug mode: {'ENABLED' if DEBUG_MODE else 'DISABLED'}")
        phase_1_split()
        phase_2_enrich()
        phase_3_consolidate_by_year()
    except SecurityError as e:
        logger.critical(f"Security validation failed: {e}")
        raise
    except Exception as e:
        logger.error(f"Pipeline failed: {e}", exc_info=True)
        raise
    finally:
        save_cache()  # Always save cache, even if interrupted
        logger.info("=== Processing Complete ===")
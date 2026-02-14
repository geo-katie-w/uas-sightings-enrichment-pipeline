"""Text extraction utilities (regex-based parsing)."""
from __future__ import annotations

import logging
import re
import threading
from typing import Dict, Iterable, Optional, Set

import pandas as pd

DEFAULT_AIRPORT_BLACKLIST: Set[str] = {
    'FBI', 'FAA', 'TSA', 'DHS', 'LEO', 'ATC', 'VFR', 'IFR', 'UAS',
    'UFO', 'USA', 'UTC', 'EST', 'PST', 'MST', 'CST', 'EDT', 'PDT', 'MDT', 'CDT'
}

DEFAULT_STATE_ABBREV: Dict[str, str] = {
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

logger = logging.getLogger(__name__)


def safe_regex_search(pattern: str, text: str, timeout_seconds: int, flags: int = 0) -> Optional[re.Match]:
    """Regex search with timeout protection using threading."""
    if not text:
        return None

    result = [None]
    exception = [None]

    def worker() -> None:
        try:
            result[0] = re.search(pattern, text, flags)
        except Exception as exc:  # pragma: no cover - defensive
            exception[0] = exc

    thread = threading.Thread(target=worker)
    thread.daemon = True
    thread.start()
    thread.join(timeout=timeout_seconds)

    if thread.is_alive():
        logger.warning("Regex timeout on text: %s...", text[:100])
        return None

    if exception[0]:
        raise exception[0]

    return result[0]


def standardize_value(val):
    """Standardize missing/unknown values to None."""
    if pd.isna(val) or val is None:
        return None
    if isinstance(val, str):
        val_upper = val.strip().upper()
        if val_upper in ['N/A', 'NA', 'UNKNOWN', 'NOT REPORTED', 'NONE', 'NULL', '', 'UNREPORTED']:
            return None
    return val


def normalize_state(state: Optional[str], state_abbrev: Dict[str, str]) -> Optional[str]:
    """Normalize state name to 2-letter abbreviation."""
    if not state or not isinstance(state, str):
        return None
    state = state.strip().upper()
    if len(state) == 2:
        return state
    return state_abbrev.get(state, state)


def get_best_col(df: pd.DataFrame, keywords: Iterable[str]) -> Optional[str]:
    for col in df.columns:
        if any(key.lower() in col.lower() for key in keywords):
            return col
    return None


def extract_details(text: str, max_text_length: int, timeout_seconds: int) -> pd.Series:
    if not isinstance(text, str):
        return pd.Series([None, "UNKNOWN", None, "NO"])

    if len(text) > max_text_length:
        logger.warning("Text truncated from %s to %s chars", len(text), max_text_length)
        text = text[:max_text_length]

    acft = None
    acft_patterns = [
        r'ADVISED,\s*([A-Z0-9]{2,6}),',
        r'AIRCRAFT TYPE[:\s]+([A-Z0-9]{2,6})\b',
        r'\b(CESSNA|BOEING|AIRBUS|PIPER|BEECH|CIRRUS|GULFSTREAM|EMBRAER)\b',
    ]
    for pattern in acft_patterns:
        match = safe_regex_search(pattern, text, timeout_seconds)
        if match:
            acft = match.group(1)
            break

    has_drone = bool(safe_regex_search(r'\b(UAS|DRONE)\b', text, timeout_seconds, flags=re.IGNORECASE))
    color = "UNKNOWN"
    if has_drone:
        color_match = safe_regex_search(
            r'\b(RED|BLACK|GREY|GRAY|WHITE|ORANGE|GREEN|BLUE|SILVER|YELLOW|BROWN|TAN|PINK|PURPLE|GOLD|BEIGE|MULTI[- ]COLOR)\b',
            text,
            timeout_seconds,
            flags=re.IGNORECASE
        )
        if color_match:
            color = (
                color_match.group(1)
                .upper()
                .replace('MULTI-COLOR', 'MULTI-COLORED')
                .replace('MULTI COLOR', 'MULTI-COLORED')
            )

    alt = None
    alt_patterns = [
        r'(\d{1,3}(?:,\d{3})*)\s*(?:FEET|FT)\b',
        r'FL\s*(\d{2,3})\b',
    ]
    for pattern in alt_patterns:
        match = safe_regex_search(pattern, text, timeout_seconds)
        if match:
            alt_str = match.group(1).replace(',', '')
            if 'FL' in pattern:
                alt = str(int(alt_str) * 100)
            else:
                alt = alt_str
            break

    evasive = "YES" if "EVASIVE ACTION" in text and "NO EVASIVE" not in text else "NO"
    return pd.Series([acft, color, alt, evasive])


def extract_leo_agency(text: str, max_text_length: int) -> str:
    if not isinstance(text, str):
        return "UNKNOWN"

    if len(text) > max_text_length:
        text = text[:max_text_length]

    if any(p in text.upper() for p in ["NOT REPORTED", "NO LEO", "NOT NOTIFIED", "NOTIFICATION NOT REPORTED", "LEOS NOT NOTIFIED"]):
        return "NONE REPORTED"

    faa_facilities = ['ATCT', 'TRACON', 'APCH', 'APPROACH', 'TWR', 'TOWER', 'CENTER', 'FSS', 'ARTCC']

    matches = []
    for match in re.finditer(r'([A-Z][A-Z\s]{2,40}?)\s+NOTIFIED', text):
        matches.append(match)

    for match in reversed(matches):
        agency = match.group(1).strip()
        if any(fac in agency for fac in faa_facilities):
            continue

        agency = re.sub(r'^(LEO|THE|AND|ACTION|EVASIVE)\s+', '', agency)
        agency = re.sub(r'\s+(NO|NOT|TAKEN|REPORTED)\.?$', '', agency)
        agency = agency.strip('. ')

        if len(agency) >= 2 and agency not in ['NO', 'WAS', 'WERE', 'ACTION', 'EVASIVE', 'WOC']:
            return agency

    return "UNKNOWN"


def extract_airport_code(
    text: str,
    max_text_length: int,
    us_airports: Dict[str, Dict],
    icao_to_iata: Dict[str, str],
    airport_blacklist: Set[str],
) -> Optional[str]:
    """Extract 3-letter IATA or 4-letter ICAO airport code from text."""
    if not isinstance(text, str):
        return None

    if len(text) > max_text_length:
        text = text[:max_text_length]

    patterns = [
        (r'(\d+\.?\d*)\s+(N|S|E|W|NE|NW|SE|SW|NNE|NNW|SSE|SSW|ENE|ESE|WNW|WSW)\s+([A-Z]{3})\b', 'critical', 3),
        (r'RUNWAY\s+\d+[LRC]?\s+([A-Z]{3})\b', 'high', 1),
        (r'\b(K[A-Z]{3})\b', 'high', 1),
        (r'\b([A-Z]{3})\s+(?:AIRPORT|ARPT|TWR|TOWER|ATCT)', 'high', 1),
        (r'\(([A-Z]{3})\)', 'medium', 1),
        (r'\b([A-Z]{3})\s+(?:CLASS|AIRSPACE)', 'medium', 1),
        (r'(?:NEAR|AT|OVER|BY|FROM)\s+([A-Z]{3})\b', 'medium', 1),
        (r'([A-Z]{3})\s*-\s*[A-Z]', 'low', 1),
    ]

    candidates = []
    for pattern, priority, group_num in patterns:
        for match in re.finditer(pattern, text, re.IGNORECASE):
            code = match.group(group_num)

            if len(code) == 4 and code.startswith('K'):
                if code in icao_to_iata:
                    code = icao_to_iata[code]
                else:
                    continue

            if code in us_airports and code not in airport_blacklist:
                candidates.append({
                    'code': code,
                    'priority': priority,
                    'position': match.start(),
                })

    if not candidates:
        return None

    priority_order = {'critical': 4, 'high': 3, 'medium': 2, 'low': 1}
    candidates.sort(key=lambda x: (priority_order[x['priority']], x['position']), reverse=True)
    return candidates[0]['code']

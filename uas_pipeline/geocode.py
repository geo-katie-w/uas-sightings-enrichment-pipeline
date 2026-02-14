"""Geocoding helpers."""
from __future__ import annotations

import json
import logging
import time
from pathlib import Path
from typing import Callable, Dict, Optional

from geopy.distance import geodesic
from geopy.exc import GeocoderTimedOut, GeocoderServiceError

from .security import secure_cache_permissions

try:
    from jsonschema import validate, ValidationError
    JSONSCHEMA_AVAILABLE = True
except ImportError:  # pragma: no cover - optional dependency
    JSONSCHEMA_AVAILABLE = False

CACHE_SCHEMA = {
    "type": "object",
    "patternProperties": {
        "^.+,\\s[A-Z]{2}$": {
            "type": "string",
            "pattern": "^([A-Z]{3}|UNKNOWN|GEO_TIMEOUT)$"
        }
    }
}

logger = logging.getLogger(__name__)


def load_cache_safely(cache_file: Path) -> Dict[str, str]:
    if not cache_file.exists():
        return {}

    try:
        with open(cache_file, 'r') as f:
            cache_data = json.load(f)

        if JSONSCHEMA_AVAILABLE:
            validate(instance=cache_data, schema=CACHE_SCHEMA)
            logger.info("Loaded %s cached locations (validated)", len(cache_data))
        else:
            logger.info("Loaded %s cached locations (validation skipped)", len(cache_data))

        return cache_data

    except json.JSONDecodeError as exc:
        logger.error("Cache file corrupted (invalid JSON): %s", exc)
        logger.info("Starting with empty cache")
        return {}
    except ValidationError as exc:
        logger.error("Cache file contains invalid data: %s", exc.message)
        logger.info("Starting with empty cache")
        return {}
    except Exception as exc:  # pragma: no cover - defensive
        logger.error("Error loading cache: %s", exc)
        return {}


def save_cache_with_validation(cache_file: Path, city_cache: Dict[str, str]) -> None:
    try:
        if JSONSCHEMA_AVAILABLE:
            validate(instance=city_cache, schema=CACHE_SCHEMA)

        with open(cache_file, 'w') as f:
            json.dump(city_cache, f, indent=2)

        secure_cache_permissions(cache_file)
        logger.info("Saved %s locations to cache", len(city_cache))
    except ValidationError as exc:
        logger.error("Cache data invalid, not saving: %s", exc.message)
    except Exception as exc:  # pragma: no cover - defensive
        logger.error("Error saving cache: %s", exc)


def find_nearest_airport(
    city: Optional[str],
    state: Optional[str],
    normalize_state: Callable[[Optional[str]], Optional[str]],
    city_cache: Dict[str, str],
    geolocator,
    us_airports: Dict[str, Dict],
    max_retry_attempts: int,
    retry_delay_base_seconds: int,
) -> str:
    if not city or not state:
        return "UNKNOWN"

    state = normalize_state(state)
    if not state:
        return "UNKNOWN"

    loc_key = f"{city}, {state}"
    if loc_key in city_cache:
        return city_cache[loc_key]

    try:
        loc = geolocator.geocode(loc_key + ", USA", timeout=15)
        if loc:
            coords = (loc.latitude, loc.longitude)
            closest = min(
                us_airports.keys(),
                key=lambda k: geodesic(coords, (us_airports[k]['lat'], us_airports[k]['lon'])).miles
            )
            city_cache[loc_key] = closest
            return closest
        city_cache[loc_key] = "UNKNOWN"
        return "UNKNOWN"
    except (GeocoderTimedOut, GeocoderServiceError):
        if max_retry_attempts >= 1:
            for attempt in range(1, max_retry_attempts + 1):
                time.sleep(retry_delay_base_seconds * attempt)
                try:
                    loc = geolocator.geocode(loc_key + ", USA", timeout=15)
                    if loc:
                        coords = (loc.latitude, loc.longitude)
                        closest = min(
                            us_airports.keys(),
                            key=lambda k: geodesic(coords, (us_airports[k]['lat'], us_airports[k]['lon'])).miles
                        )
                        city_cache[loc_key] = closest
                        return closest
                except (GeocoderTimedOut, GeocoderServiceError):
                    continue
        city_cache[loc_key] = "GEO_TIMEOUT"
        return "GEO_TIMEOUT"

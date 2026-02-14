import tempfile
import unittest
from dataclasses import dataclass
from pathlib import Path

from uas_pipeline.extractors import DEFAULT_STATE_ABBREV, normalize_state
from uas_pipeline.geocode import (
    find_nearest_airport,
    load_cache_safely,
    save_cache_with_validation,
)


@dataclass
class DummyLocation:
    latitude: float
    longitude: float


class DummyGeolocator:
    def __init__(self, location=None, raise_error=False):
        self._location = location
        self._raise_error = raise_error

    def geocode(self, *_args, **_kwargs):
        if self._raise_error:
            raise Exception("geocode failed")
        return self._location


class TestGeocode(unittest.TestCase):
    def test_load_cache_safely_missing(self):
        with tempfile.TemporaryDirectory() as tmp:
            cache_file = Path(tmp) / "cache.json"
            cache = load_cache_safely(cache_file)
            self.assertEqual(cache, {})

    def test_save_and_load_cache(self):
        with tempfile.TemporaryDirectory() as tmp:
            cache_file = Path(tmp) / "cache.json"
            data = {"Seattle, WA": "SEA"}
            save_cache_with_validation(cache_file, data)
            loaded = load_cache_safely(cache_file)
            self.assertEqual(loaded, data)

    def test_load_cache_invalid_json(self):
        with tempfile.TemporaryDirectory() as tmp:
            cache_file = Path(tmp) / "cache.json"
            cache_file.write_text("not json")
            loaded = load_cache_safely(cache_file)
            self.assertEqual(loaded, {})

    def test_find_nearest_airport_returns_unknown_for_missing(self):
        result = find_nearest_airport(
            None,
            None,
            lambda s: normalize_state(s, DEFAULT_STATE_ABBREV),
            {},
            DummyGeolocator(None),
            {"SEA": {"lat": 47.448, "lon": -122.309}},
            1,
            1,
        )
        self.assertEqual(result, "UNKNOWN")

    def test_find_nearest_airport_selects_closest(self):
        geolocator = DummyGeolocator(DummyLocation(47.45, -122.30))
        us_airports = {
            "SEA": {"lat": 47.448, "lon": -122.309},
            "PDX": {"lat": 45.589, "lon": -122.595},
        }
        result = find_nearest_airport(
            "Seattle",
            "WA",
            lambda s: normalize_state(s, DEFAULT_STATE_ABBREV),
            {},
            geolocator,
            us_airports,
            1,
            1,
        )
        self.assertEqual(result, "SEA")


if __name__ == "__main__":
    unittest.main()

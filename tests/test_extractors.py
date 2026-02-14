import unittest

import pandas as pd

from uas_pipeline.extractors import (
    DEFAULT_AIRPORT_BLACKLIST,
    DEFAULT_STATE_ABBREV,
    extract_airport_code,
    extract_details,
    extract_leo_agency,
    get_best_col,
    normalize_state,
)


class TestExtractors(unittest.TestCase):
    def test_get_best_col(self):
        df = pd.DataFrame({"summary_text": ["a"], "city": ["b"]})
        self.assertEqual(get_best_col(df, ["summary", "narrative"]), "summary_text")

    def test_normalize_state(self):
        self.assertEqual(normalize_state("California", DEFAULT_STATE_ABBREV), "CA")
        self.assertEqual(normalize_state("TX", DEFAULT_STATE_ABBREV), "TX")

    def test_extract_details(self):
        text = "UAS RED DRONE ADVISED, C172, 1,500 FEET EVASIVE ACTION"
        acft, color, alt, evasive = extract_details(text, 50000, 2)
        self.assertEqual(acft, "C172")
        self.assertEqual(color, "RED")
        self.assertEqual(alt, "1500")
        self.assertEqual(evasive, "YES")

    def test_extract_leo_agency(self):
        text = "STATE POLICE NOTIFIED"
        self.assertEqual(extract_leo_agency(text, 50000), "STATE POLICE")

    def test_extract_airport_code_distance_direction(self):
        us_airports = {"LAX": {"country": "US"}}
        code = extract_airport_code(
            "5 NW LAX", 50000, us_airports, {}, DEFAULT_AIRPORT_BLACKLIST
        )
        self.assertEqual(code, "LAX")

    def test_extract_airport_code_icao(self):
        us_airports = {"SEA": {"country": "US"}}
        icao_to_iata = {"KSEA": "SEA"}
        code = extract_airport_code(
            "KSEA", 50000, us_airports, icao_to_iata, DEFAULT_AIRPORT_BLACKLIST
        )
        self.assertEqual(code, "SEA")


if __name__ == "__main__":
    unittest.main()

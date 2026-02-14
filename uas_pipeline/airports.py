"""Airport lookup data access."""
from __future__ import annotations

import airportsdata


def load_airports():
    all_airports = airportsdata.load('IATA')
    us_airports = {k: v for k, v in all_airports.items() if v['country'] == 'US'}

    icao_airports = airportsdata.load('ICAO')
    icao_to_iata = {
        icao: data['iata']
        for icao, data in icao_airports.items()
        if data.get('country') == 'US' and data.get('iata') and icao.startswith('K')
    }
    return us_airports, icao_airports, icao_to_iata

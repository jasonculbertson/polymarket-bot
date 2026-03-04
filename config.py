"""
City configs: series IDs, Wunderground stations, Open-Meteo coordinates.
Station is parsed live from the resolutionSource field — this is the fallback/reference map.

unit: "F" = US cities (brackets in °F, Polymarket resolves in °F)
      "C" = international cities (brackets in °C, Polymarket resolves in °C)
"""

CITIES = {
    "NYC": {
        "series_id": 10005,
        "station": "KLGA",
        "lat": 40.7769,
        "lon": -73.8740,
        "tz": "America/New_York",
        "unit": "F",
    },
    "Chicago": {
        "series_id": 10726,
        "station": "KORD",
        "lat": 41.9742,
        "lon": -87.9073,
        "tz": "America/Chicago",
        "unit": "F",
    },
    "Miami": {
        "series_id": 10728,
        "station": "KMIA",
        "lat": 25.7959,
        "lon": -80.2870,
        "tz": "America/New_York",
        "unit": "F",
    },
    "Dallas": {
        "series_id": 10727,
        "station": "KDAL",
        "lat": 32.8481,
        "lon": -96.8512,
        "tz": "America/Chicago",
        "unit": "F",
    },
    "Seattle": {
        "series_id": 10734,
        "station": "KSEA",
        "lat": 47.4489,
        "lon": -122.3094,
        "tz": "America/Los_Angeles",
        "unit": "F",
    },
    "Atlanta": {
        "series_id": 10739,
        "station": "KATL",
        "lat": 33.6407,
        "lon": -84.4277,
        "tz": "America/New_York",
        "unit": "F",
    },
    "Toronto": {
        "series_id": 10743,
        "station": "CYYZ",
        "lat": 43.6772,
        "lon": -79.6306,
        "tz": "America/Toronto",
        "unit": "C",
    },
    "London": {
        "series_id": 10006,
        "station": "EGLC",
        "lat": 51.5048,
        "lon": 0.0495,
        "tz": "Europe/London",
        "unit": "C",
    },
    "Paris": {
        "series_id": 11168,
        "station": "LFPG",
        "lat": 49.0097,
        "lon": 2.5479,
        "tz": "Europe/Paris",
        "unit": "C",
    },
    "Munich": {
        "series_id": 11272,
        "station": "EDDM",
        "lat": 48.3538,
        "lon": 11.7861,
        "tz": "Europe/Berlin",
        "unit": "C",
    },
    "Buenos Aires": {
        "series_id": 10744,
        "station": "SAEZ",
        "lat": -34.8222,
        "lon": -58.5358,
        "tz": "America/Argentina/Buenos_Aires",
        "unit": "C",
    },
    "Sao Paulo": {
        "series_id": 11169,
        "station": "SBGR",
        "lat": -23.4356,
        "lon": -46.4731,
        "tz": "America/Sao_Paulo",
        "unit": "C",
    },
    "Seoul": {
        "series_id": 10742,
        "station": "RKSI",
        "lat": 37.4691,
        "lon": 126.4510,
        "tz": "Asia/Seoul",
        "unit": "C",
    },
    "Lucknow": {
        "series_id": 11271,
        "station": "VILK",
        "lat": 26.7606,
        "lon": 80.8893,
        "tz": "Asia/Kolkata",
        "unit": "C",
    },
    "Ankara": {
        "series_id": 10900,
        "station": "LTAC",
        "lat": 40.1281,
        "lon": 32.9951,
        "tz": "Europe/Istanbul",
        "unit": "C",
    },
    "Wellington": {
        "series_id": 10902,
        "station": "NZWN",
        "lat": -41.3272,
        "lon": 174.8052,
        "tz": "Pacific/Auckland",
        "unit": "C",
    },
}

# Gamma API base
GAMMA_API = "https://gamma-api.polymarket.com"

# CLOB API base
CLOB_API = "https://clob.polymarket.com"

# Strategy parameters
STRATEGY = {
    # BUY NO: bracket must be at least this far from forecast (°F for F cities, °C for C cities)
    "no_min_distance_f": 4,    # °F
    "no_min_distance_c": 2.5,  # °C  (~4.5°F equivalent)
    # BUY NO: minimum NO price
    "no_min_price": 0.65,
    # BUY YES: maximum YES price to consider (looking for underpriced YES)
    "yes_max_price": 0.55,
    # Forecast must land inside bracket (distance == 0) for YES signal
    "yes_require_in_bracket": True,
    # Minimum expected return % to include in results
    "min_return_pct": 8.0,
    # Minimum order size (USDC) — Polymarket minimum is 5
    "min_order_size": 5,
    # Default order size for NO bets (USDC)
    "default_no_size": 20,
    # Default order size for YES bets (USDC)
    "default_yes_size": 10,
    # Max total capital to deploy per run (USDC)
    "max_capital": 400,
}

"""
City configs: series IDs, Wunderground station codes, and coordinates.
Station codes match the ICAO identifiers Wunderground uses for resolution.

unit: "F" = US cities (brackets in °F, Polymarket resolves in °F)
      "C" = international cities (brackets in °C, Polymarket resolves in °C)
"""

import os

CITIES = {
    "NYC": {
        "series_id": 10005,
        "station": "KLGA",
        "lat": 40.7769,
        "lon": -73.8740,
        "tz": "America/New_York",
        "unit": "F",
        "wu_path": "/hourly/us/ny/new-york-city/KLGA/date/",
    },
    "Chicago": {
        "series_id": 10726,
        "station": "KORD",
        "lat": 41.9742,
        "lon": -87.9073,
        "tz": "America/Chicago",
        "unit": "F",
        "wu_path": "/hourly/us/il/chicago/KORD/date/",
    },
    "Miami": {
        "series_id": 10728,
        "station": "KMIA",
        "lat": 25.7959,
        "lon": -80.2870,
        "tz": "America/New_York",
        "unit": "F",
        "wu_path": "/hourly/us/fl/miami/KMIA/date/",
    },
    "Dallas": {
        "series_id": 10727,
        "station": "KDAL",
        "lat": 32.8481,
        "lon": -96.8512,
        "tz": "America/Chicago",
        "unit": "F",
        "wu_path": "/hourly/us/tx/dallas/KDAL/date/",
    },
    "Seattle": {
        "series_id": 10734,
        "station": "KSEA",
        "lat": 47.4489,
        "lon": -122.3094,
        "tz": "America/Los_Angeles",
        "unit": "F",
        "wu_path": "/hourly/us/wa/seattle/KSEA/date/",
    },
    "Atlanta": {
        "series_id": 10739,
        "station": "KATL",
        "lat": 33.6407,
        "lon": -84.4277,
        "tz": "America/New_York",
        "unit": "F",
        "wu_path": "/hourly/us/ga/atlanta/KATL/date/",
    },
    "Toronto": {
        "series_id": 10743,
        "station": "CYYZ",
        "lat": 43.6772,
        "lon": -79.6306,
        "tz": "America/Toronto",
        "unit": "C",
        "wu_path": "/hourly/ca/on/toronto/CYYZ/date/",
    },
    "London": {
        "series_id": 10006,
        "station": "EGLC",
        "lat": 51.5048,
        "lon": 0.0495,
        "tz": "Europe/London",
        "unit": "C",
        "wu_path": "/hourly/gb/england/london/EGLC/date/",
    },
    "Paris": {
        "series_id": 11168,
        "station": "LFPG",
        "lat": 49.0097,
        "lon": 2.5479,
        "tz": "Europe/Paris",
        "unit": "C",
        "wu_path": "/hourly/fr/ile-de-france/paris/LFPG/date/",
    },
    "Munich": {
        "series_id": 11272,
        "station": "EDDM",
        "lat": 48.3538,
        "lon": 11.7861,
        "tz": "Europe/Berlin",
        "unit": "C",
        "wu_path": "/hourly/de/bavaria/munich/EDDM/date/",
    },
    "Buenos Aires": {
        "series_id": 10744,
        "station": "SAEZ",
        "lat": -34.8222,
        "lon": -58.5358,
        "tz": "America/Argentina/Buenos_Aires",
        "unit": "C",
        "wu_path": "/hourly/ar/buenos-aires/buenos-aires/SAEZ/date/",
    },
    "Sao Paulo": {
        "series_id": 11169,
        "station": "SBGR",
        "lat": -23.4356,
        "lon": -46.4731,
        "tz": "America/Sao_Paulo",
        "unit": "C",
        "wu_path": "/hourly/br/sao-paulo/sao-paulo/SBGR/date/",
    },
    "Seoul": {
        "series_id": 10742,
        "station": "RKSI",
        "lat": 37.4691,
        "lon": 126.4510,
        "tz": "Asia/Seoul",
        "unit": "C",
        "wu_path": "/hourly/kr/incheon/incheon/RKSI/date/",
    },
    "Lucknow": {
        "series_id": 11271,
        "station": "VILK",
        "lat": 26.7606,
        "lon": 80.8893,
        "tz": "Asia/Kolkata",
        "unit": "C",
        "wu_path": "/hourly/in/uttar-pradesh/lucknow/VILK/date/",
    },
    "Ankara": {
        "series_id": 10900,
        "station": "LTAC",
        "lat": 40.1281,
        "lon": 32.9951,
        "tz": "Europe/Istanbul",
        "unit": "C",
        "wu_path": "/hourly/tr/ankara/ankara/LTAC/date/",
    },
    "Wellington": {
        "series_id": 10902,
        "station": "NZWN",
        "lat": -41.3272,
        "lon": 174.8052,
        "tz": "Pacific/Auckland",
        "unit": "C",
        "wu_path": "/hourly/nz/wellington/wellington/NZWN/date/",
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
    # Default order size for NO bets (USDC) — used as Kelly cap
    "default_no_size": 20,
    # Default order size for YES bets (USDC) — used as Kelly cap
    "default_yes_size": 10,
    # Max total capital to deploy per run (USDC)
    "max_capital": 400,
    # Kelly criterion fraction (0.5 = half-Kelly for safety)
    "kelly_fraction": 0.5,
    # Hard cap per single bet (USDC) — overrides Kelly if larger
    "max_single_bet": 50,
}

# Notification settings (override via env vars)
NOTIFY = {
    "slack_webhook": os.environ.get("SLACK_WEBHOOK_URL", ""),
    "min_return_pct": float(os.environ.get("NOTIFY_MIN_RETURN_PCT", "20")),
}

# Forecast source weights when WU_API_KEY is configured (primary setup)
# Wunderground IS the resolution source, so it gets dominant weight.
# NWS provides an independent US-only sanity check.
FORECAST_WEIGHTS = {
    "F": {"wunderground": 0.70, "nws": 0.30, "wttr": 0.00},
    "C": {"wunderground": 1.00, "nws": 0.00, "wttr": 0.00},
}

# Fallback weights when WU_API_KEY is not set (uses wttr.in + NWS)
FORECAST_WEIGHTS_FALLBACK = {
    "F": {"wunderground": 0.00, "nws": 0.40, "wttr": 0.60},
    "C": {"wunderground": 0.00, "nws": 0.00, "wttr": 1.00},
}

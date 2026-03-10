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
        "lat": 40.761,
        "lon": -73.864,
        "tz": "America/New_York",
        "unit": "F",
        "wu_path": "/hourly/us/ny/new-york-city/KLGA/date/",
    },
    "Chicago": {
        "series_id": 10726,
        "station": "KORD",
        "lat": 41.977,
        "lon": -87.905,
        "tz": "America/Chicago",
        "unit": "F",
        "wu_path": "/hourly/us/il/chicago/KORD/date/",
    },
    "Miami": {
        "series_id": 10728,
        "station": "KMIA",
        "lat": 25.848,
        "lon": -80.242,
        "tz": "America/New_York",
        "unit": "F",
        "wu_path": "/hourly/us/fl/miami/KMIA/date/",
    },
    "Dallas": {
        "series_id": 10727,
        "station": "KDAL",
        "lat": 32.846,
        "lon": -96.87,
        "tz": "America/Chicago",
        "unit": "F",
        "wu_path": "/hourly/us/tx/dallas/KDAL/date/",
    },
    "Seattle": {
        "series_id": 10734,
        "station": "KSEA",
        "lat": 47.441,
        "lon": -122.3,
        "tz": "America/Los_Angeles",
        "unit": "F",
        "wu_path": "/hourly/us/wa/seattle/KSEA/date/",
    },
    "Atlanta": {
        "series_id": 10739,
        "station": "KATL",
        "lat": 33.639,
        "lon": -84.405,
        "tz": "America/New_York",
        "unit": "F",
        "wu_path": "/hourly/us/ga/atlanta/KATL/date/",
    },
    "Toronto": {
        "series_id": 10743,
        "station": "CYYZ",
        "lat": 43.712,
        "lon": -79.655,
        "tz": "America/Toronto",
        "unit": "C",
        "wu_path": "/hourly/ca/on/toronto/CYYZ/date/",
    },
    "London": {
        "series_id": 10006,
        "station": "EGLC",
        "lat": 51.51,
        "lon": 0.028,
        "tz": "Europe/London",
        "unit": "C",
        "wu_path": "/hourly/gb/england/london/EGLC/date/",
    },
    "Paris": {
        "series_id": 11168,
        "station": "LFPG",
        "lat": 49.017,
        "lon": 2.594,
        "tz": "Europe/Paris",
        "unit": "C",
        "wu_path": "/hourly/fr/ile-de-france/paris/LFPG/date/",
    },
    "Munich": {
        "series_id": 11272,
        "station": "EDDM",
        "lat": 48.354,
        "lon": 11.792,
        "tz": "Europe/Berlin",
        "unit": "C",
        "wu_path": "/hourly/de/bavaria/munich/EDDM/date/",
    },
    "Buenos Aires": {
        "series_id": 10744,
        "station": "SAEZ",
        "lat": -34.788,
        "lon": -58.523,
        "tz": "America/Argentina/Buenos_Aires",
        "unit": "C",
        "wu_path": "/hourly/ar/buenos-aires/buenos-aires/SAEZ/date/",
    },
    "Sao Paulo": {
        "series_id": 11169,
        "station": "SBGR",
        "lat": -23.424,
        "lon": -46.478,
        "tz": "America/Sao_Paulo",
        "unit": "C",
        "wu_path": "/hourly/br/sao-paulo/sao-paulo/SBGR/date/",
    },
    "Seoul": {
        "series_id": 10742,
        "station": "RKSI",
        "lat": 37.4943,
        "lon": 126.4905,
        "tz": "Asia/Seoul",
        "unit": "C",
        "wu_path": "/hourly/kr/incheon/incheon/RKSI/date/",
    },
    "Lucknow": {
        "series_id": 11271,
        "station": "VILK",
        "lat": 26.738,
        "lon": 80.857,
        "tz": "Asia/Kolkata",
        "unit": "C",
        "wu_path": "/hourly/in/uttar-pradesh/lucknow/VILK/date/",
    },
    "Ankara": {
        "series_id": 10900,
        "station": "LTAC",
        "lat": 40.239,
        "lon": 33.029,
        "tz": "Europe/Istanbul",
        "unit": "C",
        "wu_path": "/hourly/tr/ankara/ankara/LTAC/date/",
    },
    "Wellington": {
        "series_id": 10902,
        "station": "NZWN",
        "lat": -41.318,
        "lon": 174.796,
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
    "no_min_distance_f": 6,    # °F  (raised from 4 — adjacent-bracket losses too frequent)
    "no_min_distance_c": 3.5,  # °C  (raised from 2.5 — ~6.3°F equivalent)
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
    # Default order size for NO bets (USDC) — base size at min_distance; scales up with distance
    "default_no_size": 20,
    # NO sizing: max scale multiplier for distant brackets (3× = up to $60, capped by max_single_bet)
    "no_max_distance_scale": 3.0,
    # NO bets: only place when forecast confidence is "high" (all sources agree)
    "no_require_high_confidence": True,
    # Default order size for YES bets (USDC) — used as Kelly cap
    "default_yes_size": 10,
    # YES clusters: forecast must be ≥ this many degrees inside the cluster edge
    "yes_min_margin_f": 2.0,   # °F  (filters edge cases where forecast is near cluster boundary)
    "yes_min_margin_c": 1.0,   # °C
    # YES lottery: clusters with total_price < threshold get smaller per-bracket sizing
    "yes_lottery_threshold": 0.25,   # total_price below this = lottery cluster
    "yes_lottery_size": 5,           # $ per bracket for lottery clusters (vs default_yes_size)
    # Max total capital to deploy per run (USDC)
    "max_capital": 400,
    # Kelly criterion fraction (0.5 = half-Kelly for safety)
    "kelly_fraction": 0.5,
    # Hard cap per single bet (USDC) — overrides Kelly if larger
    "max_single_bet": 50,
}

# Live trading settings (all overridable via Railway env vars)
# LIVE_MODE=false by default — bot paper-trades until you explicitly enable it.
TRADING = {
    "live_mode":             os.environ.get("LIVE_MODE", "false").lower() == "true",
    "stop_loss_pct":         float(os.environ.get("STOP_LOSS_PCT") or "50"),    # exit if down X%
    "take_profit_pct":       float(os.environ.get("TAKE_PROFIT_PCT") or "0"),   # 0 = disabled
    "monitor_interval_secs": int(os.environ.get("MONITOR_INTERVAL_SECS") or "300"),
    "slippage_pct":          float(os.environ.get("SLIPPAGE_PCT") or "1"),      # price tolerance %
    # Circuit breaker: pause auto-scan when today's paper P&L falls below this (0 = disabled)
    "daily_loss_limit_usd":  float(os.environ.get("DAILY_LOSS_LIMIT_USD") or "0"),
}

# Daily market-open cron time (UTC). Polymarket adds next-day markets around midnight UTC.
# Set MARKET_OPEN_UTC=00:30 to catch them as soon as they're live.
MARKET_OPEN_UTC = os.environ.get("MARKET_OPEN_UTC", "00:30")

# Notification settings (override via env vars)
NOTIFY = {
    "slack_webhook": os.environ.get("SLACK_WEBHOOK_URL", ""),
    "min_return_pct": float(os.environ.get("NOTIFY_MIN_RETURN_PCT") or "20"),
}

# Forecast source weights when WU_API_KEY is configured (primary setup)
# Wunderground IS the resolution source, so it gets dominant weight.
# NWS (US) / Open-Meteo (international) provide cross-check.
FORECAST_WEIGHTS = {
    "F": {"wunderground": 0.70, "nws": 0.30, "open_meteo": 0.00},
    "C": {"wunderground": 1.00, "nws": 0.00, "open_meteo": 0.00},
}

# Fallback when WU not configured (NWS + Open-Meteo for international)
FORECAST_WEIGHTS_FALLBACK = {
    "F": {"wunderground": 0.00, "nws": 0.40, "open_meteo": 0.60},
    "C": {"wunderground": 0.00, "nws": 0.00, "open_meteo": 1.00},
}

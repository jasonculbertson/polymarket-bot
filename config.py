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
    "LA": {
        "series_id": 10725,
        "station": "KLAX",
        "lat": 33.942,
        "lon": -118.408,
        "tz": "America/Los_Angeles",
        "unit": "F",
        "wu_path": "/hourly/us/ca/los-angeles/KLAX/date/",
    },
    "Phoenix": {
        "series_id": 10729,
        "station": "KPHX",
        "lat": 33.437,
        "lon": -112.008,
        "tz": "America/Phoenix",
        "unit": "F",
        "wu_path": "/hourly/us/az/phoenix/KPHX/date/",
    },
    "Denver": {
        "series_id": 10730,
        "station": "KDEN",
        "lat": 39.856,
        "lon": -104.674,
        "tz": "America/Denver",
        "unit": "F",
        "wu_path": "/hourly/us/co/denver/KDEN/date/",
    },
    "Boston": {
        "series_id": 10735,
        "station": "KBOS",
        "lat": 42.364,
        "lon": -71.005,
        "tz": "America/New_York",
        "unit": "F",
        "wu_path": "/hourly/us/ma/boston/KBOS/date/",
    },
    "Minneapolis": {
        "series_id": 10736,
        "station": "KMSP",
        "lat": 44.880,
        "lon": -93.218,
        "tz": "America/Chicago",
        "unit": "F",
        "wu_path": "/hourly/us/mn/minneapolis/KMSP/date/",
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
    "Dubai": {
        "series_id": 10115,
        "station": "OMDB",
        "lat": 25.256,
        "lon": 55.364,
        "tz": "Asia/Dubai",
        "unit": "C",
        "wu_path": "/hourly/ae/dubai/dubai/OMDB/date/",
    },
    "Tokyo": {
        "series_id": 10740,
        "station": "RJTT",
        "lat": 35.553,
        "lon": 139.781,
        "tz": "Asia/Tokyo",
        "unit": "C",
        "wu_path": "/hourly/jp/tokyo/tokyo/RJTT/date/",
    },
    "Shanghai": {
        "series_id": 10741,
        "station": "ZSSS",
        "lat": 31.198,
        "lon": 121.336,
        "tz": "Asia/Shanghai",
        "unit": "C",
        "wu_path": "/hourly/cn/shanghai/shanghai/ZSSS/date/",
    },
    "Singapore": {
        "series_id": 11314,
        "station": "WSSS",
        "lat": 1.359,
        "lon": 103.989,
        "tz": "Asia/Singapore",
        "unit": "C",
        "wu_path": "/hourly/sg/singapore/singapore/WSSS/date/",
    },
    "Hong Kong": {
        "series_id": 11312,
        "station": "VHHH",
        "lat": 22.308,
        "lon": 113.918,
        "tz": "Asia/Hong_Kong",
        "unit": "C",
        "wu_path": "/hourly/hk/hong-kong/hong-kong/VHHH/date/",
    },
    "Auckland": {
        "series_id": 10901,
        "station": "NZAA",
        "lat": -37.008,
        "lon": 174.791,
        "tz": "Pacific/Auckland",
        "unit": "C",
        "wu_path": "/hourly/nz/auckland/auckland/NZAA/date/",
    },
    "Warsaw": {
        "series_id": 11342,
        "station": "EPWA",
        "lat": 52.165,
        "lon": 20.967,
        "tz": "Europe/Warsaw",
        "unit": "C",
        "wu_path": "/hourly/pl/masovian/warsaw/EPWA/date/",
    },
    "Tel Aviv": {
        "series_id": 11295,
        "station": "LLBG",
        "lat": 32.011,
        "lon": 34.887,
        "tz": "Asia/Jerusalem",
        "unit": "C",
        "wu_path": "/hourly/il/tel-aviv/tel-aviv/LLBG/date/",
    },
    "Milan": {
        "series_id": 11343,
        "station": "LIML",
        "lat": 45.445,
        "lon": 9.277,
        "tz": "Europe/Rome",
        "unit": "C",
        "wu_path": "/hourly/it/lombardy/milan/LIML/date/",
    },
    "Madrid": {
        "series_id": 11345,
        "station": "LEMD",
        "lat": 40.472,
        "lon": -3.561,
        "tz": "Europe/Madrid",
        "unit": "C",
        "wu_path": "/hourly/es/madrid/madrid/LEMD/date/",
    },
    "Taipei": {
        "series_id": 11346,
        "station": "RCTP",
        "lat": 25.077,
        "lon": 121.233,
        "tz": "Asia/Taipei",
        "unit": "C",
        "wu_path": "/hourly/tw/taipei/taipei/RCTP/date/",
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
    # Historical MAE = 4.45°F. At 5°F min distance we get ~82% win rate with σ=4.0.
    # At 6°F we get ~93% which is needed to profit at 85¢+ entry prices.
    "no_min_distance_f": 6,    # °F  — raised from 4; need ≥6°F for profitable NO bets
    "no_min_distance_c": 3.3,  # °C  — raised from 2.2; proportional to F threshold
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
    "default_yes_size": 5,
    # YES clusters: minimum total_price — below this means market has likely already resolved
    "yes_min_total_price": 0.04,
    # YES clusters: forecast must be ≥ this many degrees inside the cluster edge
    "yes_min_margin_f": 2.0,   # °F  (filters edge cases where forecast is near cluster boundary)
    "yes_min_margin_c": 1.0,   # °C
    # YES clusters: maximum entry price per token — no buying expensive consensus bets
    # At 0.85, a win pays only 15¢; at 50% win rate that's a guaranteed loser
    "yes_max_entry_price": 0.75,
    # NO bets: hard YES-price ceiling — skip brackets the market already prices at ≥X% YES.
    # High YES price = close bracket = tiny return with huge loss on miss.
    # At 88¢ NO (12¢ YES), a loss costs 88¢ but a win only pays 12¢ → need 88% win rate.
    "no_max_yes_price": float(os.environ.get("NO_MAX_YES_PRICE") or "0.88"),
    # NO bets: minimum NO price (maximum we'll pay for a NO token)
    # Higher NO price = lower return but higher win probability.
    # Cap at 88¢ to ensure minimum 13.6% return on winners.
    "no_max_no_price": float(os.environ.get("NO_MAX_NO_PRICE") or "0.88"),
    # NO bets: minimum return % — need enough return to overcome occasional losses.
    # At 73% win rate and 85¢ avg entry, need ≥8% return to approach break-even.
    "no_min_return_pct": float(os.environ.get("NO_MIN_RETURN_PCT") or "8.0"),
    # YES clusters: cities excluded due to high/bi-modal forecast error
    # Chicago MAE=6.27°F, Dallas MAE=5.18°F (bi-modal: 1°F when stable, 11-16°F during fronts)
    # Paris MAE=3.69°C — cluster window is ~3°F/1.5°C wide, can't reliably cover the error
    # Denver: rapid temperature swings (can drop 40°F in 24h) — too unpredictable for clusters
    # Boston: New England frontal systems cause frequent day-of forecast errors
    # Tokyo/Shanghai/HK/Taipei: typhoon season + rapid marine/continental swings
    # Warsaw: Central European fronts, large day-to-day variability
    # Auckland: Maritime southern-hemisphere weather, high cross-source error
    "yes_exclude_cities": [
        "Chicago", "Dallas", "Paris",           # original exclusions
        "Denver", "Boston",                      # high US variability
        "Tokyo", "Shanghai", "Hong Kong", "Taipei",  # Asia-Pacific typhoon/frontal risk
        "Warsaw", "Auckland",                    # high variability, limited WU accuracy data
    ],
    # NO bets: reject markets with less than this total liquidity (USDC)
    # Low-liquidity markets have wide bid-ask spreads and unreliable stop-loss exits
    "no_min_liquidity_usd": 100,
    # Ensemble model spread thresholds — proxy for frontal instability / forecast uncertainty
    # Source: Open-Meteo ensemble API (ICON Seamless, ~40 members); spread = std dev of daily highs
    # yes_skip: abort YES clusters when spread ≥ threshold (too risky to pin exact bracket)
    # no_boost: raise NO min-distance by ~2°F when spread ≥ threshold (extra margin for error)
    "ensemble_spread_yes_skip_f": 4.0,    # °F std dev across ~40 members → skip YES clusters
    "ensemble_spread_yes_skip_c": 2.2,    # °C equivalent (~4°F)
    "ensemble_spread_no_boost_f": 6.0,    # °F std dev → raise NO distance requirement by 2°F
    "ensemble_spread_no_boost_c": 3.3,    # °C equivalent
    # YES lottery: clusters with total_price < threshold get smaller per-bracket sizing
    "yes_lottery_threshold": 0.25,   # total_price below this = lottery cluster
    "yes_lottery_size": 2,           # $ per bracket for lottery clusters (vs default_yes_size)
    # ── Live-trading quality gate (A-tier) ─────────────────────────────────────
    # In LIVE_MODE, only A-tier opportunities are executed. B-tier still paper-tracks.
    # A-tier thresholds (what actually auto-executes with real money):
    # Based on actual MAE of 4.45°F, need ≥8°F distance for 90%+ win rate
    # which is the minimum to be profitable at typical 85-90¢ entry prices.
    "live_no_min_distance_f": 8.0,   # °F — need 90%+ win rate for profitability
    "live_no_min_distance_c": 4.5,   # °C — proportional (8°F / 1.8)
    "live_yes_min_margin_f":  4.0,   # °F inside bracket — wider margin for YES safety
    "live_yes_min_margin_c":  2.2,   # °C — proportional
    "live_min_ev_score":     15.0,   # raised from 12; higher bar for live trades
    # Probability-edge thresholds for A-tier qualification
    # NO A-tier: edge ≤ −0.15  (market charges 15%+ more than our Gaussian says bracket is worth)
    # YES A-tier: edge ≥ 0.12  (our Gaussian says bracket is 12%+ more likely than market implies)
    "live_no_min_edge":  0.15,   # |negative edge| threshold for NO A-tier
    "live_yes_min_edge": 0.12,   # positive edge threshold for YES A-tier
    # ── Bankroll & compounding ────────────────────────────────────────────────
    "initial_bankroll":    float(os.environ.get("INITIAL_BANKROLL") or "150"),
    # Daily return target — informational only, does NOT reduce bet sizes when hit
    "daily_target_pct":    float(os.environ.get("DAILY_TARGET_PCT") or "5"),
    # Hard stop: halt new scans if today's loss exceeds X% of opening bankroll
    "daily_loss_cap_pct":  float(os.environ.get("DAILY_LOSS_CAP_PCT") or "15"),
    # Per-bet cap: max single bet = X% of current bankroll.
    # At $150: 10% = $15 max/bet. Max 3 bets per scan = $45 deployed.
    # Conservative: survive multiple losing days while still compounding wins.
    "max_bet_pct":         float(os.environ.get("MAX_BET_PCT") or "10"),
    # Outsized-edge bonus: if prob_edge magnitude ≥ threshold, allow up to 1.5× max_bet_pct
    "outsized_edge_threshold": 0.25,
    "kelly_fraction": 0.25,
    # Hard cap per single bet (USDC) — absolute ceiling regardless of bankroll size
    "max_single_bet": float(os.environ.get("MAX_SINGLE_BET") or "20"),
}

# Live trading settings (all overridable via Railway env vars)
# LIVE_MODE=false by default — bot paper-trades until you explicitly enable it.
TRADING = {
    "live_mode":             os.environ.get("LIVE_MODE", "false").lower() == "true",
    # YES bets are disabled in live trading — model not yet profitable (21% win rate).
    # Paper trading continues tracking YES bets for model improvement.
    # Set LIVE_YES_ENABLED=true in Railway to re-enable when model improves.
    "live_yes_enabled":      os.environ.get("LIVE_YES_ENABLED", "false").lower() == "true",
    "stop_loss_pct":         float(os.environ.get("STOP_LOSS_PCT") or "10"),    # exit if down 10%
    # Don't trigger stop-loss within this many hours of resolution (price may just be stale)
    "stop_loss_min_hours_to_resolution": float(os.environ.get("STOP_LOSS_MIN_HOURS") or "2"),
    "take_profit_pct":       float(os.environ.get("TAKE_PROFIT_PCT") or "8"),   # exit if up 8%
    # Force-exit all positions this many hours before resolution — don't hold to binary outcome
    "force_exit_hours_before_resolution": float(os.environ.get("FORCE_EXIT_HOURS") or "24"),
    "monitor_interval_secs": int(os.environ.get("MONITOR_INTERVAL_SECS") or "300"),
    "slippage_pct":          float(os.environ.get("SLIPPAGE_PCT") or "1"),      # price tolerance %
    # Circuit breaker: pause auto-scan when today's paper P&L falls below this
    # If 0, falls back to STRATEGY["daily_loss_cap_pct"] × current bankroll (recommended).
    # Set a fixed $ value via DAILY_LOSS_LIMIT_USD env var to override percentage-based cap.
    "daily_loss_limit_usd":  float(os.environ.get("DAILY_LOSS_LIMIT_USD") or "0"),
}

# ── Auto-applied strategy overrides from optimizer ──────────────────────────
# The optimizer may auto-raise thresholds based on performance data.
# These overrides are stored in Postgres under 'strategy_overrides'.
# Load them at startup and merge into STRATEGY (overrides win).
def _load_strategy_overrides():
    """Load auto-applied overrides from Postgres, merge into STRATEGY dict."""
    try:
        import psycopg2
        db_url = os.environ.get("DATABASE_URL")
        if not db_url:
            return
        conn = psycopg2.connect(db_url, connect_timeout=5)
        cur = conn.cursor()
        cur.execute("SELECT value FROM kv_store WHERE key = 'strategy_overrides'")
        row = cur.fetchone()
        conn.close()
        if row and row[0]:
            import json
            overrides = json.loads(row[0]) if isinstance(row[0], str) else row[0]
            # Only apply numeric/list overrides that exist in STRATEGY
            applied = []
            for k, v in overrides.items():
                if k in STRATEGY and k not in ("last_auto_applied", "last_changes"):
                    STRATEGY[k] = v
                    applied.append(k)
            if applied:
                print(f"[config] loaded {len(applied)} strategy overrides: {applied}")
    except Exception as e:
        pass  # Silently skip if Postgres unavailable (local dev)

_load_strategy_overrides()

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

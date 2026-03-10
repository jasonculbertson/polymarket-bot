"""
Recursive learning engine.

After each day's markets resolve, this module:

  1. Fetches the actual temperature (winning bracket midpoint) from Polymarket's Gamma API
  2. Computes per-source forecast errors (wttr.in, NWS, Open-Meteo) vs actual
  3. Updates forecast_weights.json via exponential moving average (EMA)
     → future scans automatically weight more-accurate sources higher

  4. Compares our predicted win probabilities to actual win rates
  5. Updates calibration.json with adjusted sigma values
     → Kelly sizing and opportunity filtering improve over time

Storage (all in DATA_DIR):
  forecast_weights.json  — source accuracy weights used by fetch_forecasts.py
  calibration.json       — sigma values used by analyzer.py for win prob estimates

Env vars:
  LEARNING_RATE   — EMA alpha (default 0.15 ≈ last ~7 data points weighted average)
"""

from __future__ import annotations

import json
import os
import re
import requests
from datetime import datetime
from typing import Optional

DATA_DIR = os.environ.get("DATA_DIR", os.path.join(os.path.dirname(__file__), "data"))
WEIGHTS_FILE          = os.path.join(DATA_DIR, "forecast_weights.json")
CALIBRATION_FILE      = os.path.join(DATA_DIR, "calibration.json")
CITY_ADJUSTMENTS_FILE = os.path.join(DATA_DIR, "city_adjustments.json")

GAMMA_API = "https://gamma-api.polymarket.com"
LEARNING_RATE = float(os.environ.get("LEARNING_RATE", "0.15"))

# Defaults (mirrors config.py — kept here to avoid circular import)
_DEFAULT_WEIGHTS = {
    "F": {"wunderground": 0.70, "nws": 0.30, "wttr": 0.00},
    "C": {"wunderground": 1.00, "nws": 0.00, "wttr": 0.00},
}
_DEFAULT_CALIBRATION = {
    "no_sigma":  {"F": {"high": 1.8, "medium": 3.2}, "C": {"high": 1.0, "medium": 1.8}},
    "yes_sigma": {"F": {"high": 1.8, "medium": 3.2}, "C": {"high": 1.0, "medium": 1.8}},
    "samples": 0,
    "updated": None,
}


# ─── Storage helpers ──────────────────────────────────────────────────────────

def load_weights() -> dict:
    try:
        if os.path.exists(WEIGHTS_FILE):
            with open(WEIGHTS_FILE) as f:
                return json.load(f)
    except Exception:
        pass
    return {**_DEFAULT_WEIGHTS, "samples": 0, "updated": None}


def _save_weights(w: dict):
    os.makedirs(DATA_DIR, exist_ok=True)
    with open(WEIGHTS_FILE, "w") as f:
        json.dump(w, f, indent=2)


def load_calibration() -> dict:
    try:
        if os.path.exists(CALIBRATION_FILE):
            with open(CALIBRATION_FILE) as f:
                return json.load(f)
    except Exception:
        pass
    return dict(_DEFAULT_CALIBRATION)


def _save_calibration(c: dict):
    os.makedirs(DATA_DIR, exist_ok=True)
    with open(CALIBRATION_FILE, "w") as f:
        json.dump(c, f, indent=2)


# ─── Wunderground: PWS API + history page scrape (exact station from config) ───

WU_PWS_KEY = os.environ.get("WU_PWS_KEY", "")
WU_PWS_API = "https://api.weather.com/v2/pws"
WU_HISTORY_BASE = "https://www.wunderground.com"
# Weather Company v3 historical daily summary (last 30 days) — same source as the Summary table on WU history pages
WU_HISTORY_API = "https://api.weather.com/v3/wx/conditions/historical/dailysummary/30day"
# Public key used by wunderground.com; override with WU_HISTORY_API_KEY if you have one
WU_HISTORY_API_KEY = os.environ.get("WU_HISTORY_API_KEY", "e1f10a1e78da46f5b10a1e78da96f525")
# Browser-like User-Agent so history page returns parseable HTML
WU_HEADERS = {"User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"}


def _wu_location_id(city: str) -> Optional[str]:
    """
    Build a Weather Company location ID for a city from config, e.g. 'KATL:9:US'.
    Derives the 2-letter country code from the wu_path, e.g. '/hourly/us/ga/...' → 'US'.
    """
    try:
        from config import CITIES
        cfg = CITIES.get(city)
        if not cfg:
            return None
        icao = cfg.get("station", "")
        wu_path = cfg.get("wu_path", "")
        # wu_path: /hourly/us/ga/atlanta/KATL/date/  → parts[2] = 'us'
        parts = [p for p in wu_path.split("/") if p]
        if len(parts) >= 2 and parts[0] == "hourly":
            country = parts[1].upper()   # 'us' → 'US', 'gb' → 'GB', etc.
        else:
            country = "US"
        if not icao:
            return None
        return f"{icao}:9:{country}"
    except Exception:
        return None


def fetch_actual_temp_wu_observations_api(city: str, date_str: str, unit: str = "F") -> Optional[float]:
    """
    Fetch daily high temperature from the Weather Company hourly-observations history API.

    This is the SAME data source the wunderground.com history page uses to render the
    "Summary → High Temp" table (loaded client-side via JavaScript).  The API returns
    one observation per ~30 min; we take max(temp) for the date as the day's high.

    Endpoint:
        GET https://api.weather.com/v1/location/{ICAO}:9:{CC}/observations/historical.json
            ?units=e|m&startDate=YYYYMMDD&endDate=YYYYMMDD&apiKey=...

    The public key embedded in every wunderground.com page is used by default;
    override with the WU_HISTORY_API_KEY env var if desired.

    Works for all cities in config.py (US and international).
    Returns the daily high temperature, or None on error.
    """
    try:
        loc_id = _wu_location_id(city)
        if not loc_id:
            return None
        # Normalize date_str -> YYYYMMDD
        parts = date_str.split("-")
        if len(parts) != 3:
            return None
        date_compact = f"{parts[0]}{parts[1].zfill(2)}{parts[2].zfill(2)}"
        units_param = "e" if unit == "F" else "m"
        url = f"https://api.weather.com/v1/location/{loc_id}/observations/historical.json"
        r = requests.get(
            url,
            params={
                "units": units_param,
                "startDate": date_compact,
                "endDate": date_compact,
                "apiKey": WU_HISTORY_API_KEY,
            },
            headers=WU_HEADERS,
            timeout=12,
        )
        if r.status_code != 200:
            return None
        data = r.json()
        obs = data.get("observations", [])
        if not obs:
            return None
        temps = [float(o["temp"]) for o in obs if o.get("temp") is not None]
        if not temps:
            return None
        high = max(temps)
        if -40 <= high <= 150:
            return high
        return None
    except Exception:
        return None


def fetch_actual_temp_wu_api_historical(city: str, date_str: str, unit: str = "F") -> Optional[float]:
    """
    Fetch daily high from Weather Company v3 historical API (last 30 days).
    Same data as the "Summary" table on wunderground.com/history/daily/... — literal recorded high.
    Returns temperatureMax for the given date, or None if date not in window or API error.
    """
    try:
        from config import CITIES
        cfg = CITIES.get(city)
        if not cfg:
            return None
        icao = cfg.get("station")
        if not icao or len(icao) != 4:
            return None
        units_param = "e" if unit == "F" else "m"
        params = {
            "icaoCode": icao,
            "units": units_param,
            "language": "en-US",
            "format": "json",
            "apiKey": WU_HISTORY_API_KEY,
        }
        r = requests.get(WU_HISTORY_API, params=params, headers=WU_HEADERS, timeout=12)
        if r.status_code != 200:
            return None
        data = r.json()
        valid_local = data.get("validTimeLocal") or []
        temp_max = data.get("temperatureMax") or []
        if not valid_local or not temp_max:
            return None
        # Normalize date_str to YYYY-MM-DD for comparison
        parts = date_str.split("-")
        if len(parts) != 3:
            return None
        target = f"{parts[0]}-{parts[1].zfill(2)}-{parts[2].zfill(2)}"
        for i, v in enumerate(valid_local):
            if not v:
                continue
            date_part = v.split("T")[0] if "T" in v else v[:10]
            if date_part == target and i < len(temp_max):
                val = temp_max[i]
                if val is not None and -40 <= float(val) <= 130:
                    return float(val)
        return None
    except Exception:
        return None


def fetch_actual_temp_wu_history(city: str, date_str: str, unit: str = "F") -> Optional[float]:
    """
    Scrape the daily high from Wunderground's history page for the exact station in config.
    The WU/Weather Company API does not expose history for airport stations (only PWS).
    The Summary table (High Temp) is rendered client-side from JSON embedded in a <script> tag;
    we parse that JSON instead of the visible HTML. URL from wu_path, e.g.:
    /history/daily/us/il/chicago/KORD/date/2026-3-8
    Returns the maximum temperature or None if page has no data / parse failure.
    """
    try:
        from config import CITIES
        cfg = CITIES.get(city)
        if not cfg:
            return None
        wu_path = cfg.get("wu_path")
        if not wu_path or "/hourly/" not in wu_path:
            return None
        # /hourly/us/il/chicago/KORD/date/ -> /history/daily/us/il/chicago/KORD/date/2026-3-8
        path = wu_path.replace("/hourly/", "/history/daily/", 1).rstrip("/")
        # date_str "2026-03-08" -> "2026-3-8" for URL
        parts = date_str.split("-")
        if len(parts) != 3:
            return None
        y, m, d = parts[0], str(int(parts[1])), str(int(parts[2]))
        date_slug = f"{y}-{m}-{d}"
        url = f"{WU_HISTORY_BASE}{path}/date/{date_slug}"
        r = requests.get(url, headers=WU_HEADERS, timeout=12)
        if r.status_code != 200:
            return None
        html = r.text

        # 1) Prefer: parse JSON from <script> (same data the Summary table uses in the browser).
        #    Page often returns "No data recorded" in the body but still embeds observation data in script.
        script_high = _parse_wu_history_script_for_high_temp(html, date_str)
        if script_high is not None:
            return script_high

        # 2) If no script data, skip if body says no data (no point in parsing table).
        if "no data recorded" in html.lower():
            return None

        # 3) Fallback: parse "High Temp" from server-rendered table (some pages may still have it).
        for label in ("High Temp", "High Temperature", "Maximum Temperature", "Maximum Temp", "Actual"):
            idx = html.find(label)
            if idx == -1:
                continue
            chunk = html[idx : idx + 500]
            m = re.search(r'class="wx-value"[^>]*>([^<]+)</span>', chunk, re.I)
            if m:
                try:
                    v = float(m.group(1).strip().replace(",", ""))
                    if -40 <= v <= 130:
                        return v
                except ValueError:
                    pass
            m = re.search(r">\s*([0-9]+(?:\.[0-9]+)?)\s*[°º]?\s*[FC]?\s*</", chunk, re.I)
            if m:
                try:
                    v = float(m.group(1))
                    if -40 <= v <= 130:
                        return v
                except (ValueError, IndexError):
                    pass
            m = re.search(r">\s*([0-9]+(?:\.[0-9]+)?)\s*[°º]\s*[FC]", chunk, re.I)
            if m:
                try:
                    v = float(m.group(1))
                    if -40 <= v <= 130:
                        return v
                except (ValueError, IndexError):
                    pass
        for m in re.finditer(r'class="wx-value"[^>]*>([^<]+)</span>', html, re.I):
            try:
                v = float(m.group(1).strip().replace(",", ""))
                if -40 <= v <= 130:
                    return v
            except ValueError:
                pass
        return None
    except Exception:
        return None


def _parse_wu_history_script_for_high_temp(html: str, date_str: str) -> Optional[float]:
    """
    Extract daily high from WU history page's embedded script JSON.
    The Summary table is built client-side from this data; we parse it server-side.
    """
    # Normalize date for matching: "2025-03-08" -> "2025-03-08"; also accept 2025-3-8
    parts = date_str.split("-")
    if len(parts) != 3:
        return None
    target_date = f"{parts[0]}-{parts[1].zfill(2)}-{parts[2].zfill(2)}"  # 2025-03-08

    for m in re.finditer(r"<script[^>]*>([^<]+)</script>", html):
        script = m.group(1)
        if "calendarDayTemperatureMax" not in script and "temperatureMax24Hour" not in script:
            continue

        # Try to align calendarDayTemperatureMax with validTimeLocal so we use the requested date.
        cal_max = re.search(r'"calendarDayTemperatureMax"\s*:\s*\[(\d+(?:,\d+)*)\]', script)
        valid_local = re.search(r'"validTimeLocal"\s*:\s*\[([^\]]+)\]', script)
        if cal_max and valid_local:
            values = [int(x) for x in cal_max.group(1).split(",")]
            # Parse dates from validTimeLocal: "2025-03-08T07:00:00-0400", ...
            date_strs = re.findall(r'"([^"]*?)"', valid_local.group(1))
            for i, ds in enumerate(date_strs):
                if target_date in ds or ds.startswith(target_date):
                    if i < len(values):
                        v = float(values[i])
                        if -40 <= v <= 130:
                            return v
                    break
            # No matching date found: the page returned forecast data, not history.
            # Do NOT fall back to first value — it would be wrong data.
            return None
        elif cal_max:
            # Only safe to use if there's exactly one value (single-day history page with no date array).
            values = [int(x) for x in cal_max.group(1).split(",")]
            if len(values) == 1:
                v = float(values[0])
                if -40 <= v <= 130:
                    return v

        # temperatureMax24Hour is a rolling 24-hr window, not a calendar-day high.
        # It is unreliable at resolution time (bot runs early morning, WU hasn't
        # compiled yesterday's full history yet — the value reflects the current
        # temp, not the previous day's peak). Skip this field to avoid bad resolves.
    return None


# Maps each city to a nearby high-quality PWS station (qcStatus=1 from WU near search)
# Used when PWS key is set; history scrape uses config station (exact WU location) first.
_CITY_PWS_STATION = {
    "NYC":          "KNYNEWYO1509",   # Upper East Side, qcStatus=1
    "Chicago":      "KILILLIN294",    # fallback: use Gamma API if missing
    "Miami":        "KFLMIAMI3180",
    "Dallas":       "KTXDALLA1890",
    "Seattle":      "KWASEATL1243",
    "Atlanta":      "KGAATLAN1156",
    "Toronto":      "ONTORONT1195",
    "London":       "IENGLOND101",
    "Paris":        "IILEDEFA101",
    "Munich":       "IBAVARIA101",
}


def fetch_actual_temp_pws(station_id: str, date_str: str, unit: str = "F") -> Optional[float]:
    """
    Fetch the actual recorded daily high from a Wunderground PWS station.
    Requires WU_PWS_KEY env var (free Wunderground.com account key).

    date_str: "YYYY-MM-DD"
    Returns the daily high temperature or None on error.
    """
    if not WU_PWS_KEY or not station_id:
        return None
    try:
        date_compact = date_str.replace("-", "")   # "20260302"
        r = requests.get(
            f"{WU_PWS_API}/history/daily",
            params={
                "stationId": station_id,
                "format":    "json",
                "units":     "e" if unit == "F" else "m",
                "date":      date_compact,
                "apiKey":    WU_PWS_KEY,
            },
            timeout=10,
        )
        if r.status_code != 200:
            return None
        data = r.json()
        obs = data.get("observations", [])
        if not obs:
            return None
        hi = obs[0].get("imperial" if unit == "F" else "metric", {}).get("tempHigh")
        return float(hi) if hi is not None else None
    except Exception:
        return None


# ─── Gamma API helpers ────────────────────────────────────────────────────────

def _parse_bracket_bounds(text: str):
    """Parse (lo, hi) temperature bounds from a market question or groupItemTitle."""
    m = re.search(r"between (-?\d+)-(-?\d+)°[FC]", text)
    if m:
        return float(m.group(1)), float(m.group(2))
    m = re.search(r"be (-?\d+)°[FC] or (?:below|lower)", text)
    if m:
        return None, float(m.group(1))
    m = re.search(r"be (-?\d+)°[FC] or (?:above|higher)", text)
    if m:
        return float(m.group(1)), None
    # "42°C" style single bracket
    m = re.search(r"^(-?\d+)°[FC]$", text.strip())
    if m:
        v = float(m.group(1))
        return v, v + 1.0
    return None, None


def fetch_actual_temperature(event_slug: str, city: str = "", unit: str = "F",
                             resolution_date: str = "") -> Optional[float]:
    """
    Fetch the actual recorded temperature for a resolved event.

    Strategy (in order):
      1. WU observations history API: hourly obs → max = High Temp. Works for all cities, no scraping.
      2. Weather Company v3 historical API: last 30 days window (dailysummary).
      3. WU history page scrape: exact station from config (wu_path), older dates or API failures.
      4. WU PWS API: if key set, fetch from PWS station.
      5. Polymarket Gamma API: infer from which YES bracket resolved.

    Returns the actual temperature, or None if not yet resolved / unavailable.
    """
    # Strategy 1: observations history API — same source as WU history page "Summary → High Temp"
    # Works for all cities (US + international), no extra key needed, no scraping required.
    if city and resolution_date:
        actual = fetch_actual_temp_wu_observations_api(city, resolution_date, unit)
        if actual is not None:
            return actual
    # Strategy 2: v3 historical dailysummary (last 30 days window)
    if city and resolution_date:
        actual = fetch_actual_temp_wu_api_historical(city, resolution_date, unit)
        if actual is not None:
            return actual
    # Strategy 3: scrape WU history page (fallback for older dates or API failures)
    if city and resolution_date:
        actual = fetch_actual_temp_wu_history(city, resolution_date, unit)
        if actual is not None:
            return actual
    # Strategy 4 (5 total): PWS API when key is set
    if WU_PWS_KEY and city and resolution_date:
        pws_id = _CITY_PWS_STATION.get(city)
        if pws_id:
            actual = fetch_actual_temp_pws(pws_id, resolution_date, unit)
            if actual is not None:
                return actual
    # Strategy 4: Gamma API (which bracket won)
    try:
        r = requests.get(
            f"{GAMMA_API}/events",
            params={"slug": event_slug, "closed": "true"},
            timeout=10,
        )
        if r.status_code != 200:
            return None
        events = r.json()
        if not events:
            return None
        event = events[0] if isinstance(events, list) else events
        markets = event.get("markets", [])

        for mkt in markets:
            prices_raw = mkt.get("outcomePrices", "[]")
            if isinstance(prices_raw, str):
                try:
                    prices_raw = json.loads(prices_raw)
                except Exception:
                    continue
            try:
                yes_price = float(prices_raw[0]) if prices_raw else 0.0
            except (ValueError, IndexError):
                yes_price = 0.0

            if yes_price >= 0.95:
                question = mkt.get("question", "") or mkt.get("groupItemTitle", "")
                lo, hi = _parse_bracket_bounds(question)
                if lo is None and hi is None:
                    lo, hi = _parse_bracket_bounds(mkt.get("groupItemTitle", ""))
                if lo is not None and hi is not None:
                    return (lo + hi) / 2.0
                elif hi is not None:
                    return hi - 0.5   # open-low: just below upper bound
                elif lo is not None:
                    return lo + 0.5   # open-high: just above lower bound

        return None
    except Exception:
        return None


# ─── EMA weight updater ───────────────────────────────────────────────────────

def _ema_update(old: float, new: float, alpha: float) -> float:
    return round(alpha * new + (1.0 - alpha) * old, 5)


def _errors_to_weights(errors: dict[str, list[float]]) -> dict[str, float]:
    """
    Convert per-source error lists to normalized accuracy weights.
    Lower mean absolute error → higher weight.
    Sources with no data are excluded from the update.
    """
    avgs = {src: sum(e) / len(e) for src, e in errors.items() if e}
    if not avgs:
        return {}

    # Convert errors to accuracy scores (complement of normalized error)
    # Add small epsilon to avoid divide-by-zero when all errors are equal
    scores = {src: 1.0 / (avg + 0.01) for src, avg in avgs.items()}
    total = sum(scores.values())
    return {src: score / total for src, score in scores.items()}


# ─── Main learning function ───────────────────────────────────────────────────

def load_city_adjustments() -> dict:
    """Load per-city distance bonus (°F) from file. Returns {} if not found."""
    try:
        if os.path.exists(CITY_ADJUSTMENTS_FILE):
            with open(CITY_ADJUSTMENTS_FILE) as f:
                return json.load(f)
    except Exception:
        pass
    return {}


def _save_city_adjustments(adjustments: dict) -> None:
    os.makedirs(DATA_DIR, exist_ok=True)
    with open(CITY_ADJUSTMENTS_FILE, "w") as f:
        json.dump(adjustments, f, indent=2)


def _compute_city_distance_adjustments(data: dict) -> dict:
    """
    Compute per-city extra distance (°F) based on historical wu_error from resolved NO bets.

    Cities where WU forecasts have been consistently inaccurate need a higher effective
    min_distance to avoid adjacent-bracket losses.

    Returns {city: bonus_distance_f} — only cities with enough samples and meaningful error.
    """
    from collections import defaultdict
    city_errors: dict = defaultdict(list)
    for opp in data.get("opportunities", []):
        if opp.get("type") != "no":
            continue
        if opp.get("wu_error") is None:
            continue
        city_errors[opp["city"]].append(opp["wu_error"])

    adjustments: dict = {}
    min_samples = 5          # need at least 5 resolved NO bets to be statistically meaningful
    base_accuracy_f = 2.0   # WU expected accuracy in ideal conditions (°F)

    for city, errors in city_errors.items():
        if len(errors) < min_samples:
            continue
        avg_err = sum(errors) / len(errors)
        bonus = max(0.0, round(avg_err - base_accuracy_f, 1))
        if bonus > 0:
            adjustments[city] = {
                "bonus_f": bonus,
                "samples": len(errors),
                "avg_wu_error_f": round(avg_err, 2),
            }
    return adjustments


def learn_from_outcomes() -> dict:
    """
    Main learning step — called once per scan after resolve_outcomes().

    For each resolved opportunity that hasn't been learned from yet:
      1. Fetch actual temperature from Gamma API (winning bracket midpoint)
      2. Compute per-source forecast errors
      3. Update source weights via EMA → more accurate sources weighted higher
      4. Track predicted_win_prob vs actual outcome → calibrate sigma values

    Returns a summary dict with what changed.
    """
    from tracker import _load, _save as _save_tracker, _tracker_lock

    with _tracker_lock:
        data = _load()
    weights = load_weights()
    calibration = load_calibration()

    alpha = LEARNING_RATE

    # Accumulate errors and calibration samples across all newly resolved opps
    source_errors: dict[str, dict[str, list]] = {
        "F": {"wunderground": [], "nws": [], "wttr": []},
        "C": {"wunderground": [], "nws": [], "wttr": []},
    }
    # Separate by confidence for sigma calibration
    win_prob_samples: dict[str, dict[str, list]] = {
        "F": {"high": [], "medium": []},
        "C": {"high": [], "medium": []},
    }

    learned_count  = 0
    temp_found     = 0
    tracker_dirty  = False

    for opp in data["opportunities"]:
        if opp["outcome"] is None:
            continue
        if opp.get("learned"):
            continue

        unit            = opp.get("temp_unit", "F")
        confidence      = opp.get("confidence", "medium")
        sources         = opp.get("forecast_sources") or {}
        slug            = opp.get("event_slug", "")
        city            = opp.get("city", "")
        resolution_date = opp.get("resolution_date", "")

        # ── Source weight learning ────────────────────────────────────────
        if sources and (slug or (WU_PWS_KEY and city and resolution_date)):
            actual = fetch_actual_temperature(slug, city=city, unit=unit,
                                              resolution_date=resolution_date)
            if actual is not None:
                temp_found += 1
                for src in ["wunderground", "nws", "wttr"]:
                    val = sources.get(src)
                    if val is not None:
                        source_errors[unit][src].append(abs(val - actual))

        # ── Win probability calibration ───────────────────────────────────
        predicted_prob = opp.get("predicted_win_prob")
        if predicted_prob is not None and confidence in ("high", "medium"):
            actual_win = 1 if opp["outcome"] == "win" else 0
            win_prob_samples[unit][confidence].append((predicted_prob, actual_win))

        opp["learned"] = True
        learned_count += 1
        tracker_dirty = True

    # ── Update source weights ─────────────────────────────────────────────────
    weights_updated = False
    for unit in ("F", "C"):
        new_w = _errors_to_weights(source_errors[unit])
        if not new_w:
            continue

        if unit not in weights or not isinstance(weights.get(unit), dict):
            weights[unit] = dict(_DEFAULT_WEIGHTS.get(unit, {}))

        for src in ("wunderground", "nws", "wttr"):
            if src in new_w:
                old = weights[unit].get(src, _DEFAULT_WEIGHTS.get(unit, {}).get(src, 0.33))
                weights[unit][src] = _ema_update(old, new_w[src], alpha)

        # Renormalize so weights sum to 1
        total = sum(weights[unit].get(s, 0) for s in ("wunderground", "nws", "wttr"))
        if total > 0:
            for src in ("wunderground", "nws", "wttr"):
                weights[unit][src] = round(weights[unit].get(src, 0) / total, 4)

        n_samples = sum(len(e) for e in source_errors[unit].values())
        weights["samples"] = weights.get("samples", 0) + n_samples
        weights_updated = True

    if weights_updated:
        weights["updated"] = datetime.utcnow().isoformat()
        _save_weights(weights)

    # ── Update sigma calibration ──────────────────────────────────────────────
    calib_updated = False
    sigma_cfg = calibration.get("no_sigma", {})
    total_new_samples = 0

    for unit in ("F", "C"):
        for conf in ("high", "medium"):
            samples = win_prob_samples[unit][conf]
            if len(samples) < 2:
                continue

            predicted_avg = sum(p for p, _ in samples) / len(samples)
            actual_avg    = sum(a for _, a in samples) / len(samples)

            if predicted_avg <= 0:
                continue

            # Calibration ratio: if predicted > actual we're over-confident
            # → increase sigma (widen uncertainty), which lowers win_prob estimates
            ratio = actual_avg / predicted_avg   # < 1 means over-confident
            ratio = max(0.75, min(1.25, ratio))  # clamp to ±25% adjustment

            old_sigma = sigma_cfg.get(unit, {}).get(conf, 1.8)

            if ratio < 0.97:
                # ratio < 1: actual spread < predicted spread → we're over-confident → widen sigma (÷ratio > 1)
                target_sigma = old_sigma / ratio
            elif ratio > 1.03:
                # ratio > 1: actual spread > predicted spread → we're under-confident → narrow sigma (÷ratio < 1)
                target_sigma = old_sigma / ratio
            else:
                target_sigma = old_sigma  # no meaningful drift

            new_sigma = _ema_update(old_sigma, target_sigma, alpha)
            # Keep within sane bounds
            new_sigma = max(0.8, min(6.0, new_sigma))

            if unit not in sigma_cfg:
                sigma_cfg[unit] = {}
            sigma_cfg[unit][conf] = round(new_sigma, 3)

            total_new_samples += len(samples)
            calib_updated = True

    if calib_updated:
        calibration["no_sigma"]  = sigma_cfg
        # Apply same calibration to yes_sigma (same forecast error model)
        calibration["yes_sigma"] = sigma_cfg
        calibration["samples"]   = calibration.get("samples", 0) + total_new_samples
        calibration["updated"]   = datetime.utcnow().isoformat()
        _save_calibration(calibration)

    if tracker_dirty:
        with _tracker_lock:
            _save_tracker(data)

    # Per-city distance adjustments — recomputed from full history each time
    city_adj = _compute_city_distance_adjustments(data)
    _save_city_adjustments(city_adj)

    return {
        "learned":           learned_count,
        "temps_fetched":     temp_found,
        "weights_updated":   weights_updated,
        "calib_updated":     calib_updated,
        "city_adjustments":  city_adj,
        "source_samples": {u: {s: len(e) for s, e in source_errors[u].items()} for u in ("F", "C")},
        "current_weights": {
            u: weights.get(u, {}) for u in ("F", "C")
        },
    }


# ─── Dashboard stats ──────────────────────────────────────────────────────────

def get_learning_stats() -> dict:
    """Return current weights + calibration for the dashboard /learning endpoint."""
    weights     = load_weights()
    calibration = load_calibration()
    return {
        "weights":       weights,
        "calibration":   calibration,
        "learning_rate": LEARNING_RATE,
        "weights_file":  WEIGHTS_FILE,
        "calib_file":    CALIBRATION_FILE,
    }

"""
Fetches temperature forecasts for each city in their NATIVE unit.

Source priority:
  1. Wunderground HOURLY API — PRIMARY, uses the public key embedded in every
     WU page (api.weather.com/v3/wx/forecast/hourly/10day). Gives:
       - Hourly temps for the exact ICAO station Polymarket resolves against
       - Daily high = max of hourly temps (same calculation Wunderground uses)
       - Peak hour — when during the day the high is expected
     The embedded key is extracted once per session, cached for 4 hours, and
     refreshed automatically if it stops working. No paid API key needed.
  2. NWS — US only, free, secondary validation.
  3. wttr.in — fallback for international cities.

WU_PWS_KEY env var: optional Wunderground PWS observation key.
  Used by learner.py to fetch actual historical station highs for outcome
  verification.

US cities  (unit="F"): forecasts in °F
Intl cities (unit="C"): forecasts in °C
"""

from __future__ import annotations

import json
import os
import re
import time
import requests
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date, timedelta
from typing import Optional
from config import CITIES, FORECAST_WEIGHTS, FORECAST_WEIGHTS_FALLBACK

WU_API_KEY  = os.environ.get("WU_API_KEY", "")   # legacy — no longer required
WU_PWS_KEY  = os.environ.get("WU_PWS_KEY", "")   # PWS observation key (free, for history)

WU_BASE     = "https://www.wunderground.com"
WU_API_BASE = "https://api.weather.com"
_WU_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/122.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "en-US,en;q=0.9",
}

# ─── Embedded API key cache ───────────────────────────────────────────────────
import threading as _threading
_wu_key_cache: dict = {"key": "", "ts": 0.0}
_wu_key_lock = _threading.Lock()
_WU_KEY_TTL = 4 * 3600  # refresh every 4 hours


def _get_wu_embedded_key() -> str:
    """
    WU embeds a public API key in every page for their own front-end API calls.
    We extract it once, cache for 4 h, and reuse for the hourly forecast API.
    The key is stable for long periods but may rotate; we detect 401s and refresh.
    Lock held for the entire operation to prevent concurrent threads all fetching.
    """
    with _wu_key_lock:
        now = time.time()
        if _wu_key_cache["key"] and now - _wu_key_cache["ts"] < _WU_KEY_TTL:
            return _wu_key_cache["key"]
        try:
            r = requests.get(f"{WU_BASE}/", headers=_WU_HEADERS, timeout=12)
            if r.status_code == 200:
                m = re.search(r'apiKey=([a-f0-9]{32})', r.text)
                if m:
                    _wu_key_cache["key"] = m.group(1)
                    _wu_key_cache["ts"]  = now
                    print(f"  [WU] embedded API key refreshed: {m.group(1)[:8]}…")
                    return _wu_key_cache["key"]
        except Exception as exc:
            print(f"  [WARN] could not fetch WU embedded key: {exc}")
        return _wu_key_cache.get("key", "")

DATA_DIR     = os.environ.get("DATA_DIR", os.path.join(os.path.dirname(__file__), "data"))
WEIGHTS_FILE = os.path.join(DATA_DIR, "forecast_weights.json")


def load_source_weights() -> dict:
    """
    Load per-source accuracy weights from file (falls back to config defaults).
    Updated automatically by learner.py as historical accuracy data accumulates.
    """
    try:
        if os.path.exists(WEIGHTS_FILE):
            with open(WEIGHTS_FILE) as f:
                return json.load(f)
    except Exception:
        pass
    return FORECAST_WEIGHTS


def _weighted_consensus(values: dict, weights: dict) -> float:
    """
    Weighted average of non-None source values, renormalizing weights to sum to 1.
    Falls back to simple mean if no weights are configured.
    """
    available = {k: v for k, v in values.items() if v is not None}
    if not available:
        raise ValueError("No values to average")
    total_w = sum(weights.get(k, 0.0) for k in available)
    if total_w == 0:
        return round(sum(available.values()) / len(available), 1)
    return round(
        sum(v * weights.get(k, 0.0) / total_w for k, v in available.items()),
        1,
    )


# ─────────────────────────────────────────────────────────────────────────────
# Wunderground hourly forecast — clean JSON API (uses embedded public key)
# ─────────────────────────────────────────────────────────────────────────────

def fetch_wunderground_hourly(station: str, unit: str = "F",
                               lat: float = 0.0, lon: float = 0.0,
                               _retry: bool = True) -> Optional[dict]:
    """
    Fetch 10-day hourly forecast from api.weather.com using WU's embedded public key.

    Always fetches in imperial (°F) using the station's geocode — this is the same
    grid point WU uses when displaying the hourly page, ensuring our temperatures
    match exactly what WU shows (and what Polymarket resolves against).

    For Celsius cities, temperatures are converted from °F using standard rounding,
    matching WU's own Celsius display: round((F - 32) * 5 / 9).

    station: ICAO code, e.g. "KLGA" (used for logging only)
    unit:    "F" or "C" — determines output unit and conversion
    lat/lon: WU-resolved station geocode (from config.py)

    Returns:
    {
        "YYYY-MM-DD": {
            "max":       float,              # daily high in native unit
            "peak_hour": "14:00",            # local hour when high hits
            "hours":     [("09:00", 61.0), ...]  # full curve in native unit
        }
    }
    """
    api_key = _get_wu_embedded_key()
    if not api_key:
        print(f"    [WARN] WU: could not obtain embedded API key for {station}")
        return None

    # Always fetch in imperial — WU stores all data in °F internally.
    # Using geocode (not icaoCode) matches the exact forecast grid WU's website uses.
    if lat and lon:
        location_param = f"geocode={lat},{lon}"
    else:
        location_param = f"icaoCode={station}"

    url = (
        f"{WU_API_BASE}/v3/wx/forecast/hourly/10day"
        f"?{location_param}&units=e&language=en-US&format=json&apiKey={api_key}"
    )

    try:
        r = requests.get(url, headers=_WU_HEADERS, timeout=12)

        if r.status_code == 401 and _retry:
            _wu_key_cache["ts"] = 0.0
            return fetch_wunderground_hourly(station, unit, lat, lon, _retry=False)

        if r.status_code == 429 and _retry:
            # Rate limited — wait briefly and retry once
            import time as _time
            _time.sleep(2)
            return fetch_wunderground_hourly(station, unit, lat, lon, _retry=False)

        if r.status_code != 200:
            print(f"    [WARN] WU API returned {r.status_code} for {station}")
            return None

        data  = r.json()
        temps = data.get("temperature", [])
        times = data.get("validTimeLocal", [])

        if not temps or not times:
            return None

        def _convert(f: float) -> float:
            """Convert °F → °C preserving 1 decimal place, or keep as °F."""
            if unit == "C":
                return round((f - 32) * 5 / 9, 1)
            return float(f)

        daily: dict = {}
        for time_str, temp in zip(times, temps):
            if temp is None or not time_str:
                continue
            date_str = time_str[:10]
            hour_str = time_str[11:16]
            t = _convert(float(temp))

            if date_str not in daily:
                daily[date_str] = {"max": t, "peak_hour": hour_str, "hours": []}

            daily[date_str]["hours"].append((hour_str, t))
            if t > daily[date_str]["max"]:
                daily[date_str]["max"]       = t
                daily[date_str]["peak_hour"] = hour_str

        return daily if daily else None

    except Exception as exc:
        print(f"    [WARN] WU API failed for {station}: {exc}")
        return None


def _wu_daily_max(wu_hourly_result: Optional[dict]) -> Optional[dict]:
    """Extract {date: max_temp} from the hourly result dict for use in consensus."""
    if not wu_hourly_result:
        return None
    return {d: v["max"] for d, v in wu_hourly_result.items()}


# ─────────────────────────────────────────────────────────────────────────────
# NWS — US only, always returns °F
# ─────────────────────────────────────────────────────────────────────────────

def fetch_nws_forecast(lat: float, lon: float) -> Optional[dict]:
    """
    Fetch hourly max temperatures (°F) from the NWS API (US only).

    Uses the /forecast/hourly endpoint so we compute the daily high the same
    way Wunderground does — max of all hours — rather than relying on NWS's
    own "daytime high" summary which can differ by 1-3°F.

    Returns {date_str: max_temp_f} or None on failure.
    """
    try:
        r = requests.get(
            f"https://api.weather.gov/points/{lat},{lon}",
            headers={"User-Agent": "polymarket-weather-bot"},
            timeout=8,
        )
        if r.status_code != 200:
            return None
        props = r.json().get("properties", {})
        hourly_url = props.get("forecastHourly")
        if not hourly_url:
            return None

        r2 = requests.get(
            hourly_url,
            headers={"User-Agent": "polymarket-weather-bot"},
            timeout=10,
        )
        if r2.status_code != 200:
            return None

        periods = r2.json().get("properties", {}).get("periods", [])
        result: dict = {}
        for period in periods:
            start = period.get("startTime", "")[:10]
            temp  = period.get("temperature")
            unit_p = period.get("temperatureUnit", "F")
            if not start or temp is None:
                continue
            temp = float(temp)
            if unit_p == "C":
                temp = temp * 9 / 5 + 32
            if start not in result or temp > result[start]:
                result[start] = temp

        return result if result else None
    except Exception:
        return None


# ─────────────────────────────────────────────────────────────────────────────
# Open-Meteo — free, no API key, global coverage, cross-check for intl cities
# ─────────────────────────────────────────────────────────────────────────────

def fetch_open_meteo_forecast(lat: float, lon: float, unit: str = "C") -> Optional[dict]:
    """
    Fetch daily high temperatures from Open-Meteo (free, no key, global).
    Used as the cross-check source for international cities.

    Returns {date_str: max_temp} in °C (or °F if unit="F").
    """
    try:
        from collections import defaultdict as _dd
        r = requests.get(
            "https://api.open-meteo.com/v1/forecast",
            params={
                "latitude": lat, "longitude": lon,
                "hourly": "temperature_2m",
                "temperature_unit": "celsius",
                "forecast_days": 7,
                "timezone": "auto",
            },
            timeout=10,
        )
        if r.status_code != 200:
            return None
        data = r.json()
        times = data.get("hourly", {}).get("time", [])
        temps = data.get("hourly", {}).get("temperature_2m", [])
        if not times or not temps:
            return None
        daily: dict = _dd(list)
        for t, temp in zip(times, temps):
            if temp is not None and t:
                daily[t[:10]].append(float(temp))
        result = {d: round(max(v), 1) for d, v in daily.items() if v}
        if unit == "F":
            result = {d: round(v * 9 / 5 + 32, 1) for d, v in result.items()}
        return result if result else None
    except Exception as exc:
        print(f"    [WARN] Open-Meteo failed for ({lat},{lon}): {exc}")
        return None


# ─────────────────────────────────────────────────────────────────────────────
# Open-Meteo Ensemble API — frontal instability / forecast uncertainty detector
# ─────────────────────────────────────────────────────────────────────────────

def fetch_ensemble_spread(lat: float, lon: float, unit: str = "C") -> Optional[dict]:
    """
    Query Open-Meteo's free ensemble API (ICON Seamless, ~40 model members) to measure
    forecast uncertainty for each day.

    Method:
      - Fetch hourly temperature_2m for all ensemble members
      - Compute each member's daily high (same way WU computes it)
      - Return std dev across members for each date

    High spread = models disagree on timing/magnitude of temperature → likely frontal passage.
    Low spread  = models agree → stable conditions, safer to bet.

    Returns {date_str: spread_in_native_unit} or None on any failure.
    spread_in_native_unit: °F if unit=="F", else °C
    """
    import statistics
    from collections import defaultdict as _dd

    try:
        r = requests.get(
            "https://ensemble-api.open-meteo.com/v1/ensemble",
            params={
                "latitude":         lat,
                "longitude":        lon,
                "hourly":           "temperature_2m",
                "models":           "icon_seamless",
                "forecast_days":    7,
                "timezone":         "auto",
                "temperature_unit": "celsius",   # always °C; convert delta if needed
            },
            timeout=15,
        )
        if r.status_code != 200:
            return None

        data    = r.json()
        hourly  = data.get("hourly", {})
        times   = hourly.get("time", [])
        if not times:
            return None

        # Member columns named temperature_2m_member01, temperature_2m_member02, …
        member_cols = {
            k: v for k, v in hourly.items()
            if k.startswith("temperature_2m_member") and isinstance(v, list)
        }
        if not member_cols:
            return None

        # Compute daily max per member
        member_daily: dict = {}
        for member_key, temps in member_cols.items():
            daily_max: dict = _dd(list)
            for time_str, temp in zip(times, temps):
                if temp is not None and time_str:
                    daily_max[time_str[:10]].append(float(temp))
            member_daily[member_key] = {d: max(v) for d, v in daily_max.items() if v}

        # Compute std dev across members for each date
        all_dates: set = set()
        for dm in member_daily.values():
            all_dates.update(dm.keys())

        result: dict = {}
        for date_str in sorted(all_dates):
            vals = [dm[date_str] for dm in member_daily.values() if date_str in dm]
            if len(vals) >= 5:
                spread_c = statistics.stdev(vals)   # std dev in °C
                # Spread is a temperature *difference* — multiply by 9/5 to get °F, no offset
                result[date_str] = round(spread_c * 9 / 5 if unit == "F" else spread_c, 2)

        return result if result else None

    except Exception as exc:
        print(f"    [WARN] Ensemble spread failed for ({lat},{lon}): {exc}")
        return None


# ─────────────────────────────────────────────────────────────────────────────
# wttr.in — unused; international cross-check uses Open-Meteo (fetch_open_meteo_forecast).
# fetch_wttr_forecast is dead code; kept for reference only.
# ─────────────────────────────────────────────────────────────────────────────

def _fetch_wttr_forecast_unused(station: str, days: int = 2, unit: str = "F") -> Optional[dict]:
    """
    Fetch hourly forecast from wttr.in for an ICAO station.
    Returns {date_str: max_temp} in °F or °C depending on unit.

    Used as fallback only when WU_API_KEY is not set.
    wttr.in queries the same ICAO stations as Wunderground, so it's correlated
    but not identical to what Polymarket resolves against.
    """
    try:
        r = requests.get(
            f"https://wttr.in/{station}",
            params={"format": "j1"},
            headers={"User-Agent": "polymarket-weather-bot"},
            timeout=8,
        )
        if r.status_code != 200:
            return None
        data = r.json()

        result = {}
        weather_days = data.get("weather", [])
        today    = date.today()
        temp_key = "tempF"    if unit == "F" else "tempC"
        max_key  = "maxtempF" if unit == "F" else "maxtempC"

        for i, day_data in enumerate(weather_days[:days]):
            # Prefer the API-returned date if present to avoid midnight off-by-one errors
            api_date = day_data.get("date")
            if api_date:
                try:
                    day_date = date.fromisoformat(api_date).isoformat()
                except (ValueError, TypeError):
                    day_date = (today + timedelta(days=i)).isoformat()
            else:
                day_date = (today + timedelta(days=i)).isoformat()
            hourly = day_data.get("hourly", [])
            if hourly:
                temps = []
                for h in hourly:
                    tv = h.get(temp_key)
                    if tv is not None:
                        try:
                            temps.append(float(tv))
                        except (ValueError, TypeError):
                            pass
                if temps:
                    result[day_date] = max(temps)
                    continue
            max_v = day_data.get(max_key)
            if max_v is not None:
                try:
                    result[day_date] = float(max_v)
                except (ValueError, TypeError):
                    pass

        return result if result else None
    except Exception as e:
        print(f"    [WARN] wttr.in failed for {station}: {e}")
        return None


# ─────────────────────────────────────────────────────────────────────────────
# Main forecast builder
# ─────────────────────────────────────────────────────────────────────────────

def fetch_city_forecast(city_name: str, days: int = 2) -> dict:
    """
    Fetch forecasts for a city. All temperatures in the city's native unit (°F or °C).

    Source logic:
      - Wunderground HOURLY (api.weather.com): primary when WU_API_KEY is set.
        Daily high = max of the 48-hour hourly forecast for that day.
        Also stores peak hour and full hourly curve for dashboard display.
      - NWS: US cities only, secondary validation.
      - Open-Meteo: international cities cross-check (free, no key).

    Confidence:
      - "high"   : sources agree within threshold (2°F or 1°C)
      - "medium" : single source or moderate disagreement
      - "low"    : sources disagree >5°F / >3°C → skip in analyzer

    Returns:
    {
        "city": str,
        "station": str,
        "unit": str,
        "wu_active": bool,
        "forecasts": {
            "YYYY-MM-DD": {
                "wunderground":  float | None,   # max from hourly (most accurate)
                "wu_peak_hour":  str | None,      # e.g. "14:00" — when high hits
                "wu_hours":      list | None,      # [(hour, temp), ...] full curve
                "nws":           float | None,
                "open_meteo":    float | None,
                "consensus":     float,
                "confidence":    "high" | "medium" | "low",
                "ensemble_spread": float | None,  # std dev of ~40 ICON model members (°F or °C)
                                                   # None if ensemble API unavailable
                                                   # High (>4°F/>2.2°C) = frontal instability
            }
        }
    }
    """
    cfg     = CITIES[city_name]
    lat, lon = cfg["lat"], cfg["lon"]
    station  = cfg["station"]
    unit     = cfg.get("unit", "F")
    is_us    = unit == "F"

    high_threshold = 2.0 if unit == "F" else 1.0   # sources agree → safe to bet
    low_threshold  = 4.0 if unit == "F" else 2.0   # sources diverge → skip (was 5°F/3°C)
    # WU API is always available (uses public embedded key — no paid key needed)
    wu_active = True

    # Fetch sources concurrently within this city
    wu_hourly_result   = None
    nws_result         = None
    open_meteo_result  = None

    with ThreadPoolExecutor(max_workers=4) as pool:
        fu = {}
        fu["wu"]       = pool.submit(fetch_wunderground_hourly, station, unit, lat, lon)
        fu["ensemble"] = pool.submit(fetch_ensemble_spread, lat, lon, unit)
        if is_us:
            fu["nws"] = pool.submit(fetch_nws_forecast, lat, lon)
        else:
            fu["open_meteo"] = pool.submit(fetch_open_meteo_forecast, lat, lon, unit)

        if "wu"          in fu: wu_hourly_result  = fu["wu"].result()
        if "nws"         in fu: nws_result        = fu["nws"].result()
        if "open_meteo"  in fu: open_meteo_result = fu["open_meteo"].result()
        ensemble_result = fu["ensemble"].result()

    # Flatten hourly → daily max for consensus calculation
    wu_daily = _wu_daily_max(wu_hourly_result)

    today = date.today()
    forecasts = {}

    for i in range(days):
        d = (today + timedelta(days=i)).isoformat()

        wu_day  = wu_hourly_result.get(d) if wu_hourly_result else None
        wu_t    = wu_day["max"]       if wu_day else None
        peak_hr = wu_day["peak_hour"] if wu_day else None
        hours   = wu_day["hours"]     if wu_day else []

        nws_t = nws_result.get(d) if nws_result else None
        om_t  = open_meteo_result.get(d) if open_meteo_result else None

        all_vals = [v for v in [wu_t, nws_t, om_t] if v is not None]
        if not all_vals:
            continue

        # Weighted consensus — WU weights when key is set, fallback otherwise
        default_weights = FORECAST_WEIGHTS if wu_active else FORECAST_WEIGHTS_FALLBACK
        unit_weights = dict(load_source_weights().get(unit, default_weights.get(unit, {})))
        if "open_meteo" not in unit_weights and "wttr" in unit_weights:
            unit_weights["open_meteo"] = unit_weights["wttr"]
        consensus = _weighted_consensus(
            {"wunderground": wu_t, "nws": nws_t, "open_meteo": om_t},
            unit_weights,
        )

        # Confidence based on spread between available sources
        if len(all_vals) >= 2:
            spread = max(all_vals) - min(all_vals)
            confidence = "high"   if spread <= high_threshold else (
                         "medium" if spread <= low_threshold  else "low")
        else:
            # Single source: WU alone is "high"; NWS or Open-Meteo alone is "medium"
            confidence = "high" if wu_t is not None else "medium"

        ensemble_spread_val = ensemble_result.get(d) if ensemble_result else None

        forecasts[d] = {
            "wunderground":   wu_t,
            "wu_peak_hour":   peak_hr,
            "wu_hours":       hours,
            "nws":            nws_t,
            "open_meteo":     om_t,
            "consensus":      consensus,
            "confidence":     confidence,
            "ensemble_spread": ensemble_spread_val,  # °F or °C std dev across ~40 model members
        }

    return {
        "city":      city_name,
        "station":   station,
        "unit":      unit,
        "wu_active": wu_active,
        "forecasts": forecasts,
    }


def fetch_all_forecasts(cities=None, days: int = 2) -> dict:
    """
    Fetch forecasts for all (or specified) cities in parallel.
    Returns {city_name: forecast_dict}
    """
    if cities is None:
        cities = list(CITIES.keys())

    all_forecasts = {}
    tomorrow = (date.today() + timedelta(days=1)).isoformat()

    pws_note = f"PWS history key: {'✓' if WU_PWS_KEY else '✗ (set WU_PWS_KEY for accurate learning)'}"
    print(f"  [WU hourly API active (embedded key) · {pws_note}]")

    # Pre-warm the key once before launching parallel city fetches
    _get_wu_embedded_key()

    with ThreadPoolExecutor(max_workers=4) as pool:
        futures = {pool.submit(fetch_city_forecast, city, days): city for city in cities}
        for future in as_completed(futures):
            city = futures[future]
            try:
                result = future.result()
                all_forecasts[city] = result
                unit = result["unit"]
                f = result["forecasts"].get(tomorrow)
                if f:
                    parts = []
                    if f["wunderground"] is not None:
                        peak = f"@{f['wu_peak_hour']}" if f.get("wu_peak_hour") else ""
                        parts.append(f"wu={f['wunderground']:.1f}{peak}")
                    if f["nws"]  is not None: parts.append(f"nws={f['nws']:.1f}")
                    if f.get("open_meteo") is not None: parts.append(f"om={f['open_meteo']:.1f}")
                    ens = f["ensemble_spread"]
                    if ens is not None:
                        frontal_flag = " ⚠ FRONTAL?" if ens >= (4.0 if unit == "F" else 2.2) else ""
                        parts.append(f"ens_spread={ens:.1f}°{unit}{frontal_flag}")
                    print(f"  {city}: {f['consensus']:.1f}°{unit} conf={f['confidence']} ({', '.join(parts)})")
                else:
                    print(f"  {city}: no tomorrow forecast")
            except Exception as e:
                print(f"  [ERROR] {city} forecast: {e}")
                all_forecasts[city] = {"city": city, "station": "", "unit": "F",
                                       "wu_active": False, "forecasts": {}}

    return all_forecasts


if __name__ == "__main__":
    print("Fetching weather forecasts via WU embedded API key (next day focus)...\n")
    forecasts = fetch_all_forecasts(["NYC", "Chicago", "Miami", "London", "Paris", "Seoul", "Toronto"])

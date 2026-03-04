"""
Fetches temperature forecasts for each city in their NATIVE unit.

Source priority:
  1. Wunderground HOURLY (page scraper) — PRIMARY, no API key needed.
     Wunderground pre-fetches 15 days of hourly station data into every page's
     Redux store as a JSON blob. We parse it directly. This gives:
       - Hourly temps for the exact ICAO station Polymarket resolves against
       - Daily high = max of hourly temps (same calculation Wunderground uses)
       - Peak hour — when during the day the high is expected
     No key required. Uses city-specific wu_path from config.py.
  2. NWS — US only, free, secondary validation.
  3. wttr.in — fallback only when wu_path not configured for a city.

WU_PWS_KEY env var: optional Wunderground PWS observation key.
  Used by learner.py to fetch actual historical station highs for outcome
  verification (replaces the Polymarket page scraping approach).

US cities  (unit="F"): forecasts in °F
Intl cities (unit="C"): forecasts in °C
"""

from __future__ import annotations

import json
import os
import re
import requests
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date, timedelta
from typing import Optional
from config import CITIES, FORECAST_WEIGHTS, FORECAST_WEIGHTS_FALLBACK

WU_API_KEY  = os.environ.get("WU_API_KEY", "")   # TWC forecast key (paid/PWS submitter)
WU_PWS_KEY  = os.environ.get("WU_PWS_KEY", "")   # PWS observation key (free, for history)

WU_BASE     = "https://www.wunderground.com"
_WU_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/122.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "en-US,en;q=0.9",
}

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
# Wunderground hourly forecast — page scraper
# ─────────────────────────────────────────────────────────────────────────────

def fetch_wunderground_hourly(wu_path: str, unit: str = "F") -> Optional[dict]:
    """
    Scrape the Wunderground hourly forecast page for a station.

    Wunderground pre-fetches 15 days of hourly data into the page's Redux store
    as a JSON blob — no API key needed. We extract the temperature and
    validTimeLocal arrays and group by date.

    This is the EXACT same forecast Polymarket uses for resolution, from the
    exact same station, updated in real time.

    wu_path: city-specific path from config.py, e.g. "/hourly/us/ny/new-york-city/KLGA/date/"
    unit:    "F" or "C"

    Returns:
    {
        "YYYY-MM-DD": {
            "max":       float,              # daily high = max of hourly temps
            "peak_hour": "14:00",            # local hour when high hits
            "hours":     [("09:00", 61.0), ...]  # full curve for charting
        }
    }
    """
    if not wu_path:
        return None

    # Any date works — page always returns 15 days of hourly data from today
    today_str = date.today().isoformat()
    url = f"{WU_BASE}{wu_path}{today_str}"

    try:
        r = requests.get(url, headers=_WU_HEADERS, timeout=15)
        if r.status_code != 200:
            print(f"    [WARN] WU page returned {r.status_code} for {wu_path}")
            return None

        # Extract hourly temps and timestamps from embedded JSON store
        # WU embeds the full forecast as: "temperature":[38,39,...] and
        # "validTimeLocal":["2026-03-04T00:00:00-0500",...]
        # Find the temperature + validTimeLocal arrays that contain FUTURE forecast data.
        # We search for every occurrence pair and pick the one where the timestamps are
        # mostly in the future (i.e., a forecast, not historic observations).
        today_str = date.today().isoformat()

        temps_all = list(re.finditer(r'"temperature":\[([0-9,null-]+)\]', r.text))
        times_all = list(re.finditer(r'"validTimeLocal":\[([^\]]+)\]',    r.text))

        if not temps_all or not times_all:
            print(f"    [WARN] WU page: could not find hourly arrays for {wu_path}")
            return None

        # Pair each temperature array with the nearest validTimeLocal array
        # and pick the pair where most timestamps are today or future
        best_daily: dict = {}
        best_future = 0

        for temps_m in temps_all:
            # Find the closest times array (by position in the page)
            closest = min(times_all, key=lambda t: abs(t.start() - temps_m.start()))

            raw_temps = temps_m.group(1).split(",")
            raw_times = [t.strip().strip('"') for t in closest.group(1).split(",")]

            if len(raw_temps) < 6:
                continue

            daily: dict = {}
            future_count = 0
            for time_str, raw_t in zip(raw_times, raw_temps):
                if raw_t in ("null", "") or not time_str:
                    continue
                try:
                    t = float(raw_t)
                except ValueError:
                    continue

                date_str = time_str[:10]
                hour_str = time_str[11:16]

                if date_str >= today_str:
                    future_count += 1

                if date_str not in daily:
                    daily[date_str] = {"max": t, "peak_hour": hour_str, "hours": []}

                daily[date_str]["hours"].append((hour_str, t))
                if t > daily[date_str]["max"]:
                    daily[date_str]["max"]       = t
                    daily[date_str]["peak_hour"] = hour_str

            if future_count > best_future and daily:
                best_future = future_count
                best_daily  = daily

        if not best_daily or best_future < 6:
            print(f"    [WARN] WU page: no future forecast data found for {wu_path}")
            return None

        # Sanity check: for °C cities the WU page may embed °F temps from US PWS stations.
        # If max temp > 55 and unit == "C", convert °F → °C.
        # (55°C / 130°F are both implausible for any real forecast.)
        if unit == "C":
            sample_max = max(v["max"] for v in best_daily.values())
            if sample_max > 55:
                # Convert all temps from °F to °C
                for d_data in best_daily.values():
                    d_data["max"] = round((d_data["max"] - 32) * 5 / 9, 1)
                    d_data["hours"] = [(h, round((t - 32) * 5 / 9, 1))
                                       for h, t in d_data["hours"]]
                    # Recalculate peak after conversion
                    if d_data["hours"]:
                        pk = max(d_data["hours"], key=lambda x: x[1])
                        d_data["peak_hour"] = pk[0]
                        d_data["max"]       = pk[1]

        return best_daily

    except Exception as e:
        print(f"    [WARN] WU page scrape failed ({wu_path}): {e}")
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
    Fetch daily max temperatures (°F) from the NWS API (US only).
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
        grid = r.json()
        props = grid.get("properties", {})
        forecast_url = props.get("forecast")
        if not forecast_url:
            return None

        r2 = requests.get(
            forecast_url,
            headers={"User-Agent": "polymarket-weather-bot"},
            timeout=8,
        )
        if r2.status_code != 200:
            return None
        forecast = r2.json()
        periods = forecast.get("properties", {}).get("periods", [])

        result = {}
        for period in periods:
            if not period.get("isDaytime"):
                continue
            start = period.get("startTime", "")[:10]
            temp  = period.get("temperature")
            unit_p = period.get("temperatureUnit", "F")
            if temp is None:
                continue
            if unit_p == "C":
                temp = temp * 9 / 5 + 32
            if start not in result or temp > result[start]:
                result[start] = float(temp)

        return result if result else None
    except Exception:
        return None


# ─────────────────────────────────────────────────────────────────────────────
# wttr.in — fallback when WU API key not configured
# ─────────────────────────────────────────────────────────────────────────────

def fetch_wttr_forecast(station: str, days: int = 2, unit: str = "F") -> Optional[dict]:
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
            timeout=10,
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
      - wttr.in: fallback when WU_API_KEY is not configured.

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
                "wunderground": float | None,   # max from hourly (most accurate)
                "wu_peak_hour": str | None,      # e.g. "14:00" — when high hits
                "wu_hours":    list | None,      # [(hour, temp), ...] full curve
                "nws":         float | None,
                "wttr":        float | None,
                "consensus":   float,
                "confidence":  "high" | "medium" | "low",
            }
        }
    }
    """
    cfg     = CITIES[city_name]
    lat, lon, tz = cfg["lat"], cfg["lon"], cfg["tz"]
    station  = cfg["station"]
    wu_path  = cfg.get("wu_path", "")
    unit     = cfg.get("unit", "F")
    is_us    = unit == "F"

    high_threshold = 2.0 if unit == "F" else 1.0
    low_threshold  = 5.0 if unit == "F" else 3.0
    # WU page scraping is always available (no API key needed)
    wu_active = bool(wu_path)

    # Fetch sources concurrently within this city
    wu_hourly_result = None
    nws_result       = None
    wttr_result      = None

    with ThreadPoolExecutor(max_workers=3) as pool:
        fu = {}
        if wu_active:
            fu["wu"]   = pool.submit(fetch_wunderground_hourly, wu_path, unit)
        if is_us:
            fu["nws"]  = pool.submit(fetch_nws_forecast, lat, lon)
        # Always fetch wttr.in for international cities — WU page may not embed
        # the 15-day forecast for non-US stations, so wttr is a critical fallback
        if not is_us:
            fu["wttr"] = pool.submit(fetch_wttr_forecast, station, days, unit)

        if "wu"   in fu: wu_hourly_result = fu["wu"].result()
        if "nws"  in fu: nws_result       = fu["nws"].result()
        if "wttr" in fu: wttr_result      = fu["wttr"].result()

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

        nws_t   = nws_result.get(d)  if nws_result  else None
        wttr_t  = wttr_result.get(d) if wttr_result else None

        all_vals = [v for v in [wu_t, nws_t, wttr_t] if v is not None]
        if not all_vals:
            continue

        # Weighted consensus — WU weights when key is set, fallback otherwise
        default_weights = FORECAST_WEIGHTS if wu_active else FORECAST_WEIGHTS_FALLBACK
        unit_weights = load_source_weights().get(unit, default_weights.get(unit, {}))
        consensus = _weighted_consensus(
            {"wunderground": wu_t, "nws": nws_t, "wttr": wttr_t},
            unit_weights,
        )

        # Confidence based on spread between available sources
        if len(all_vals) >= 2:
            spread = max(all_vals) - min(all_vals)
            confidence = "high"   if spread <= high_threshold else (
                         "medium" if spread <= low_threshold  else "low")
        else:
            # Single source: WU alone is "high" (IS the resolution source);
            # wttr or NWS alone is "medium"
            confidence = "high" if wu_t is not None else "medium"

        forecasts[d] = {
            "wunderground": wu_t,
            "wu_peak_hour": peak_hr,
            "wu_hours":     hours,
            "nws":          nws_t,
            "wttr":         wttr_t,
            "consensus":    consensus,
            "confidence":   confidence,
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

    pws_note = f"PWS history key: {'✓' if WU_PWS_KEY else '✗ (set WU_PWS_KEY for learning)'}"
    print(f"  [WU hourly page scraper active · {pws_note}]")

    with ThreadPoolExecutor(max_workers=8) as pool:
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
                    if f["wttr"] is not None: parts.append(f"wttr={f['wttr']:.1f}")
                    print(f"  {city}: {f['consensus']:.1f}°{unit} conf={f['confidence']} ({', '.join(parts)})")
                else:
                    print(f"  {city}: no tomorrow forecast")
            except Exception as e:
                print(f"  [ERROR] {city} forecast: {e}")
                all_forecasts[city] = {"city": city, "station": "", "unit": "F",
                                       "wu_active": False, "forecasts": {}}

    return all_forecasts


if __name__ == "__main__":
    print(f"WU_API_KEY: {'SET' if WU_API_KEY else 'NOT SET (will use wttr.in fallback)'}\n")
    print("Fetching weather forecasts (next day focus)...\n")
    forecasts = fetch_all_forecasts(["NYC", "Chicago", "Miami", "London", "Paris", "Seoul", "Toronto"])

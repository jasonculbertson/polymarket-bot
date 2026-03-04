"""
Fetches temperature forecasts for each city in their NATIVE unit.

US cities (unit="F"):  forecasts in °F
International (unit="C"): forecasts in °C — matching how Polymarket resolves

Sources (in priority order):
  1. wttr.in hourly — uses same ICAO station codes as Wunderground, most correlated
  2. NWS API — US only, very accurate daytime highs
  3. Open-Meteo — international fallback (ECMWF/GFS)

Focus: NEXT DAY by default.
"""

from __future__ import annotations

import requests
from datetime import date, timedelta
from typing import Optional
from config import CITIES


# ─────────────────────────────────────────────────────────────────────────────
# wttr.in
# ─────────────────────────────────────────────────────────────────────────────

def fetch_wttr_forecast(station: str, days: int = 2, unit: str = "F") -> Optional[dict]:
    """
    Fetch hourly forecast from wttr.in for an ICAO station.
    Returns {date_str: max_temp} in °F or °C depending on unit.

    wttr.in uses the same station codes as Wunderground — most directly relevant
    since Polymarket resolves against Wunderground data.
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
        today = date.today()
        temp_key = "tempF" if unit == "F" else "tempC"
        max_key = "maxtempF" if unit == "F" else "maxtempC"

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

            # Fall back to daily max
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
            temp = period.get("temperature")
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
# Open-Meteo — international fallback
# ─────────────────────────────────────────────────────────────────────────────

def fetch_open_meteo(lat: float, lon: float, tz: str, days: int = 2,
                     unit: str = "F") -> Optional[dict]:
    """
    Fetch daily max temperatures from Open-Meteo.
    Returns {date_str: max_temp} in °F or °C depending on unit.
    """
    try:
        r = requests.get(
            "https://api.open-meteo.com/v1/forecast",
            params={
                "latitude": lat,
                "longitude": lon,
                "daily": "temperature_2m_max",
                "temperature_unit": "fahrenheit" if unit == "F" else "celsius",
                "timezone": tz,
                "forecast_days": days,
            },
            timeout=10,
        )
        r.raise_for_status()
        data = r.json()
        daily = data.get("daily", {})
        dates = daily.get("time", [])
        temps = daily.get("temperature_2m_max", [])
        return {d: t for d, t in zip(dates, temps) if t is not None}
    except Exception as e:
        print(f"    [WARN] Open-Meteo failed ({lat},{lon}): {e}")
        return None


# ─────────────────────────────────────────────────────────────────────────────
# Main forecast builder
# ─────────────────────────────────────────────────────────────────────────────

def fetch_city_forecast(city_name: str, days: int = 2) -> dict:
    """
    Fetch forecasts for a city. All temperatures in the city's native unit (°F or °C).

    Confidence:
      - "high"   : two sources agree ≤ threshold (2°F or 1°C)
      - "medium" : one source, or two disagree moderately
      - "low"    : sources disagree >5°F / >3°C → skip in analyzer

    Returns:
    {
        "city": str,
        "station": str,
        "unit": str,   # "F" or "C"
        "forecasts": {
            "date_str": {
                "wttr": float | None,
                "nws": float | None,      # None for non-US
                "open_meteo": float | None,
                "consensus": float,
                "confidence": "high" | "medium" | "low",
            }
        }
    }
    """
    cfg = CITIES[city_name]
    lat, lon, tz = cfg["lat"], cfg["lon"], cfg["tz"]
    station = cfg["station"]
    unit = cfg.get("unit", "F")

    # NWS only for genuine US locations (not Canada)
    is_nws_eligible = unit == "F"

    # Agreement thresholds differ by unit
    high_threshold = 2.0 if unit == "F" else 1.0
    low_threshold = 5.0 if unit == "F" else 3.0

    wttr = fetch_wttr_forecast(station, days=days, unit=unit)
    nws = fetch_nws_forecast(lat, lon) if is_nws_eligible else None
    open_meteo = fetch_open_meteo(lat, lon, tz, days=days, unit=unit)

    today = date.today()
    forecasts = {}

    for i in range(days):
        d = (today + timedelta(days=i)).isoformat()
        wttr_t = wttr.get(d) if wttr else None
        nws_t = nws.get(d) if nws else None
        om_t = open_meteo.get(d) if open_meteo else None

        sources = [t for t in [wttr_t, nws_t, om_t] if t is not None]
        if not sources:
            continue

        # Build consensus: prefer wttr (station-matched) + NWS/OM as second opinion
        primary_a = wttr_t
        primary_b = nws_t if nws_t is not None else om_t

        if primary_a is not None and primary_b is not None:
            spread = abs(primary_a - primary_b)
            consensus = round((primary_a + primary_b) / 2, 1)
            confidence = "high" if spread <= high_threshold else (
                "medium" if spread <= low_threshold else "low"
            )
        elif len(sources) == 1:
            consensus = round(sources[0], 1)
            confidence = "medium"
        else:
            consensus = round(sum(sources) / len(sources), 1)
            confidence = "medium"

        forecasts[d] = {
            "wttr": wttr_t,
            "nws": nws_t,
            "open_meteo": om_t,
            "consensus": consensus,
            "confidence": confidence,
        }

    return {"city": city_name, "station": station, "unit": unit, "forecasts": forecasts}


def fetch_all_forecasts(cities=None, days: int = 2) -> dict:
    """
    Fetch forecasts for all (or specified) cities.
    Returns {city_name: forecast_dict}
    """
    if cities is None:
        cities = list(CITIES.keys())

    all_forecasts = {}
    for city in cities:
        print(f"  Fetching forecast for {city}...")
        result = fetch_city_forecast(city, days)
        all_forecasts[city] = result

        unit = result["unit"]
        today = date.today()
        tomorrow = (today + timedelta(days=1)).isoformat()
        f = result["forecasts"].get(tomorrow)
        if f:
            sources = []
            if f["wttr"] is not None:
                sources.append(f"wttr={f['wttr']:.1f}")
            if f["nws"] is not None:
                sources.append(f"nws={f['nws']:.1f}")
            if f["open_meteo"] is not None:
                sources.append(f"om={f['open_meteo']:.1f}")
            src_str = ", ".join(sources)
            print(f"    {tomorrow} [TOMORROW]: {f['consensus']:.1f}°{unit}  conf={f['confidence']}  ({src_str})")
        else:
            print(f"    {tomorrow}: no forecast data")

    return all_forecasts


if __name__ == "__main__":
    print("Fetching weather forecasts (next day focus)...\n")
    forecasts = fetch_all_forecasts(["NYC", "Chicago", "Miami", "London", "Paris", "Seoul", "Toronto"])

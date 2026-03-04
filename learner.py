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
WEIGHTS_FILE     = os.path.join(DATA_DIR, "forecast_weights.json")
CALIBRATION_FILE = os.path.join(DATA_DIR, "calibration.json")

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


# ─── Wunderground PWS history — actual station temperature verification ───────

WU_PWS_KEY = os.environ.get("WU_PWS_KEY", "")
WU_PWS_API = "https://api.weather.com/v2/pws"

# Maps each city to a nearby high-quality PWS station (qcStatus=1 from WU near search)
# These are fallbacks — used when PWS key is set to verify actual temps
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
      1. WU PWS key: fetch actual daily high directly from the PWS station.
         Most accurate — same network Wunderground uses for station history.
      2. Polymarket Gamma API: infer temperature from which YES bracket resolved.
         Fallback when PWS key not available or station unknown.

    Returns the actual temperature, or None if not yet resolved / unavailable.
    """
    # Strategy 1: direct PWS station history (most accurate)
    if WU_PWS_KEY and city and resolution_date:
        pws_id = _CITY_PWS_STATION.get(city)
        if pws_id:
            actual = fetch_actual_temp_pws(pws_id, resolution_date, unit)
            if actual is not None:
                return actual
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
    max_err = max(avgs.values()) or 1.0
    # Add small epsilon to avoid divide-by-zero when all errors are equal
    scores = {src: 1.0 / (avg + 0.01) for src, avg in avgs.items()}
    total = sum(scores.values())
    return {src: score / total for src, score in scores.items()}


# ─── Main learning function ───────────────────────────────────────────────────

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
    from tracker import _load, _save as _save_tracker

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
                # Over-confident: increase sigma
                target_sigma = old_sigma / ratio
            elif ratio > 1.03:
                # Under-confident: decrease sigma (but keep ≥ floor)
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
        _save_tracker(data)

    return {
        "learned":        learned_count,
        "temps_fetched":  temp_found,
        "weights_updated": weights_updated,
        "calib_updated":  calib_updated,
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

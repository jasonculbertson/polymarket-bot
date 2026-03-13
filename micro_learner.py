"""
Per-scan micro-learning engine.

Runs after every scan cycle (every 6h) — no outcome data needed.
Tracks signals that improve between resolving: city forecast volatility,
source divergence, and opportunity volume trends.

Three things tracked:
  1. Forecast drift per city  — how much does WU change scan-to-scan?
                                High drift = higher uncertainty, deserves higher distance.
  2. Source divergence        — do WU and open_meteo disagree by >3°F for a city?
                                High divergence = frontal boundary, unsafe to bet.
  3. Opportunity volume       — how many NO/YES found per scan?
                                Declining trend = market getting efficient.

Results saved to Postgres under key 'scan_metrics'. The daily optimizer
reads scan_metrics to get city volatility scores and adjust bonus_f recommendations.
"""

from __future__ import annotations

import json
import os
from collections import defaultdict, deque
from datetime import datetime, timezone
from typing import Optional

DATA_DIR     = os.environ.get("DATA_DIR", os.path.join(os.path.dirname(__file__), "data"))
DATABASE_URL = os.environ.get("DATABASE_URL", "")

# Rolling window size for scan history (each scan = ~6 hours → 20 scans ≈ 5 days)
MAX_SCAN_HISTORY = 20

# Flag city as volatile if avg drift exceeds these thresholds
DRIFT_WARN_F = 3.0   # °F per scan
DRIFT_WARN_C = 1.7   # °C per scan

# Flag city as source-divergent if WU vs open_meteo differ by this much
DIVERGENCE_WARN_F = 4.0
DIVERGENCE_WARN_C = 2.2

# Volume trend: flag declining if avg over last 5 scans < half the overall avg
VOLUME_DECLINE_RATIO = 0.5


# ─── Postgres helpers ─────────────────────────────────────────────────────────

def _pg_load(key: str) -> Optional[dict]:
    if not DATABASE_URL:
        return None
    try:
        import psycopg2
        conn = psycopg2.connect(DATABASE_URL)
        try:
            with conn.cursor() as cur:
                cur.execute("SELECT data FROM kv_store WHERE key = %s", (key,))
                row = cur.fetchone()
            if not row:
                return None
            val = row[0]
            if isinstance(val, (dict, list)):
                return val
            return json.loads(val)
        finally:
            conn.close()
    except Exception as e:
        print(f"[micro_learner] _pg_load({key}) failed: {e}")
        return None


def _pg_save(key: str, data: dict) -> bool:
    if not DATABASE_URL:
        return False
    try:
        import psycopg2
        conn = psycopg2.connect(DATABASE_URL)
        try:
            with conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO kv_store (key, data, updated_at)
                    VALUES (%s, %s::jsonb, NOW())
                    ON CONFLICT (key) DO UPDATE
                      SET data = EXCLUDED.data,
                          updated_at = EXCLUDED.updated_at
                """, (key, json.dumps(data)))
            conn.commit()
            return True
        finally:
            conn.close()
    except Exception as e:
        print(f"[micro_learner] _pg_save({key}) failed: {e}")
        return False


# ─── Per-scan data collection ─────────────────────────────────────────────────

def _get_city_drift_from_positions() -> dict:
    """
    For each city with open positions, compute the drift between the entry forecast
    and the forecast stored by the most recent drift check.

    Uses data already on the opportunity objects — no extra API calls needed.
    Returns {city: drift_f} where drift_f is the absolute °F change from entry.
    """
    try:
        from tracker import _load
        data = _load()
    except Exception as e:
        print(f"[micro_learner] could not load tracker: {e}")
        return {}

    city_drifts: dict[str, list] = defaultdict(list)
    for opp in data.get("opportunities", []):
        if opp.get("outcome") is not None:
            continue  # resolved — not useful for current drift
        entry_fc  = opp.get("forecast_temp")
        edge_fc   = opp.get("edge_gone_forecast_temp")  # latest forecast from drift check
        if entry_fc is None:
            continue
        # If edge_gone_forecast_temp exists, use it as the latest snapshot.
        # Otherwise there was no significant drift (drift check didn't fire).
        if edge_fc is not None:
            drift = abs(float(edge_fc) - float(entry_fc))
        else:
            drift = 0.0

        # Convert C positions to F for unified tracking
        unit = opp.get("temp_unit", "F")
        if unit == "C":
            drift = drift * 1.8

        city = opp.get("city", "unknown")
        city_drifts[city].append(drift)

    # Average drift per city
    return {city: round(sum(ds) / len(ds), 2) for city, ds in city_drifts.items()}


def _get_source_divergence_from_positions() -> dict:
    """
    For each city with open positions, measure divergence between WU and open_meteo
    forecasts recorded at scan entry time.
    Returns {city: divergence_f}.
    """
    try:
        from tracker import _load
        data = _load()
    except Exception:
        return {}

    city_divs: dict[str, list] = defaultdict(list)
    for opp in data.get("opportunities", []):
        if opp.get("outcome") is not None:
            continue
        sources = opp.get("forecast_sources") or {}
        wu = sources.get("wunderground")
        om = sources.get("open_meteo") or sources.get("wttr")
        if wu is None or om is None:
            continue
        div = abs(float(wu) - float(om))
        unit = opp.get("temp_unit", "F")
        if unit == "C":
            div = div * 1.8
        city_divs[opp.get("city", "unknown")].append(div)

    return {city: round(sum(ds) / len(ds), 2) for city, ds in city_divs.items()}


def _get_scan_volume() -> dict:
    """
    Read the latest scan output file for opportunity counts.
    Returns {n_no, n_yes, timestamp}.
    """
    try:
        latest = os.path.join(DATA_DIR, "latest_scan.json")
        if not os.path.exists(latest):
            return {}
        with open(latest) as f:
            scan = json.load(f)
        opps = scan.get("opportunities", [])
        return {
            "n_no":  sum(1 for o in opps if o.get("type") == "no"),
            "n_yes": sum(1 for o in opps if o.get("type") == "yes"),
            "scan_ts": scan.get("scanned_at") or datetime.now(timezone.utc).isoformat(),
        }
    except Exception as e:
        print(f"[micro_learner] volume read failed: {e}")
        return {}


# ─── Rolling aggregation ──────────────────────────────────────────────────────

def _update_city_volatility(city_volatility: dict, new_drifts: dict) -> dict:
    """
    Update the rolling per-city volatility stats with this scan's drifts.
    Each city gets a deque of recent drift observations.
    """
    for city, drift in new_drifts.items():
        entry = city_volatility.setdefault(city, {"drifts": [], "avg_drift_f": 0.0, "n": 0})
        drifts = entry["drifts"]
        drifts.append(drift)
        if len(drifts) > MAX_SCAN_HISTORY:
            drifts.pop(0)
        entry["avg_drift_f"] = round(sum(drifts) / len(drifts), 2)
        entry["n"] = len(drifts)
        entry["volatile"] = entry["avg_drift_f"] >= DRIFT_WARN_F
    return city_volatility


def _update_volume_trend(volume_history: list, new_volume: dict) -> dict:
    """
    Maintain rolling scan volume log and compute trend.
    """
    if not new_volume:
        return {"history": volume_history, "trend": "unknown"}

    volume_history.append({
        "ts":    new_volume.get("scan_ts", ""),
        "n_no":  new_volume.get("n_no", 0),
        "n_yes": new_volume.get("n_yes", 0),
    })
    if len(volume_history) > MAX_SCAN_HISTORY:
        volume_history = volume_history[-MAX_SCAN_HISTORY:]

    no_counts = [v["n_no"] for v in volume_history]
    yes_counts = [v["n_yes"] for v in volume_history]
    overall_no_avg  = sum(no_counts) / len(no_counts)
    overall_yes_avg = sum(yes_counts) / len(yes_counts) if yes_counts else 0

    recent_no_avg  = sum(no_counts[-5:])  / min(5, len(no_counts))
    recent_yes_avg = sum(yes_counts[-5:]) / min(5, len(yes_counts))

    if overall_no_avg > 0 and recent_no_avg < overall_no_avg * VOLUME_DECLINE_RATIO:
        trend = "declining"
    elif overall_no_avg > 0 and recent_no_avg > overall_no_avg * 1.5:
        trend = "rising"
    else:
        trend = "stable"

    return {
        "history":          volume_history,
        "overall_no_avg":   round(overall_no_avg, 1),
        "overall_yes_avg":  round(overall_yes_avg, 1),
        "recent_no_avg":    round(recent_no_avg, 1),
        "trend":            trend,
    }


def _compute_volatile_cities(city_volatility: dict) -> list:
    """Return city names where forecast drift is above warn threshold."""
    return [c for c, v in city_volatility.items() if v.get("volatile")]


def _compute_divergent_cities(city_divergence: dict) -> list:
    """Return cities where WU vs open_meteo divergence is high this scan."""
    return [c for c, div in city_divergence.items() if div >= DIVERGENCE_WARN_F]


# ─── Main entry point ─────────────────────────────────────────────────────────

def post_scan_learn() -> dict:
    """
    Run after every scan cycle. Lightweight — no API calls, reads only local state.

    Updates scan_metrics in Postgres:
      city_volatility  — rolling avg forecast drift per city (feeds backtest optimizer)
      city_divergence  — current WU vs open_meteo spread per city
      volume_trend     — scan-to-scan opportunity count trend

    Returns a summary dict for the scan log.
    """
    now = datetime.now(timezone.utc).isoformat()

    # Load existing metrics (or start fresh)
    metrics = _pg_load("scan_metrics") or {
        "city_volatility": {},
        "city_divergence": {},
        "volume": {"history": [], "trend": "unknown"},
        "scans_recorded": 0,
    }

    # Collect this scan's signals
    new_drifts    = _get_city_drift_from_positions()
    new_divergence = _get_source_divergence_from_positions()
    new_volume    = _get_scan_volume()

    # Update rolling aggregates
    metrics["city_volatility"] = _update_city_volatility(
        metrics.get("city_volatility", {}), new_drifts
    )
    # Divergence: update per-city (point-in-time, not rolling avg)
    for city, div in new_divergence.items():
        metrics["city_divergence"][city] = {
            "divergence_f": div,
            "high": div >= DIVERGENCE_WARN_F,
            "updated": now,
        }

    metrics["volume"]         = _update_volume_trend(
        metrics.get("volume", {}).get("history", []), new_volume
    )
    metrics["scans_recorded"] = metrics.get("scans_recorded", 0) + 1
    metrics["last_updated"]   = now

    # Derived signals for easy consumption by daily optimizer + Claude task
    metrics["volatile_cities"]   = _compute_volatile_cities(metrics["city_volatility"])
    metrics["divergent_cities"]  = _compute_divergent_cities(metrics["city_divergence"])
    metrics["volume_trend"]      = metrics["volume"].get("trend", "unknown")

    _pg_save("scan_metrics", metrics)

    n_volatile  = len(metrics["volatile_cities"])
    n_divergent = len(metrics["divergent_cities"])
    vol_trend   = metrics["volume_trend"]
    print(f"[micro_learner] scan #{metrics['scans_recorded']} — "
          f"{n_volatile} volatile cities, {n_divergent} source-divergent, "
          f"volume={vol_trend}")
    if metrics["volatile_cities"]:
        print(f"  volatile: {metrics['volatile_cities']}")
    if metrics["divergent_cities"]:
        print(f"  divergent (WU≠meteo): {metrics['divergent_cities']}")

    return {
        "scans_recorded": metrics["scans_recorded"],
        "volatile_cities": metrics["volatile_cities"],
        "divergent_cities": metrics["divergent_cities"],
        "volume_trend": vol_trend,
        "new_drifts": new_drifts,
    }


if __name__ == "__main__":
    result = post_scan_learn()
    print(json.dumps(result, indent=2))

"""
Per-city strategy backtest optimizer (autoresearch-inspired).

Runs in the daily learn pipeline after resolve + learn. Tests different
threshold values against historical resolved outcomes using a train/validation
split to avoid overfitting, then saves recommendations to Postgres.

The Claude scheduled task reads 'backtest_recommendations' and applies safe
changes to config.py / city_adjustments.json.

What is optimized:
  NO bets:
    bonus_f per city  — extra distance above global no_min_distance_f
                        Range: -1.0 to +5.0 in 0.5°F steps

  YES bets:
    yes_min_margin_f per city  — tested but advisory only (fewer samples)

  Global:
    no_min_distance_f  — tested on the full resolved set
    yes_min_margin_f   — tested on full YES set

Guard rails:
  - Need ≥ MIN_SAMPLES total before testing
  - Need ≥ MIN_VAL_SAMPLES in validation set before accepting recommendation
  - Improvement threshold: val_win_rate must beat current by ≥ MIN_IMPROVEMENT
  - Max change: ≤ MAX_BONUS_DELTA °F change per day (applied by Claude task)
"""

from __future__ import annotations

import json
import os
from collections import defaultdict
from datetime import datetime, date, timedelta, timezone
from typing import Optional

DATA_DIR     = os.environ.get("DATA_DIR", os.path.join(os.path.dirname(__file__), "data"))
DATABASE_URL = os.environ.get("DATABASE_URL", "")

# Minimum resolved outcomes before testing a threshold
MIN_SAMPLES     = 10
MIN_VAL_SAMPLES = 4

# Minimum win_rate improvement required to recommend a change
MIN_IMPROVEMENT = 0.05   # 5 percentage points

# Train/validation split ratio
TRAIN_FRAC = 0.70

# Lookback for backtest data (days)
LOOKBACK_DAYS = 45

# NO bonus test range (°F above/below global no_min_distance_f)
NO_BONUS_RANGE = [-1.0, -0.5, 0.0, 0.5, 1.0, 1.5, 2.0, 2.5, 3.0, 3.5, 4.0, 5.0]

# YES margin test range (°F, absolute)
YES_MARGIN_RANGE = [0.5, 1.0, 1.5, 2.0, 2.5, 3.0, 3.5, 4.0]

# Global distance test range (°F, absolute)
NO_GLOBAL_RANGE = [4.0, 4.5, 5.0, 5.5, 6.0, 6.5, 7.0, 7.5, 8.0, 8.5, 9.0]


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
        print(f"[backtest_opt] _pg_load({key}) failed: {e}")
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
        print(f"[backtest_opt] _pg_save({key}) failed: {e}")
        return False


# ─── Config readers ───────────────────────────────────────────────────────────

def _get_current_config() -> dict:
    """Load STRATEGY from config.py for current threshold values."""
    try:
        from config import STRATEGY
        return STRATEGY
    except Exception:
        return {}


def _get_current_city_adjustments() -> dict:
    """Load city_adjustments.json for current per-city bonus_f values."""
    try:
        adj_file = os.path.join(DATA_DIR, "city_adjustments.json")
        if os.path.exists(adj_file):
            with open(adj_file) as f:
                return json.load(f)
    except Exception:
        pass
    return {}


# ─── Backtest helpers ─────────────────────────────────────────────────────────

def _win_rate(opps: list) -> Optional[float]:
    if not opps:
        return None
    wins = sum(1 for o in opps if o.get("outcome") == "win")
    return round(wins / len(opps), 4)


def _filter_by_distance(opps: list, threshold_f: float) -> list:
    """Return only bets where distance >= threshold (convert C to F for comparison)."""
    result = []
    for o in opps:
        dist = o.get("distance")
        if dist is None:
            continue
        dist = float(dist)
        if o.get("temp_unit") == "C":
            dist = dist * 1.8
        if dist >= threshold_f:
            result.append(o)
    return result


def _filter_by_margin(opps: list, margin_f: float) -> list:
    """Return YES bets where the forecast margin >= threshold."""
    result = []
    for o in opps:
        margin = o.get("yes_margin_f") or o.get("yes_margin")
        if margin is None:
            # Fall back to checking distance field if margin not stored separately
            margin = o.get("distance")
        if margin is None:
            continue
        margin = float(margin)
        if o.get("temp_unit") == "C":
            margin = margin * 1.8
        if margin >= margin_f:
            result.append(o)
    return result


def _train_val_split(opps: list) -> tuple[list, list]:
    """Chronological 70/30 split — train on older, validate on newer."""
    opps = sorted(opps, key=lambda o: o.get("date") or "")
    split = max(1, int(len(opps) * TRAIN_FRAC))
    return opps[:split], opps[split:]


def _test_distance_thresholds(
    opps: list, threshold_range: list, current_threshold: float
) -> Optional[dict]:
    """
    Test different distance threshold values. Return recommendation or None.
    opps must be pre-filtered to a single city or the global set.
    """
    if len(opps) < MIN_SAMPLES:
        return None

    train, val = _train_val_split(opps)
    current_wr = _win_rate(_filter_by_distance(val, current_threshold))

    best = None
    all_results = []
    for threshold in threshold_range:
        train_placed = _filter_by_distance(train, threshold)
        if len(train_placed) < 5:
            continue
        train_wr = _win_rate(train_placed)

        val_placed = _filter_by_distance(val, threshold)
        val_wr = _win_rate(val_placed) if len(val_placed) >= MIN_VAL_SAMPLES else None

        result = {
            "threshold_f": threshold,
            "train_win_rate": train_wr,
            "train_n": len(train_placed),
            "val_win_rate": val_wr,
            "val_n": len(val_placed),
        }
        all_results.append(result)

        # Track best (by val win rate, with adequate sample requirement)
        if val_wr is not None and len(val_placed) >= MIN_VAL_SAMPLES:
            if best is None or val_wr > best["val_win_rate"]:
                best = result

    if best is None:
        return None

    # Only recommend if it's a genuine improvement
    improvement = (best["val_win_rate"] - (current_wr or 0))
    if improvement < MIN_IMPROVEMENT:
        return None

    return {
        "recommended_threshold_f": best["threshold_f"],
        "current_threshold_f":     current_threshold,
        "improvement":             round(improvement, 4),
        "val_win_rate":            best["val_win_rate"],
        "val_n":                   best["val_n"],
        "train_win_rate":          best["train_win_rate"],
        "train_n":                 best["train_n"],
        "current_val_win_rate":    current_wr,
        "n_total":                 len(opps),
        "all_results":             all_results,
    }


def _test_yes_margin_thresholds(
    opps: list, current_margin: float
) -> Optional[dict]:
    """Test different yes_min_margin values."""
    if len(opps) < MIN_SAMPLES:
        return None

    train, val = _train_val_split(opps)
    current_wr = _win_rate(_filter_by_margin(val, current_margin))

    best = None
    for margin in YES_MARGIN_RANGE:
        train_placed = _filter_by_margin(train, margin)
        if len(train_placed) < 5:
            continue
        val_placed = _filter_by_margin(val, margin)
        val_wr = _win_rate(val_placed) if len(val_placed) >= MIN_VAL_SAMPLES else None
        if val_wr is None:
            continue
        if best is None or val_wr > best["val_win_rate"]:
            best = {
                "margin_f": margin,
                "val_win_rate": val_wr,
                "val_n": len(val_placed),
                "train_win_rate": _win_rate(train_placed),
                "train_n": len(train_placed),
            }

    if best is None:
        return None
    improvement = best["val_win_rate"] - (current_wr or 0)
    if improvement < MIN_IMPROVEMENT:
        return None

    return {
        "recommended_margin_f": best["margin_f"],
        "current_margin_f":     current_margin,
        "improvement":          round(improvement, 4),
        "val_win_rate":         best["val_win_rate"],
        "val_n":                best["val_n"],
        "current_val_win_rate": current_wr,
        "n_total":              len(opps),
    }


# ─── Main optimization functions ──────────────────────────────────────────────

def optimize_city_thresholds(data: dict) -> dict:
    """
    Test per-city and global thresholds against historical resolved outcomes.
    Returns structured recommendations for the Claude task to apply.

    data: the full tracker data dict (from tracker._load()).
    """
    config = _get_current_config()
    city_adj = _get_current_city_adjustments()

    global_no_min_f   = float(config.get("no_min_distance_f", 6.0))
    global_yes_margin = float(config.get("yes_min_margin_f", 2.0))

    # Filter to recent resolved, exclude very fresh data (may not have learned yet)
    cutoff = (date.today() - timedelta(days=LOOKBACK_DAYS)).isoformat()
    resolved = [
        o for o in data.get("opportunities", [])
        if o.get("outcome") is not None and (o.get("date") or "") >= cutoff
    ]

    no_resolved  = [o for o in resolved if o.get("type") == "no"]
    yes_resolved = [o for o in resolved if o.get("type") == "yes"]

    by_city_no: dict  = defaultdict(list)
    by_city_yes: dict = defaultdict(list)
    for o in no_resolved:
        by_city_no[o.get("city", "unknown")].append(o)
    for o in yes_resolved:
        by_city_yes[o.get("city", "unknown")].append(o)

    recommendations = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "lookback_days": LOOKBACK_DAYS,
        "n_no_resolved":  len(no_resolved),
        "n_yes_resolved": len(yes_resolved),
        "by_city":  {},
        "global":   {},
        "summary":  [],
    }

    # ── Per-city NO bonus optimization ────────────────────────────────────────
    for city, opps in by_city_no.items():
        current_bonus = 0.0
        adj = city_adj.get(city, {})
        if isinstance(adj, dict):
            current_bonus = float(adj.get("bonus_f", 0.0))
        current_threshold = global_no_min_f + current_bonus

        # Build test range: global + each bonus offset
        threshold_range = [global_no_min_f + b for b in NO_BONUS_RANGE]
        threshold_range = sorted(set(threshold_range))

        rec = _test_distance_thresholds(opps, threshold_range, current_threshold)
        if rec:
            # Convert recommended threshold back to bonus
            rec["recommended_bonus_f"] = round(
                rec["recommended_threshold_f"] - global_no_min_f, 1
            )
            rec["current_bonus_f"] = current_bonus
            city_entry = recommendations["by_city"].setdefault(city, {})
            city_entry["no"] = rec
            recommendations["summary"].append(
                f"{city} NO: bonus {current_bonus:+.1f}→{rec['recommended_bonus_f']:+.1f}°F "
                f"(val wr {rec['current_val_win_rate']:.1%}→{rec['val_win_rate']:.1%}, n={rec['val_n']})"
            )

    # ── Per-city YES margin optimization (advisory — fewer samples) ──────────
    for city, opps in by_city_yes.items():
        rec = _test_yes_margin_thresholds(opps, global_yes_margin)
        if rec:
            city_entry = recommendations["by_city"].setdefault(city, {})
            city_entry["yes"] = rec
            recommendations["summary"].append(
                f"{city} YES: margin {global_yes_margin:.1f}→{rec['recommended_margin_f']:.1f}°F "
                f"(val wr {rec['current_val_win_rate']:.1%}→{rec['val_win_rate']:.1%}, n={rec['val_n']})"
            )

    # ── Global NO distance optimization ───────────────────────────────────────
    if len(no_resolved) >= MIN_SAMPLES:
        rec = _test_distance_thresholds(no_resolved, NO_GLOBAL_RANGE, global_no_min_f)
        if rec:
            recommendations["global"]["no_min_distance_f"] = rec
            recommendations["summary"].append(
                f"GLOBAL NO distance: {global_no_min_f:.1f}→{rec['recommended_threshold_f']:.1f}°F "
                f"(val wr {rec['current_val_win_rate']:.1%}→{rec['val_win_rate']:.1%}, n={rec['val_n']})"
            )

    # ── Global YES margin optimization ────────────────────────────────────────
    if len(yes_resolved) >= MIN_SAMPLES:
        rec = _test_yes_margin_thresholds(yes_resolved, global_yes_margin)
        if rec:
            recommendations["global"]["yes_min_margin_f"] = rec
            recommendations["summary"].append(
                f"GLOBAL YES margin: {global_yes_margin:.1f}→{rec['recommended_margin_f']:.1f}°F "
                f"(val wr {rec['current_val_win_rate']:.1%}→{rec['val_win_rate']:.1%}, n={rec['val_n']})"
            )

    recommendations["n_recommendations"] = (
        sum(len(v) for v in recommendations["by_city"].values())
        + len(recommendations["global"])
    )

    # Print summary
    print(f"[backtest_opt] {len(no_resolved)} NO + {len(yes_resolved)} YES resolved "
          f"| {recommendations['n_recommendations']} recommendations")
    for line in recommendations["summary"]:
        print(f"  → {line}")

    _pg_save("backtest_recommendations", recommendations)
    return recommendations


if __name__ == "__main__":
    from tracker import _load
    data = _load()
    rec = optimize_city_thresholds(data)
    print(json.dumps({
        "n_recommendations": rec["n_recommendations"],
        "summary": rec["summary"],
        "global": rec.get("global"),
        "n_cities": len(rec.get("by_city", {})),
    }, indent=2))

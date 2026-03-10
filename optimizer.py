"""
Daily optimization engine.

After each resolve + learn cycle, this module:
  1. Computes comprehensive performance stats across multiple dimensions
  2. Identifies issues — win rates, ROI, city accuracy, threshold drift
  3. Flags actionable problems for the daily Claude task to act on

The report is saved to Postgres under key 'daily_report' and read by the
scheduled Claude task which then makes targeted config/code changes and
pushes to GitHub.

Dimensions analyzed:
  - Overall P&L and win rate
  - By type (YES / NO)
  - By confidence (high / medium)
  - NO by distance bucket (validates distance thresholds)
  - YES by total_price bucket (validates lottery threshold)
  - By city — NO win rate and avg wu_error (feeds city_adjustments)
  - By date — rolling 14-day trend
"""

from __future__ import annotations

import json
from collections import defaultdict
from datetime import datetime, date, timedelta
from typing import Optional


# ─── Issue severity thresholds ───────────────────────────────────────────────

# NO bets: expected win rate ≥ this. Below → distance thresholds too low.
NO_WIN_RATE_WARN   = 0.80   # warn
NO_WIN_RATE_CRIT   = 0.70   # critical

# YES clusters: expected win rate for 3-bracket clusters (~30-45¢ range)
YES_WIN_RATE_WARN  = 0.30   # warn if below
YES_WIN_RATE_HIGH  = 0.55   # warn if above (might be mismeasured)

# Lottery YES (<0.25 total_price): higher variance but should win occasionally
YES_LOTTERY_WIN_WARN = 0.10  # warn if below — lottery threshold may be too loose

# City-level NO: flag cities with poor win rate AND high forecast error
CITY_NO_WIN_WARN   = 0.70
CITY_WU_ERR_WARN   = 4.0    # °F avg error to flag a city

# Min samples before flagging (avoid noise from small N)
MIN_SAMPLES_GENERAL = 10
MIN_SAMPLES_CITY    = 5

# Lookback window for recent analysis
LOOKBACK_DAYS = 14


# ─── Core stats helpers ───────────────────────────────────────────────────────

def _compute_stats(opps: list) -> dict:
    """Basic win/loss/ROI stats for a list of resolved opportunities."""
    if not opps:
        return {"n": 0, "wins": 0, "losses": 0, "win_rate": None,
                "total_staked": 0.0, "total_pnl": 0.0, "roi_pct": None}
    wins   = sum(1 for o in opps if o.get("outcome") == "win")
    losses = sum(1 for o in opps if o.get("outcome") == "loss")
    staked = sum(float(o.get("paper_size_usd") or 0) for o in opps)
    pnl    = sum(float(o.get("paper_pnl_usd") or 0) for o in opps)
    n = wins + losses
    return {
        "n":           n,
        "wins":        wins,
        "losses":      losses,
        "win_rate":    round(wins / n, 4) if n > 0 else None,
        "total_staked": round(staked, 2),
        "total_pnl":   round(pnl, 2),
        "roi_pct":     round(pnl / staked * 100, 2) if staked > 0 else None,
    }


def _compute_city_stats(no_opps: list) -> dict:
    """Per-city stats for NO bets. Only cities with ≥ MIN_SAMPLES_CITY."""
    by_city: dict = defaultdict(list)
    for o in no_opps:
        by_city[o.get("city", "unknown")].append(o)

    result = {}
    for city, opps in by_city.items():
        if len(opps) < MIN_SAMPLES_CITY:
            continue
        stats = _compute_stats(opps)
        wu_errors = [float(o["wu_error"]) for o in opps if o.get("wu_error") is not None]
        stats["avg_wu_error_f"] = round(sum(wu_errors) / len(wu_errors), 2) if wu_errors else None
        stats["wu_error_samples"] = len(wu_errors)
        result[city] = stats
    return result


def _compute_distance_buckets(no_opps: list) -> dict:
    """NO win rate by distance bucket (°F). Validates distance thresholds."""
    buckets: dict = defaultdict(list)
    for o in no_opps:
        dist = o.get("distance")
        if dist is None:
            continue
        dist = float(dist)
        if o.get("temp_unit") == "C":
            dist = dist * 1.8   # convert to °F for unified bucketing
        if dist < 8:
            key = "6-8°F"
        elif dist < 12:
            key = "8-12°F"
        elif dist < 18:
            key = "12-18°F"
        else:
            key = "18+°F"
        buckets[key].append(o)

    return {k: _compute_stats(v) for k, v in sorted(buckets.items())}


def _compute_yes_price_buckets(yes_opps: list) -> dict:
    """YES win rate by total_price bucket. Validates lottery threshold."""
    buckets: dict = defaultdict(list)
    for o in yes_opps:
        price = float(o.get("entry_price") or 0)
        if price < 0.10:
            key = "<0.10 (extreme lottery)"
        elif price < 0.25:
            key = "0.10-0.25 (lottery)"
        elif price < 0.50:
            key = "0.25-0.50 (normal)"
        else:
            key = "0.50+ (high cost)"
        buckets[key].append(o)

    return {k: _compute_stats(v) for k, v in sorted(buckets.items())}


def _compute_rolling_trend(resolved: list, days: int = 14) -> list:
    """Daily win rate and P&L for the last N days."""
    by_date: dict = defaultdict(list)
    for o in resolved:
        d = o.get("date") or o.get("resolution_date")
        if d:
            by_date[d].append(o)

    today = date.today()
    trend = []
    for i in range(days - 1, -1, -1):
        d = (today - timedelta(days=i)).isoformat()
        opps = by_date.get(d, [])
        stats = _compute_stats(opps)
        stats["date"] = d
        trend.append(stats)
    return trend


# ─── Issue detection ──────────────────────────────────────────────────────────

def _identify_issues(
    overall: dict,
    by_type: dict,
    by_city_no: dict,
    by_confidence: dict,
    by_distance: dict,
    by_price: dict,
) -> list:
    """Return list of flagged issues with severity, category, and context."""
    issues = []

    def _flag(severity: str, category: str, message: str, **data):
        issues.append({"severity": severity, "category": category,
                       "message": message, **data})

    # ── Overall NO win rate ───────────────────────────────────────────────────
    no_stats = by_type.get("no", {})
    if no_stats.get("n", 0) >= MIN_SAMPLES_GENERAL and no_stats.get("win_rate") is not None:
        wr = no_stats["win_rate"]
        if wr < NO_WIN_RATE_CRIT:
            _flag("critical", "no_win_rate",
                  f"NO win rate {wr:.1%} is critically low (expected ≥{NO_WIN_RATE_WARN:.0%})",
                  win_rate=wr, n=no_stats["n"])
        elif wr < NO_WIN_RATE_WARN:
            _flag("warning", "no_win_rate",
                  f"NO win rate {wr:.1%} below target {NO_WIN_RATE_WARN:.0%}",
                  win_rate=wr, n=no_stats["n"])

    # ── YES win rate ─────────────────────────────────────────────────────────
    yes_stats = by_type.get("yes", {})
    if yes_stats.get("n", 0) >= MIN_SAMPLES_GENERAL and yes_stats.get("win_rate") is not None:
        wr = yes_stats["win_rate"]
        if wr < YES_WIN_RATE_WARN:
            _flag("warning", "yes_win_rate",
                  f"YES win rate {wr:.1%} is low — clusters may be poorly centered",
                  win_rate=wr, n=yes_stats["n"])

    # ── YES lottery win rate ──────────────────────────────────────────────────
    for bucket, stats in by_price.items():
        if "lottery" in bucket and stats.get("n", 0) >= MIN_SAMPLES_GENERAL:
            wr = stats.get("win_rate")
            if wr is not None and wr < YES_LOTTERY_WIN_WARN:
                _flag("warning", "yes_lottery_win_rate",
                      f"Lottery YES bucket '{bucket}' win rate {wr:.1%} — threshold may be too loose",
                      bucket=bucket, win_rate=wr, n=stats["n"])

    # ── NO distance buckets ───────────────────────────────────────────────────
    for bucket, stats in by_distance.items():
        if stats.get("n", 0) >= MIN_SAMPLES_GENERAL and stats.get("win_rate") is not None:
            wr = stats["win_rate"]
            if wr < NO_WIN_RATE_WARN:
                _flag("warning", "distance_threshold",
                      f"NO win rate {wr:.1%} in distance bucket '{bucket}' — min distance may be too low",
                      bucket=bucket, win_rate=wr, n=stats["n"])

    # ── City-level NO problems ────────────────────────────────────────────────
    for city, stats in by_city_no.items():
        wr = stats.get("win_rate")
        wu_err = stats.get("avg_wu_error_f")
        n = stats.get("n", 0)
        if n < MIN_SAMPLES_CITY:
            continue
        if wr is not None and wr < CITY_NO_WIN_WARN:
            severity = "critical" if wr < 0.60 else "warning"
            extra = f", avg WU error {wu_err:.1f}°F" if wu_err else ""
            _flag(severity, "city_loss",
                  f"{city} NO win rate {wr:.1%}{extra} — consider raising city distance bonus",
                  city=city, win_rate=wr, avg_wu_error_f=wu_err, n=n)

    # ── Confidence gate check ─────────────────────────────────────────────────
    medium_stats = by_confidence.get("medium", {})
    if medium_stats.get("n", 0) >= MIN_SAMPLES_GENERAL:
        wr = medium_stats.get("win_rate")
        if wr is not None and wr > 0.85:
            _flag("info", "confidence_gate",
                  f"Medium-confidence win rate {wr:.1%} — no_require_high_confidence may be too strict",
                  win_rate=wr, n=medium_stats["n"])

    return sorted(issues, key=lambda x: {"critical": 0, "warning": 1, "info": 2}.get(x["severity"], 3))


# ─── Main entry point ─────────────────────────────────────────────────────────

def run_daily_optimizer(data: dict) -> dict:
    """
    Analyze all resolved outcomes and return a structured report.
    Called by the /api/learn endpoint and daily cron in app.py.
    Results saved to Postgres under key 'daily_report'.
    """
    all_resolved = [o for o in data.get("opportunities", [])
                    if o.get("outcome") is not None]

    # Use last LOOKBACK_DAYS for analysis to avoid stale data dominating
    cutoff = (date.today() - timedelta(days=LOOKBACK_DAYS)).isoformat()
    recent = [o for o in all_resolved if (o.get("date") or "") >= cutoff]

    overall        = _compute_stats(recent)
    by_type        = {
        "yes": _compute_stats([o for o in recent if o["type"] == "yes"]),
        "no":  _compute_stats([o for o in recent if o["type"] == "no"]),
    }
    by_city_no     = _compute_city_stats([o for o in recent if o["type"] == "no"])
    by_confidence  = {
        "high":   _compute_stats([o for o in recent if o.get("confidence") == "high"]),
        "medium": _compute_stats([o for o in recent if o.get("confidence") == "medium"]),
    }
    by_distance    = _compute_distance_buckets([o for o in recent if o["type"] == "no"])
    by_price       = _compute_yes_price_buckets([o for o in recent if o["type"] == "yes"])
    trend          = _compute_rolling_trend(all_resolved, days=LOOKBACK_DAYS)

    issues = _identify_issues(overall, by_type, by_city_no, by_confidence, by_distance, by_price)

    report = {
        "generated_at":  datetime.utcnow().isoformat(),
        "lookback_days": LOOKBACK_DAYS,
        "n_total":       len(all_resolved),
        "n_recent":      len(recent),
        "overall":       overall,
        "by_type":       by_type,
        "by_city_no":    by_city_no,
        "by_confidence": by_confidence,
        "by_distance":   by_distance,
        "by_price":      by_price,
        "trend":         trend,
        "issues":        issues,
        "issue_count":   len(issues),
        "critical_count": sum(1 for i in issues if i["severity"] == "critical"),
    }

    print(f"[optimizer] {len(recent)} recent resolved | "
          f"overall win_rate={overall.get('win_rate')} | "
          f"{len(issues)} issues ({report['critical_count']} critical)")
    for issue in issues:
        print(f"  [{issue['severity'].upper()}] {issue['message']}")

    return report


if __name__ == "__main__":
    # Quick local test — loads from tracker and prints report
    import sys, os
    sys.path.insert(0, os.path.dirname(__file__))
    from tracker import _load
    data = _load()
    report = run_daily_optimizer(data)
    print(json.dumps(report, indent=2))

#!/usr/bin/env python3
"""
Manual market resolution tool.

Lists all unresolved markets with their Weather Underground history URLs,
then lets you enter the actual high temperature to resolve each one.

Usage:
  python scripts/resolve_manual.py            # interactive mode
  python scripts/resolve_manual.py --auto     # attempt auto-resolve via WU + Gamma first
  python scripts/resolve_manual.py --show     # just show pending markets + WU URLs

WU History URL format:
  https://www.wunderground.com/history/daily/{path}/date/{YYYY-M-D}
  e.g. https://www.wunderground.com/history/daily/us/ga/atlanta/KATL/date/2026-3-4
  Look at the Summary table → High Temp → Actual column.
"""

import sys
import os
import argparse
import json
from datetime import datetime, timedelta

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from tracker import _load, _save, _tracker_lock, _infer_outcome_from_actual_temp, PAPER_SIZE_USD
from config import CITIES


def _wu_history_url(city: str, date_str: str) -> str:
    """Build a direct WU history URL for the given city and date."""
    cfg = CITIES.get(city, {})
    wu_path = cfg.get("wu_path", "")
    if "/hourly/" in wu_path:
        path = wu_path.replace("/hourly/", "/history/daily/", 1).rstrip("/")
    else:
        return f"(no WU path configured for {city})"
    parts = date_str.split("-")
    if len(parts) == 3:
        slug = f"{parts[0]}-{int(parts[1])}-{int(parts[2])}"
    else:
        slug = date_str
    return f"https://www.wunderground.com{path}/date/{slug}"


def _forecast_day(opp: dict) -> str:
    """Return the actual observation day (resolution_date - 1 for overnight-resolve markets)."""
    res = opp.get("resolution_date") or ""
    day = opp.get("date") or ""
    if day and res and day < res:
        return day
    if res:
        try:
            r = datetime.fromisoformat(res[:10]).date()
            return (r - timedelta(days=1)).isoformat()
        except Exception:
            pass
    return day or res


def show_pending(data: dict) -> list:
    pending = [o for o in data["opportunities"] if o["outcome"] is None]
    if not pending:
        print("No pending markets — all resolved.")
        return []

    print(f"\n{'='*80}")
    print(f"  PENDING MARKETS ({len(pending)} total)")
    print(f"{'='*80}")
    for i, opp in enumerate(pending):
        obs_day = _forecast_day(opp)
        url = _wu_history_url(opp["city"], obs_day)
        unit = opp.get("temp_unit", "F")
        print(f"\n  [{i+1}] {opp['city']} — {opp['bracket']}")
        print(f"       Event:     {opp['event_slug']}")
        print(f"       Obs date:  {obs_day}  (market resolves: {opp.get('resolution_date','?')})")
        print(f"       Forecast:  {opp.get('forecast_temp','?')}°{unit}   entry={opp.get('entry_price','?')}")
        print(f"       WU URL:    {url}")
        print(f"       → Look at: Summary table > High Temp > Actual")
    print()
    return pending


def auto_resolve(data: dict) -> int:
    """Attempt to auto-resolve pending markets via WU scraper + Gamma API."""
    from tracker import resolve_outcomes
    count = resolve_outcomes()
    return count


def manual_resolve(data: dict, pending: list) -> int:
    resolved = 0
    print("Enter the actual HIGH temperature from WU for each market.")
    print("Press Enter to skip, 'q' to quit.\n")

    for opp in pending:
        obs_day = _forecast_day(opp)
        unit = opp.get("temp_unit", "F")
        url = _wu_history_url(opp["city"], obs_day)

        print(f"  {opp['city']} | {opp['bracket']} | {obs_day}")
        print(f"  WU URL: {url}")
        raw = input(f"  Actual high (°{unit}) [Enter=skip, q=quit]: ").strip()

        if raw.lower() == "q":
            break
        if not raw:
            continue

        try:
            actual = float(raw)
        except ValueError:
            print("  Invalid number, skipping.")
            continue

        outcome = _infer_outcome_from_actual_temp(opp, actual)
        if not outcome:
            print("  Could not infer outcome from bracket, skipping.")
            continue

        entry = opp["entry_price"]
        stake = opp.get("paper_size_usd") or round(PAPER_SIZE_USD * opp.get("cluster_size", 1), 2)
        pnl_pct = round((1.0 - entry) / entry * 100, 2) if outcome == "win" else -100.0

        opp["outcome"] = outcome
        opp["actual_temp"] = actual
        opp["final_yes_price"] = 0.0 if outcome == "win" else 1.0
        opp["pnl_pct"] = pnl_pct
        opp["paper_size_usd"] = stake
        # Equal-shares paper P&L: payout = shares×$1 when any bracket wins
        if outcome == "win" and opp.get("shares") and opp["shares"] > 0:
            opp["paper_pnl_usd"] = round(opp["shares"] - stake, 2)
        else:
            opp["paper_pnl_usd"] = round(stake * (pnl_pct / 100.0), 2)

        wu_pred = (opp.get("forecast_sources") or {}).get("wunderground")
        if wu_pred is not None:
            opp["wu_error"] = round(abs(wu_pred - actual), 1)

        print(f"  → {outcome.upper()} (actual {actual}°{unit} {'in' if outcome=='win' else 'outside'} bracket)")
        resolved += 1

    return resolved


def main():
    parser = argparse.ArgumentParser(description="Manual market resolution tool")
    parser.add_argument("--auto", action="store_true", help="Auto-resolve via WU + Gamma first")
    parser.add_argument("--show", action="store_true", help="Show pending markets and WU URLs then exit")
    args = parser.parse_args()

    with _tracker_lock:
        data = _load()

    pending = show_pending(data)
    if not pending:
        return

    if args.show:
        return

    if args.auto:
        print("Attempting auto-resolution via WU + Gamma API...")
        count = auto_resolve(data)
        print(f"Auto-resolved: {count}")
        with _tracker_lock:
            data = _load()
        pending = [o for o in data["opportunities"] if o["outcome"] is None]
        if not pending:
            print("All markets resolved automatically.")
            return
        print(f"{len(pending)} still pending — switching to manual entry.\n")

    resolved = manual_resolve(data, pending)

    if resolved:
        data["last_resolved"] = datetime.utcnow().isoformat()
        with _tracker_lock:
            _save(data)
        print(f"\nSaved {resolved} new outcomes.")

    # Final summary
    all_opps = data["opportunities"]
    still_pending = [o for o in all_opps if o["outcome"] is None]
    wins   = [o for o in all_opps if o["outcome"] == "win"]
    losses = [o for o in all_opps if o["outcome"] == "loss"]
    total_pnl = sum(o.get("paper_pnl_usd", 0) or 0 for o in all_opps if o["outcome"])

    print(f"\n{'─'*50}")
    print(f"  Resolved: {len(wins)+len(losses)}  |  Wins: {len(wins)}  |  Losses: {len(losses)}")
    print(f"  Paper P&L: ${total_pnl:+.2f}")
    if still_pending:
        print(f"  Still pending: {len(still_pending)}")
    print(f"{'─'*50}")


if __name__ == "__main__":
    main()

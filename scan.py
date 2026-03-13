"""
Main scanner: fetches markets + forecasts, finds arbitrage opportunities,
prints ranked list and saves results to data/.

Usage:
  python3 scan.py                        # scan all cities, TOMORROW only
  python3 scan.py --cities NYC Chicago   # specific cities
  python3 scan.py --days 3              # look further ahead (default: tomorrow only)
  python3 scan.py --capital 400         # capital budget (default: $400)
  python3 scan.py --no-clob             # skip CLOB price enrichment (faster)
"""

import argparse
import json
import os
from datetime import datetime, date, timedelta

from config import CITIES, STRATEGY
from fetch_markets import fetch_all_markets, enrich_with_clob_prices
from fetch_forecasts import fetch_all_forecasts
from analyzer import analyze_all, print_yes_clusters, print_no_opps


# Use same DATA_DIR as app (env on Railway with volume at /data)
DATA_DIR = os.environ.get("DATA_DIR", os.path.join(os.path.dirname(__file__), "data"))
os.makedirs(DATA_DIR, exist_ok=True)


def build_forecast_rows(forecasts: dict, target_date: str) -> list:
    """Flatten forecast data for the dashboard's forecast table."""
    rows = []
    for city, fc_data in forecasts.items():
        station = fc_data.get("station", "")
        unit = fc_data.get("unit", "F")
        day = fc_data.get("forecasts", {}).get(target_date)
        if not day:
            continue
        rows.append({
            "city":         city,
            "station":      station,
            "date":         target_date,
            "unit":         unit,
            "wunderground": day.get("wunderground"),
            "wu_peak_hour": day.get("wu_peak_hour"),
            "wu_hours":     day.get("wu_hours", []),
            "nws":          day.get("nws"),
            "open_meteo":   day.get("open_meteo") or day.get("wttr"),
            "consensus":    day.get("consensus"),
            "confidence":   day.get("confidence"),
        })
    rows.sort(key=lambda r: r["city"])
    return rows


def save_results(clusters, no_opps, markets, forecasts, scan_time: str, target_date: str = ""):
    """Save scan results to data/ directory."""
    def _serialize_cluster(c) -> dict:
        return {
            "type": "YES_CLUSTER",
            "city": c.city,
            "date": c.date,
            "resolution_date": getattr(c, "resolution_date", "") or c.date,
            "resolution_time": getattr(c, "resolution_time", ""),
            "station": c.station,
            "event_slug": c.event_slug,
            "temp_unit": c.temp_unit,
            "brackets": [
                {
                    "market_id": b.market_id,
                    "group_title": b.group_title,
                    "yes_price": b.yes_price,
                    "amount_usd": round(getattr(b, "amount_usd", 0), 2),
                    "yes_token_id": b.yes_token_id,
                    "is_forecast_bracket": b.is_forecast_bracket,
                    "liquidity": b.liquidity,
                    "market_slug": b.market_slug,
                    "market_url": (
                        f"https://polymarket.com/event/{c.event_slug}/{b.market_slug}"
                        if b.market_slug else ""
                    ),
                }
                for b in c.brackets
            ],
            "cluster_size": c.cluster_size,
            "shares": round(getattr(c, "shares", 0), 2),
            "total_price": c.total_price,
            "return_pct": round(c.return_pct, 2),
            "win_lo": c.win_lo if c.win_lo != float("-inf") else None,
            "win_hi": c.win_hi,
            "forecast_temp": c.forecast_temp,
            "forecast_confidence": c.forecast_confidence,
            "size_each": c.size_each,
            "total_cost": c.total_cost,
            "liquidity_min": c.liquidity_min,
            "ev_score": round(getattr(c, "ev_score", 0.0), 1),
            "predicted_win_prob": round(getattr(c, "predicted_win_prob", 0.75), 4),
            "polymarket_url": f"https://polymarket.com/event/{c.event_slug}",
        }

    clusters_data = []
    for c in clusters:
        row = _serialize_cluster(c)
        alt = getattr(c, "alt", None)
        if alt is not None:
            row["alt"] = _serialize_cluster(alt)
        clusters_data.append(row)

    no_opps_data = []
    for o in no_opps:
        market_slug = getattr(o, "market_slug", "")
        no_opps_data.append({
            "type": "NO",
            "city": o.city,
            "date": o.date,
            "resolution_date": getattr(o, "resolution_date", "") or o.date,
            "station": o.station,
            "event_slug": o.event_slug,
            "market_id": o.market_id,
            "market_slug": market_slug,
            "bracket": o.group_title,
            "bracket_lo": o.bracket_lo,
            "bracket_hi": o.bracket_hi,
            "no_price": o.no_price,
            "yes_price": o.yes_price,
            "return_pct": round(o.return_pct, 2),
            "ev_score": round(getattr(o, "ev_score", 0.0), 1),
            "predicted_win_prob": round(getattr(o, "predicted_win_prob", 0.75), 4),
            "effective_return_pct": round(getattr(o, "effective_return_pct", 0.0), 1),
            "spread_pct": round(getattr(o, "spread_pct", 0.0), 1),
            "distance": o.distance_f,
            "temp_unit": o.temp_unit,
            "forecast_temp": o.forecast_temp,
            "forecast_confidence": o.forecast_confidence,
            "liquidity": o.liquidity,
            "recommended_size": o.recommended_size,
            "yes_token_id": o.yes_token_id,
            "no_token_id": o.no_token_id,
            "polymarket_url": (
                f"https://polymarket.com/event/{o.event_slug}/{market_slug}"
                if market_slug else f"https://polymarket.com/event/{o.event_slug}"
            ),
        })

    total_deploy = (
        sum(c["total_cost"] for c in clusters_data) +
        sum(o["recommended_size"] for o in no_opps_data)
    )

    # Forecast rows for dashboard — include all dates present in results
    if not target_date:
        target_date = (date.today() + timedelta(days=1)).isoformat()
    dates_in_results = sorted({r["date"] for r in clusters_data + no_opps_data} or [target_date])
    forecast_rows = []
    for d in dates_in_results:
        forecast_rows.extend(build_forecast_rows(forecasts, d))

    out = {
        "scan_time": scan_time,
        "yes_clusters": clusters_data,
        "no_opportunities": no_opps_data,
        "forecasts": forecast_rows,
        "summary": {
            "yes_clusters": len(clusters_data),
            "no_bets": len(no_opps_data),
            "total_opportunities": len(clusters_data) + len(no_opps_data),
            "estimated_deploy_usd": round(total_deploy, 2),
        },
    }

    date_str = datetime.now().strftime("%Y%m%d_%H%M")
    out_path = os.path.join(DATA_DIR, f"scan_{date_str}.json")
    latest_path = os.path.join(DATA_DIR, "latest_scan.json")

    with open(out_path, "w") as f:
        json.dump(out, f, indent=2)
    with open(latest_path, "w") as f:
        json.dump(out, f, indent=2)

    # Persist each date's results separately in Postgres so they survive redeployments.
    # Always write (even when 0 opportunities) so the dashboard has a "latest" to load.
    _pg_save_scan_dates(out, dates_in_results)
    if not clusters_data and not no_opps_data:
        print("  [WARN] No opportunities found — saved empty scan so dashboard can load")

    return out_path


def _pg_save_scan_dates(out: dict, dates: list):
    """Store scan results per-date in Postgres kv_store under key 'scan_YYYY-MM-DD'."""
    database_url = os.environ.get("DATABASE_URL", "")
    if not database_url:
        return
    try:
        import psycopg2
        conn = psycopg2.connect(database_url)
        try:
            with conn.cursor() as cur:
                cur.execute("""
                    CREATE TABLE IF NOT EXISTS kv_store (
                        key TEXT PRIMARY KEY,
                        data JSONB NOT NULL,
                        updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
                    )
                """)
                for d in dates:
                    date_out = {
                        **out,
                        "yes_clusters":    [c for c in out["yes_clusters"]    if c.get("date") == d],
                        "no_opportunities":[o for o in out["no_opportunities"] if o.get("date") == d],
                        "forecasts":       [f for f in out["forecasts"]        if f.get("date") == d],
                    }
                    cur.execute("""
                        INSERT INTO kv_store (key, data, updated_at)
                        VALUES (%s, %s, NOW())
                        ON CONFLICT (key) DO UPDATE
                            SET data = EXCLUDED.data, updated_at = NOW()
                    """, (f"scan_{d}", json.dumps(date_out)))
            conn.commit()
            print(f"  Scan results saved to Postgres for dates: {dates}")
        finally:
            conn.close()
    except Exception as e:
        print(f"[WARN] Postgres scan save failed: {e}")


def _print_bracket_proximity(forecasts: dict, markets: dict, target_date: str, cities: list):
    """
    For each city on target_date, print the WU hourly forecast alongside every
    open bracket and how far the forecast sits from each edge.
    This shows at a glance which NO bets and YES clusters are closest.
    """
    print(f"── Bracket proximity for {target_date} ──")
    any_printed = False

    for city in cities:
        fc_data  = forecasts.get(city, {})
        unit     = fc_data.get("unit", "F")
        day      = fc_data.get("forecasts", {}).get(target_date)
        if not day:
            continue

        consensus  = day.get("consensus")
        wu_max     = day.get("wunderground")
        peak_hr    = day.get("wu_peak_hour", "")
        confidence = day.get("confidence", "?")
        if consensus is None:
            continue

        city_events = markets.get(city, [])
        target_event = next((e for e in city_events if e.get("date") == target_date), None)
        if not target_event:
            continue

        wu_str = f"WU={wu_max:.1f}°{unit}@{peak_hr}" if wu_max else ""
        print(f"\n  {city} [{unit}]  forecast={consensus:.1f}°{unit}  {wu_str}  conf={confidence}")

        brackets = target_event.get("markets", [])
        # Sort brackets by midpoint
        def _mid(b):
            lo, hi = b.get("bracket_lo"), b.get("bracket_hi")
            if lo is not None and hi is not None: return (lo + hi) / 2
            if lo is not None: return lo + 5
            return (b.get("bracket_hi") or 0) - 5

        for b in sorted(brackets, key=_mid):
            lo  = b.get("bracket_lo")
            hi  = b.get("bracket_hi")
            yp  = b.get("yes_price") or b.get("price_yes")
            np_ = b.get("no_price")  or b.get("price_no")

            if lo is not None and hi is not None:
                label = f"{int(lo)}-{int(hi)}°{unit}"
                # Distance from forecast to nearest edge
                if consensus < lo:
                    dist = lo - consensus
                    side = f"↑{dist:.1f}° to low edge"
                elif consensus > hi:
                    dist = consensus - hi
                    side = f"↓{dist:.1f}° past high edge"
                else:
                    dist_lo = consensus - lo
                    dist_hi = hi - consensus
                    side = f"INSIDE (lo+{dist_lo:.1f} hi-{dist_hi:.1f})"
            elif hi is not None:
                label = f"≤{int(hi)}°{unit}"
                dist = hi - consensus
                side = f"{'INSIDE' if consensus <= hi else f'{consensus-hi:.1f}° above'}"
            else:
                label = f"≥{int(lo)}°{unit}"
                side = f"{'INSIDE' if consensus >= lo else f'{lo-consensus:.1f}° below'}"

            yp_s  = f"YES={yp:.2f}" if yp  else "YES=?"
            np_s  = f"NO={np_:.2f}" if np_ else "NO=?"
            print(f"    {label:<14} {yp_s}  {np_s}  {side}")

        any_printed = True

    if not any_printed:
        print(f"  No open markets found for {target_date}")
    print()


def print_forecast_summary(forecasts: dict, cities: list, target_date: str):
    """Print forecast table for tomorrow across all cities."""
    print(f"\n{'='*90}")
    print(f"{'WEATHER FORECASTS — ' + target_date:^90}")
    print(f"{'='*90}")
    print(f"{'City':<16} {'Station':<8} {'WU':>8} {'NWS':>8} {'OM':>8} {'Consensus':>10} {'Conf':>6}")
    print("-" * 90)

    for city in cities:
        fc_data = forecasts.get(city, {})
        fc = fc_data.get("forecasts", {})
        station = fc_data.get("station", "?")
        unit = fc_data.get("unit", "F")
        day = fc.get(target_date)
        if day:
            wu_s   = f"{day['wunderground']:.1f}°{unit}" if day.get("wunderground") is not None else "--"
            nws_s  = f"{day['nws']:.1f}°{unit}"          if day.get("nws")          is not None else "--"
            om_s = f"{day['open_meteo']:.1f}°{unit}" if day.get("open_meteo") is not None else (f"{day['wttr']:.1f}°{unit}" if day.get("wttr") is not None else "--")
            conf_icon = "✓✓ HIGH" if day["confidence"] == "high" else ("✓ MED" if day["confidence"] == "medium" else "? LOW")
            print(f"{city:<16} {station:<8} {wu_s:>8} {nws_s:>8} {om_s:>8} {day['consensus']:>7.1f}°{unit} {conf_icon:>6}")
        else:
            print(f"{city:<16} {station:<8} {'--':>8} {'--':>8} {'--':>8} {'no data':>10}")

    print(f"{'='*90}\n")


def print_links(clusters, no_opps, limit=10):
    """Print Polymarket links for top opportunities."""
    print("\n── Polymarket links ──")
    seen = set()
    count = 0
    for c in clusters[:limit]:
        if c.event_slug not in seen:
            print(f"  YES {c.city} {c.date}: https://polymarket.com/event/{c.event_slug}")
            seen.add(c.event_slug)
            count += 1
    for o in no_opps[:limit - count]:
        if o.event_slug not in seen:
            print(f"  NO  {o.city} {o.date}: https://polymarket.com/event/{o.event_slug}")
            seen.add(o.event_slug)


def main():
    parser = argparse.ArgumentParser(description="Polymarket weather arbitrage scanner")
    parser.add_argument("--cities", nargs="+", choices=list(CITIES.keys()), help="Cities to scan")
    parser.add_argument("--days", type=int, default=1,
                        help="Days ahead to scan (default: 1 = tomorrow only)")
    parser.add_argument("--date", type=str, default=None,
                        help="Target resolution date to focus on: YYYY-MM-DD, 'today', or 'tomorrow'")
    parser.add_argument("--capital", type=float, default=STRATEGY["max_capital"],
                        help="Capital budget in USDC")
    parser.add_argument("--no-clob", action="store_true",
                        help="Skip CLOB live price enrichment (faster, less accurate)")
    parser.add_argument("--limit", type=int, default=20,
                        help="Max opportunities to display per section")
    parser.add_argument("--min-return", type=float, default=STRATEGY["min_return_pct"],
                        help="Minimum return %% to show (default: 8)")
    args = parser.parse_args()

    cities = args.cities or list(CITIES.keys())
    scan_time = datetime.now().isoformat()

    today    = date.today()
    yesterday = today - timedelta(days=1)
    tomorrow = today + timedelta(days=1)

    # Scan window: yesterday (resolving now) + today + tomorrow + up to args.days ahead.
    # The real NO-bet edge lives at 24-72h out — forecasts are genuine predictions there,
    # not observations. Scanning further ahead exposes more brackets ≥6°F from the forecast.
    # Default --days 3 = yesterday + today + tomorrow + day-after-tomorrow (3 future days).
    if args.date in (None, "both"):
        future_days = args.days  # how many future dates beyond today to include
        scan_dates = [yesterday.isoformat()] + [
            (today + timedelta(days=i)).isoformat() for i in range(future_days + 1)
        ]
        date_label = f"yesterday + {future_days + 1} forward days (today → {(today + timedelta(days=future_days)).isoformat()})"
        fetch_days = future_days + 3   # extra buffer for forecast API
    elif args.date == "today":
        scan_dates = [today.isoformat()]
        date_label = "today"
        fetch_days = 2
    elif args.date == "tomorrow":
        scan_dates = [tomorrow.isoformat()]
        date_label = "tomorrow"
        fetch_days = 3
    else:
        target_dt  = date.fromisoformat(args.date)
        scan_dates = [args.date]
        date_label = args.date
        fetch_days = max((target_dt - today).days, 1) + 2

    print(f"\nPolymarket Weather Arbitrage Scanner")
    print(f"Scan time:   {scan_time}")
    print(f"Scanning:    {date_label}  ({', '.join(scan_dates)})")
    print(f"Cities:      {', '.join(cities)}")
    print(f"Capital: ${args.capital} | Min return: {args.min_return}%\n")

    # 1. Fetch markets
    print("── Fetching markets ──")
    markets = fetch_all_markets(cities, days_ahead=fetch_days)

    if not args.no_clob:
        markets = enrich_with_clob_prices(markets)

    # 2. Fetch forecasts
    print("\n── Fetching forecasts ──")
    forecasts = fetch_all_forecasts(cities, days=fetch_days)

    # 3. Analyze — override min_return if specified
    original_min = STRATEGY["min_return_pct"]
    STRATEGY["min_return_pct"] = args.min_return

    print("── Analyzing opportunities ──")
    clusters, no_opps = analyze_all(markets, forecasts, max_capital=args.capital)

    STRATEGY["min_return_pct"] = original_min

    # Keep only the requested dates
    clusters = [c for c in clusters if c.date in scan_dates]
    no_opps  = [o for o in no_opps  if o.date in scan_dates]

    # 4. Print summary per date
    for d in scan_dates:
        d_clusters = [c for c in clusters if c.date == d]
        d_no       = [o for o in no_opps  if o.date == d]
        print_forecast_summary(forecasts, cities, d)
        print(f"  → {len(d_clusters)} YES clusters, {len(d_no)} NO bets for {d}\n")
        _print_bracket_proximity(forecasts, markets, d, cities)

    # 5. Print all results together
    print_yes_clusters(clusters, limit=args.limit)
    print_no_opps(no_opps, limit=args.limit)
    print_links(clusters, no_opps, limit=10)

    # 6. Save merged results
    target_date = scan_dates[0]   # used for forecast rows fallback
    save_results(clusters, no_opps, markets, forecasts, scan_time, target_date)

    # 8. Notifications
    try:
        from notify import notify_opportunities
        notified = notify_opportunities(clusters, no_opps, scan_time)
        if notified:
            print(f"  → Sent Slack alert for {notified} high-confidence opportunities")
    except Exception as e:
        print(f"  [WARN] Notifications skipped: {e}")

    # 9. Outcome tracking + recursive learning
    try:
        from tracker import record_scan, resolve_outcomes
        total_tracked = record_scan(clusters, no_opps, forecasts)
        resolved = resolve_outcomes()
        print(f"  → Tracker: {total_tracked} opportunities on record", end="")
        if resolved:
            print(f", resolved {resolved} new outcomes")
        else:
            print()
    except Exception as e:
        import traceback
        print(f"  [WARN] Tracker skipped: {e}")
        traceback.print_exc()

    try:
        from learner import learn_from_outcomes
        result = learn_from_outcomes()
        if result["learned"]:
            print(f"  → Learner: processed {result['learned']} resolved outcomes "
                  f"({result['temps_fetched']} actual temps fetched)")
            if result["weights_updated"]:
                w = result["current_weights"]
                print(f"    Source weights F: wu={w.get('F',{}).get('wunderground',0):.2f} "
                      f"nws={w.get('F',{}).get('nws',0):.2f} "
                      f"om={w.get('F',{}).get('open_meteo',0):.2f}")
            if result["calib_updated"]:
                print(f"    Sigma calibration updated")
    except Exception as e:
        print(f"  [WARN] Learner skipped: {e}")


if __name__ == "__main__":
    main()

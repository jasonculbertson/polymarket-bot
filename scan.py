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


DATA_DIR = os.path.join(os.path.dirname(__file__), "data")
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
            "wttr":         day.get("wttr"),
            "consensus":    day.get("consensus"),
            "confidence":   day.get("confidence"),
        })
    rows.sort(key=lambda r: r["city"])
    return rows


def save_results(clusters, no_opps, markets, forecasts, scan_time: str):
    """Save scan results to data/ directory."""
    clusters_data = []
    for c in clusters:
        clusters_data.append({
            "type": "YES_CLUSTER",
            "city": c.city,
            "date": c.date,
            "station": c.station,
            "event_slug": c.event_slug,
            "temp_unit": c.temp_unit,
            "brackets": [
                {
                    "market_id": b.market_id,
                    "group_title": b.group_title,
                    "yes_price": b.yes_price,
                    "yes_token_id": b.yes_token_id,
                    "is_forecast_bracket": b.is_forecast_bracket,
                    "liquidity": b.liquidity,
                }
                for b in c.brackets
            ],
            "cluster_size": c.cluster_size,
            "total_price": c.total_price,
            "return_pct": round(c.return_pct, 2),
            "win_lo": c.win_lo if c.win_lo != float("-inf") else None,
            "win_hi": c.win_hi,
            "forecast_temp": c.forecast_temp,
            "forecast_confidence": c.forecast_confidence,
            "size_each": c.size_each,
            "total_cost": c.total_cost,
            "liquidity_min": c.liquidity_min,
            "polymarket_url": f"https://polymarket.com/event/{c.event_slug}",
        })

    no_opps_data = []
    for o in no_opps:
        no_opps_data.append({
            "type": "NO",
            "city": o.city,
            "date": o.date,
            "station": o.station,
            "event_slug": o.event_slug,
            "market_id": o.market_id,
            "bracket": o.group_title,
            "bracket_lo": o.bracket_lo,
            "bracket_hi": o.bracket_hi,
            "no_price": o.no_price,
            "yes_price": o.yes_price,
            "return_pct": round(o.return_pct, 2),
            "distance": o.distance_f,
            "temp_unit": o.temp_unit,
            "forecast_temp": o.forecast_temp,
            "forecast_confidence": o.forecast_confidence,
            "liquidity": o.liquidity,
            "recommended_size": o.recommended_size,
            "yes_token_id": o.yes_token_id,
            "no_token_id": o.no_token_id,
            "polymarket_url": f"https://polymarket.com/event/{o.event_slug}",
        })

    total_deploy = (
        sum(c["total_cost"] for c in clusters_data) +
        sum(o["recommended_size"] for o in no_opps_data)
    )

    # Forecast rows for dashboard
    today = date.today()
    tomorrow = (today + timedelta(days=1)).isoformat()
    forecast_rows = build_forecast_rows(forecasts, tomorrow)

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

    print(f"\nResults saved to: {out_path}")
    return out_path


def print_forecast_summary(forecasts: dict, cities: list, target_date: str):
    """Print forecast table for tomorrow across all cities."""
    print(f"\n{'='*90}")
    print(f"{'WEATHER FORECASTS — ' + target_date:^90}")
    print(f"{'='*90}")
    print(f"{'City':<16} {'Station':<8} {'WU':>8} {'NWS':>8} {'wttr':>8} {'Consensus':>10} {'Conf':>6}")
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
            wttr_s = f"{day['wttr']:.1f}°{unit}"         if day.get("wttr")         is not None else "--"
            conf_icon = "✓✓ HIGH" if day["confidence"] == "high" else ("✓ MED" if day["confidence"] == "medium" else "? LOW")
            print(f"{city:<16} {station:<8} {wu_s:>8} {nws_s:>8} {wttr_s:>8} {day['consensus']:>7.1f}°{unit} {conf_icon:>6}")
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

    # Target date: tomorrow (or further if --days specified)
    today = date.today()
    tomorrow = (today + timedelta(days=1)).isoformat()
    target_date = tomorrow  # always highlight tomorrow

    print(f"\nPolymarket Weather Arbitrage Scanner")
    print(f"Scan time:   {scan_time}")
    print(f"Target date: {target_date} (tomorrow)")
    print(f"Cities:      {', '.join(cities)}")
    print(f"Days ahead:  {args.days} | Capital: ${args.capital} | Min return: {args.min_return}%\n")

    # 1. Fetch markets (fetch days+1 to ensure tomorrow is included)
    fetch_days = max(args.days + 1, 2)
    print("── Fetching markets ──")
    markets = fetch_all_markets(cities, days_ahead=fetch_days)

    if not args.no_clob:
        markets = enrich_with_clob_prices(markets)

    # 2. Fetch forecasts (always fetch 2 days minimum)
    print("\n── Fetching forecasts ──")
    forecasts = fetch_all_forecasts(cities, days=fetch_days)

    # 3. Print forecast summary for tomorrow
    print_forecast_summary(forecasts, cities, target_date)

    # 4. Analyze — override min_return if specified
    original_min = STRATEGY["min_return_pct"]
    STRATEGY["min_return_pct"] = args.min_return

    print("── Analyzing opportunities ──")
    clusters, no_opps = analyze_all(markets, forecasts, max_capital=args.capital)

    STRATEGY["min_return_pct"] = original_min

    print(f"Found {len(clusters)} YES clusters, {len(no_opps)} NO bets\n")

    # 5. Print results
    print_yes_clusters(clusters, limit=args.limit)
    print_no_opps(no_opps, limit=args.limit)

    # 6. Links
    print_links(clusters, no_opps, limit=10)

    # 7. Save
    save_results(clusters, no_opps, markets, forecasts, scan_time)

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
        print(f"  [WARN] Tracker skipped: {e}")

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
                      f"wttr={w.get('F',{}).get('wttr',0):.2f}")
            if result["calib_updated"]:
                print(f"    Sigma calibration updated")
    except Exception as e:
        print(f"  [WARN] Learner skipped: {e}")


if __name__ == "__main__":
    main()

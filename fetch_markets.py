"""
Fetches open weather markets from Polymarket for all configured cities.

For each city, retrieves:
  - Open events (next 7 days)
  - Per-bracket: question, bracket range, YES/NO prices, token IDs, liquidity
  - Wunderground station parsed from resolutionSource (authoritative for resolution)

Bracket values are kept in their NATIVE unit (°F for US cities, °C for international).
The city's "unit" field in config.py determines which unit is used.
"""

from __future__ import annotations

import re
import json
import requests
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Optional
from config import CITIES, GAMMA_API, CLOB_API


def parse_bracket(question: str, unit: str = "F") -> tuple:
    """
    Extract (low, high) temperature bounds from a market question string.

    For unit="F": returns °F values (native for US markets)
    For unit="C": returns °C values (native for international markets, NO conversion)

    Returns (lo, hi) where None means open-ended.
    """
    if unit == "F":
        m = re.search(r"be (\d+)°F or below", question)
        if m:
            return (None, float(m.group(1)))

        m = re.search(r"between (\d+)-(\d+)°F", question)
        if m:
            return (float(m.group(1)), float(m.group(2)))

        m = re.search(r"be (\d+)°F or (?:higher|above)", question)
        if m:
            return (float(m.group(1)), None)

        return (None, None)

    else:  # unit == "C"
        # "between X-Y°C"
        m = re.search(r"between (-?\d+)-(-?\d+)°C", question)
        if m:
            return (float(m.group(1)), float(m.group(2)))

        # "X°C or below / lower"
        m = re.search(r"be (-?\d+)°C or (?:below|lower)", question)
        if m:
            return (None, float(m.group(1)))

        # "X°C or higher / above"
        m = re.search(r"be (-?\d+)°C or (?:higher|above)", question)
        if m:
            return (float(m.group(1)), None)

        # single "be -3°C" → integer bracket [N, N+1)
        m = re.search(r"be (-?\d+)°C\b", question)
        if m:
            c = float(m.group(1))
            return (c, c + 1.0)

        # group_title style: "12-13°C"
        m = re.search(r"^(-?\d+)-(-?\d+)°C", question)
        if m:
            return (float(m.group(1)), float(m.group(2)))

        # group_title style: "12°C"
        m = re.search(r"^(-?\d+)°C", question)
        if m:
            c = float(m.group(1))
            return (c, c + 1.0)

        return (None, None)


def parse_station(resolution_source: str) -> Optional[str]:
    """Extract ICAO station code from a Wunderground URL."""
    if not resolution_source:
        return None
    return resolution_source.rstrip("/").split("/")[-1] or None


def fetch_clob_price(token_id: str) -> Optional[dict]:
    """
    Get current best bid/ask from the CLOB orderbook for a YES token.
    Returns {'best_bid': float, 'best_ask': float} or None on failure.
    """
    try:
        r = requests.get(f"{CLOB_API}/book", params={"token_id": token_id}, timeout=5)
        if r.status_code != 200:
            return None
        data = r.json()
        bids = data.get("bids", [])
        asks = data.get("asks", [])
        best_bid = float(bids[0]["price"]) if bids else None
        best_ask = float(asks[0]["price"]) if asks else None
        return {"best_bid": best_bid, "best_ask": best_ask}
    except Exception:
        return None


def fetch_city_markets(city_name: str, days_ahead: int = 7) -> list:
    """
    Fetch all open weather events for a city and return structured market data.
    Bracket values are in the city's native unit (°F or °C).
    """
    cfg = CITIES[city_name]
    series_id = cfg["series_id"]
    city_unit = cfg.get("unit", "F")

    try:
        r = requests.get(
            f"{GAMMA_API}/events",
            params={
                "series_id": series_id,
                "closed": "false",
                "limit": days_ahead,
            },
            timeout=10,
        )
        r.raise_for_status()
        events = r.json()
    except Exception as e:
        print(f"  [ERROR] {city_name}: failed to fetch events: {e}")
        return []

    results = []
    for event in events:
        markets_raw = event.get("markets", [])
        if not markets_raw:
            continue

        station = parse_station(markets_raw[0].get("resolutionSource", "")) or cfg["station"]

        date_str = markets_raw[0].get("endDateIso") or ""
        if not date_str:
            slug = event.get("slug", "")
            m = re.search(r"(\d{4}-\d{2}-\d{2})", slug)
            date_str = m.group(1) if m else ""

        markets_out = []
        for m_raw in markets_raw:
            question = m_raw.get("question", "")
            group_title = m_raw.get("groupItemTitle", "")

            lo, hi = parse_bracket(question, unit=city_unit)

            # Fallback: parse from groupItemTitle
            if lo is None and hi is None and group_title:
                lo, hi = parse_bracket(group_title, unit=city_unit)
                if lo is None and hi is None:
                    lo, hi = parse_bracket(f"be {group_title} on date", unit=city_unit)

            prices_raw = m_raw.get("outcomePrices", "[]")
            if isinstance(prices_raw, str):
                prices_raw = json.loads(prices_raw)
            try:
                yes_price = float(prices_raw[0]) if prices_raw else 0.0
                no_price = float(prices_raw[1]) if len(prices_raw) > 1 else 0.0
            except (ValueError, IndexError):
                yes_price, no_price = 0.0, 0.0

            token_ids_raw = m_raw.get("clobTokenIds", "[]")
            if isinstance(token_ids_raw, str):
                token_ids_raw = json.loads(token_ids_raw)
            yes_token = token_ids_raw[0] if len(token_ids_raw) > 0 else ""
            no_token = token_ids_raw[1] if len(token_ids_raw) > 1 else ""

            markets_out.append(
                {
                    "market_id": str(m_raw.get("id", "")),
                    "question": question,
                    "group_title": group_title,
                    "bracket_lo": lo,
                    "bracket_hi": hi,
                    "temp_unit": city_unit,
                    "yes_price": yes_price,
                    "no_price": no_price,
                    "yes_token_id": yes_token,
                    "no_token_id": no_token,
                    "liquidity": float(m_raw.get("liquidityNum", 0) or 0),
                    "volume_24hr": float(m_raw.get("volume24hr", 0) or 0),
                    "accepting_orders": m_raw.get("acceptingOrders", False),
                    "neg_risk": m_raw.get("negRisk", False),
                    "neg_risk_market_id": m_raw.get("negRiskMarketID", ""),
                }
            )

        results.append(
            {
                "city": city_name,
                "event_id": str(event.get("id", "")),
                "event_slug": event.get("slug", ""),
                "date": date_str,
                "station": station,
                "temp_unit": city_unit,
                "markets": markets_out,
            }
        )

    return results


def fetch_all_markets(cities=None, days_ahead: int = 7) -> dict:
    """Fetch markets for all (or specified) cities in parallel. Returns {city_name: [event, ...]}"""
    if cities is None:
        cities = list(CITIES.keys())

    all_markets = {}
    with ThreadPoolExecutor(max_workers=8) as pool:
        futures = {
            pool.submit(fetch_city_markets, city, days_ahead): city
            for city in cities
        }
        for future in as_completed(futures):
            city = futures[future]
            try:
                events = future.result()
                all_markets[city] = events
                print(f"  {city}: {len(events)} open events")
            except Exception as e:
                print(f"  [ERROR] {city} markets: {e}")
                all_markets[city] = []

    return all_markets


def enrich_with_clob_prices(all_markets: dict) -> dict:
    """Fetch live CLOB orderbook prices in parallel and overlay on gamma snapshot prices."""
    print("\nEnriching with CLOB live prices...")

    # Collect all tokens that need enrichment
    tasks = []
    for city, events in all_markets.items():
        for ei, event in enumerate(events):
            for mi, mkt in enumerate(event["markets"]):
                if mkt["accepting_orders"] and mkt["yes_token_id"]:
                    tasks.append((city, ei, mi, mkt["yes_token_id"]))

    if not tasks:
        return all_markets

    # Fetch all CLOB prices concurrently (20 parallel workers)
    clob_results: dict = {}
    with ThreadPoolExecutor(max_workers=20) as pool:
        futures = {
            pool.submit(fetch_clob_price, token_id): (city, ei, mi)
            for city, ei, mi, token_id in tasks
        }
        for future in as_completed(futures):
            key = futures[future]
            try:
                clob_results[key] = future.result()
            except Exception:
                clob_results[key] = None

    # Apply results back to market dicts
    count = 0
    for city, events in all_markets.items():
        for ei, event in enumerate(events):
            for mi, mkt in enumerate(event["markets"]):
                clob = clob_results.get((city, ei, mi))
                if clob:
                    mkt["clob_best_bid"] = clob["best_bid"]
                    mkt["clob_best_ask"] = clob["best_ask"]
                    if clob["best_ask"] is not None:
                        mkt["yes_price_live"] = clob["best_ask"]
                        mkt["no_price_live"] = round(1.0 - clob["best_ask"], 4)
                        count += 1

    print(f"  Enriched {count} markets with live CLOB prices")
    return all_markets


if __name__ == "__main__":
    print("Fetching all weather markets...\n")
    markets = fetch_all_markets()
    markets = enrich_with_clob_prices(markets)

    print("\n=== Market Summary ===")
    for city, events in markets.items():
        if not events:
            continue
        unit = CITIES[city].get("unit", "F")
        print(f"\n{city} ({len(events)} events, unit=°{unit}):")
        for event in events[:2]:
            print(f"  {event['date']} | station={event['station']}")
            for m in event["markets"]:
                lo, hi = m["bracket_lo"], m["bracket_hi"]
                if lo is not None and hi is not None:
                    bracket = f"{lo}-{hi}°{unit}"
                elif hi is not None:
                    bracket = f"≤{hi}°{unit}"
                elif lo is not None:
                    bracket = f"≥{lo}°{unit}"
                else:
                    bracket = "?"
                live_yes = m.get("yes_price_live", m["yes_price"])
                print(f"    {bracket:15s}  YES={live_yes:.3f}  NO={1-live_yes:.3f}  liq=${m['liquidity']:.0f}")

"""
Arbitrage analyzer: compares weather forecasts against Polymarket bracket prices.

Two strategies:

  1. NO — buy NO on brackets that are clearly far from forecast. You win if that bracket
     doesn't resolve. Low risk, 8-50% returns depending on how far from forecast.

  2. YES CLUSTER — buy YES on 2-3 adjacent brackets that together "surround" the forecast.
     Exactly one bracket always resolves YES, so you are guaranteed to win ONE of the cluster
     as long as the actual temp falls within the cluster range.

     Economics:
       Total cost  = sum(yes_price_i for bracket_i in cluster)
       Payout      = $1 × (shares of whichever bracket wins)
       Return %    = (1 - total_price) / total_price × 100

     Example — forecast 46°F, buy YES on 44-45, 46-47, 48-49 at 26¢ each:
       total_price = 0.78  →  return = 28%  (covers ±4°F window)

     Smaller cluster (2 brackets) → higher return, narrower safety window
     Larger cluster (3 brackets) → lower return, wider safety window

Confidence:
  - "high" (NWS + Open-Meteo agree ≤2°F): use cluster of 2
  - "medium": use cluster of 3 for safety
  - "low": skip
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Optional, Literal
from config import STRATEGY


# ─────────────────────────────────────────────────────────────────────────────
# Data classes
# ─────────────────────────────────────────────────────────────────────────────

@dataclass
class NoOpp:
    """Single bracket NO opportunity."""
    city: str
    date: str
    event_slug: str
    station: str
    market_id: str
    group_title: str
    bracket_lo: Optional[float]
    bracket_hi: Optional[float]
    no_price: float
    yes_price: float
    return_pct: float
    distance_f: float        # distance in native unit (°F or °C)
    forecast_temp: float
    forecast_confidence: str
    yes_token_id: str
    no_token_id: str
    liquidity: float
    accepting_orders: bool
    recommended_size: float
    temp_unit: str = "F"     # "F" or "C"


@dataclass
class BracketSlot:
    """One bracket within a YES cluster."""
    market_id: str
    group_title: str
    bracket_lo: Optional[float]
    bracket_hi: Optional[float]
    yes_price: float
    yes_token_id: str
    liquidity: float
    is_forecast_bracket: bool   # True for the bracket directly containing the forecast


@dataclass
class YesCluster:
    """YES cluster: 2-3 adjacent brackets surrounding the forecast."""
    city: str
    date: str
    event_slug: str
    station: str
    brackets: list           # list of BracketSlot
    cluster_size: int
    total_price: float       # sum of yes_prices → your total cost per $1 payout
    return_pct: float        # (1 - total_price) / total_price * 100
    win_lo: float            # lowest guaranteed win boundary
    win_hi: Optional[float]  # highest guaranteed win boundary (None = open high)
    forecast_temp: float
    forecast_confidence: str
    size_each: float         # USDC per bracket
    total_cost: float        # size_each × cluster_size
    liquidity_min: float     # min liquidity across brackets
    temp_unit: str = "F"     # "F" or "C"

    def bracket_labels(self) -> str:
        return " + ".join(b.group_title for b in self.brackets)


# ─────────────────────────────────────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────────────────────────────────────

def bracket_distance(forecast_temp: float, lo: Optional[float], hi: Optional[float]) -> float:
    """Distance in °F from forecast to nearest bracket edge. 0 = inside bracket."""
    if lo is None and hi is not None:
        return 0.0 if forecast_temp <= hi else forecast_temp - hi
    elif hi is None and lo is not None:
        return 0.0 if forecast_temp >= lo else lo - forecast_temp
    elif lo is not None and hi is not None:
        if lo <= forecast_temp <= hi:
            return 0.0
        return (lo - forecast_temp) if forecast_temp < lo else (forecast_temp - hi)
    return float("inf")


def sort_brackets(markets: list) -> list:
    """Sort bracket markets low→high by bracket_lo (None = -inf for open low)."""
    def sort_key(m):
        lo = m.get("bracket_lo")
        return lo if lo is not None else float("-inf")
    return sorted(markets, key=sort_key)


def no_size(return_pct: float, confidence: str, capital: float, n_opps: int) -> float:
    cfg = STRATEGY
    conf_factor = 1.0 if confidence == "high" else 0.7
    base = min(cfg["default_no_size"], capital / max(n_opps, 1))
    size = base * conf_factor * min(return_pct / 15.0, 1.5)
    return max(round(size, 1), cfg["min_order_size"])


def yes_cluster_size_each(total_cost_target: float, cluster_size: int) -> float:
    cfg = STRATEGY
    per = total_cost_target / cluster_size
    return max(round(per, 1), cfg["min_order_size"])


# ─────────────────────────────────────────────────────────────────────────────
# Core analysis
# ─────────────────────────────────────────────────────────────────────────────

def find_no_opps(event: dict, forecast_temp: float, confidence: str,
                 capital: float, n_opps: int) -> list:
    """Find NO opportunities: brackets far enough from forecast."""
    cfg = STRATEGY
    unit = event.get("temp_unit", "F")
    min_dist = cfg["no_min_distance_c"] if unit == "C" else cfg["no_min_distance_f"]
    opps = []
    for mkt in event["markets"]:
        if not mkt["accepting_orders"]:
            continue
        lo, hi = mkt["bracket_lo"], mkt["bracket_hi"]
        yes_price = mkt.get("yes_price_live") or mkt["yes_price"]
        no_price = 1.0 - yes_price
        dist = bracket_distance(forecast_temp, lo, hi)

        if dist >= min_dist and no_price >= cfg["no_min_price"]:
            ret_pct = (1.0 - no_price) / no_price * 100
            if ret_pct >= cfg["min_return_pct"]:
                opps.append(NoOpp(
                    city=event["city"],
                    date=event["date"],
                    event_slug=event["event_slug"],
                    station=event["station"],
                    market_id=mkt["market_id"],
                    group_title=mkt["group_title"],
                    bracket_lo=lo,
                    bracket_hi=hi,
                    no_price=no_price,
                    yes_price=yes_price,
                    return_pct=ret_pct,
                    distance_f=dist,
                    forecast_temp=forecast_temp,
                    forecast_confidence=confidence,
                    yes_token_id=mkt["yes_token_id"],
                    no_token_id=mkt["no_token_id"],
                    liquidity=mkt["liquidity"],
                    accepting_orders=mkt["accepting_orders"],
                    recommended_size=no_size(ret_pct, confidence, capital, n_opps),
                    temp_unit=unit,
                ))
    return opps


def find_yes_clusters(event: dict, forecast_temp: float, confidence: str,
                      capital: float) -> list:
    """
    Build YES clusters of 2-3 adjacent brackets surrounding the forecast.

    Returns up to 2 clusters per event:
      - 2-bracket cluster (higher return, narrower window)
      - 3-bracket cluster (lower return, wider window) if confidence is medium
    """
    cfg = STRATEGY
    sorted_mkts = sort_brackets(event["markets"])
    active = [m for m in sorted_mkts if m["accepting_orders"]]
    if not active:
        return []

    # Find forecast bracket index
    center_idx = None
    for i, m in enumerate(active):
        if bracket_distance(forecast_temp, m["bracket_lo"], m["bracket_hi"]) == 0:
            center_idx = i
            break

    if center_idx is None:
        return []

    clusters = []

    def make_cluster(indices: list) -> Optional[YesCluster]:
        slots = []
        for idx in indices:
            if idx < 0 or idx >= len(active):
                return None
            m = active[idx]
            yes_price = m.get("yes_price_live") or m["yes_price"]
            if yes_price <= 0:
                return None
            slots.append(BracketSlot(
                market_id=m["market_id"],
                group_title=m["group_title"],
                bracket_lo=m["bracket_lo"],
                bracket_hi=m["bracket_hi"],
                yes_price=yes_price,
                yes_token_id=m["yes_token_id"],
                liquidity=m["liquidity"],
                is_forecast_bracket=(idx == center_idx),
            ))

        total_price = round(sum(s.yes_price for s in slots), 4)
        if total_price >= 0.97:  # not enough edge
            return None

        ret_pct = round((1.0 - total_price) / total_price * 100, 1)
        if ret_pct < cfg["min_return_pct"]:
            return None

        # Win range: from the lowest bracket's lo to the highest bracket's hi
        win_lo_val = slots[0].bracket_lo if slots[0].bracket_lo is not None else float("-inf")
        win_hi_val = slots[-1].bracket_hi  # None = open high

        # Size: target ~$15 total for 3-cluster, ~$12 for 2-cluster
        total_target = min(cfg["default_yes_size"] * len(slots), capital * 0.05)
        size_each = yes_cluster_size_each(total_target, len(slots))
        total_cost = round(size_each * len(slots), 2)

        unit = event.get("temp_unit", "F")
        return YesCluster(
            city=event["city"],
            date=event["date"],
            event_slug=event["event_slug"],
            station=event["station"],
            brackets=slots,
            cluster_size=len(slots),
            total_price=total_price,
            return_pct=ret_pct,
            win_lo=win_lo_val,
            win_hi=win_hi_val,
            forecast_temp=forecast_temp,
            forecast_confidence=confidence,
            size_each=size_each,
            total_cost=total_cost,
            liquidity_min=min(s.liquidity for s in slots),
            temp_unit=unit,
        )

    # Always generate 3-bracket cluster (center ± 1)
    cluster3 = make_cluster([center_idx - 1, center_idx, center_idx + 1])
    if cluster3:
        clusters.append(cluster3)

    # Also generate 2-bracket cluster:
    # Pick the neighbor where the forecast is closer to the edge (more likely to win)
    center = active[center_idx]
    lo_c, hi_c = center["bracket_lo"], center["bracket_hi"]
    if lo_c is not None and hi_c is not None:
        dist_to_lo = forecast_temp - lo_c
        dist_to_hi = hi_c - forecast_temp
        # If forecast is closer to the low edge → include bracket below
        # If closer to the high edge → include bracket above
        if dist_to_lo <= dist_to_hi and center_idx > 0:
            cluster2 = make_cluster([center_idx - 1, center_idx])
        else:
            cluster2 = make_cluster([center_idx, center_idx + 1])
        if cluster2 and (not cluster3 or cluster2.return_pct > cluster3.return_pct + 5):
            clusters.append(cluster2)
    elif lo_c is None:
        # Open-low bracket: pair with the one above
        cluster2 = make_cluster([center_idx, center_idx + 1])
        if cluster2:
            clusters.append(cluster2)
    elif hi_c is None:
        # Open-high bracket: pair with the one below
        cluster2 = make_cluster([center_idx - 1, center_idx])
        if cluster2:
            clusters.append(cluster2)

    return clusters


def analyze_event(event: dict, forecast: dict, capital: float, n_opps: int = 20) -> tuple:
    """Returns (yes_clusters, no_opps) for a single event."""
    date_str = event["date"]
    day_fc = forecast.get("forecasts", {}).get(date_str)
    if not day_fc:
        return [], []
    if day_fc["confidence"] == "low":
        return [], []

    forecast_temp = day_fc["consensus"]
    confidence = day_fc["confidence"]

    yes_clusters = find_yes_clusters(event, forecast_temp, confidence, capital)
    no_opps = find_no_opps(event, forecast_temp, confidence, capital, n_opps)
    return yes_clusters, no_opps


def analyze_all(all_markets: dict, all_forecasts: dict,
                max_capital: Optional[float] = None) -> tuple:
    """
    Run full analysis.
    Returns (yes_clusters, no_opps) sorted by return_pct descending.
    """
    if max_capital is None:
        max_capital = STRATEGY["max_capital"]

    all_clusters = []
    all_no_opps = []

    for city, events in all_markets.items():
        city_forecast = all_forecasts.get(city)
        if not city_forecast:
            continue
        for event in events:
            clusters, no_opps = analyze_event(event, city_forecast, max_capital)
            all_clusters.extend(clusters)
            all_no_opps.extend(no_opps)

    all_clusters.sort(key=lambda c: (c.forecast_confidence == "high", c.return_pct), reverse=True)
    all_no_opps.sort(key=lambda o: (o.forecast_confidence == "high", o.return_pct), reverse=True)
    return all_clusters, all_no_opps


# ─────────────────────────────────────────────────────────────────────────────
# Display
# ─────────────────────────────────────────────────────────────────────────────

def print_yes_clusters(clusters: list, limit: int = 20):
    if not clusters:
        print("No YES cluster opportunities.")
        return

    print(f"\n{'='*110}")
    print(f"{'YES CLUSTERS  (buy 2-3 adjacent brackets — guaranteed 1 winner)':^110}")
    print(f"{'='*110}")
    print(
        f"{'City':<14} {'Date':<12} {'Brackets':^36} {'n':>2} "
        f"{'TotalP':>7} {'Return':>7} {'WinRange':>16} "
        f"{'Forecast':>10} {'Conf':>5} {'Liq':>7} {'$/bkt':>6} {'Total$':>7}"
    )
    print("-" * 110)

    total_deployed = 0.0
    for c in clusters[:limit]:
        labels = " + ".join(b.group_title for b in c.brackets)
        u = c.temp_unit
        win_lo_str = f"{c.win_lo:.0f}" if c.win_lo != float("-inf") else "<lo"
        win_hi_str = f"{c.win_hi:.0f}" if c.win_hi is not None else "→∞"
        win_range = f"{win_lo_str}-{win_hi_str}°{u}"
        conf_icon = "✓✓" if c.forecast_confidence == "high" else "✓"

        print(
            f"{c.city:<14} {c.date:<12} {labels:<36} {c.cluster_size:>2} "
            f"{c.total_price:>7.3f} {c.return_pct:>6.1f}%  {win_range:>16} "
            f"{c.forecast_temp:>6.1f}°{u}  {conf_icon:>3}  "
            f"${c.liquidity_min:>5.0f}  ${c.size_each:>4.0f}  ${c.total_cost:>5.0f}"
        )
        total_deployed += c.total_cost

    print("-" * 110)
    print(f"Total: {len(clusters[:limit])} clusters | Estimated deploy: ${total_deployed:.0f}")
    print(f"{'='*110}\n")


def print_no_opps(opps: list, limit: int = 30):
    if not opps:
        print("No NO opportunities.")
        return

    print(f"\n{'='*105}")
    print(f"{'NO BETS  (bracket clearly far from forecast — low risk)':^105}")
    print(f"{'='*105}")
    print(
        f"{'City':<14} {'Date':<12} {'Bracket':<13} {'NO price':>8} "
        f"{'Return':>7} {'Dist':>6} {'Forecast':>10} {'Conf':>5} {'Liq':>7} {'Size':>6}"
    )
    print("-" * 105)

    total_deployed = 0.0
    for o in opps[:limit]:
        u = o.temp_unit
        dist_str = f"{o.distance_f:.0f}°{u}" if o.distance_f != float("inf") else "far"
        conf_icon = "✓✓" if o.forecast_confidence == "high" else "✓"
        print(
            f"{o.city:<14} {o.date:<12} {o.group_title:<13} "
            f"{o.no_price:>8.3f} {o.return_pct:>6.1f}%  {dist_str:>6} "
            f"{o.forecast_temp:>6.1f}°{u}  {conf_icon:>3}  "
            f"${o.liquidity:>5.0f}  ${o.recommended_size:>4.0f}"
        )
        total_deployed += o.recommended_size

    print("-" * 105)
    print(f"Total: {len(opps[:limit])} NO bets | Estimated deploy: ${total_deployed:.0f}")
    print(f"{'='*105}\n")

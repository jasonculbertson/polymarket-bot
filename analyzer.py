"""
Arbitrage analyzer: compares weather forecasts against Polymarket bracket prices.

Two strategies:

  1. NO — buy NO on brackets that are clearly far from forecast. You win if that bracket
     doesn't resolve. Low risk, 8-50% returns depending on how far from forecast.

  2. YES CLUSTER — buy YES on exactly 3 adjacent brackets that together "surround" the forecast.
     Exactly one bracket always resolves YES when the temp falls in your range — but that
     does *not* guarantee profit. You only profit if the total you pay for all 3 brackets
     is less than $1 (one bracket pays $1). So we only suggest clusters where total_price < 0.97.

     MATH (equal shares so same payout whichever bracket wins):
       P = p1+p2+p3 (sum of YES prices; must be < 1). Buy S shares in EACH bracket.
       Cost = S*P,  Payout = S*$1  →  Profit = S(1-P).  Return % = (1-P)/P*100.
       Target cost T  →  S = T/P.  Spend on bracket i = S*p_i (we set amount_usd per slot).

     Example — 76-77@12c, 78-79@35c, 80-81@28c: P=0.75. T=$30 → S=40. Spend $4.80+$14+$11.20=$30.

     Any bracket wins → 40*$1 = $40, profit $10. We only suggest P < 0.97.
     Always 3 brackets — 2-bracket clusters have too-high total price, winning barely covers cost.
     One cluster per city per date — multiple clusters on the same city/date are correlated risk.

Confidence:
  - "high" (NWS + Open-Meteo agree ≤2°F): build 3-bracket cluster
  - "medium" or "low": skip YES clusters entirely
"""

from __future__ import annotations

import json
import os
from dataclasses import dataclass, field
from math import erf, sqrt
from typing import Optional, Literal
from config import STRATEGY, CITIES


def _load_calibrated_sigma() -> dict:
    """Load sigma values calibrated by learner.py. Falls back to defaults."""
    defaults = {"F": {"high": 1.8, "medium": 3.2}, "C": {"high": 1.0, "medium": 1.8}}
    try:
        calib_file = os.path.join(
            os.environ.get("DATA_DIR", os.path.join(os.path.dirname(__file__), "data")),
            "calibration.json",
        )
        if os.path.exists(calib_file):
            with open(calib_file) as f:
                data = json.load(f)
            return data.get("no_sigma", defaults)
    except Exception:
        pass
    return defaults


def _adjust_for_peak_passed(city: str, date_str: str, confidence: str) -> str:
    """
    Boost 'medium' to 'high' for same-day markets after 14:00 local city time.
    At this point the daily high is essentially locked in, so medium confidence
    (sources agree within 2-4°F) is as reliable as high confidence.
    """
    if confidence != "medium":
        return confidence
    try:
        from zoneinfo import ZoneInfo
        from datetime import datetime
        tz_name = CITIES.get(city, {}).get("tz")
        if not tz_name:
            return confidence
        now_local = datetime.now(ZoneInfo(tz_name))
        if date_str != now_local.date().isoformat():
            return confidence
        if now_local.hour >= 14:
            return "high"
    except Exception:
        pass
    return confidence


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
    predicted_win_prob: float = 0.75
    temp_unit: str = "F"     # "F" or "C"
    market_slug: str = ""
    resolution_time: str = ""
    resolution_date: str = ""  # settle date (YYYY-MM-DD) for resolve logic; date = forecast/weather day
    ev_score: float = 0.0        # EV = win_prob × return_pct − (1−win_prob) × 100
    effective_return_pct: float = 0.0  # return_pct adjusted for CLOB bid-ask spread
    spread_pct: float = 0.0      # cost increase from spread (0 if no CLOB data)


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
    market_slug: str = ""
    amount_usd: float = 0.0    # spend this much on this bracket (shares × yes_price); set when cluster is sized


@dataclass
class YesCluster:
    """YES cluster: exactly 3 adjacent brackets surrounding the forecast."""
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
    shares: float            # equal shares in each bracket → same $1 payout whichever wins
    size_each: float         # average USDC per bracket (total_cost / cluster_size), for display
    total_cost: float        # shares × total_price; same payout = shares × $1 whichever bracket wins
    liquidity_min: float     # min liquidity across brackets
    predicted_win_prob: float = 0.75
    temp_unit: str = "F"     # "F" or "C"
    resolution_time: str = ""  # ISO UTC timestamp from Gamma endDate
    resolution_date: str = ""   # settle date (YYYY-MM-DD) for resolve logic; date = forecast/weather day
    ev_score: float = 0.0        # EV = win_prob × return_pct − (1−win_prob) × 100

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


def estimate_no_win_prob(distance: float, confidence: str, unit: str = "F") -> float:
    """
    Estimate probability that a NO bet wins (actual temp does NOT land in the bracket).

    Models forecast error as Normal(0, sigma). Sigma is loaded from calibration.json
    if available — learner.py adjusts it over time based on actual vs predicted outcomes.
    """
    sigma_cfg = _load_calibrated_sigma()
    sigma = sigma_cfg.get(unit, {}).get(confidence, 1.8 if unit == "F" else 1.0)

    if distance <= 0:
        return 0.5

    z = distance / (sigma * sqrt(2))
    p_in_bracket = 0.5 * (1.0 - erf(z))
    win_prob = 1.0 - p_in_bracket
    return max(0.55, min(0.97, win_prob))


def estimate_yes_win_prob(cluster_size: int, confidence: str, unit: str = "F") -> float:
    """
    Estimate probability that a YES cluster wins (actual temp lands in one of its brackets).
    Each bracket is ~2°F / 1°C wide; cluster covers cluster_size × width.
    Sigma is loaded from calibration.json if available.
    """
    sigma_cfg = _load_calibrated_sigma()
    sigma = sigma_cfg.get(unit, {}).get(confidence, 1.8 if unit == "F" else 1.0)

    bracket_width = 2.0 if unit == "F" else 1.0
    cluster_range = cluster_size * bracket_width

    half = cluster_range / 2.0
    z = half / (sigma * sqrt(2))
    win_prob = erf(z)
    return max(0.40, min(0.95, win_prob))


def kelly_size(
    win_prob: float,
    price: float,
    capital: float,
    kelly_fraction: float = 0.5,
    min_size: float = 5.0,
    max_size: float = 50.0,
) -> float:
    """
    Compute fractional Kelly position size.

    win_prob : estimated probability of winning
    price    : cost per $1 payout (the stake; price < 1 means positive expected value)
    capital  : total available capital
    """
    if price <= 0 or price >= 1:
        return min_size

    b = (1.0 / price) - 1          # net odds: win b for each $1 risked
    q = 1.0 - win_prob
    full_kelly = (win_prob * b - q) / b

    if full_kelly <= 0:
        return min_size

    size = capital * full_kelly * kelly_fraction
    return max(min_size, min(round(size, 1), max_size))


def no_size(
    return_pct: float,
    confidence: str,
    capital: float,
    n_opps: int,
    distance: float = 4.0,
    unit: str = "F",
) -> float:
    cfg = STRATEGY
    win_prob = estimate_no_win_prob(distance, confidence, unit)
    no_price = 100.0 / (100.0 + return_pct)   # derived from return_pct definition
    cap_per_opp = capital / max(n_opps / 3, 1)
    return kelly_size(
        win_prob,
        no_price,
        capital,
        kelly_fraction=cfg.get("kelly_fraction", 0.5),
        min_size=cfg["min_order_size"],
        max_size=min(cfg.get("max_single_bet", 50), cfg["default_no_size"] * 2, cap_per_opp),
    )


def yes_cluster_size_each(
    total_cost_target: float,
    cluster_size: int,
    win_prob: float = 0.75,
    total_price: float = 0.75,
    capital: float = 400.0,
) -> float:
    cfg = STRATEGY
    total_kelly = kelly_size(
        win_prob,
        total_price,
        capital,
        kelly_fraction=cfg.get("kelly_fraction", 0.5),
        min_size=cfg["min_order_size"] * cluster_size,
        max_size=min(cfg.get("max_single_bet", 50) * cluster_size, total_cost_target * 1.5),
    )
    per = total_kelly / cluster_size
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
                win_prob = estimate_no_win_prob(dist, confidence, unit)
                ev = round(win_prob * ret_pct - (1 - win_prob) * 100, 1)
                # Spread-adjusted effective return using CLOB bid
                clob_bid = mkt.get("clob_best_bid")
                if clob_bid and 0 < clob_bid < 1:
                    eff_no = 1.0 - clob_bid   # cost to buy NO = sell YES at bid price
                    eff_ret = round((1.0 - eff_no) / eff_no * 100, 1) if 0 < eff_no < 1 else ret_pct
                    sprd = round((eff_no - no_price) / no_price * 100, 1) if no_price > 0 else 0.0
                else:
                    eff_ret = ret_pct
                    sprd = 0.0
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
                    recommended_size=no_size(ret_pct, confidence, capital, n_opps, dist, unit),
                    predicted_win_prob=win_prob,
                    temp_unit=unit,
                    market_slug=mkt.get("market_slug", ""),
                    resolution_time=event.get("resolution_time", ""),
                    resolution_date=event.get("resolution_date", event.get("date", "")),
                    ev_score=ev,
                    effective_return_pct=eff_ret,
                    spread_pct=sprd,
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

    # Find forecast bracket index.
    # When the forecast sits exactly on a bracket boundary (forecast == bracket hi),
    # the forecast is at the very top of that bracket. Prefer the bracket above
    # (where forecast == lo) so the cluster expands symmetrically upward.
    center_idx = None
    for i, m in enumerate(active):
        if bracket_distance(forecast_temp, m["bracket_lo"], m["bracket_hi"]) == 0:
            lo_c = m["bracket_lo"]
            hi_c = m["bracket_hi"]
            # If forecast is exactly at the upper edge, check if the next bracket
            # also contains it (forecast == next bracket's lo). If so, prefer next.
            if hi_c is not None and forecast_temp == hi_c and i + 1 < len(active):
                next_m = active[i + 1]
                if bracket_distance(forecast_temp, next_m["bracket_lo"], next_m["bracket_hi"]) == 0:
                    center_idx = i + 1
                    break
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
                market_slug=m.get("market_slug", ""),
            ))

        # Hard minimum: 3-bracket clusters only. 2-bracket total prices are too high
        # — winning barely covers cost, and some wins show negative P&L.
        if len(slots) < 3:
            return None

        total_price = round(sum(s.yes_price for s in slots), 4)
        # Profit only when one bracket pays $1 and total cost < $1. Reject thin or losing clusters.
        if total_price >= 0.97:  # need at least ~3% edge
            return None
        if total_price >= 1.0:   # would lose even when "right" (temp in range)
            return None

        ret_pct = round((1.0 - total_price) / total_price * 100, 1)
        if ret_pct < cfg["min_return_pct"]:
            return None

        # Win range: from the lowest bracket's lo to the highest bracket's hi
        win_lo_val = slots[0].bracket_lo if slots[0].bracket_lo is not None else float("-inf")
        win_hi_val = slots[-1].bracket_hi  # None = open high

        # Size by equal SHARES so payout = S×$1 whichever bracket wins (math above).
        # T = target total cost from Kelly; S = T/P → cost = S·P, payout = S.
        # S is shares PER BRACKET. Payout when any one bracket wins = S×$1.
        # Profit = S - total_cost = S(1 - P). Correct formula: S = total_cost / total_price.
        unit_val = event.get("temp_unit", "F")
        win_prob = estimate_yes_win_prob(len(slots), confidence, unit_val)
        # Lottery clusters (total_price < threshold) get capped at a smaller size
        # to limit exposure while preserving upside.
        lottery_threshold = cfg.get("yes_lottery_threshold", 0.25)
        is_lottery = total_price < lottery_threshold
        if is_lottery:
            lottery_size = cfg.get("yes_lottery_size", 5)
            total_target = lottery_size * len(slots)
        else:
            total_target = min(cfg["default_yes_size"] * len(slots), capital * 0.05)
        size_each_legacy = yes_cluster_size_each(total_target, len(slots), win_prob, total_price, capital)
        total_cost = round(size_each_legacy * len(slots), 2)
        shares = round(total_cost / total_price, 2)  # S = T/P — shares per bracket
        total_cost = round(shares * total_price, 2)  # recalc to match
        size_each_avg = round(total_cost / len(slots), 2)
        for s in slots:
            s.amount_usd = round(shares * s.yes_price, 2)  # amount_i = S·p_i

        ev_s = round(win_prob * ret_pct - (1 - win_prob) * 100, 1)
        unit = event.get("temp_unit", "F")
        return YesCluster(
            predicted_win_prob=round(win_prob, 4),
            ev_score=ev_s,
            city=event["city"],
            date=event["date"],
            event_slug=event["event_slug"],
            station=event["station"],
            resolution_time=event.get("resolution_time", ""),
            brackets=slots,
            cluster_size=len(slots),
            total_price=total_price,
            return_pct=ret_pct,
            win_lo=win_lo_val,
            win_hi=win_hi_val,
            forecast_temp=forecast_temp,
            forecast_confidence=confidence,
            shares=round(shares, 2),
            size_each=size_each_avg,
            total_cost=total_cost,
            liquidity_min=min(s.liquidity for s in slots),
            temp_unit=unit,
            resolution_date=event.get("resolution_date", event.get("date", "")),
        )

    # Prefer 3-bracket cluster (center ± 1). If forecast is at an edge, shift inward.
    n = len(active)
    if center_idx == 0:
        indices3 = [0, 1, 2]
    elif center_idx == n - 1:
        indices3 = [n - 3, n - 2, n - 1]
    else:
        indices3 = [center_idx - 1, center_idx, center_idx + 1]

    cluster3 = make_cluster(indices3)
    if cluster3:
        clusters.append(cluster3)

    return clusters


def analyze_event(event: dict, forecast: dict, capital: float, n_opps: int = 20) -> tuple:
    """Returns (yes_clusters, no_opps) for a single event."""
    date_str = event["date"]
    day_fc = forecast.get("forecasts", {}).get(date_str)
    if not day_fc:
        return [], []
    confidence = day_fc.get("confidence", "low")
    confidence = _adjust_for_peak_passed(event["city"], date_str, confidence)

    # Skip entirely if sources strongly disagree
    if confidence == "low":
        return [], []

    # YES clusters require both sources to agree closely (confidence = "high").
    # Medium confidence means WU and NWS diverge 2-4°F — too risky for directional bets.
    # NO bets are allowed on medium confidence (betting against outlier brackets is safer).
    wu_temp = day_fc.get("wunderground")
    forecast_temp = wu_temp if wu_temp is not None else day_fc["consensus"]

    yes_clusters = find_yes_clusters(event, forecast_temp, confidence, capital) if confidence == "high" else []
    no_opps = find_no_opps(event, forecast_temp, confidence, capital, n_opps)
    return yes_clusters, no_opps


def analyze_all(all_markets: dict, all_forecasts: dict,
                max_capital: Optional[float] = None) -> tuple:
    """
    Run full analysis.
    Returns (yes_clusters, no_opps) sorted by ev_score descending.

    Each event produces at most one YES cluster (3 adjacent brackets).
    At most one cluster per city per date is kept (highest ev_score wins),
    preventing correlated over-concentration on the same temperature.
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

            if clusters:
                clusters[0].alt = None
                all_clusters.append(clusters[0])

            all_no_opps.extend(no_opps)

    # Keep at most 1 cluster per city per date (highest ev_score wins)
    seen_city_date: dict = {}
    for c in all_clusters:
        key = (c.city, c.date)
        if key not in seen_city_date or c.ev_score > seen_city_date[key].ev_score:
            seen_city_date[key] = c
    all_clusters = list(seen_city_date.values())

    all_clusters.sort(key=lambda c: (c.forecast_confidence == "high", c.ev_score), reverse=True)
    all_no_opps.sort(key=lambda o: (o.forecast_confidence == "high", o.ev_score), reverse=True)
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

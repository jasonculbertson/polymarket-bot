"""
Outcome tracker: records scanned opportunities and resolves their P&L
after the market resolution date passes.

Paper trading is simulated at PAPER_SIZE_USD per position.
  - Win  P&L = PAPER_SIZE_USD × (return_pct / 100)
  - Loss P&L = −PAPER_SIZE_USD

Storage: DATA_DIR/outcomes.json  (persists across Railway deploys when volume is mounted)
"""

import json
import os
import re
import requests
from datetime import datetime, date
from typing import Optional

DATA_DIR = os.environ.get("DATA_DIR", os.path.join(os.path.dirname(__file__), "data"))
OUTCOMES_FILE = os.path.join(DATA_DIR, "outcomes.json")
GAMMA_API = "https://gamma-api.polymarket.com"

# Paper trading stake per position (USDC). Override via env var.
PAPER_SIZE_USD = float(os.environ.get("PAPER_SIZE_USD", "10.0"))


# ─── Storage ──────────────────────────────────────────────────────────────────

def _load() -> dict:
    if not os.path.exists(OUTCOMES_FILE):
        return {"opportunities": [], "last_resolved": None}
    with open(OUTCOMES_FILE) as f:
        return json.load(f)


def _save(data: dict):
    os.makedirs(DATA_DIR, exist_ok=True)
    with open(OUTCOMES_FILE, "w") as f:
        json.dump(data, f, indent=2)


# ─── ID helpers ───────────────────────────────────────────────────────────────

def _no_id(market_id: str) -> str:
    return f"no_{market_id}"


def _yes_id(event_slug: str, market_ids: list) -> str:
    key = "_".join(sorted(market_ids))
    # Truncate to keep IDs manageable
    return f"yes_{event_slug[:20]}_{key[:24]}"


# ─── Record ───────────────────────────────────────────────────────────────────

def record_scan(yes_clusters, no_opps, all_forecasts: dict = None) -> int:
    """
    Add newly found opportunities to the tracker (skips duplicates).

    all_forecasts: the full forecast dict from fetch_all_forecasts(), used to store
                   per-source temperature predictions for post-resolution learning.
    Returns total tracked opportunity count.
    """
    data = _load()
    existing_ids = {o["id"] for o in data["opportunities"]}
    now = datetime.utcnow().isoformat()
    added = 0

    def _get_sources(city: str, resolution_date: str) -> dict:
        """Extract raw source forecasts for a city/date for later accuracy tracking."""
        if not all_forecasts:
            return {}
        day_fc = all_forecasts.get(city, {}).get("forecasts", {}).get(resolution_date, {})
        return {
            "wunderground": day_fc.get("wunderground"),
            "nws":          day_fc.get("nws"),
            "wttr":         day_fc.get("wttr"),
        }

    for o in no_opps:
        oid = _no_id(o.market_id)
        if oid in existing_ids:
            continue
        data["opportunities"].append({
            "id": oid,
            "type": "no",
            "city": o.city,
            "bracket": o.group_title,
            "event_slug": o.event_slug,
            "market_id": o.market_id,
            "no_token_id": o.no_token_id,
            "entry_price": round(o.no_price, 4),
            "return_pct": round(o.return_pct, 2),
            "forecast_temp": o.forecast_temp,
            "temp_unit": o.temp_unit,
            "confidence": o.forecast_confidence,
            "predicted_win_prob": round(getattr(o, "predicted_win_prob", 0.75), 4),
            "forecast_sources": _get_sources(o.city, o.date),
            "resolution_date": o.date,
            "resolution_time": getattr(o, "resolution_time", ""),
            "first_seen": now,
            "paper_size_usd": PAPER_SIZE_USD,
            "outcome": None,
            "final_yes_price": None,
            "pnl_pct": None,
            "paper_pnl_usd": None,
            "learned": False,
        })
        existing_ids.add(oid)
        added += 1

    for c in yes_clusters:
        mids = [b.market_id for b in c.brackets]
        oid = _yes_id(c.event_slug, mids)
        if oid in existing_ids:
            continue
        data["opportunities"].append({
            "id": oid,
            "type": "yes",
            "city": c.city,
            "bracket": " + ".join(b.group_title for b in c.brackets),
            "event_slug": c.event_slug,
            "market_ids": mids,
            "yes_token_ids": [b.yes_token_id for b in c.brackets],
            "entry_price": round(c.total_price, 4),
            "return_pct": round(c.return_pct, 2),
            "cluster_size": c.cluster_size,
            "forecast_temp": c.forecast_temp,
            "temp_unit": c.temp_unit,
            "confidence": c.forecast_confidence,
            "predicted_win_prob": round(getattr(c, "predicted_win_prob", 0.75), 4),
            "forecast_sources": _get_sources(c.city, c.date),
            "resolution_date": c.date,
            "resolution_time": getattr(c, "resolution_time", ""),
            "first_seen": now,
            "paper_size_usd": PAPER_SIZE_USD,
            "outcome": None,
            "final_yes_price": None,
            "pnl_pct": None,
            "paper_pnl_usd": None,
            "learned": False,
        })
        existing_ids.add(oid)
        added += 1

    if added:
        _save(data)
    return len(data["opportunities"])


# ─── Resolve ──────────────────────────────────────────────────────────────────

def _fetch_market_yes_price(market_id: str) -> Optional[float]:
    """Fetch current YES price for a market from Gamma API."""
    try:
        r = requests.get(f"{GAMMA_API}/markets/{market_id}", timeout=8)
        if r.status_code != 200:
            return None
        mkt = r.json()
        prices = mkt.get("outcomePrices", "[]")
        if isinstance(prices, str):
            prices = json.loads(prices)
        return float(prices[0]) if prices else None
    except Exception:
        return None


def _parse_bracket_midpoint(text: str, unit: str = "F") -> Optional[float]:
    """Parse bracket text and return its midpoint temperature."""
    m = re.search(r"between (-?\d+)-(-?\d+)°[FC]", text)
    if m:
        return (float(m.group(1)) + float(m.group(2))) / 2.0
    m = re.search(r"(-?\d+)-(-?\d+)°[FC]", text)
    if m:
        return (float(m.group(1)) + float(m.group(2))) / 2.0
    m = re.search(r"be (-?\d+)°[FC] or (?:below|lower)", text)
    if m:
        return float(m.group(1)) - 0.5
    m = re.search(r"(-?\d+)°[FC] or (?:below|lower)", text)
    if m:
        return float(m.group(1)) - 0.5
    m = re.search(r"be (-?\d+)°[FC] or (?:above|higher)", text)
    if m:
        return float(m.group(1)) + 0.5
    m = re.search(r"(-?\d+)°[FC] or (?:above|higher)", text)
    if m:
        return float(m.group(1)) + 0.5
    return None


def _fetch_actual_temp_from_gamma(event_slug: str) -> Optional[float]:
    """
    Infer actual temperature from the resolved Polymarket event.
    Finds the bracket with YES price ≥ 0.95 and returns its midpoint.
    """
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
        for mkt in event.get("markets", []):
            prices_raw = mkt.get("outcomePrices", "[]")
            if isinstance(prices_raw, str):
                prices_raw = json.loads(prices_raw)
            yes_price = float(prices_raw[0]) if prices_raw else 0.0
            if yes_price >= 0.95:
                label = mkt.get("groupItemTitle") or mkt.get("question", "")
                t = _parse_bracket_midpoint(label)
                if t is not None:
                    return t
    except Exception:
        pass
    return None


def resolve_outcomes() -> int:
    """
    Check past-resolution-date opportunities and record wins/losses.
    Safe to call after every scan — only touches markets past their date.
    Returns count of newly resolved outcomes.
    """
    data = _load()
    today = date.today().isoformat()
    resolved_count = 0

    for opp in data["opportunities"]:
        if opp["outcome"] is not None:
            continue
        if opp["resolution_date"] >= today:
            continue

        if opp["type"] == "no":
            final_yes = _fetch_market_yes_price(opp["market_id"])
            if final_yes is None:
                continue
            if final_yes <= 0.05:
                opp["outcome"] = "win"
                opp["final_yes_price"] = final_yes
                opp["pnl_pct"] = round((1.0 - opp["entry_price"]) / opp["entry_price"] * 100, 2)
            elif final_yes >= 0.95:
                opp["outcome"] = "loss"
                opp["final_yes_price"] = final_yes
                opp["pnl_pct"] = -100.0
            else:
                continue
            stake = opp.get("paper_size_usd", PAPER_SIZE_USD)
            opp["paper_size_usd"] = stake
            opp["paper_pnl_usd"] = round(stake * (opp["pnl_pct"] / 100.0), 2)
            # Record actual temperature for accuracy tracking
            if opp.get("actual_temp") is None:
                actual = _fetch_actual_temp_from_gamma(opp.get("event_slug", ""))
                if actual is not None:
                    opp["actual_temp"] = actual
                    wu_pred = (opp.get("forecast_sources") or {}).get("wunderground")
                    if wu_pred is not None:
                        opp["wu_error"] = round(abs(wu_pred - actual), 1)
            resolved_count += 1

        elif opp["type"] == "yes":
            prices = [_fetch_market_yes_price(mid) for mid in opp.get("market_ids", [])]
            prices = [p for p in prices if p is not None]
            if len(prices) < len(opp.get("market_ids", [])):
                continue
            if not prices:
                continue
            max_p = max(prices)
            if max_p >= 0.95:
                opp["outcome"] = "win"
                opp["final_yes_price"] = max_p
                opp["pnl_pct"] = round((1.0 - opp["entry_price"]) / opp["entry_price"] * 100, 2)
            elif max_p <= 0.05:
                opp["outcome"] = "loss"
                opp["final_yes_price"] = max_p
                opp["pnl_pct"] = -100.0
            else:
                continue
            stake = opp.get("paper_size_usd", PAPER_SIZE_USD)
            opp["paper_size_usd"] = stake
            opp["paper_pnl_usd"] = round(stake * (opp["pnl_pct"] / 100.0), 2)
            # Record actual temperature for accuracy tracking
            if opp.get("actual_temp") is None:
                actual = _fetch_actual_temp_from_gamma(opp.get("event_slug", ""))
                if actual is not None:
                    opp["actual_temp"] = actual
                    wu_pred = (opp.get("forecast_sources") or {}).get("wunderground")
                    if wu_pred is not None:
                        opp["wu_error"] = round(abs(wu_pred - actual), 1)
            resolved_count += 1

    if resolved_count:
        data["last_resolved"] = datetime.utcnow().isoformat()
        _save(data)

    return resolved_count


# ─── Summary ──────────────────────────────────────────────────────────────────

def get_summary() -> dict:
    """Return P&L summary for the outcomes dashboard tab."""
    data = _load()
    opps = data["opportunities"]
    resolved = [o for o in opps if o["outcome"] is not None]
    wins = [o for o in resolved if o["outcome"] == "win"]
    losses = [o for o in resolved if o["outcome"] == "loss"]
    pending = [o for o in opps if o["outcome"] is None]

    total_pnl_pct = sum(o["pnl_pct"] or 0 for o in resolved)
    avg_win = sum(o["pnl_pct"] for o in wins) / len(wins) if wins else None
    avg_loss = sum(o["pnl_pct"] for o in losses) / len(losses) if losses else None

    # Paper trading dollar P&L
    # Use paper_pnl_usd if stored; otherwise derive from paper_size_usd (or default $10)
    def _paper_pnl(o: dict) -> float:
        if o.get("paper_pnl_usd") is not None:
            return o["paper_pnl_usd"]
        stake = o.get("paper_size_usd", PAPER_SIZE_USD)
        pnl_pct = o.get("pnl_pct")
        if pnl_pct is None:
            return 0.0
        return round(stake * (pnl_pct / 100.0), 2)

    paper_pnl_total = round(sum(_paper_pnl(o) for o in resolved), 2)
    paper_staked_total = round(sum(o.get("paper_size_usd", PAPER_SIZE_USD) for o in resolved), 2)
    paper_roi = round(paper_pnl_total / paper_staked_total * 100, 1) if paper_staked_total else None
    paper_bankroll = round(paper_staked_total + paper_pnl_total, 2)  # if you reinvested nothing

    paper_wins_pnl  = round(sum(_paper_pnl(o) for o in wins), 2)
    paper_losses_pnl = round(sum(_paper_pnl(o) for o in losses), 2)

    # WU forecast accuracy: average absolute error on resolved opps with actual_temp
    wu_errors = [o["wu_error"] for o in resolved if o.get("wu_error") is not None]
    wu_avg_error = round(sum(wu_errors) / len(wu_errors), 2) if wu_errors else None

    # Bracket hit rate: for YES clusters, was the actual temp inside the bracket?
    yes_resolved = [o for o in resolved if o["type"] == "yes"]
    bracket_hits = [o for o in yes_resolved if o["outcome"] == "win"]
    bracket_hit_rate = round(len(bracket_hits) / len(yes_resolved) * 100, 1) if yes_resolved else None

    # Attach computed paper_pnl_usd to each row for the dashboard (don't mutate stored data)
    recent_rows = sorted(opps, key=lambda o: o["first_seen"], reverse=True)[:100]
    for row in recent_rows:
        if row.get("paper_pnl_usd") is None and row.get("pnl_pct") is not None:
            row = dict(row)  # shallow copy so we don't dirty the original
        row["paper_size_usd"] = row.get("paper_size_usd", PAPER_SIZE_USD)
        if row.get("paper_pnl_usd") is None and row.get("pnl_pct") is not None:
            row["paper_pnl_usd"] = round(row["paper_size_usd"] * (row["pnl_pct"] / 100.0), 2)

    return {
        "total": len(opps),
        "resolved": len(resolved),
        "wins": len(wins),
        "losses": len(losses),
        "pending": len(pending),
        "win_rate": round(len(wins) / len(resolved) * 100, 1) if resolved else None,
        "avg_win_pct": round(avg_win, 1) if avg_win is not None else None,
        "avg_loss_pct": round(avg_loss, 1) if avg_loss is not None else None,
        "total_pnl_pct": round(total_pnl_pct, 1),
        # Paper trading
        "paper_size_usd": PAPER_SIZE_USD,
        "paper_pnl_total": paper_pnl_total,
        "paper_staked_total": paper_staked_total,
        "paper_roi": paper_roi,
        "paper_bankroll": paper_bankroll,
        "paper_wins_pnl": paper_wins_pnl,
        "paper_losses_pnl": paper_losses_pnl,
        # Accuracy
        "wu_avg_error": wu_avg_error,
        "wu_error_samples": len(wu_errors),
        "bracket_hit_rate": bracket_hit_rate,
        "last_resolved": data.get("last_resolved"),
        "recent": recent_rows,
    }


def get_all() -> list:
    """Return all tracked opportunities (raw)."""
    return _load()["opportunities"]

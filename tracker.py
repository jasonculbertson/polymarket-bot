"""
Outcome tracker: records scanned opportunities and resolves their P&L
after the market resolution date passes.

Storage: DATA_DIR/outcomes.json  (persists across Railway deploys when volume is mounted)
"""

import json
import os
import requests
from datetime import datetime, date
from typing import Optional

DATA_DIR = os.environ.get("DATA_DIR", os.path.join(os.path.dirname(__file__), "data"))
OUTCOMES_FILE = os.path.join(DATA_DIR, "outcomes.json")
GAMMA_API = "https://gamma-api.polymarket.com"


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

def record_scan(yes_clusters, no_opps) -> int:
    """
    Add newly found opportunities to the tracker (skips duplicates).
    Returns total tracked opportunity count.
    """
    data = _load()
    existing_ids = {o["id"] for o in data["opportunities"]}
    now = datetime.utcnow().isoformat()
    added = 0

    for o in no_opps:
        oid = _no_id(o.market_id)
        if oid in existing_ids:
            continue
        data["opportunities"].append({
            "id": oid,
            "type": "no",
            "city": o.city,
            "bracket": o.group_title,
            "market_id": o.market_id,
            "no_token_id": o.no_token_id,
            "entry_price": round(o.no_price, 4),
            "return_pct": round(o.return_pct, 2),
            "forecast_temp": o.forecast_temp,
            "temp_unit": o.temp_unit,
            "confidence": o.forecast_confidence,
            "resolution_date": o.date,
            "first_seen": now,
            "outcome": None,
            "final_yes_price": None,
            "pnl_pct": None,
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
            "market_ids": mids,
            "yes_token_ids": [b.yes_token_id for b in c.brackets],
            "entry_price": round(c.total_price, 4),
            "return_pct": round(c.return_pct, 2),
            "cluster_size": c.cluster_size,
            "forecast_temp": c.forecast_temp,
            "temp_unit": c.temp_unit,
            "confidence": c.forecast_confidence,
            "resolution_date": c.date,
            "first_seen": now,
            "outcome": None,
            "final_yes_price": None,
            "pnl_pct": None,
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

    total_pnl = sum(o["pnl_pct"] or 0 for o in resolved)
    avg_win = sum(o["pnl_pct"] for o in wins) / len(wins) if wins else None
    avg_loss = sum(o["pnl_pct"] for o in losses) / len(losses) if losses else None

    return {
        "total": len(opps),
        "resolved": len(resolved),
        "wins": len(wins),
        "losses": len(losses),
        "pending": len(pending),
        "win_rate": round(len(wins) / len(resolved) * 100, 1) if resolved else None,
        "avg_win_pct": round(avg_win, 1) if avg_win is not None else None,
        "avg_loss_pct": round(avg_loss, 1) if avg_loss is not None else None,
        "total_pnl_pct": round(total_pnl, 1),
        "last_resolved": data.get("last_resolved"),
        "recent": sorted(opps, key=lambda o: o["first_seen"], reverse=True)[:50],
    }


def get_all() -> list:
    """Return all tracked opportunities (raw)."""
    return _load()["opportunities"]

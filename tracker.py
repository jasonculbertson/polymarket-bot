"""
Outcome tracker: records scanned opportunities and resolves their P&L
after the market resolution date passes.

Resolution: we do not require Polymarket. Once past resolution time we can infer
win/loss from the actual temperature for the forecast day. Actual temp is fetched
in order: (1) Wunderground PWS history (same source Polymarket uses) if WU_PWS_KEY
is set, (2) Polymarket Gamma API (which bracket won). Then we compare actual vs
bracket to infer outcome.

Paper trading: stake and P&L per position.
  - YES clusters (equal shares): stake = total_cost, win P&L = shares − total_cost (payout = shares×$1).
  - NO / single: stake = PAPER_SIZE_USD; win P&L = stake × (return_pct/100), loss = −stake.

Storage: DATA_DIR/outcomes.json  (persists across Railway deploys when volume is mounted)
"""

import json
import logging
import os
import re
import threading
import requests
from datetime import datetime, date, timedelta
from typing import Optional

log = logging.getLogger(__name__)

_tracker_lock = threading.Lock()

DATA_DIR = os.environ.get("DATA_DIR", os.path.join(os.path.dirname(__file__), "data"))
OUTCOMES_FILE = os.path.join(DATA_DIR, "outcomes.json")
GAMMA_API = "https://gamma-api.polymarket.com"

# Paper trading stake per position (USDC). Override via env var.
PAPER_SIZE_USD = float(os.environ.get("PAPER_SIZE_USD", "10.0"))

# ─── PostgreSQL persistence (Railway) ────────────────────────────────────────
# When DATABASE_URL is set (Railway provides this automatically after adding
# a Postgres plugin), outcomes are stored in Postgres instead of a local file.
# Falls back to file storage for local development.

DATABASE_URL = os.environ.get("DATABASE_URL", "")


def _pg_conn():
    import psycopg2
    return psycopg2.connect(DATABASE_URL)


def _pg_ensure_table():
    """Create the kv_store table if it doesn't exist (JSONB, TIMESTAMPTZ)."""
    conn = _pg_conn()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS kv_store (
                    key        TEXT PRIMARY KEY,
                    data       JSONB NOT NULL,
                    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
                )
            """)
            # Migrate existing TEXT column to JSONB if needed
            cur.execute("""
                DO $$
                BEGIN
                    IF EXISTS (
                        SELECT 1 FROM information_schema.columns
                        WHERE table_name='kv_store' AND column_name='data'
                          AND data_type='text'
                    ) THEN
                        ALTER TABLE kv_store ALTER COLUMN data TYPE JSONB
                            USING data::jsonb;
                    END IF;
                END$$;
            """)
        conn.commit()
    finally:
        conn.close()


def _pg_load(key: str) -> Optional[dict]:
    try:
        _pg_ensure_table()
        conn = _pg_conn()
        try:
            with conn.cursor() as cur:
                cur.execute("SELECT data FROM kv_store WHERE key = %s", (key,))
                row = cur.fetchone()
            if not row:
                return None
            val = row[0]
            # psycopg2 returns dict/list when column is JSONB, str when TEXT
            if isinstance(val, (dict, list)):
                return val
            return json.loads(val)
        finally:
            conn.close()
    except Exception as e:
        print(f"[WARN] PG load({key}) failed: {e}")
        return None


def _pg_save(key: str, data: dict):
    try:
        _pg_ensure_table()
        conn = _pg_conn()
        try:
            with conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO kv_store (key, data)
                    VALUES (%s, %s)
                    ON CONFLICT (key) DO UPDATE
                        SET data = EXCLUDED.data,
                            updated_at = NOW()
                """, (key, json.dumps(data)))
            conn.commit()
        finally:
            conn.close()
    except Exception as e:
        print(f"[WARN] PG save({key}) failed: {e}")


# ─── Bankroll ─────────────────────────────────────────────────────────────────

_BANKROLL_KEY = "bankroll"


def get_bankroll() -> float:
    """
    Return current bankroll (USDC). Reads from KV store; falls back to
    STRATEGY['initial_bankroll'] (default $200) on first run.
    """
    from config import STRATEGY
    initial = float(STRATEGY.get("initial_bankroll", 200.0))
    try:
        val = _pg_load(_BANKROLL_KEY)
        if val is not None:
            # Stored as {"amount": float, "updated_at": str}
            amt = val.get("amount") if isinstance(val, dict) else val
            if amt and float(amt) >= 10:  # sanity: bankroll < $10 is clearly corrupt
                return float(amt)
            elif amt:
                log.warning("[tracker] bankroll looks corrupt ($%.2f) — falling back to initial", float(amt))
    except Exception:
        pass
    # If no valid bankroll in Postgres, try live CLOB balance
    try:
        import trader as _trader
        live = _trader.get_balance()
        if live is not None and live >= 10:
            set_bankroll(live)  # persist so future calls use it
            return live
    except Exception:
        pass
    return initial


def set_bankroll(amount: float) -> float:
    """Persist bankroll to KV store. Returns the stored amount."""
    amount = round(max(0.01, amount), 2)
    _pg_save(_BANKROLL_KEY, {
        "amount":     amount,
        "updated_at": datetime.utcnow().isoformat(),
    })
    return amount


def add_to_bankroll(delta: float) -> float:
    """
    Add delta (profit) or subtract (loss) from bankroll.
    Returns new bankroll.  Thread-safe via optimistic read-modify-write.
    """
    current = get_bankroll()
    return set_bankroll(current + delta)


def get_daily_bankroll_stats() -> dict:
    """
    Return today's P&L vs bankroll, progress toward daily target, and
    whether the daily loss cap or win target has been triggered.

    Uses today's RESOLVED paper P&L (first_seen = today UTC).
    """
    from config import STRATEGY
    bankroll    = get_bankroll()
    today_pnl   = get_today_pnl()           # already defined below
    target_pct  = float(STRATEGY.get("daily_target_pct",   7.0))
    loss_cap_pct = float(STRATEGY.get("daily_loss_cap_pct", 10.0))
    target_usd  = round(bankroll * target_pct   / 100, 2)
    loss_cap_usd = round(bankroll * loss_cap_pct / 100, 2)
    today_pct   = round(today_pnl / bankroll * 100, 2) if bankroll else 0.0
    return {
        "bankroll":         bankroll,
        "today_pnl_usd":    today_pnl,
        "today_pnl_pct":    today_pct,
        "daily_target_usd": target_usd,
        "daily_target_pct": target_pct,
        "loss_cap_usd":     loss_cap_usd,
        "loss_cap_pct":     loss_cap_pct,
        "target_hit":       today_pct >= target_pct,
        "loss_cap_hit":     today_pct <= -loss_cap_pct,
        "pct_of_target":    round(today_pct / target_pct * 100, 1) if target_pct else 0.0,
    }


# ─── Storage ──────────────────────────────────────────────────────────────────

_OUTCOMES_KEY = "outcomes"


def _load() -> dict:
    if DATABASE_URL:
        pg = _pg_load(_OUTCOMES_KEY)
        if pg is not None:
            return pg
        # _pg_load returned None — either a connection failure or the key
        # genuinely doesn't exist yet (first ever run).
        # Check specifically whether the outcomes key exists:
        # - If it EXISTS but we couldn't read it → dangerous, raise so
        #   callers never overwrite real data with an empty state.
        # - If it DOESN'T EXIST → safe to return empty (first run).
        # - If Postgres is unreachable entirely → also raise.
        try:
            conn = _pg_conn()
            try:
                with conn.cursor() as cur:
                    cur.execute(
                        "SELECT 1 FROM kv_store WHERE key = %s LIMIT 1",
                        (_OUTCOMES_KEY,)
                    )
                    key_exists = cur.fetchone() is not None
            finally:
                conn.close()
            if key_exists:
                raise RuntimeError(
                    f"Postgres outcomes key exists but could not be read — "
                    "refusing to return empty state to avoid data loss"
                )
            # Key genuinely doesn't exist yet — first run, safe to start fresh
        except RuntimeError:
            raise
        except Exception as e:
            raise RuntimeError(f"Postgres unreachable, refusing to load empty state: {e}")

    if not os.path.exists(OUTCOMES_FILE):
        return {"opportunities": [], "last_resolved": None}
    with open(OUTCOMES_FILE) as f:
        return json.load(f)


def _save(data: dict):
    if DATABASE_URL:
        _pg_save(_OUTCOMES_KEY, data)
        return
    os.makedirs(DATA_DIR, exist_ok=True)
    with open(OUTCOMES_FILE, "w") as f:
        json.dump(data, f, indent=2)


def _migrate_forecast_dates(data: dict) -> bool:
    """
    One-time: set date (forecast day) to resolution_date - 1 for any opportunity
    where date is missing or >= resolution_date, so 3/8-style markets get date=3/8.
    Returns True if any change was made.
    """
    changed = False
    for opp in data.get("opportunities", []):
        res = opp.get("resolution_date") or ""
        if not res:
            continue
        day = opp.get("date") or ""
        need_fix = not day or day >= res
        if not need_fix:
            continue
        try:
            r = datetime.fromisoformat(res[:10]).date()
            forecast_day = (r - timedelta(days=1)).isoformat()
            opp["date"] = forecast_day
            changed = True
        except Exception:
            pass
    return changed


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
    try:
        data = _load()
    except RuntimeError as e:
        print(f"[WARN] record_scan: skipping tracking — {e}")
        return 0
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
            "consensus":    day_fc.get("consensus"),
        }

    # Build lookup for re-grading existing unresolved opportunities
    existing_by_id = {o["id"]: o for o in data["opportunities"]}
    updated = 0

    for o in no_opps:
        oid = _no_id(o.market_id)
        if oid in existing_ids:
            # Re-grade quality_tier and refresh price/edge fields for open positions.
            # This ensures config changes (e.g. moving A-tier thresholds) take effect
            # on already-tracked opportunities, not just newly discovered ones.
            existing = existing_by_id.get(oid)
            if existing and existing.get("outcome") is None and not existing.get("is_live"):
                existing["quality_tier"] = getattr(o, "quality_tier", existing["quality_tier"])
                existing["entry_price"]  = round(o.no_price, 4)
                existing["return_pct"]   = round(o.return_pct, 2)
                existing["prob_edge"]    = getattr(o, "prob_edge", existing.get("prob_edge"))
                existing["model_prob"]   = getattr(o, "model_prob", existing.get("model_prob"))
                existing["market_prob"]  = getattr(o, "market_prob", existing.get("market_prob"))
                existing["no_token_id"]  = o.no_token_id or existing.get("no_token_id", "")
                updated += 1
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
            "distance": round(o.distance_f, 2) if getattr(o, "distance_f", None) is not None else None,
            "temp_unit": o.temp_unit,
            "confidence": o.forecast_confidence,
            "predicted_win_prob": round(getattr(o, "predicted_win_prob", 0.75), 4),
            "forecast_sources": _get_sources(o.city, o.date),
            "date": o.date,
            "resolution_date": getattr(o, "resolution_date", None) or o.date,
            "resolution_time": getattr(o, "resolution_time", ""),
            "first_seen": now,
            "paper_size_usd": PAPER_SIZE_USD,
            "quality_tier": getattr(o, "quality_tier", "B"),  # A = clear winner, B = paper-only
            # Probability-edge model fields
            "model_prob":     getattr(o, "model_prob", None),
            "market_prob":    getattr(o, "market_prob", None),
            "prob_edge":      getattr(o, "prob_edge", None),
            "forecast_sigma": getattr(o, "forecast_sigma", None),
            "outcome": None,
            "final_yes_price": None,
            "pnl_pct": None,
            "paper_pnl_usd": None,
            "learned": False,
        })
        existing_ids.add(oid)
        added += 1

    # Build a set of (city, date) pairs already tracked as unresolved YES clusters
    # so we never accumulate multiple YES clusters for the same city on the same day.
    # Different scans can produce different bracket windows for the same city — we keep only the first.
    existing_yes_city_dates = {
        (o["city"], o.get("date", ""))
        for o in data["opportunities"]
        if o.get("type") == "yes" and o.get("outcome") is None
    }

    for c in yes_clusters:
        mids = [b.market_id for b in c.brackets]
        oid = _yes_id(c.event_slug, mids)
        if oid in existing_ids:
            # Re-grade quality_tier and refresh price/edge fields for open YES clusters.
            # Same pattern as NO opps — ensures config changes take effect on already-tracked entries.
            existing = existing_by_id.get(oid)
            if existing and existing.get("outcome") is None and not existing.get("is_live"):
                existing["quality_tier"] = getattr(c, "quality_tier", existing.get("quality_tier", "B"))
                existing["entry_price"]  = round(c.total_price, 4)
                existing["return_pct"]   = round(c.return_pct, 2)
                existing["prob_edge"]    = getattr(c, "prob_edge", existing.get("prob_edge"))
                existing["model_prob"]   = getattr(c, "model_prob", existing.get("model_prob"))
                existing["market_prob"]  = getattr(c, "market_prob", existing.get("market_prob"))
                updated += 1
            continue
        city_date_key = (c.city, c.date)
        if city_date_key in existing_yes_city_dates:
            continue  # already have an open YES cluster for this city+date
        existing_yes_city_dates.add(city_date_key)
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
            "shares": round(getattr(c, "shares", 0), 2),  # equal shares per bracket → same payout whichever wins
            "forecast_temp": c.forecast_temp,
            "temp_unit": c.temp_unit,
            "confidence": c.forecast_confidence,
            "predicted_win_prob": round(getattr(c, "predicted_win_prob", 0.75), 4),
            "forecast_sources": _get_sources(c.city, c.date),
            "date": c.date,
            "resolution_date": getattr(c, "resolution_date", None) or c.date,
            "resolution_time": getattr(c, "resolution_time", ""),
            "first_seen": now,
            "paper_size_usd": round(c.total_cost, 2),  # actual cost (shares × total_price)
            "quality_tier": getattr(c, "quality_tier", "B"),  # A = clear winner, B = paper-only
            # Probability-edge model fields
            "model_prob":     getattr(c, "model_prob", None),
            "market_prob":    getattr(c, "market_prob", None),
            "prob_edge":      getattr(c, "prob_edge", None),
            "forecast_sigma": getattr(c, "forecast_sigma", None),
            "outcome": None,
            "final_yes_price": None,
            "pnl_pct": None,
            "paper_pnl_usd": None,
            "learned": False,
        })
        existing_ids.add(oid)
        added += 1

    if added or updated:
        with _tracker_lock:
            _save(data)
    return len(data["opportunities"])


def record_scan_from_merged(merged: dict) -> int:
    """
    Add opportunities from a merged scan payload (dict form from Postgres) into the tracker.
    Used when serving /data so dashboard and outcomes stay in sync even if record_scan
    failed during the scan (e.g. transient DB error). Idempotent: skips existing IDs.
    """
    try:
        data = _load()
    except RuntimeError as e:
        print(f"[WARN] record_scan_from_merged: skip — {e}")
        return 0
    existing_ids = {o["id"] for o in data["opportunities"]}
    now = datetime.utcnow().isoformat()
    added = 0

    for o in merged.get("no_opportunities") or []:
        oid = _no_id(o.get("market_id", ""))
        if not oid or oid == "no_" or oid in existing_ids:
            continue
        data["opportunities"].append({
            "id": oid,
            "type": "no",
            "city": o.get("city", ""),
            "bracket": o.get("bracket", ""),
            "event_slug": o.get("event_slug", ""),
            "market_id": o.get("market_id", ""),
            "no_token_id": o.get("no_token_id", ""),
            "entry_price": round(float(o.get("no_price", 0) or 0), 4),
            "return_pct": round(float(o.get("return_pct", 0) or 0), 2),
            "forecast_temp": float(o.get("forecast_temp", 0) or 0),
            "distance": o.get("distance"),
            "temp_unit": o.get("temp_unit", "F"),
            "confidence": o.get("forecast_confidence", "medium"),
            "predicted_win_prob": float(o.get("predicted_win_prob", 0.75) or 0.75),
            "forecast_sources": {},
            "date": o.get("date", ""),
            "resolution_date": o.get("resolution_date") or o.get("date", ""),
            "resolution_time": o.get("resolution_time", ""),
            "first_seen": now,
            "paper_size_usd": PAPER_SIZE_USD,
            "quality_tier": o.get("quality_tier", "B"),
            "model_prob": o.get("model_prob"),
            "market_prob": o.get("market_prob"),
            "prob_edge": o.get("prob_edge"),
            "outcome": None,
            "final_yes_price": None,
            "pnl_pct": None,
            "paper_pnl_usd": None,
            "learned": False,
        })
        existing_ids.add(oid)
        added += 1

    for c in merged.get("yes_clusters") or []:
        brackets = c.get("brackets") or []
        mids = [b.get("market_id", "") for b in brackets if b.get("market_id")]
        if not mids:
            continue
        oid = _yes_id(c.get("event_slug", ""), mids)
        if oid in existing_ids:
            continue
        bracket_str = c.get("bracket") or " + ".join(b.get("group_title", "") for b in brackets)
        data["opportunities"].append({
            "id": oid,
            "type": "yes",
            "city": c.get("city", ""),
            "bracket": bracket_str,
            "event_slug": c.get("event_slug", ""),
            "market_ids": mids,
            "yes_token_ids": [b.get("yes_token_id", "") for b in brackets],
            "entry_price": round(float(c.get("total_price", 0) or 0), 4),
            "return_pct": round(float(c.get("return_pct", 0) or 0), 2),
            "cluster_size": int(c.get("cluster_size", 0) or 0),
            "shares": round(float(c.get("shares", 0) or 0), 2),
            "forecast_temp": float(c.get("forecast_temp", 0) or 0),
            "temp_unit": c.get("temp_unit", "F"),
            "confidence": c.get("forecast_confidence", "medium"),
            "predicted_win_prob": 0.75,
            "forecast_sources": {},
            "date": c.get("date", ""),
            "resolution_date": c.get("resolution_date") or c.get("date", ""),
            "resolution_time": c.get("resolution_time", ""),
            "first_seen": now,
            "paper_size_usd": round(float(c.get("total_cost", 0) or 0), 2) or round(PAPER_SIZE_USD * (c.get("cluster_size") or 1), 2),
            "outcome": None,
            "final_yes_price": None,
            "pnl_pct": None,
            "paper_pnl_usd": None,
            "learned": False,
        })
        existing_ids.add(oid)
        added += 1

    if added:
        with _tracker_lock:
            _save(data)
        print(f"[tracker] backfill: added {added} opportunities from merged scan")
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
    """Parse bracket label text and return its midpoint temperature."""
    # Range: "between 82-83°F" or "82-83°F"
    m = re.search(r"between (-?\d+(?:\.\d+)?)-(-?\d+(?:\.\d+)?)°[FC]", text)
    if m:
        return (float(m.group(1)) + float(m.group(2))) / 2.0
    m = re.search(r"(-?\d+(?:\.\d+)?)-(-?\d+(?:\.\d+)?)°[FC]", text)
    if m:
        return (float(m.group(1)) + float(m.group(2))) / 2.0
    # Or below/lower: "25°C or below"
    m = re.search(r"(-?\d+(?:\.\d+)?)°[FC] or (?:below|lower)", text, re.I)
    if m:
        return float(m.group(1)) - 0.5
    # Or above/higher: "37°C or higher"
    m = re.search(r"(-?\d+(?:\.\d+)?)°[FC] or (?:above|higher)", text, re.I)
    if m:
        return float(m.group(1)) + 0.5
    # Exact single value: "29°C" or "be 29°C"
    m = re.search(r"(?:^|be |reach )?(-?\d+(?:\.\d+)?)°[FC](?:\s*$|\s*\?)", text, re.I)
    if m:
        return float(m.group(1))
    # Last-resort bare number followed by degree sign anywhere
    m = re.search(r"\b(-?\d+(?:\.\d+)?)°", text)
    if m:
        return float(m.group(1))
    return None


def _parse_bracket_ranges(bracket_str: str) -> list:
    """Parse bracket string like '74-75°F' or '76-77°F + 78-79°F' into [(lo, hi), ...]. Returns [] on parse failure."""
    out = []
    # Split by "+" (with or without spaces) for YES clusters
    for part in re.split(r"\s*\+\s*", (bracket_str or "")):
        part = part.strip()
        m = re.search(r"between (-?\d+(?:\.\d+)?)-(-?\d+(?:\.\d+)?)[°º]?", part)
        if m:
            out.append((float(m.group(1)), float(m.group(2))))
            continue
        m = re.search(r"(-?\d+(?:\.\d+)?)-(-?\d+(?:\.\d+)?)[°º]?", part)
        if m:
            out.append((float(m.group(1)), float(m.group(2))))
            continue
        m = re.search(r"(-?\d+(?:\.\d+)?)[°º]?[FC]?\s*or\s*(?:below|lower)", part, re.I)
        if m:
            t = float(m.group(1))
            out.append((float("-inf"), t))
            continue
        m = re.search(r"(-?\d+(?:\.\d+)?)[°º]?[FC]?\s*or\s*(?:above|higher)", part, re.I)
        if m:
            t = float(m.group(1))
            out.append((t, float("inf")))
            continue
        m = re.search(r"(-?\d+(?:\.\d+)?)[°º]?", part)
        if m:
            t = float(m.group(1))
            out.append((t, t))
    return out


def _infer_outcome_from_actual_temp(opp: dict, actual_temp: float) -> Optional[str]:
    """
    Infer win/loss from actual temperature and bracket(s). Returns 'win', 'loss', or None if cannot infer.
    """
    unit = opp.get("temp_unit", "F")
    bracket_str = opp.get("bracket", "")
    ranges = _parse_bracket_ranges(bracket_str)
    if not ranges:
        return None
    if opp.get("type") == "no":
        (lo, hi) = ranges[0]
        if actual_temp < lo or actual_temp > hi:
            return "win"
        return "loss"
    # YES cluster: we win if actual_temp falls in any of our brackets
    for (lo, hi) in ranges:
        if lo <= actual_temp <= hi:
            return "win"
    return "loss"


def _forecast_date_for_actual(opp: dict) -> str:
    """
    Return the forecast day (YYYY-MM-DD) we need actual temp for. Weather markets
    resolve the morning after the forecast day, so use date if set and before
    resolution_date; else resolution_date - 1 day so 3/8-style markets always get 3/8.
    """
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


def _get_actual_temp_for_opp(opp: dict) -> Optional[float]:
    """
    Get actual high temp for the opportunity's forecast day. Prefer Wunderground
    (same source Polymarket uses) so we can resolve without Polymarket API.
    Order: (1) WU PWS history if WU_PWS_KEY set, (2) Gamma API (which bracket won).
    """
    forecast_day = _forecast_date_for_actual(opp)
    if not forecast_day:
        return _fetch_actual_temp_from_gamma(opp.get("event_slug", ""))
    try:
        from learner import fetch_actual_temperature
        actual = fetch_actual_temperature(
            opp.get("event_slug", ""),
            city=opp.get("city", ""),
            unit=opp.get("temp_unit", "F"),
            resolution_date=forecast_day,
        )
        if actual is not None:
            return actual
    except Exception:
        pass
    return _fetch_actual_temp_from_gamma(opp.get("event_slug", ""))


def _fetch_actual_temp_from_gamma(event_slug: str) -> Optional[float]:
    """
    Infer actual temperature from the resolved Polymarket event.
    Finds the bracket with YES price ≥ 0.95 and returns its midpoint.
    Tries closed=true first, then omits the filter so partially-settled
    events (where only the winner has resolved) are also caught.
    """
    def _scan_event(event: dict) -> Optional[float]:
        best_price = 0.0
        best_temp  = None
        for mkt in event.get("markets", []):
            prices_raw = mkt.get("outcomePrices", "[]")
            if isinstance(prices_raw, str):
                try:
                    prices_raw = json.loads(prices_raw)
                except Exception:
                    continue
            yes_price = float(prices_raw[0]) if prices_raw else 0.0
            if yes_price >= 0.90 and yes_price > best_price:
                label = mkt.get("groupItemTitle") or mkt.get("question", "")
                t = _parse_bracket_midpoint(label)
                if t is not None:
                    best_price = yes_price
                    best_temp  = t
        return best_temp

    for params in [{"slug": event_slug, "closed": "true"},
                   {"slug": event_slug}]:
        try:
            r = requests.get(f"{GAMMA_API}/events", params=params, timeout=10)
            if r.status_code != 200:
                continue
            events = r.json()
            if not events:
                continue
            event = events[0] if isinstance(events, list) else events
            t = _scan_event(event)
            if t is not None:
                return t
        except Exception:
            pass
    return None


def _backfill_resolution_times(data: dict) -> bool:
    """
    One-time backfill: fetch resolution_time from Gamma for any entry missing it.
    Called automatically from resolve_outcomes(). Returns True if any were filled.
    """
    missing = [o for o in data["opportunities"] if not o.get("resolution_time")]
    if not missing:
        return False

    filled = 0
    for o in missing:
        slug = o.get("event_slug", "")
        if not slug:
            continue
        for closed in ("false", "true"):
            try:
                r = requests.get(
                    f"{GAMMA_API}/events",
                    params={"slug": slug, "closed": closed},
                    timeout=8,
                )
                if r.status_code == 200:
                    events = r.json()
                    if events:
                        ev = events[0] if isinstance(events, list) else events
                        end = ev.get("endDate", "")
                        if not end and ev.get("markets"):
                            end = ev["markets"][0].get("endDate", "")  # full ISO timestamp only, not endDateIso
                        if end:
                            o["resolution_time"] = end
                            filled += 1
                            break
            except Exception:
                pass

    return filled > 0


def resolve_outcomes() -> int:
    """
    Check past-resolution-date opportunities and record wins/losses.
    Safe to call after every scan — only touches markets past their date.
    Also re-attempts actual_temp fetch for resolved rows still missing it.
    Returns count of newly resolved outcomes.
    """
    # Load snapshot under lock, do HTTP work outside lock, then save under lock
    with _tracker_lock:
        data = _load()
    # Manually fix dates: set date = resolution_date - 1 for any opp where date is missing or >= resolution_date (e.g. 3/8 markets)
    if _migrate_forecast_dates(data):
        with _tracker_lock:
            _save(data)
    # Use UTC now so Railway (always UTC) never fires before a market has actually closed.
    now_utc = datetime.utcnow()
    today = now_utc.date().isoformat()
    resolved_count = 0

    # Auto-backfill any entries missing resolution_time
    if _backfill_resolution_times(data):
        with _tracker_lock:
            _save(data)

    backfill_actual = 0
    for opp in data["opportunities"]:
        # Fix YES clusters: paper stake = total cost; backfill shares for equal-shares model
        if (opp.get("type") == "yes"
                and opp.get("cluster_size", 1) > 1):
            entry = float(opp.get("entry_price", 0) or 0)
            if entry > 0 and opp.get("paper_size_usd", PAPER_SIZE_USD) == PAPER_SIZE_USD:
                correct = round(PAPER_SIZE_USD * opp["cluster_size"], 2)
                opp["paper_size_usd"] = correct
                if opp.get("pnl_pct") is not None:
                    opp["paper_pnl_usd"] = round(correct * (opp["pnl_pct"] / 100.0), 2)
                backfill_actual += 1
            # Backfill shares so paper P&L uses equal-shares payout (shares×$1) when resolving
            if not opp.get("shares") and entry > 0:
                stake = opp.get("paper_size_usd") or round(PAPER_SIZE_USD * opp["cluster_size"], 2)
                opp["shares"] = round(stake / entry, 2)
                backfill_actual += 1

        # Re-attempt actual_temp for already-resolved rows that are missing it
        if opp["outcome"] is not None and opp.get("actual_temp") is None:
            actual = _get_actual_temp_for_opp(opp)
            if actual is not None:
                opp["actual_temp"] = actual
                wu_pred = (opp.get("forecast_sources") or {}).get("wunderground")
                if wu_pred is not None:
                    opp["wu_error"] = round(abs(wu_pred - actual), 1)
                backfill_actual += 1

    # resolution_date in the past → allow. Same day → allow after noon UTC (weather resolves morning local).
    noon_utc_on_res_date = None

    for opp in data["opportunities"]:
        if opp["outcome"] is not None:
            continue
        res_date = opp.get("resolution_date") or ""
        if not res_date:
            continue
        if res_date > today:
            continue
        if res_date == today:
            if noon_utc_on_res_date is None:
                noon_utc_on_res_date = now_utc.replace(hour=12, minute=0, second=0, microsecond=0)
            if now_utc < noon_utc_on_res_date:
                res_time = opp.get("resolution_time") or ""
                passed_res_time = False
                if res_time:
                    try:
                        res_dt = datetime.fromisoformat(res_time.replace("Z", "+00:00"))
                        res_dt_naive = res_dt.replace(tzinfo=None)
                        passed_res_time = res_dt_naive <= now_utc
                    except (ValueError, TypeError):
                        pass
                if not passed_res_time:
                    continue

        # Prefer Wunderground (previous day's observed high): resolve from actual temp first so we learn faster
        # and don't wait for Polymarket. Fall back to Polymarket market price only if we can't get actual temp.
        def _resolve_via_actual_temp() -> bool:
            actual = _get_actual_temp_for_opp(opp)
            if actual is None:
                return False
            outcome = _infer_outcome_from_actual_temp(opp, actual)
            if not outcome:
                return False
            opp["outcome"] = outcome
            opp["actual_temp"] = actual
            opp["final_yes_price"] = 0.0 if outcome == "win" else 1.0
            cost_basis = opp.get("execution_price") or opp["entry_price"]
            opp["pnl_pct"] = round((1.0 - cost_basis) / cost_basis * 100, 2) if outcome == "win" else -100.0
            wu_pred = (opp.get("forecast_sources") or {}).get("wunderground")
            if wu_pred is not None:
                opp["wu_error"] = round(abs(wu_pred - actual), 1)
            return True

        resolved = False
        if opp["type"] == "no":
            resolved = _resolve_via_actual_temp()
            if not resolved:
                final_yes = _fetch_market_yes_price(opp["market_id"])
                if final_yes is not None and (final_yes <= 0.05 or final_yes >= 0.95):
                    opp["outcome"] = "win" if final_yes <= 0.05 else "loss"
                    opp["final_yes_price"] = final_yes
                    cost_basis = opp.get("execution_price") or opp["entry_price"]
                    opp["pnl_pct"] = round((1.0 - cost_basis) / cost_basis * 100, 2) if final_yes <= 0.05 else -100.0
                    resolved = True
            if resolved:
                stake = opp.get("paper_size_usd", PAPER_SIZE_USD)
                opp["paper_size_usd"] = stake
                opp["paper_pnl_usd"] = round(stake * (opp["pnl_pct"] / 100.0), 2)
                if opp.get("actual_temp") is None:
                    actual = _get_actual_temp_for_opp(opp)
                    if actual is not None:
                        opp["actual_temp"] = actual
                        wu_pred = (opp.get("forecast_sources") or {}).get("wunderground")
                        if wu_pred is not None:
                            opp["wu_error"] = round(abs(wu_pred - actual), 1)
                # Compare simulated stop-loss to actual outcome
                if opp.get("simulated_stop_loss_triggered") and opp.get("simulated_exit_pnl_usd") is not None:
                    opp["stop_loss_saved_usd"] = round(
                        opp["simulated_exit_pnl_usd"] - opp["paper_pnl_usd"], 2
                    )  # positive = stop-loss would have helped
                # Compound bankroll: add resolved P&L so next bet sizes scale up with wins
                if opp.get("paper_pnl_usd") is not None:
                    try:
                        add_to_bankroll(opp["paper_pnl_usd"])
                    except Exception:
                        pass
                resolved_count += 1

        elif opp["type"] == "yes":
            resolved = _resolve_via_actual_temp()
            if not resolved:
                prices = [_fetch_market_yes_price(mid) for mid in opp.get("market_ids", [])]
                prices = [p for p in prices if p is not None]
                max_p = max(prices) if prices else None
                if max_p is not None and len(prices) >= len(opp.get("market_ids", [])):
                    if max_p >= 0.95:
                        opp["outcome"] = "win"
                        opp["final_yes_price"] = max_p
                        cost_basis = opp.get("execution_price") or opp["entry_price"]
                        opp["pnl_pct"] = round((1.0 - cost_basis) / cost_basis * 100, 2)
                        resolved = True
                    elif max_p <= 0.05:
                        opp["outcome"] = "loss"
                        opp["final_yes_price"] = max_p
                        opp["pnl_pct"] = -100.0
                        resolved = True
            if resolved:
                stake = opp.get("paper_size_usd") or round(PAPER_SIZE_USD * opp.get("cluster_size", 1), 2)
                opp["paper_size_usd"] = stake
                # Paper P&L: equal-shares model — payout = shares×$1 when any bracket wins
                if opp.get("outcome") == "win" and opp.get("shares") and opp["shares"] > 0:
                    opp["paper_pnl_usd"] = round(opp["shares"] - stake, 2)
                else:
                    opp["paper_pnl_usd"] = round(stake * (opp["pnl_pct"] / 100.0), 2)
                if opp.get("actual_temp") is None:
                    actual = _get_actual_temp_for_opp(opp)
                    if actual is not None:
                        opp["actual_temp"] = actual
                        wu_pred = (opp.get("forecast_sources") or {}).get("wunderground")
                        if wu_pred is not None:
                            opp["wu_error"] = round(abs(wu_pred - actual), 1)
                # Compare simulated stop-loss to actual outcome
                if opp.get("simulated_stop_loss_triggered") and opp.get("simulated_exit_pnl_usd") is not None:
                    opp["stop_loss_saved_usd"] = round(
                        opp["simulated_exit_pnl_usd"] - opp["paper_pnl_usd"], 2
                    )
                # Compound bankroll: add resolved P&L so next bet sizes scale up with wins
                if opp.get("paper_pnl_usd") is not None:
                    try:
                        add_to_bankroll(opp["paper_pnl_usd"])
                    except Exception:
                        pass
                resolved_count += 1

    if resolved_count or backfill_actual:
        data["last_resolved"] = datetime.utcnow().isoformat()
        with _tracker_lock:
            # Merge: re-read current state and preserve any live trade fields
            # that record_live_trade() may have written while we were resolving.
            fresh = _load()
            fresh_by_id = {o["id"]: o for o in fresh.get("opportunities", [])}
            _LIVE_FIELDS = ("is_live", "live_order_id", "live_size_usd", "shares",
                            "token_id", "execution_price", "exit_price", "exit_reason",
                            "live_at")
            for opp in data["opportunities"]:
                fresh_opp = fresh_by_id.get(opp["id"])
                if fresh_opp:
                    for field in _LIVE_FIELDS:
                        if fresh_opp.get(field) and not opp.get(field):
                            opp[field] = fresh_opp[field]
            # Also preserve the taken list
            fresh_taken = fresh.get("taken", [])
            data_taken = data.get("taken", [])
            data["taken"] = list(set(data_taken + fresh_taken))
            _save(data)

    return resolved_count


# ─── Claim winning live positions ─────────────────────────────────────────────

def redeem_all_live_wins() -> dict:
    """
    For every opportunity that is both live (real money placed) AND has just
    resolved as a win, attempt to sell the winning tokens on the CLOB or redeem
    them on-chain.

    Called automatically by the resolve cron after resolve_outcomes().
    Also exposed as POST /admin/redeem-all-wins.

    Returns {"attempted": n, "results": [...]}
    """
    import logging as _log_mod
    _log = _log_mod.getLogger(__name__)

    try:
        from trader import redeem_winning_position, get_condition_id_for_market, LIVE_MODE
    except ImportError:
        return {"attempted": 0, "results": [], "error": "trader module unavailable"}

    if not LIVE_MODE:
        return {"attempted": 0, "results": [], "note": "paper mode — no real redemptions"}

    from datetime import datetime, timedelta
    data = _load()

    # Auto-mark wins older than 7 days as redeemed (Polymarket auto-redeems within 24h).
    # This prevents infinite retry loops for stale positions.
    _stale_cutoff = datetime.utcnow() - timedelta(days=7)
    _stale_marked = []
    for o in data["opportunities"]:
        if o.get("outcome") != "win" or not o.get("is_live") or o.get("redeemed"):
            continue
        rd = o.get("resolved_at") or o.get("resolution_date") or ""
        if rd:
            try:
                rd_dt = datetime.fromisoformat(rd[:10])
                if rd_dt < _stale_cutoff:
                    o["redeemed"] = True
                    o["redeem_method"] = "auto_stale"
                    _stale_marked.append(o.get("id"))
            except Exception:
                pass
    if _stale_marked:
        _save(data)
        _log.warning("[tracker] auto-marked %d stale wins as redeemed: %s", len(_stale_marked), _stale_marked)

    live_wins = [
        o for o in data["opportunities"]
        if o.get("outcome") == "win"
        and o.get("is_live")
        and not o.get("redeemed")          # avoid re-processing
        and o.get("token_id")              # must have a real on-chain token ID
    ]

    results = []
    for opp in live_wins:
        token_id  = opp.get("token_id", "")
        market_id = opp.get("market_id", "")
        bet_type  = opp.get("type", "no")

        condition_id = ""
        if market_id:
            try:
                condition_id = get_condition_id_for_market(market_id) or ""
            except Exception:
                pass

        try:
            result = redeem_winning_position(token_id, condition_id, bet_type)
        except Exception as e:
            result = {"redeemed": False, "message": str(e)}

        result["opp_id"] = opp.get("id")
        results.append(result)

        if result.get("redeemed") or result.get("method") == "clob_sell":
            # Mark as redeemed so we don't retry
            with _tracker_lock:
                d = _load()
                for o in d["opportunities"]:
                    if o.get("id") == opp.get("id"):
                        o["redeemed"] = True
                        o["redeem_method"] = result.get("method", "unknown")
                        o["redeem_tx"] = result.get("tx_hash")
                        break
                _save(d)

        _log.warning("[tracker] redeem %s → %s", opp.get("id"), result.get("message", ""))

    return {"attempted": len(live_wins), "results": results}


def redeem_all_clob_wins() -> dict:
    """
    Scan ALL token balances on the CLOB for this account and redeem any winning
    positions — including ones not tracked by the bot (e.g. placed before the
    tracker recorded them, or placed manually).

    A token is winning when:
      - Its current YES price is ≥ 0.99 (market resolved YES) AND we hold YES tokens
      - Its current YES price is ≤ 0.01 (market resolved NO)  AND we hold NO tokens
        → NO tokens are the complement, so NO_token price = 1 - YES_price

    Uses the Gamma API positions endpoint to get all token holdings, then
    calls redeem_winning_position for any that look resolved.

    Returns {"attempted": n, "results": [...]}
    """
    import logging as _log_mod
    import requests as _req
    _log = _log_mod.getLogger(__name__)

    try:
        from trader import redeem_winning_position, LIVE_MODE, POLY_FUNDER
    except ImportError:
        return {"attempted": 0, "results": [], "error": "trader module unavailable"}

    if not LIVE_MODE:
        return {"attempted": 0, "results": [], "note": "paper mode"}

    # POLY_FUNDER is the actual Polymarket proxy wallet address where tokens are held
    wallet = (POLY_FUNDER or "").lower()
    if not wallet:
        return {"attempted": 0, "results": [], "error": "POLY_FUNDER not set — cannot query positions"}

    # Fetch positions from Gamma API
    try:
        # data-api returns positions with redeemable=true already filtered
        r = _req.get(
            "https://data-api.polymarket.com/positions",
            params={"user": wallet, "sizeThreshold": "0", "redeemable": "true"},
            timeout=15,
        )
        r.raise_for_status()
        body = r.json()
        positions = body if isinstance(body, list) else body.get("positions", [])
    except Exception as e:
        _log.warning("[tracker] redeem_all_clob_wins: positions fetch failed: %s", e)
        return {"attempted": 0, "results": [], "error": f"positions fetch failed: {e}"}

    results = []
    # Also load already-redeemed token IDs so we don't retry
    data = _load()
    already_redeemed = {
        o.get("token_id") for o in data["opportunities"]
        if o.get("redeemed") and o.get("token_id")
    }

    for pos in positions:
        size         = float(pos.get("size") or 0)
        token_id     = str(pos.get("asset") or "")
        condition_id = str(pos.get("conditionId") or "")
        outcome      = str(pos.get("outcome") or "").lower()  # "yes" or "no"

        if not token_id or size < 0.01:   # skip dust positions (< 1¢)
            continue
        if token_id in already_redeemed:
            continue
        if not pos.get("redeemable"):
            continue

        bet_type = outcome if outcome in ("yes", "no") else "no"

        _log.warning("[tracker] redeem_all_clob_wins: redeeming %s token=%s size=%.3f",
                     bet_type, token_id[:16], size)
        try:
            result = redeem_winning_position(token_id, condition_id, bet_type)
        except Exception as e:
            result = {"redeemed": False, "message": str(e)}

        result["token_id"] = token_id
        result["condition_id"] = condition_id
        results.append(result)

        if result.get("redeemed"):
            already_redeemed.add(token_id)

    return {"attempted": len(results), "results": results}


# ─── Summary ──────────────────────────────────────────────────────────────────

def get_summary() -> dict:
    """Return P&L summary for the outcomes dashboard tab."""
    data = _load()
    opps = data["opportunities"]
    resolved = [o for o in opps if o["outcome"] is not None]
    wins = [o for o in resolved if o["outcome"] == "win"]
    losses = [o for o in resolved if o["outcome"] == "loss"]
    pending = [o for o in opps if o["outcome"] is None]

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
    recent_rows = sorted(opps, key=lambda o: o.get("first_seen", ""), reverse=True)[:100]
    for row in recent_rows:
        # Mutate in-place — recent_rows is already an in-memory copy from _load()
        if not row.get("paper_size_usd"):
            row["paper_size_usd"] = PAPER_SIZE_USD
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


def get_today_pnl() -> float:
    """Return sum of paper_pnl_usd for all positions first_seen today (UTC)."""
    data = _load()
    today = datetime.utcnow().date().isoformat()
    total = 0.0
    for opp in data["opportunities"]:
        if opp.get("outcome") is None:
            continue
        if not (opp.get("first_seen") or "").startswith(today):
            continue
        pnl = opp.get("paper_pnl_usd")
        if pnl is not None:
            total += pnl
    return round(total, 2)


def get_all() -> list:
    """Return all tracked opportunities (raw)."""
    return _load()["opportunities"]


# ─── Live trade helpers ────────────────────────────────────────────────────────

def record_live_trade(
    opp_id: str,
    order_id: str,
    size_usd: float,
    shares: float,
    token_id: str,
    execution_price: Optional[float] = None,
) -> bool:
    """
    Mark an opportunity as live-traded. Call this right after buy() succeeds.
    Returns True if the opportunity was found and updated.
    """
    if shares <= 0:
        log.error("[tracker] record_live_trade: invalid shares=%s for opp=%s — skipping", shares, opp_id)
        return False
    with _tracker_lock:
        data = _load()
        for opp in data["opportunities"]:
            if opp["id"] == opp_id:
                opp["is_live"]          = True
                opp["live_order_id"]    = order_id
                opp["live_size_usd"]    = size_usd
                opp["shares"]           = shares
                opp["token_id"]         = token_id
                opp["execution_price"]  = execution_price  # actual fill price (None = paper/unknown)
                opp["exit_price"]       = None
                opp["exit_reason"]      = None
                opp["live_at"]          = datetime.utcnow().isoformat()
                # Add to taken set so auto-execute never re-places this bet
                taken_list = data.setdefault("taken", [])
                if opp_id not in taken_list:
                    taken_list.append(opp_id)
                log.warning("[tracker] marked %s as taken (%d total)", opp_id, len(taken_list))
                _save(data)
                # Also persist to a SEPARATE "live_bets" key so this dedup
                # survives race conditions where resolve_outcomes or
                # update_open_position_prices overwrites the main outcomes blob.
                try:
                    lb = _pg_load("live_bets") or {"ids": []}
                    lb_ids = lb.get("ids", [])
                    if opp_id not in lb_ids:
                        lb_ids.append(opp_id)
                    _pg_save("live_bets", {
                        "ids":     lb_ids,
                        "updated": datetime.utcnow().isoformat(),
                    })
                    log.warning("[tracker] live_bets key updated: %d IDs", len(lb_ids))
                except Exception as _lbe:
                    log.warning("[tracker] WARNING: could not save live_bets key: %s", _lbe)
                return True
    log.warning("[tracker] record_live_trade: opp_id %r not found in tracker", opp_id)
    return False


def update_open_position_prices() -> dict:
    """
    For all unresolved paper (and live) positions, fetch current market prices and update:
      - current_price: current token value (NO = 1-YES price, YES = sum of bracket YES prices)
      - unrealized_pnl_pct / unrealized_pnl_usd: mark-to-market vs entry
      - price_updated_at: UTC timestamp of last price fetch

    Also runs simulated stop-loss check:
      - Triggers if loss >= stop_loss_pct AND hours_to_resolution >= stop_loss_min_hours
      - Records simulated_stop_loss_price, simulated_exit_pnl_usd for later comparison vs actual
      - At resolution, stop_loss_saved_usd = simulated_exit_pnl - actual_pnl (+ = SL helped)

    Returns {updated, stop_loss_triggered, errors}.
    Safe to call every scan cycle — skips resolved positions.
    """
    from config import TRADING
    stop_loss_pct    = TRADING.get("stop_loss_pct", 8) / 100.0
    take_profit_pct  = TRADING.get("take_profit_pct", 10) / 100.0
    force_exit_hours = TRADING.get("force_exit_hours_before_resolution", 24.0)
    min_hours        = TRADING.get("stop_loss_min_hours_to_resolution", 4)

    try:
        data = _load()
    except RuntimeError as e:
        return {"updated": 0, "stop_loss_triggered": 0, "errors": 1, "detail": str(e)}

    updated = 0
    stop_loss_triggered = 0
    errors = 0
    live_exits = []   # live positions that crossed a stop-loss / take-profit / force-exit threshold
    now = datetime.utcnow()
    changed = False

    open_opps = [o for o in data["opportunities"] if o.get("outcome") is None]

    for opp in open_opps:
        try:
            if opp["type"] == "no":
                market_id = opp.get("market_id")
                if not market_id:
                    continue
                yes_price = _fetch_market_yes_price(market_id)
                if yes_price is None:
                    continue
                current_price = round(1.0 - yes_price, 4)

            elif opp["type"] == "yes":
                market_ids = opp.get("market_ids") or []
                if not market_ids:
                    continue
                prices = [_fetch_market_yes_price(mid) for mid in market_ids]
                prices = [p for p in prices if p is not None]
                if not prices:
                    continue
                # Sum of current YES prices = total current cost of the cluster
                # (mirrors how entry_price = total_price = sum of bracket prices)
                current_price = round(sum(prices), 4)
            else:
                continue

            entry_price = float(opp["entry_price"])
            opp["current_price"]    = current_price
            opp["price_updated_at"] = now.isoformat()

            if entry_price > 0:
                pnl_pct = round((current_price - entry_price) / entry_price * 100, 2)
                opp["unrealized_pnl_pct"] = pnl_pct
                size = float(opp.get("paper_size_usd") or 0)
                if size > 0:
                    opp["unrealized_pnl_usd"] = round(size * pnl_pct / 100.0, 2)

            updated += 1
            changed = True

            # ── Paper early-exit simulation (mirrors live monitor logic) ─────
            # Resolve paper positions using the same stop-loss / take-profit /
            # force-exit thresholds as live trading. This gives accurate P&L data
            # for deciding when to promote YES (or NO) to live.
            if entry_price <= 0:
                continue

            # Compute hours to resolution
            hours_left = float("inf")
            res_time = opp.get("resolution_time") or opp.get("resolution_date") or ""
            if res_time:
                try:
                    from datetime import timezone
                    if "T" in str(res_time):
                        res_dt = datetime.fromisoformat(str(res_time).replace("Z", "+00:00"))
                    else:
                        res_dt = datetime.fromisoformat(f"{res_time}T23:59:00+00:00")
                    now_utc = datetime.now(timezone.utc)
                    hours_left = (res_dt - now_utc).total_seconds() / 3600
                except Exception:
                    pass

            size = float(opp.get("paper_size_usd") or 0)
            pnl_chg = (current_price - entry_price) / entry_price

            def _resolve_paper(reason: str, exit_p: float):
                """Mark this paper position as resolved at exit_p."""
                pnl = round(pnl_chg * 100, 2)
                opp["outcome"]       = "win" if pnl_chg > 0 else "loss"
                opp["exit_reason"]   = reason
                opp["exit_price"]    = round(exit_p, 4)
                opp["exit_at"]       = now.isoformat()
                opp["pnl_pct"]       = pnl
                if size > 0:
                    opp["paper_pnl_usd"] = round(size * pnl_chg, 2)
                print(f"[paper-exit] {reason.upper()} {opp['type'].upper()} "
                      f"{opp.get('city')} entry={entry_price:.3f} exit={exit_p:.3f} "
                      f"pnl={pnl:+.1f}%")

            is_live = opp.get("is_live", False)
            exit_reason = None

            # Priority 1: force exit — within N hours of resolution
            if hours_left <= force_exit_hours and hours_left >= 0:
                exit_reason = "force_exit"
            # Priority 2: stop-loss
            elif pnl_chg <= -stop_loss_pct and hours_left > min_hours:
                exit_reason = "stop_loss"
            # Priority 3: take-profit
            elif take_profit_pct > 0 and pnl_chg >= take_profit_pct:
                exit_reason = "take_profit"

            if exit_reason:
                stop_loss_triggered += 1
                if is_live:
                    # For live positions: signal caller to execute a real sell.
                    # Build token list: YES clusters have yes_token_ids, NO bets have no_token_id.
                    tokens = opp.get("yes_token_ids") or []
                    if not tokens:
                        tok = opp.get("no_token_id") or opp.get("token_id")
                        if tok:
                            tokens = [tok]
                    shares_each = float(opp.get("shares") or 0)
                    live_exits.append({
                        "opp_id":      opp.get("id"),
                        "city":        opp.get("city"),
                        "exit_reason": exit_reason,
                        "current_price": current_price,
                        "pnl_pct":     round(pnl_chg * 100, 2),
                        "tokens":      tokens,
                        "shares_each": shares_each,
                    })
                    # Don't mark outcome yet — caller will call mark_exited_early after sell
                else:
                    _resolve_paper(exit_reason, current_price)
                continue

        except Exception as e:
            errors += 1
            print(f"[WARN] update_open_position_prices {opp.get('id')}: {e}")

    if changed:
        with _tracker_lock:
            # Merge: preserve live trade fields that may have been written concurrently
            fresh = _load()
            fresh_by_id = {o["id"]: o for o in fresh.get("opportunities", [])}
            _LIVE_FIELDS = ("is_live", "live_order_id", "live_size_usd", "shares",
                            "token_id", "execution_price", "exit_price", "exit_reason",
                            "live_at")
            for opp in data["opportunities"]:
                fresh_opp = fresh_by_id.get(opp["id"])
                if fresh_opp:
                    for field in _LIVE_FIELDS:
                        if fresh_opp.get(field) and not opp.get(field):
                            opp[field] = fresh_opp[field]
            data["taken"] = list(set(data.get("taken", []) + fresh.get("taken", [])))
            _save(data)

    print(f"[price-monitor] {updated} positions updated | "
          f"{stop_loss_triggered} exit triggers ({len(live_exits)} live) | {errors} errors")
    return {
        "updated": updated,
        "stop_loss_triggered": stop_loss_triggered,
        "live_exits": live_exits,
        "errors": errors,
    }


def _bracket_dist(forecast: float, lo, hi) -> float:
    """Distance from forecast to nearest bracket edge. 0.0 = inside bracket."""
    if lo is None and hi is not None:
        return 0.0 if forecast <= hi else forecast - hi
    if hi is None and lo is not None:
        return 0.0 if forecast >= lo else lo - forecast
    if lo is not None and hi is not None:
        if lo <= forecast <= hi:
            return 0.0
        return (lo - forecast) if forecast < lo else (forecast - hi)
    return float("inf")


def _hours_to_resolution(opp: dict) -> float:
    """
    Compute hours between now (UTC) and the position's resolution time.
    Falls back to end-of-day on resolution_date if resolution_time is absent.
    Returns float('inf') if neither field is present.
    """
    now = datetime.utcnow()

    # Try exact resolution_time first
    rt = opp.get("resolution_time")
    if rt:
        try:
            # Strip trailing Z and parse
            rt_clean = rt.rstrip("Z").split("+")[0]
            res_dt = datetime.fromisoformat(rt_clean)
            return max(0.0, (res_dt - now).total_seconds() / 3600)
        except Exception:
            pass

    # Fall back to end-of-day on resolution_date
    rd = opp.get("resolution_date") or opp.get("date")
    if rd:
        try:
            res_dt = datetime.fromisoformat(rd) + timedelta(hours=23, minutes=59)
            return max(0.0, (res_dt - now).total_seconds() / 3600)
        except Exception:
            pass

    return float("inf")


def check_forecast_drift(all_forecasts: dict = None) -> dict:
    """
    For all open positions, compare the CURRENT forecast vs the forecast at entry time.
    If the forecast has drifted enough to eliminate our edge, flag the position.

    Edge-gone criteria:
      NO bet  : current distance to bracket < no_min_distance threshold
                (forecast moved close enough that the bracket could resolve YES)
      YES cluster: current forecast is outside the cluster's win range
                   (forecast moved beyond the brackets we bought)

    Time-aware thresholds — as resolution approaches, the "forecast" converges
    to the observed temperature rather than a genuine prediction, so we apply
    progressively higher bars to avoid false positives:

      > 24h to resolution : standard thresholds (genuine forecast uncertainty)
      12–24h              : thresholds raised ~65% (forecast less predictive)
      < 12h               : no edge_gone flags — let it ride to resolution

    Flags positions with:
      edge_gone               : True
      edge_gone_at            : UTC timestamp
      edge_gone_reason        : human-readable explanation
      edge_gone_forecast_temp : what the forecast shifted to
      edge_gone_entry_forecast: what we entered at

    all_forecasts: optional pre-fetched {city: forecast_dict}. If None, fetches
                   fresh data for each city that has open positions.

    Returns {checked, flagged, skipped_near_resolution, errors}.
    Safe to call every scan cycle — skips already-flagged and resolved positions.
    """
    from config import STRATEGY

    try:
        data = _load()
    except RuntimeError as e:
        return {"checked": 0, "flagged": 0, "errors": 1, "detail": str(e)}

    open_opps = [o for o in data["opportunities"]
                 if o.get("outcome") is None and not o.get("edge_gone")]
    if not open_opps:
        return {"checked": 0, "flagged": 0, "errors": 0}

    # Fetch current forecasts for affected cities if not supplied
    if all_forecasts is None:
        try:
            from fetch_forecasts import fetch_city_forecast
            cities_needed = list({o["city"] for o in open_opps if o.get("city")})
            all_forecasts = {}
            for city in cities_needed:
                try:
                    all_forecasts[city] = fetch_city_forecast(city, days=2)
                except Exception as exc:
                    print(f"  [drift] forecast fetch failed for {city}: {exc}")
        except Exception as exc:
            return {"checked": 0, "flagged": 0, "errors": 1, "detail": str(exc)}

    no_min_dist_f = float(STRATEGY.get("no_min_distance_f", 6.0))
    no_min_dist_c = float(STRATEGY.get("no_min_distance_c", 3.5))

    # Time-aware multipliers: as resolution nears, "forecast" → observed temp.
    # Raise bar proportionally so intraday convergence doesn't trigger false flags.
    # < 12h: skip entirely  |  12-24h: 1.65× harder to flag  |  >24h: standard
    _NEAR_HOURS   = 12.0   # below this → skip drift check entirely
    _MEDIUM_HOURS = 24.0   # below this → raise threshold by multiplier
    _MEDIUM_MULT  = 1.65   # threshold multiplier for 12-24h window

    now_str = datetime.utcnow().isoformat()
    checked = flagged = skipped_near = errors = 0
    changed = False

    for opp in open_opps:
        try:
            city     = opp.get("city", "")
            date_str = opp.get("date", "")
            unit     = opp.get("temp_unit", "F")

            # ── Time gate ────────────────────────────────────────────────────
            hrs_left = _hours_to_resolution(opp)
            if hrs_left < _NEAR_HOURS:
                # Market resolves soon — "forecast" is basically observed temp.
                # No point flagging; let it ride to resolution.
                skipped_near += 1
                continue

            # Threshold multiplier for the 12–24h window
            threshold_mult = _MEDIUM_MULT if hrs_left < _MEDIUM_HOURS else 1.0

            city_fc = (all_forecasts or {}).get(city, {})
            day_fc  = city_fc.get("forecasts", {}).get(date_str)
            if not day_fc:
                continue

            current_fc = day_fc.get("wunderground") or day_fc.get("consensus")
            if current_fc is None:
                continue

            entry_fc     = float(opp.get("forecast_temp") or current_fc)
            fc_shift     = round(abs(current_fc - entry_fc), 1)
            bracket_str  = opp.get("bracket", "")
            ranges       = _parse_bracket_ranges(bracket_str)
            if not ranges:
                continue

            checked += 1
            edge_gone = False
            reason = ""

            if opp["type"] == "no":
                lo, hi = ranges[0]
                lo = None if lo == float("-inf") else lo
                hi = None if hi == float("inf") else hi
                current_dist = _bracket_dist(current_fc, lo, hi)
                min_dist     = (no_min_dist_f if unit == "F" else no_min_dist_c) * threshold_mult
                entry_dist   = float(opp.get("distance") or min_dist + 1)
                if current_dist < min_dist:
                    edge_gone = True
                    reason = (f"Forecast drifted {fc_shift}°{unit}: distance "
                              f"{entry_dist:.1f}°→{current_dist:.1f}°{unit} "
                              f"(min {min_dist:.1f}° required, {hrs_left:.0f}h to resolution)")

            elif opp["type"] == "yes":
                lows  = [lo for lo, hi in ranges if lo != float("-inf")]
                highs = [hi for lo, hi in ranges if hi != float("inf")]
                win_lo = min(lows)  if lows  else float("-inf")
                win_hi = max(highs) if highs else float("inf")
                # In the 12-24h window, only flag if drift is large enough
                # (threshold_mult applied as required minimum shift)
                base_drift_threshold = 1.5 if unit == "C" else 2.7   # °C / °F
                required_shift = base_drift_threshold * threshold_mult
                if not (win_lo <= current_fc <= win_hi) and fc_shift >= required_shift:
                    edge_gone = True
                    reason = (f"Forecast drifted {fc_shift}°{unit}: "
                              f"{current_fc:.1f}°{unit} outside win range "
                              f"[{win_lo:.0f}–{win_hi:.0f}°{unit}] "
                              f"({hrs_left:.0f}h to resolution)")

            if edge_gone:
                opp["edge_gone"]                = True
                opp["edge_gone_at"]             = now_str
                opp["edge_gone_reason"]         = reason
                opp["edge_gone_forecast_temp"]  = current_fc
                opp["edge_gone_entry_forecast"] = entry_fc
                opp["edge_gone_hrs_left"]       = round(hrs_left, 1)
                print(f"  [drift] ⚠ Edge gone: {city} {date_str} "
                      f"{opp['type'].upper()} {bracket_str} — {reason}")
                flagged += 1
                changed = True

        except Exception as exc:
            errors += 1
            print(f"[WARN] check_forecast_drift {opp.get('id')}: {exc}")

    if changed:
        with _tracker_lock:
            _save(data)

    print(f"[forecast-drift] {checked} checked | {flagged} edge-gone | "
          f"{skipped_near} skipped (<{_NEAR_HOURS:.0f}h) | {errors} errors")
    return {"checked": checked, "flagged": flagged,
            "skipped_near_resolution": skipped_near, "errors": errors}


def get_live_positions() -> list:
    """Return all positions that have real money in them and are not yet exited."""
    data = _load()
    return [
        o for o in data["opportunities"]
        if o.get("is_live")
        and o.get("outcome") is None
        and o.get("exit_reason") is None
        and o.get("token_id")
    ]


def mark_stopped_out(opp_id: str, exit_price: float) -> bool:
    """Record that a position was exited via stop-loss."""
    return _mark_exit(opp_id, exit_price, "stop_loss")


def mark_exited_early(opp_id: str, exit_price: float, reason: str = "take_profit") -> bool:
    """Record that a position was sold early (stop_loss, take_profit, or force_exit)."""
    return _mark_exit(opp_id, exit_price, reason)


def mark_order_cancelled(opp_id: str) -> bool:
    """
    Mark a live position as cancelled because its CLOB buy order never filled.

    Sets is_live=False, outcome=loss (no real P&L), exit_reason=order_cancelled.
    Also removes opp_id from the live_bets Postgres key so it stops appearing
    in get_live_positions() and the monitor loop.
    """
    with _tracker_lock:
        data = _load()
        for opp in data["opportunities"]:
            if opp["id"] == opp_id:
                opp["is_live"]       = False
                opp["outcome"]       = "loss"
                opp["exit_reason"]   = "order_cancelled"
                opp["exit_at"]       = datetime.utcnow().isoformat()
                opp["exit_price"]    = opp.get("entry_price", 0)
                opp["pnl_pct"]       = 0.0
                opp["paper_pnl_usd"] = 0.0

                _save(data)

                # Also remove from Postgres live_bets key so cancelled IDs
                # don't permanently block re-tries on future scans.
                try:
                    lb = _pg_load("live_bets") or {}
                    lb_ids = lb.get("ids", [])
                    if opp_id in lb_ids:
                        lb_ids.remove(opp_id)
                        _pg_save("live_bets", {"ids": lb_ids, "updated": datetime.utcnow().isoformat()})
                except Exception as _lbe:
                    log.warning("[tracker] mark_order_cancelled: could not update pg live_bets: %s", _lbe)

                log.info("[tracker] marked order_cancelled: %s", opp_id)
                return True
    log.warning("[tracker] mark_order_cancelled: opp_id not found: %s", opp_id)
    return False


def _mark_exit(opp_id: str, exit_price: float, reason: str) -> bool:
    with _tracker_lock:
        data = _load()
        for opp in data["opportunities"]:
            if opp["id"] == opp_id:
                shares = opp.get("shares", 0)
                stake  = opp.get("live_size_usd") or opp.get("paper_size_usd", PAPER_SIZE_USD)

                proceeds    = shares * exit_price
                pnl_usd     = round(proceeds - stake, 2)
                pnl_pct     = round((proceeds - stake) / stake * 100, 2) if stake else 0.0

                opp["is_live"]       = False   # explicitly closed — no longer active
                opp["exit_price"]    = exit_price
                opp["exit_reason"]   = reason
                opp["exit_at"]       = datetime.utcnow().isoformat()
                opp["pnl_pct"]       = pnl_pct
                opp["paper_pnl_usd"] = pnl_usd
                opp["outcome"]       = "win" if pnl_usd > 0 else "loss"
                _save(data)

                # Clean up Postgres live_bets dedup key so exited positions
                # don't permanently block future scans for the same market.
                try:
                    lb = _pg_load("live_bets") or {}
                    lb_ids = lb.get("ids", [])
                    if opp_id in lb_ids:
                        lb_ids.remove(opp_id)
                        _pg_save("live_bets", {"ids": lb_ids, "updated": datetime.utcnow().isoformat()})
                except Exception as _lbe:
                    log.warning("[tracker] _mark_exit: could not update pg live_bets: %s", _lbe)

                return True
    return False

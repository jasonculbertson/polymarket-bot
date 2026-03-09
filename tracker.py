"""
Outcome tracker: records scanned opportunities and resolves their P&L
after the market resolution date passes.

Resolution: we do not require Polymarket. Once past resolution time we can infer
win/loss from the actual temperature for the forecast day. Actual temp is fetched
in order: (1) Wunderground PWS history (same source Polymarket uses) if WU_PWS_KEY
is set, (2) Polymarket Gamma API (which bracket won). Then we compare actual vs
bracket to infer outcome.

Paper trading is simulated at PAPER_SIZE_USD per position.
  - Win  P&L = PAPER_SIZE_USD × (return_pct / 100)
  - Loss P&L = −PAPER_SIZE_USD

Storage: DATA_DIR/outcomes.json  (persists across Railway deploys when volume is mounted)
"""

import json
import os
import re
import threading
import requests
from datetime import datetime, date, timedelta
from typing import Optional

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
            "date": o.date,
            "resolution_date": getattr(o, "resolution_date", None) or o.date,
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
            "date": c.date,
            "resolution_date": getattr(c, "resolution_date", None) or c.date,
            "resolution_time": getattr(c, "resolution_time", ""),
            "first_seen": now,
            "paper_size_usd": round(PAPER_SIZE_USD * c.cluster_size, 2),
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
            "temp_unit": o.get("temp_unit", "F"),
            "confidence": o.get("forecast_confidence", "medium"),
            "predicted_win_prob": 0.75,
            "forecast_sources": {},
            "date": o.get("date", ""),
            "resolution_date": o.get("resolution_date") or o.get("date", ""),
            "resolution_time": o.get("resolution_time", ""),
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
            "forecast_temp": float(c.get("forecast_temp", 0) or 0),
            "temp_unit": c.get("temp_unit", "F"),
            "confidence": c.get("forecast_confidence", "medium"),
            "predicted_win_prob": 0.75,
            "forecast_sources": {},
            "date": c.get("date", ""),
            "resolution_date": c.get("resolution_date") or c.get("date", ""),
            "resolution_time": c.get("resolution_time", ""),
            "first_seen": now,
            "paper_size_usd": round(PAPER_SIZE_USD * (c.get("cluster_size") or 1), 2),
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
    # Split by " + " for YES clusters
    for part in (bracket_str or "").split("+"):
        part = part.strip()
        m = re.search(r"between (-?\d+(?:\.\d+)?)-(-?\d+(?:\.\d+)?)°", part)
        if m:
            out.append((float(m.group(1)), float(m.group(2))))
            continue
        m = re.search(r"(-?\d+(?:\.\d+)?)-(-?\d+(?:\.\d+)?)°", part)
        if m:
            out.append((float(m.group(1)), float(m.group(2))))
            continue
        m = re.search(r"(-?\d+(?:\.\d+)?)°\s*or\s*(?:below|lower)", part, re.I)
        if m:
            t = float(m.group(1))
            out.append((float("-inf"), t))
            continue
        m = re.search(r"(-?\d+(?:\.\d+)?)°\s*or\s*(?:above|higher)", part, re.I)
        if m:
            t = float(m.group(1))
            out.append((t, float("inf")))
            continue
        m = re.search(r"(-?\d+(?:\.\d+)?)°", part)
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
        # Fix YES clusters that were recorded with single-bracket paper stake
        if (opp.get("type") == "yes"
                and opp.get("cluster_size", 1) > 1
                and opp.get("paper_size_usd", PAPER_SIZE_USD) == PAPER_SIZE_USD):
            correct = round(PAPER_SIZE_USD * opp["cluster_size"], 2)
            opp["paper_size_usd"] = correct
            if opp.get("pnl_pct") is not None:
                opp["paper_pnl_usd"] = round(correct * (opp["pnl_pct"] / 100.0), 2)
            backfill_actual += 1  # reuse flag to trigger save

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
            opp["pnl_pct"] = round((1.0 - opp["entry_price"]) / opp["entry_price"] * 100, 2) if outcome == "win" else -100.0
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
                    opp["pnl_pct"] = round((1.0 - opp["entry_price"]) / opp["entry_price"] * 100, 2) if final_yes <= 0.05 else -100.0
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
                        opp["pnl_pct"] = round((1.0 - opp["entry_price"]) / opp["entry_price"] * 100, 2)
                        resolved = True
                    elif max_p <= 0.05:
                        opp["outcome"] = "loss"
                        opp["final_yes_price"] = max_p
                        opp["pnl_pct"] = -100.0
                        resolved = True
            if resolved:
                stake = opp.get("paper_size_usd") or round(PAPER_SIZE_USD * opp.get("cluster_size", 1), 2)
                opp["paper_size_usd"] = stake
                opp["paper_pnl_usd"] = round(stake * (opp["pnl_pct"] / 100.0), 2)
                if opp.get("actual_temp") is None:
                    actual = _get_actual_temp_for_opp(opp)
                    if actual is not None:
                        opp["actual_temp"] = actual
                        wu_pred = (opp.get("forecast_sources") or {}).get("wunderground")
                        if wu_pred is not None:
                            opp["wu_error"] = round(abs(wu_pred - actual), 1)
                resolved_count += 1

    if resolved_count or backfill_actual:
        data["last_resolved"] = datetime.utcnow().isoformat()
        with _tracker_lock:
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
) -> bool:
    """
    Mark an opportunity as live-traded. Call this right after buy() succeeds.
    Returns True if the opportunity was found and updated.
    """
    with _tracker_lock:
        data = _load()
        for opp in data["opportunities"]:
            if opp["id"] == opp_id:
                opp["is_live"]       = True
                opp["live_order_id"] = order_id
                opp["live_size_usd"] = size_usd
                opp["shares"]        = shares
                opp["token_id"]      = token_id
                opp["exit_price"]    = None
                opp["exit_reason"]   = None
                opp["live_at"]       = datetime.utcnow().isoformat()
                _save(data)
                return True
    return False


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


def mark_exited_early(opp_id: str, exit_price: float) -> bool:
    """Record that a position was sold early to take profit."""
    return _mark_exit(opp_id, exit_price, "take_profit")


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

                opp["exit_price"]    = exit_price
                opp["exit_reason"]   = reason
                opp["exit_at"]       = datetime.utcnow().isoformat()
                opp["pnl_pct"]       = pnl_pct
                opp["paper_pnl_usd"] = pnl_usd
                opp["outcome"]       = "win" if pnl_usd > 0 else "loss"
                _save(data)
                return True
    return False

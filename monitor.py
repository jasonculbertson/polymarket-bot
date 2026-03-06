"""
monitor.py — Background position monitor with auto stop-loss

Runs a daemon thread that wakes every MONITOR_INTERVAL_SECS seconds and:
  1. Loads all live positions from tracker
  2. Fetches current best-bid price from CLOB for each position's token
  3. Auto-sells if:
       current_price < entry_price × (1 - STOP_LOSS_PCT/100)   → stop-loss
       current_price ≥ entry_price × (1 + TAKE_PROFIT_PCT/100) → take-profit (if enabled)

Safe to call start_monitor() unconditionally — it's a no-op when:
  - LIVE_MODE=false, OR
  - POLY_PRIVATE_KEY is not set
"""

import logging
import threading
import time

import requests

from config import TRADING, CLOB_API

log = logging.getLogger(__name__)

STOP_LOSS_PCT         = TRADING["stop_loss_pct"]
TAKE_PROFIT_PCT       = TRADING["take_profit_pct"]
MONITOR_INTERVAL_SECS = TRADING["monitor_interval_secs"]
LIVE_MODE             = TRADING["live_mode"]

import os
_POLY_KEY = os.environ.get("POLY_PRIVATE_KEY", "")

_monitor_thread: threading.Thread = None
_stop_event = threading.Event()

# Recent monitor events (shown on dashboard)
_recent_events: list = []
_MAX_EVENTS = 50


def _log_event(kind: str, opp_id: str, token_id: str, price: float, detail: str = ""):
    from datetime import datetime
    evt = {
        "ts":       datetime.utcnow().isoformat(),
        "kind":     kind,
        "opp_id":   opp_id,
        "token_id": token_id[:20] if token_id else "",
        "price":    price,
        "detail":   detail,
    }
    _recent_events.insert(0, evt)
    if len(_recent_events) > _MAX_EVENTS:
        _recent_events.pop()
    log.info("[monitor] %s opp=%s token=%s price=%.4f %s", kind, opp_id, token_id[:16], price, detail)


def _fetch_best_bid(token_id: str) -> float | None:
    """Fetch current best bid price for a token from the CLOB order book."""
    try:
        r = requests.get(
            f"{CLOB_API}/book",
            params={"token_id": token_id},
            timeout=8,
        )
        if r.status_code == 200:
            book = r.json()
            bids = book.get("bids", [])
            if bids:
                return float(bids[0]["price"])
    except Exception as e:
        log.warning("[monitor] price fetch failed for %s: %s", token_id[:16], e)
    return None


def check_positions():
    """
    Core check: iterate live positions, fetch current prices,
    trigger stop-loss or take-profit sells as needed.
    """
    import tracker
    import trader

    positions = tracker.get_live_positions()
    if not positions:
        return

    log.info("[monitor] checking %d live position(s)", len(positions))

    for pos in positions:
        opp_id   = pos["id"]
        token_id = pos.get("token_id", "")
        entry    = float(pos.get("entry_price", 0))

        if not token_id or entry <= 0:
            continue

        current = _fetch_best_bid(token_id)
        if current is None:
            log.warning("[monitor] no price for %s, skipping", opp_id)
            continue

        stop_loss_threshold   = entry * (1 - STOP_LOSS_PCT / 100)
        take_profit_threshold = entry * (1 + TAKE_PROFIT_PCT / 100) if TAKE_PROFIT_PCT > 0 else None

        if current <= stop_loss_threshold:
            _log_event("STOP_LOSS", opp_id, token_id, current,
                       f"entry={entry:.4f} threshold={stop_loss_threshold:.4f}")
            try:
                shares = float(pos.get("shares", 0))
                trader.sell(token_id, shares, current)
                tracker.mark_stopped_out(opp_id, current)
            except Exception as e:
                log.error("[monitor] stop-loss sell FAILED for %s: %s", opp_id, e)

        elif take_profit_threshold and current >= take_profit_threshold:
            _log_event("TAKE_PROFIT", opp_id, token_id, current,
                       f"entry={entry:.4f} threshold={take_profit_threshold:.4f}")
            try:
                shares = float(pos.get("shares", 0))
                trader.sell(token_id, shares, current)
                tracker.mark_exited_early(opp_id, current)
            except Exception as e:
                log.error("[monitor] take-profit sell FAILED for %s: %s", opp_id, e)

        else:
            pct_chg = (current - entry) / entry * 100
            _log_event("OK", opp_id, token_id, current,
                       f"entry={entry:.4f} chg={pct_chg:+.1f}%")


def _run_loop():
    log.info(
        "[monitor] started  interval=%ds  stop_loss=%.0f%%  take_profit=%.0f%%  live=%s",
        MONITOR_INTERVAL_SECS, STOP_LOSS_PCT, TAKE_PROFIT_PCT, LIVE_MODE,
    )
    while not _stop_event.is_set():
        try:
            check_positions()
        except Exception as e:
            log.error("[monitor] unexpected error: %s", e)
        _stop_event.wait(MONITOR_INTERVAL_SECS)
    log.info("[monitor] stopped")


def start_monitor():
    """Start the background monitor thread. Safe to call multiple times."""
    global _monitor_thread

    if not _POLY_KEY:
        log.info("[monitor] POLY_PRIVATE_KEY not set — monitor disabled")
        return

    if not LIVE_MODE:
        log.info("[monitor] LIVE_MODE=false — monitor running in observe-only mode")

    if _monitor_thread and _monitor_thread.is_alive():
        log.info("[monitor] already running")
        return

    _stop_event.clear()
    _monitor_thread = threading.Thread(target=_run_loop, name="position-monitor", daemon=True)
    _monitor_thread.start()


def stop_monitor():
    """Signal the monitor thread to stop (useful for testing)."""
    _stop_event.set()


def get_recent_events() -> list:
    """Return recent monitor events for the dashboard."""
    return list(_recent_events)


def get_status() -> dict:
    """Return monitor status dict for the /monitor/status endpoint."""
    running = bool(_monitor_thread and _monitor_thread.is_alive())
    return {
        "running":        running,
        "live_mode":      LIVE_MODE,
        "stop_loss_pct":  STOP_LOSS_PCT,
        "take_profit_pct": TAKE_PROFIT_PCT,
        "interval_secs":  MONITOR_INTERVAL_SECS,
        "recent_events":  get_recent_events()[:20],
    }

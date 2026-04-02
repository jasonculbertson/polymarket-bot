"""
monitor.py — Background position monitor with auto stop-loss and forecast-drift exit

Runs a daemon thread that wakes every MONITOR_INTERVAL_SECS seconds and:
  1. Loads all live positions from tracker
  2. Verifies each position's buy order actually filled:
       - If the CLOB order is still LIVE (unfilled) after STALE_ORDER_HOURS → cancel + mark cancelled
       - If unfilled but recent → log as PENDING and skip (let it fill)
  3. For filled positions — checks forecast drift first:
       - If tracker has flagged edge_gone=True (weather forecast moved against the bet) → sell immediately
  4. Fetches current best-bid price from CLOB for filled positions
  5. Auto-sells if:
       current_price < entry_price × (1 - STOP_LOSS_PCT/100)   → stop-loss
       current_price ≥ entry_price × (1 + TAKE_PROFIT_PCT/100) → take-profit (if enabled)

Safe to call start_monitor() unconditionally — it's a no-op when:
  - LIVE_MODE=false, OR
  - POLY_PRIVATE_KEY is not set
"""

import logging
import threading
import time
from datetime import datetime

import requests

from config import TRADING, CLOB_API

log = logging.getLogger(__name__)

STOP_LOSS_PCT               = TRADING["stop_loss_pct"]
TAKE_PROFIT_PCT             = TRADING["take_profit_pct"]
MONITOR_INTERVAL_SECS       = TRADING["monitor_interval_secs"]
LIVE_MODE                   = TRADING["live_mode"]
FORCE_EXIT_HOURS            = TRADING.get("force_exit_hours_before_resolution", 24.0)

# Cancel orders that are still showing LIVE after this many hours.
# FOK (NO bets): fill or cancel within seconds — 0.25h is a generous safety margin.
# GTC (YES cluster limit orders): intentionally sit in the book for hours/days.
STALE_ORDER_HOURS_FOK = 0.25   # 15 min for market orders (NO bets)
STALE_ORDER_HOURS_GTC = 48.0   # 48 h for limit orders (YES clusters)
STALE_ORDER_HOURS = STALE_ORDER_HOURS_FOK  # legacy default (NO bets)

import os
_POLY_KEY = os.environ.get("POLY_PRIVATE_KEY", "")

_monitor_thread: threading.Thread = None
_stop_event = threading.Event()

# Recent monitor events (shown on dashboard)
_recent_events: list = []
_recent_events_lock = threading.Lock()
_MAX_EVENTS = 50
_exiting_positions: set = set()
_exiting_lock = threading.Lock()

# Trailing stop: track peak price seen per position (in-memory, seeded from entry on first sight)
# Stops fire when current drops 15% from peak, not from entry.
# e.g. entry=0.11, peak=0.16 (+45%), stop fires at 0.16*(1-0.15)=0.136 (+23%)
_price_peaks: dict = {}  # {opp_id: peak_price}
_peaks_lock = threading.Lock()


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
    with _recent_events_lock:
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


def _order_age_hours(live_at_str: str) -> float:
    """Return how many hours have elapsed since live_at_str (ISO format). Returns 999 on parse error."""
    try:
        placed_at = datetime.fromisoformat(live_at_str)
        return (datetime.utcnow() - placed_at).total_seconds() / 3600
    except Exception:
        return 999.0  # Treat unparseable timestamp as very old → safe to cancel


def _sell_position(trader, opp_id: str, tokens: list, shares_each: float, price, log):
    """
    Sell all tokens for a position. Handles both single-token NO bets and
    multi-token YES clusters. Raises on first unexpected error.
    """
    if not tokens:
        log.warning("[monitor] _sell_position: no tokens for %s", opp_id)
        return
    for tok in tokens:
        try:
            trader.sell(tok, shares_each, price)
            log.warning("[monitor]   sold token=%s shares=%.4f", tok[:16], shares_each)
        except Exception as e:
            err_str = str(e)
            if "not enough balance" in err_str or "allowance" in err_str or "no position" in err_str.lower():
                log.warning("[monitor]   token=%s no balance to sell (may not have filled yet): %s", tok[:16], e)
            else:
                raise


def check_positions():
    """
    Core check: iterate live positions, verify fill status, then apply stop-loss/take-profit.

    Phase 1 — Fill verification (runs first, per position):
      - Reads live_order_id and live_at from the tracker record
      - Calls trader.check_order_filled() to query CLOB for current order status
      - LIVE + age ≥ STALE_ORDER_HOURS  → cancel order, mark order_cancelled in tracker
      - LIVE + age < STALE_ORDER_HOURS  → log PENDING, skip (give it more time)
      - FILLED / paper / unknown        → proceed to Phase 2

    Phase 2a — Forecast-drift exit (runs before price thresholds):
      - If tracker has set edge_gone=True (weather forecast shifted against our bet),
        sell immediately at the current bid. We'd rather take a small loss now than
        ride to a near-certain full loss at resolution.

    Phase 2b — Price monitoring (stop-loss / take-profit):
      - Fetches current best-bid from CLOB
      - Sells and marks exit if threshold crossed
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
        # Use actual fill price for stop-loss baseline; fall back to scan entry_price
        entry    = float(pos.get("execution_price") or pos.get("entry_price") or 0)
        order_id = pos.get("live_order_id", "")
        live_at  = pos.get("live_at", "")

        if not token_id or entry <= 0:
            continue

        # YES clusters have multiple tokens — use yes_token_ids list for sells/prices
        yes_token_ids = pos.get("yes_token_ids") or []
        is_yes_cluster = bool(yes_token_ids)
        # For YES clusters the shares field = shares per bracket (each token same count)
        all_sell_tokens = yes_token_ids if is_yes_cluster else ([token_id] if token_id else [])

        # ── Phase 1: verify the buy order actually filled ─────────────────────
        # Only check real (non-paper) orders. Paper orders are always "filled".
        if LIVE_MODE and order_id and not order_id.startswith("paper_"):
            fill_status = trader.check_order_filled(order_id)

            if fill_status == "LIVE":
                # Order is still open in the CLOB — has not filled yet.
                age_h = _order_age_hours(live_at)
                # GTC YES orders sit in the book intentionally — give them 48h before cancel.
                # FOK NO orders fill within seconds — cancel after 15 min if still open.
                stale_threshold = STALE_ORDER_HOURS_GTC if is_yes_cluster else STALE_ORDER_HOURS_FOK

                if age_h >= stale_threshold:
                    # Old enough to be considered dead — try to cancel.
                    # CRITICAL: only mark as cancelled if cancel actually succeeds.
                    # A cancel failure means the order already filled — the position
                    # has real money at risk and must stay in the stop-loss watch list.
                    log.warning(
                        "[monitor] STALE ORDER opp=%s order=%s age=%.1fh — cancelling",
                        opp_id, order_id[:20], age_h,
                    )
                    _log_event("STALE_ORDER", opp_id, token_id, 0.0,
                               f"order={order_id[:20]} age={age_h:.1f}h")
                    cancel_ok = False
                    try:
                        trader.cancel(order_id)
                        cancel_ok = True
                    except Exception as ce:
                        log.warning(
                            "[monitor] cancel FAILED for %s: %s — order may be filled, "
                            "keeping in stop-loss watch list",
                            order_id[:20], ce,
                        )
                    if cancel_ok:
                        tracker.mark_order_cancelled(opp_id)
                        with _exiting_lock:
                            _exiting_positions.discard(opp_id)
                        continue  # Done — genuinely unfilled, no money at risk
                    # Cancel failed → treat as filled, fall through to stop-loss check

                else:
                    # Still fresh — let it sit, log as pending.
                    log.info(
                        "[monitor] PENDING opp=%s order=%s age=%.1fh — waiting for fill",
                        opp_id, order_id[:20], age_h,
                    )
                    _log_event("PENDING", opp_id, token_id, 0.0,
                               f"order={order_id[:20]} age={age_h:.1f}h")
                    continue  # Skip price monitoring until the order fills

            elif fill_status == "CANCELLED":
                # Order was already cancelled externally — clean up tracker.
                log.warning("[monitor] order already CANCELLED opp=%s order=%s — marking",
                            opp_id, order_id[:20])
                _log_event("ORDER_CANCELLED", opp_id, token_id, 0.0,
                           f"order={order_id[:20]} (external cancel)")
                tracker.mark_order_cancelled(opp_id)
                with _exiting_lock:
                    _exiting_positions.discard(opp_id)
                continue

            # fill_status == "FILLED" or None (error → treat as filled, proceed normally)

        # ── Phase 2a-pre: forced time-based exit ──────────────────────────────
        # YES tokens: NEVER force-exit. They resolve to $1/share if they win,
        # and the bid collapses to near-zero when losing anyway. Selling a
        # winning YES bet 24h early at $0.02 instead of $1.00 is catastrophic.
        # Let YES positions ride to resolution and redeem there.
        # NO bets: force-exit makes sense — exit before market closes to recover
        # remaining value rather than waiting for resolution.
        res_time_str = pos.get("resolution_time") or pos.get("resolution_date")
        if res_time_str and FORCE_EXIT_HOURS > 0 and not is_yes_cluster:
            try:
                from datetime import timezone
                if "T" in str(res_time_str):
                    res_dt = datetime.fromisoformat(str(res_time_str).replace("Z", "+00:00"))
                else:
                    res_dt = datetime.fromisoformat(f"{res_time_str}T23:59:00+00:00")
                now_utc = datetime.now(timezone.utc)
                hours_left = (res_dt - now_utc).total_seconds() / 3600
                if hours_left <= FORCE_EXIT_HOURS:
                    with _exiting_lock:
                        if opp_id in _exiting_positions:
                            pass
                        else:
                            _exiting_positions.add(opp_id)
                            shares = float(pos.get("shares", 0))
                            current_p = _fetch_best_bid(token_id)
                            _log_event("FORCE_EXIT", opp_id, token_id, current_p or 0.0,
                                       f"{hours_left:.1f}h to resolution — selling before close")
                            log.warning("[monitor] FORCE EXIT %s — %.1fh to resolution", opp_id, hours_left)
                            try:
                                _sell_position(trader, opp_id, all_sell_tokens, shares, current_p, log)
                                tracker.mark_exited_early(opp_id, current_p or 0.0, reason="force_exit")
                            except Exception as fe:
                                log.error("[monitor] force-exit sell FAILED for %s: %s", opp_id, fe)
                                with _exiting_lock:
                                    _exiting_positions.discard(opp_id)
                    continue
            except Exception as te:
                log.warning("[monitor] force-exit time parse failed for %s: %s", opp_id, te)

        # ── Phase 2a: forecast-drift exit ─────────────────────────────────────
        # check_forecast_drift() (runs every 15 min via quick_monitor) sets
        # edge_gone=True when the weather forecast has moved enough that our
        # original edge is gone. Sell immediately — don't wait for the price
        # to crater at resolution.
        if pos.get("edge_gone"):
            with _exiting_lock:
                if opp_id in _exiting_positions:
                    continue
                _exiting_positions.add(opp_id)

            current = _fetch_best_bid(token_id)
            reason  = pos.get("edge_gone_reason", "forecast drifted against bet")
            _log_event("FORECAST_DRIFT", opp_id, token_id, current or 0.0,
                       f"edge_gone — {reason[:80]}")
            log.warning(
                "[monitor] FORECAST DRIFT EXIT opp=%s current=%.4f reason=%s",
                opp_id, current or 0.0, reason[:60],
            )

            if current is None:
                log.warning("[monitor] drift exit: no bid for %s — skipping sell", opp_id)
                with _exiting_lock:
                    _exiting_positions.discard(opp_id)
                continue

            try:
                shares = float(pos.get("shares", 0))
                _sell_position(trader, opp_id, all_sell_tokens, shares, current, log)
                tracker.mark_exited_early(opp_id, current)
            except Exception as e:
                err_str = str(e)
                if "not enough balance" in err_str or "allowance" in err_str:
                    log.warning("[monitor] drift exit SIMULATED (can't sell): %s — %s", opp_id, e)
                    tracker.mark_exited_early(opp_id, current)
                else:
                    log.error("[monitor] drift exit sell FAILED for %s: %s", opp_id, e)
                    with _exiting_lock:
                        _exiting_positions.discard(opp_id)
            continue  # Done — don't also apply stop-loss logic to this position

        # ── Phase 2b: price monitoring — trailing stop per token ─────────────
        # For YES clusters: check each token individually. Sell only the token
        # that hit its trailing stop — leave the others running.
        # For NO bets: single token, same trailing stop logic.
        if is_yes_cluster:
            shares = float(pos.get("shares", 0))
            n_tokens = max(len(all_sell_tokens), 1)
            # Seed per-token entry price from cluster entry_price / num_tokens
            # so the trailing stop is anchored to what we actually paid, not
            # whatever the price happens to be when the monitor restarts.
            entry_per_tok = entry / n_tokens if entry > 0 else 0
            for tok in list(all_sell_tokens):
                tok_current = _fetch_best_bid(tok)
                if tok_current is None:
                    continue
                tok_key = f"{opp_id}:{tok}"
                with _peaks_lock:
                    # Use entry_per_tok as floor so redeployments don't reset the peak
                    stored_peak = _price_peaks.get(tok_key)
                    if stored_peak is None:
                        # First time seeing this token — seed from max(current, entry_per_tok)
                        stored_peak = max(tok_current, entry_per_tok)
                        _price_peaks[tok_key] = stored_peak
                    tok_peak = max(stored_peak, tok_current)
                    if tok_current > stored_peak:
                        _price_peaks[tok_key] = tok_current
                        tok_peak = tok_current
                tok_threshold = tok_peak * (1 - STOP_LOSS_PCT / 100)
                if tok_current <= tok_threshold:
                    log.warning(
                        "[monitor] YES TOKEN STOP opp=%s tok=%s current=%.4f peak=%.4f thresh=%.4f",
                        opp_id, tok[:16], tok_current, tok_peak, tok_threshold,
                    )
                    _log_event("STOP_LOSS", opp_id, tok, tok_current,
                               f"peak={tok_peak:.4f} threshold={tok_threshold:.4f}")
                    try:
                        r = trader.sell(tok, shares)
                        sold_price = r.get("exit_price", tok_current)
                        log.warning("[monitor]   sold YES token=%s @ %.4f", tok[:16], sold_price)
                        fully_closed = tracker.remove_yes_token(opp_id, tok, sold_price)
                        if fully_closed:
                            log.warning("[monitor]   cluster %s fully closed", opp_id)
                        with _peaks_lock:
                            _price_peaks.pop(tok_key, None)
                    except Exception as e:
                        err_str = str(e)
                        if "not enough balance" in err_str or "allowance" in err_str:
                            log.warning("[monitor] YES stop SIMULATED no-balance %s: %s", tok[:16], e)
                            tracker.remove_yes_token(opp_id, tok, tok_current)
                        else:
                            log.error("[monitor] YES stop sell FAILED %s: %s", tok[:16], e)
            continue

        current = _fetch_best_bid(token_id)

        if current is None:
            log.warning(
                "[monitor] no price for %s (opp=%s entry=%.4f) — CLOB unavailable, "
                "will retry next cycle",
                token_id[:16], opp_id, entry,
            )
            continue

        # Trailing stop: seed peak from entry (not current) so redeployments
        # don't reset the high-water mark to a post-loss price.
        with _peaks_lock:
            stored = _price_peaks.get(opp_id)
            if stored is None:
                stored = max(current, entry)  # seed from entry on first sight
                _price_peaks[opp_id] = stored
            peak = max(stored, current)
            if current > stored:
                _price_peaks[opp_id] = current

        stop_loss_threshold   = peak * (1 - STOP_LOSS_PCT / 100)
        take_profit_threshold = entry * (1 + TAKE_PROFIT_PCT / 100) if TAKE_PROFIT_PCT > 0 else None

        if current <= stop_loss_threshold:
            with _exiting_lock:
                if opp_id in _exiting_positions:
                    continue
                _exiting_positions.add(opp_id)
            _log_event("STOP_LOSS", opp_id, token_id, current,
                       f"entry={entry:.4f} peak={peak:.4f} threshold={stop_loss_threshold:.4f}")
            try:
                shares = float(pos.get("shares", 0))
                _sell_position(trader, opp_id, all_sell_tokens, shares, current, log)
                tracker.mark_stopped_out(opp_id, current)
            except Exception as e:
                err_str = str(e)
                if "not enough balance" in err_str or "allowance" in err_str:
                    log.warning("[monitor] stop-loss SIMULATED (can't sell): %s — %s", opp_id, e)
                    tracker.mark_stopped_out(opp_id, current)
                else:
                    log.error("[monitor] stop-loss sell FAILED for %s: %s", opp_id, e)
                    with _exiting_lock:
                        _exiting_positions.discard(opp_id)

        elif take_profit_threshold and current >= take_profit_threshold:
            with _exiting_lock:
                if opp_id in _exiting_positions:
                    continue
                _exiting_positions.add(opp_id)
            _log_event("TAKE_PROFIT", opp_id, token_id, current,
                       f"entry={entry:.4f} threshold={take_profit_threshold:.4f}")
            try:
                shares = float(pos.get("shares", 0))
                _sell_position(trader, opp_id, all_sell_tokens, shares, current, log)
                tracker.mark_exited_early(opp_id, current)
            except Exception as e:
                err_str = str(e)
                if "not enough balance" in err_str or "allowance" in err_str:
                    log.warning("[monitor] take-profit SIMULATED (can't sell): %s — %s", opp_id, e)
                    tracker.mark_exited_early(opp_id, current)
                else:
                    log.error("[monitor] take-profit sell FAILED for %s: %s", opp_id, e)
                    with _exiting_lock:
                        _exiting_positions.discard(opp_id)

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

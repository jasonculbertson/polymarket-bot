"""
trader.py — Polymarket CLOB order execution

In LIVE_MODE=false (default), all orders are simulated and logged only.
Set LIVE_MODE=true on Railway (with valid POLY_* env vars) to trade real money.

Entry point functions:
  buy(token_id, size_usd, price, neg_risk)  → dict with order_id + shares
  sell(token_id, shares, price)             → dict with order_id
  cancel(order_id)                          → bool
  get_balance()                             → float (USDC)
"""

import os
import math
import logging
from typing import Optional

from config import TRADING, CLOB_API

log = logging.getLogger(__name__)

LIVE_MODE        = TRADING["live_mode"]
SLIPPAGE_PCT     = TRADING["slippage_pct"]

POLY_PRIVATE_KEY    = os.environ.get("POLY_PRIVATE_KEY", "")
POLY_API_KEY        = os.environ.get("POLY_API_KEY", "")
POLY_API_SECRET     = os.environ.get("POLY_API_SECRET", "")
POLY_API_PASSPHRASE = os.environ.get("POLY_API_PASSPHRASE", "")

# Chain ID 137 = Polygon mainnet (where Polymarket lives)
_CHAIN_ID = 137

# Minimum tick size on Polymarket (most markets use 0.01)
_DEFAULT_TICK = 0.01


def _round_price(price: float, tick: float = _DEFAULT_TICK) -> float:
    """Round price to valid Polymarket tick size."""
    return round(round(price / tick) * tick, 6)


def _get_client():
    """Build an authenticated ClobClient (EOA / MetaMask wallet)."""
    if not POLY_PRIVATE_KEY:
        raise RuntimeError(
            "POLY_PRIVATE_KEY is not set. Export your MetaMask private key "
            "and add it to Railway env vars."
        )

    from py_clob_client.client import ClobClient
    from py_clob_client.clob_types import ApiCreds

    client = ClobClient(
        CLOB_API,
        key=POLY_PRIVATE_KEY,
        chain_id=_CHAIN_ID,
        signature_type=0,   # 0 = EOA (MetaMask)
    )

    # Use provided API creds if available; otherwise derive from private key.
    if POLY_API_KEY and POLY_API_SECRET and POLY_API_PASSPHRASE:
        client.set_api_creds(ApiCreds(
            api_key=POLY_API_KEY,
            api_secret=POLY_API_SECRET,
            api_passphrase=POLY_API_PASSPHRASE,
        ))
    else:
        creds = client.create_or_derive_api_creds()
        client.set_api_creds(creds)
        log.info("[trader] API creds derived from private key")

    return client


def buy(token_id: str, size_usd: float, price: float,
        neg_risk: bool = False) -> dict:
    """
    Place a BUY limit order (GTC).

    size_usd: dollars to spend (e.g. 10.0)
    price:    price per share (e.g. 0.35 for a 35¢ YES token)
    neg_risk: set True for neg-risk markets (mutually exclusive brackets)

    Returns: {"order_id": str, "shares": float, "price": float, "live": bool}
    """
    price  = _round_price(price)
    shares = round(size_usd / price, 4)

    log.info(
        "[trader] BUY %s  shares=%.4f  price=%.4f  size_usd=%.2f  live=%s",
        token_id[:16], shares, price, size_usd, LIVE_MODE
    )

    if not LIVE_MODE:
        return {
            "order_id": f"paper_{token_id[:12]}",
            "shares": shares,
            "price": price,
            "live": False,
        }

    from py_clob_client.clob_types import OrderArgs, OrderType
    from py_clob_client.order_builder.constants import BUY as _BUY

    client = _get_client()
    order_args = OrderArgs(
        token_id=token_id,
        price=price,
        size=shares,
        side=_BUY,
    )
    signed   = client.create_order(order_args)
    response = client.post_order(signed, OrderType.GTC)

    order_id = response.get("orderID") or response.get("id", "")
    log.info("[trader] BUY placed  order_id=%s", order_id)
    return {"order_id": order_id, "shares": shares, "price": price, "live": True}


def sell(token_id: str, shares: float, price: Optional[float] = None) -> dict:
    """
    Place a SELL market order (FOK) to exit a position.

    If price is None, fetches the current best bid and applies slippage.
    Uses FOK (fill-or-kill) so the exit always completes or fails cleanly.

    Returns: {"order_id": str, "exit_price": float, "live": bool}
    """
    if price is None:
        price = _fetch_best_bid(token_id)
        if price is None:
            raise RuntimeError(f"Cannot fetch bid price for token {token_id[:16]}")

    # Apply slippage tolerance — sell slightly below bid to guarantee fill.
    exit_price = _round_price(price * (1 - SLIPPAGE_PCT / 100))
    exit_price = max(exit_price, _DEFAULT_TICK)  # never sell below minimum tick

    log.info(
        "[trader] SELL %s  shares=%.4f  price=%.4f  live=%s",
        token_id[:16], shares, exit_price, LIVE_MODE
    )

    if not LIVE_MODE:
        return {"order_id": f"paper_sell_{token_id[:12]}", "exit_price": exit_price, "live": False}

    from py_clob_client.clob_types import MarketOrderArgs, OrderType
    from py_clob_client.order_builder.constants import SELL as _SELL

    client = _get_client()
    # Use market order (FOK) for stop-loss exits to ensure immediate execution.
    market_order = MarketOrderArgs(
        token_id=token_id,
        amount=shares,
        side=_SELL,
        order_type=OrderType.FOK,
    )
    signed   = client.create_market_order(market_order)
    response = client.post_order(signed, OrderType.FOK)

    order_id = response.get("orderID") or response.get("id", "")
    log.info("[trader] SELL placed  order_id=%s  exit_price=%.4f", order_id, exit_price)
    return {"order_id": order_id, "exit_price": exit_price, "live": True}


def cancel(order_id: str) -> bool:
    """Cancel an open GTC order by order ID. Returns True on success."""
    if not LIVE_MODE:
        log.info("[trader] (paper) cancel order_id=%s", order_id)
        return True
    try:
        client = _get_client()
        client.cancel(order_id)
        return True
    except Exception as e:
        log.warning("[trader] cancel failed for %s: %s", order_id, e)
        return False


def get_balance() -> Optional[float]:
    """Return available USDC balance in the trading wallet."""
    if not POLY_PRIVATE_KEY:
        return None
    try:
        client = _get_client()
        balance_wei = client.get_balance()
        return int(balance_wei) / 1e6
    except Exception as e:
        log.warning("[trader] get_balance failed: %s", e)
        return None


def _fetch_best_bid(token_id: str) -> Optional[float]:
    """Fetch current best bid from the order book (read-only)."""
    import requests
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
        log.warning("[trader] _fetch_best_bid failed: %s", e)
    return None

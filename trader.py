"""
trader.py — Polymarket CLOB order execution

In LIVE_MODE=false (default), all orders are simulated and logged only.
Set LIVE_MODE=true on Railway (with valid POLY_* env vars) to trade real money.

Entry point functions:
  buy(token_id, size_usd, price, neg_risk)  → dict with order_id + shares
  sell(token_id, shares, price)             → dict with order_id
  cancel(order_id)                          → bool
  get_balance()                             → float (USDC)
  setup_ctf_approvals()                     → dict (enable sell capability)
"""

import os
import logging
from typing import Optional

import requests as _http_req

from config import TRADING, CLOB_API

try:
    import py_clob_client  # noqa: F401
    _CLOB_AVAILABLE = True
except ImportError:
    _CLOB_AVAILABLE = False

log = logging.getLogger(__name__)

LIVE_MODE        = TRADING["live_mode"]
SLIPPAGE_PCT     = TRADING["slippage_pct"]

POLY_PRIVATE_KEY    = os.environ.get("POLY_PRIVATE_KEY", "")
POLY_FUNDER         = os.environ.get("POLY_FUNDER", "")   # Polymarket proxy wallet address
POLY_API_KEY        = os.environ.get("POLY_API_KEY", "")
POLY_API_SECRET     = os.environ.get("POLY_API_SECRET", "")
POLY_API_PASSPHRASE = os.environ.get("POLY_API_PASSPHRASE", "")

# Chain ID 137 = Polygon mainnet (where Polymarket lives)
_CHAIN_ID = 137

# Minimum tick size on Polymarket (most markets use 0.01)
_DEFAULT_TICK = 0.01

# ── On-chain contract addresses (Polygon mainnet) ───────────────────────────
# ConditionalTokens ERC1155 contract — holds YES/NO token balances
_CTF_ADDRESS        = "0x4D97DCd97eC945f40cF65F87097ACe5EA0476045"
# Standard CTF Exchange — needs setApprovalForAll to pull tokens for SELL orders
_CTF_EXCHANGE       = "0x4bFb41d5B3570DeFd03C39a9A4D8dE6Bd8B8982E"
# Neg-risk CTF Exchange — used for mutually-exclusive bracket markets
_NEG_CTF_EXCHANGE   = "0xC5d563A36AE78145C45a50134d48A1215220f80a"

# Polygon JSON-RPC endpoints — tried in order until one works.
# NOTE: Both CTF exchanges (std + neg-risk) are already approved for the funder
# wallet (confirmed 2026-03-15 via isApprovedForAll on-chain check).
# The "not enough balance / allowance" CLOB error is a stale-cache problem;
# it is resolved by calling update_balance_allowance() before each sell order.
_POLYGON_RPC_URLS = [
    "https://polygon-bor-rpc.publicnode.com",   # public, no-key, reliable
    "https://polygon-rpc.com",                  # may require API key
    "https://rpc.ankr.com/polygon",             # requires Ankr account
]


# ── Low-level Polygon RPC helpers (no web3 required) ────────────────────────

def _rpc_call(method: str, params: list, timeout: int = 12) -> str:
    """Make a JSON-RPC call to Polygon, trying fallback endpoints."""
    last_err = None
    for url in _POLYGON_RPC_URLS:
        try:
            r = _http_req.post(
                url,
                json={"jsonrpc": "2.0", "method": method, "params": params, "id": 1},
                timeout=timeout,
            )
            data = r.json()
            if "error" in data:
                raise RuntimeError(data["error"].get("message", str(data["error"])))
            return data["result"]
        except Exception as e:
            log.warning("[rpc] %s failed (%s): %s", method, url, e)
            last_err = e
    raise RuntimeError(f"All RPC endpoints failed for {method}: {last_err}")


def _is_contract(address: str) -> bool:
    """Return True if address has contract code deployed (is not a plain EOA)."""
    try:
        code = _rpc_call("eth_getCode", [address, "latest"])
        return code not in ("0x", "0x0", "")
    except Exception as e:
        log.warning("[rpc] eth_getCode failed for %s: %s", address, e)
        return False  # Assume EOA on failure


def _is_approved_for_all(owner: str, operator: str) -> bool:
    """
    Call ConditionalTokens.isApprovedForAll(owner, operator) on-chain.
    Returns True if the CTF Exchange is authorised to pull tokens from `owner`.
    """
    try:
        from eth_abi import encode
        from eth_utils import keccak, to_checksum_address

        owner    = to_checksum_address(owner)
        operator = to_checksum_address(operator)
        selector = keccak(text="isApprovedForAll(address,address)")[:4]
        calldata = "0x" + (selector + encode(["address", "address"], [owner, operator])).hex()
        result   = _rpc_call("eth_call", [{"to": _CTF_ADDRESS, "data": calldata}, "latest"])
        return int(result, 16) == 1
    except Exception as e:
        log.warning("[rpc] isApprovedForAll check failed: %s", e)
        return False


def _send_set_approval_for_all(private_key: str, operator: str) -> str:
    """
    Build, sign, and broadcast a ConditionalTokens.setApprovalForAll(operator, True)
    transaction FROM the wallet derived from `private_key`.

    Returns the transaction hash string.
    """
    from eth_abi import encode
    from eth_account import Account
    from eth_utils import keccak, to_checksum_address

    account  = Account.from_key(private_key)
    operator = to_checksum_address(operator)
    selector = keccak(text="setApprovalForAll(address,bool)")[:4]
    calldata = "0x" + (selector + encode(["address", "bool"], [operator, True])).hex()

    nonce          = int(_rpc_call("eth_getTransactionCount", [account.address, "pending"]), 16)
    base_gas_price = int(_rpc_call("eth_gasPrice", []), 16)
    gas_price      = int(base_gas_price * 2.0)  # 2× for fast Polygon inclusion

    tx = {
        "to":       to_checksum_address(_CTF_ADDRESS),
        "data":     calldata,
        "gas":      120_000,
        "gasPrice": gas_price,
        "nonce":    nonce,
        "chainId":  _CHAIN_ID,
        "value":    0,
    }
    signed   = Account.sign_transaction(tx, private_key)
    raw_hex  = "0x" + signed.raw_transaction.hex()
    tx_hash  = _rpc_call("eth_sendRawTransaction", [raw_hex])
    log.warning("[trader] setApprovalForAll tx submitted: %s", tx_hash)
    return tx_hash


def setup_ctf_approvals() -> dict:
    """
    Ensure the CTF Exchange is approved to transfer conditional tokens from the
    funder wallet. This approval is required for SELL orders. Buys only need USDC
    allowance, which is handled separately.

    Strategy
    --------
    1. Call isApprovedForAll(funder, CTF_EXCHANGE) on-chain — if already True, done.
    2. If funder == EOA (same wallet), sign setApprovalForAll from POLY_PRIVATE_KEY.
    3. If funder is a plain EOA (different wallet), we don't hold its key — return
       instructions for manual approval.
    4. If funder is a smart contract (Polymarket proxy), return instructions for the
       Polymarket web UI or Polygonscan.

    Returns dict with keys: approved, tx_hash, pending, message
    """
    if not POLY_PRIVATE_KEY:
        return {"approved": False, "tx_hash": None, "pending": False,
                "message": "POLY_PRIVATE_KEY env var not set"}

    try:
        from eth_account import Account
        from eth_utils import to_checksum_address

        eoa_account    = Account.from_key(POLY_PRIVATE_KEY)
        eoa_address    = to_checksum_address(eoa_account.address)
        funder_address = to_checksum_address(POLY_FUNDER) if POLY_FUNDER else eoa_address
        exchange       = to_checksum_address(_CTF_EXCHANGE)

        log.warning("[trader] setup_ctf_approvals: EOA=%s  funder=%s  exchange=%s",
                    eoa_address, funder_address, exchange)

        # ── 1. Check current on-chain state ─────────────────────────────────
        approved = _is_approved_for_all(funder_address, exchange)
        if approved:
            return {"approved": True, "tx_hash": None, "pending": False,
                    "message": f"setApprovalForAll already set: {funder_address} → {exchange}"}

        log.warning("[trader] setApprovalForAll NOT set — funder=%s", funder_address)

        # ── 2. Funder == EOA: we can sign directly ───────────────────────────
        if funder_address.lower() == eoa_address.lower():
            tx_hash = _send_set_approval_for_all(POLY_PRIVATE_KEY, exchange)
            return {"approved": False, "tx_hash": tx_hash, "pending": True,
                    "message": f"Transaction submitted — wait 30s then retry sells. tx={tx_hash}"}

        # ── 3. Funder is a different wallet — need to determine type ─────────
        is_proxy = _is_contract(funder_address)
        if not is_proxy:
            return {
                "approved": False, "tx_hash": None, "pending": False,
                "message": (
                    f"Funder {funder_address} is an EOA but we don't have its private key. "
                    "MANUAL ACTION REQUIRED: Import the funder wallet into MetaMask and visit "
                    f"https://polygonscan.com/address/{_CTF_ADDRESS}#writeContract — "
                    f"call setApprovalForAll with operator={exchange}, approved=true."
                ),
            }

        # ── 4. Funder is a Polymarket proxy (smart contract) ─────────────────
        return {
            "approved": False, "tx_hash": None, "pending": False,
            "message": (
                f"Funder {funder_address} is a smart contract (Polymarket proxy). "
                "setApprovalForAll must be executed through the proxy contract. "
                "MANUAL ACTION: Connect the funder wallet to https://polymarket.com and "
                "deposit/withdraw once to trigger the approval transaction automatically, OR "
                f"call the proxy's execute() with target={_CTF_ADDRESS} and "
                f"calldata=setApprovalForAll({exchange}, true)."
            ),
        }

    except Exception as e:
        log.error("[trader] setup_ctf_approvals error: %s", e, exc_info=True)
        return {"approved": False, "tx_hash": None, "pending": False, "message": f"Error: {e}"}


def check_ctf_approval_status() -> dict:
    """
    Read-only diagnostic: returns the on-chain approval state plus CLOB cached
    balance/allowance for the funder wallet.

    Returns dict with keys: funder, exchange, is_approved_onchain, clob_balance
    """
    try:
        from eth_account import Account
        from eth_utils import to_checksum_address

        eoa_address    = Account.from_key(POLY_PRIVATE_KEY).address if POLY_PRIVATE_KEY else "unknown"
        funder_address = to_checksum_address(POLY_FUNDER) if POLY_FUNDER else eoa_address
        exchange       = to_checksum_address(_CTF_EXCHANGE)
        neg_exchange   = to_checksum_address(_NEG_CTF_EXCHANGE)

        on_chain_std     = _is_approved_for_all(funder_address, exchange)
        on_chain_neg     = _is_approved_for_all(funder_address, neg_exchange)
        is_proxy         = _is_contract(funder_address)

        return {
            "eoa":                    eoa_address,
            "funder":                 funder_address,
            "funder_is_contract":     is_proxy,
            "ctf_exchange":           exchange,
            "neg_risk_ctf_exchange":  neg_exchange,
            "is_approved_std_market": on_chain_std,
            "is_approved_neg_market": on_chain_neg,
            "action_needed":          not (on_chain_std and on_chain_neg),
        }
    except Exception as e:
        log.warning("[trader] check_ctf_approval_status error: %s", e)
        return {"error": str(e)}


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

    # signature_type=2: EOA key signs, but funds live in a Polymarket proxy wallet (funder)
    # signature_type=0: EOA key signs AND holds funds directly
    sig_type = 2 if POLY_FUNDER else 0
    client = ClobClient(
        CLOB_API,
        key=POLY_PRIVATE_KEY,
        chain_id=_CHAIN_ID,
        signature_type=sig_type,
        funder=POLY_FUNDER or None,
    )
    log.warning("[trader] Using signature_type=%d funder=%s", sig_type, POLY_FUNDER or "none")

    # Always derive API creds from the private key — stored creds go stale.
    creds = client.create_or_derive_api_creds()
    client.set_api_creds(creds)
    log.warning("[trader] API creds derived from private key")

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
    # ── Fetch live orderbook and set bid price to guarantee immediate fill ──
    # KEY: to guarantee a GTC order fills immediately, we must bid AT or ABOVE
    # the current best ask (lowest sell offer). Bidding at the midpoint leaves
    # orders unfilled indefinitely because sellers won't accept below their ask.
    book_ask  = _fetch_best_ask(token_id)
    live_price = _fetch_live_price(token_id)
    ref_price  = live_price  # used for drift check against scan price

    if ref_price is None:
        # Midpoint API failed — derive reference from orderbook
        ref_price = book_ask or _fetch_best_bid(token_id)

    if ref_price is None:
        raise ValueError(
            f"No live price or orderbook for {token_id[:16]} — "
            f"market appears closed or inactive, skipping"
        )

    # Drift check: reject if market moved >15¢ from scan price since we scored it
    drift = abs(ref_price - price)
    if drift > 0.15:
        raise ValueError(
            f"Price drift too large: scan={price:.2f} live={ref_price:.2f} "
            f"(drift={drift:.2f}) for {token_id[:16]} — skipping"
        )
    if drift > 0.05:
        log.warning("[trader] Price drift: scan=%.2f live=%.2f for %s", price, ref_price, token_id[:16])

    if book_ask is not None:
        # Bid 1 tick above the best ask → crosses the spread → guaranteed fill
        price = book_ask + _DEFAULT_TICK
        log.warning("[trader] Using ask-based price: ask=%.4f → bid=%.4f for %s",
                    book_ask, price, token_id[:16])
    else:
        # Ask side empty (rare) — fall back to midpoint + tick
        price = ref_price + _DEFAULT_TICK
        log.warning("[trader] No ask in book — using midpoint+tick: %.4f for %s", price, token_id[:16])

    # Hard floor: NO tokens below 0.50 means market has moved heavily against us
    if price < 0.50:
        raise ValueError(
            f"Live price {price:.2f} below minimum threshold (0.50) for {token_id[:16]} — "
            f"market may have already resolved or moved against forecast"
        )
    price = _round_price(min(price, 0.99))  # cap at 99¢
    if price <= 0:
        raise ValueError(f"Invalid price {price!r} for token {token_id[:16]}")
    shares = round(size_usd / price, 4)

    log.warning(
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

    if not _CLOB_AVAILABLE:
        raise RuntimeError("py-clob-client is not installed. Live trading unavailable.")

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
    # Capture actual fill price from exchange response if available
    execution_price = response.get("price") or response.get("avgPrice") or price
    log.info("[trader] BUY placed  order_id=%s  execution_price=%.4f", order_id, execution_price)
    return {
        "order_id": order_id,
        "shares": shares,
        "price": price,               # intended price (from scan)
        "execution_price": round(float(execution_price), 6),  # actual fill price
        "live": True,
    }


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

    if not _CLOB_AVAILABLE:
        raise RuntimeError("py-clob-client is not installed. Live trading unavailable.")

    from py_clob_client.clob_types import MarketOrderArgs, OrderType, BalanceAllowanceParams, AssetType
    from py_clob_client.order_builder.constants import SELL as _SELL

    client = _get_client()

    # ── Sync CLOB's cached view of our conditional-token balance ────────────
    # "not enough balance / allowance" is often a stale CLOB cache issue.
    # update_balance_allowance forces the CLOB to re-read on-chain state.
    try:
        bal_params   = BalanceAllowanceParams(asset_type=AssetType.CONDITIONAL, token_id=token_id)
        pre_balance  = client.get_balance_allowance(bal_params)
        log.warning("[trader] pre-sell CLOB balance: %s", pre_balance)
        client.update_balance_allowance(bal_params)
        post_balance = client.get_balance_allowance(bal_params)
        log.warning("[trader] post-sync CLOB balance: %s", post_balance)
    except Exception as _be:
        log.warning("[trader] balance sync (non-fatal): %s", _be)

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
    log.warning("[trader] SELL placed  order_id=%s  exit_price=%.4f", order_id, exit_price)
    return {"order_id": order_id, "exit_price": exit_price, "live": True}


def cancel(order_id: str) -> bool:
    """Cancel an open GTC order by order ID. Returns True on success."""
    if not LIVE_MODE:
        log.info("[trader] (paper) cancel order_id=%s", order_id)
        return True
    try:
        client = _get_client()
        client.cancel(order_id)
        log.warning("[trader] cancelled order %s", order_id)
        return True
    except Exception as e:
        log.warning("[trader] cancel failed for %s: %s", order_id, e)
        return False


def cancel_all_orders() -> dict:
    """
    Cancel every open CLOB order for this account and return the USDC to the wallet.

    Use when:
    - Open GTC buy orders never filled and the market has resolved against you
    - You want to free up locked USDC immediately rather than waiting for market closure

    Returns: {"cancelled": [order_ids], "failed": [order_ids], "total": int}
    """
    if not LIVE_MODE:
        return {"cancelled": [], "failed": [], "total": 0, "note": "paper mode — no real orders"}

    try:
        client = _get_client()
        orders = client.get_orders()
        if not isinstance(orders, list):
            orders = [orders] if orders else []

        live_orders = [o for o in orders if o.get("status") == "LIVE"]
        log.warning("[trader] cancelling %d live orders", len(live_orders))

        cancelled, failed = [], []
        for o in live_orders:
            oid = o.get("id", "")
            try:
                client.cancel(oid)
                cancelled.append(oid)
                log.warning("[trader] cancelled %s (%s %s @ %s)",
                            oid[:20], o.get("side"), o.get("outcome"), o.get("price"))
            except Exception as e:
                failed.append(oid)
                log.warning("[trader] cancel failed for %s: %s", oid[:20], e)

        return {"cancelled": cancelled, "failed": failed, "total": len(live_orders)}
    except Exception as e:
        log.error("[trader] cancel_all_orders error: %s", e)
        return {"cancelled": [], "failed": [], "total": 0, "error": str(e)}


def redeem_winning_position(token_id: str, condition_id: str, bet_type: str) -> dict:
    """
    Redeem winning conditional tokens for USDC after a market resolves.

    Polymarket often auto-redeems for proxy wallet users, but this provides an
    explicit fallback.  For simple YES/NO binary markets:
      - Winning NO token  → indexSets = [1]  (outcome slot 0)
      - Winning YES token → indexSets = [2]  (outcome slot 1)

    Parameters
    ----------
    token_id     : ERC1155 token ID string (stored per opportunity)
    condition_id : 32-byte hex condition ID from Polymarket Gamma API
    bet_type     : "no" or "yes"

    Returns dict with keys: redeemed (bool), tx_hash, message
    """
    if not LIVE_MODE:
        return {"redeemed": False, "tx_hash": None, "message": "paper mode — no redemption needed"}

    USDC_ADDRESS = "0x2791Bca1f2de4661ED88A30C99A7a9449Aa84174"
    index_sets   = [1] if bet_type.lower() == "no" else [2]

    # ── First try: sell winning tokens on CLOB at near-full value ───────────
    # If the market is still in the CLOB (not yet settled on-chain), selling at
    # $0.99 is faster and avoids the on-chain redemption complexity.
    try:
        best_bid = _fetch_best_bid(token_id)
        if best_bid is not None and best_bid >= 0.90:
            log.warning("[trader] winning token bid=%.4f — attempting CLOB sell first", best_bid)
            result = sell(token_id, shares=None, price=best_bid)  # type: ignore[arg-type]
            return {"redeemed": True, "tx_hash": result.get("order_id"),
                    "method": "clob_sell",
                    "message": f"Sold winning tokens on CLOB at {best_bid:.4f}"}
    except Exception as _se:
        log.warning("[trader] CLOB sell of winning tokens failed (%s) — trying on-chain redeem", _se)

    # ── Fallback: on-chain redeemPositions ──────────────────────────────────
    # Note: with signature_type=2 (proxy wallet), Polymarket usually auto-redeems.
    # This path handles the case where auto-redemption hasn't happened yet.
    try:
        from eth_abi import encode
        from eth_account import Account
        from eth_utils import keccak, to_checksum_address

        # Ensure condition_id is bytes32
        cid_bytes = bytes.fromhex(condition_id.lstrip("0x").zfill(64))

        # redeemPositions(address collateral, bytes32 parentId, bytes32 conditionId, uint256[] indexSets)
        selector = keccak(text="redeemPositions(address,bytes32,bytes32,uint256[])")[:4]
        calldata = "0x" + (selector + encode(
            ["address", "bytes32", "bytes32", "uint256[]"],
            [
                to_checksum_address(USDC_ADDRESS),
                b"\x00" * 32,   # parentCollectionId = 0 (root)
                cid_bytes,
                index_sets,
            ]
        )).hex()

        # For proxy wallets Polymarket auto-handles this; log calldata for manual fallback
        log.warning("[trader] redeemPositions calldata: %s", calldata[:60])
        log.warning("[trader] NOTE: funder is a proxy — auto-redemption should happen within 24h. "
                    "If not, call redeemPositions on CTF contract from funder wallet.")

        return {
            "redeemed": False,
            "tx_hash": None,
            "method": "manual_needed",
            "condition_id": condition_id,
            "index_sets": index_sets,
            "calldata": calldata,
            "message": (
                "Could not sell on CLOB. Polymarket should auto-redeem winning tokens within 24h. "
                "If not, call redeemPositions on the ConditionalTokens contract from your funder wallet."
            ),
        }

    except Exception as e:
        log.error("[trader] redeem_winning_position error: %s", e)
        return {"redeemed": False, "tx_hash": None, "message": f"Error: {e}"}


def get_condition_id_for_market(market_id: str) -> Optional[str]:
    """
    Fetch the condition_id for a Polymarket market from the Gamma API.
    The condition_id is needed for on-chain redeemPositions calls.
    """
    try:
        r = _http_req.get(
            f"https://gamma-api.polymarket.com/markets/{market_id}",
            timeout=8,
        )
        if r.status_code == 200:
            data = r.json()
            # Gamma API returns conditionId as a hex string
            cid = data.get("conditionId") or data.get("condition_id")
            if cid:
                return cid
    except Exception as e:
        log.warning("[trader] get_condition_id_for_market(%s) failed: %s", market_id, e)
    return None


def get_balance() -> Optional[float]:
    """Return available USDC balance in the trading wallet."""
    if not POLY_PRIVATE_KEY:
        return None
    try:
        from py_clob_client.clob_types import BalanceAllowanceParams, AssetType
        client = _get_client()
        result = client.get_balance_allowance(
            BalanceAllowanceParams(asset_type=AssetType.COLLATERAL)
        )
        # result is {"balance": "1000000", "allowance": "..."}  (USDC has 6 decimals)
        log.info("[trader] balance_allowance response: %s", result)
        if isinstance(result, dict):
            # Return wallet balance (raw on-chain USDC), not just allowance
            raw = result.get("balance", "0")
            allowance = result.get("allowance", "0")
            log.info("[trader] USDC balance=%s allowance=%s (raw wei)", raw, allowance)
        else:
            raw = str(result)
        return round(int(raw) / 1e6, 2)
    except Exception as e:
        log.warning("[trader] get_balance failed: %s", e)
        return None


def _fetch_live_price(token_id: str) -> Optional[float]:
    """
    Fetch the current market price for a token from Polymarket's midpoint API.
    Falls back to CLOB spread midpoint if midpoint API unavailable.
    """
    import requests
    try:
        # Polymarket midpoint price endpoint (most accurate)
        r = requests.get(
            f"{CLOB_API}/midpoint",
            params={"token_id": token_id},
            timeout=5,
        )
        if r.status_code == 200:
            data = r.json()
            mid = data.get("mid")
            if mid is not None:
                return float(mid)
    except Exception:
        pass

    # Fallback: derive from CLOB spread
    try:
        import requests as req
        r = req.get(f"{CLOB_API}/book", params={"token_id": token_id}, timeout=5)
        if r.status_code == 200:
            book = r.json()
            bids = book.get("bids", [])
            asks = book.get("asks", [])
            if bids and asks:
                return round((float(bids[0]["price"]) + float(asks[0]["price"])) / 2, 2)
            if asks:
                return float(asks[0]["price"])
            if bids:
                return float(bids[0]["price"])
    except Exception as e:
        log.warning("[trader] _fetch_live_price fallback failed: %s", e)
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


def _fetch_best_ask(token_id: str) -> Optional[float]:
    """
    Fetch the current best ask (lowest sell offer) from the order book.

    For BUY orders, bidding at ask+1tick guarantees immediate fill.
    Midpoint-based pricing can leave orders unfilled indefinitely.
    """
    import requests
    try:
        r = requests.get(
            f"{CLOB_API}/book",
            params={"token_id": token_id},
            timeout=8,
        )
        if r.status_code == 200:
            book = r.json()
            asks = book.get("asks", [])
            if asks:
                return float(asks[0]["price"])
    except Exception as e:
        log.warning("[trader] _fetch_best_ask failed: %s", e)
    return None


def check_order_filled(order_id: str) -> Optional[str]:
    """
    Query the CLOB for an order's current status.
    Returns 'FILLED', 'LIVE', 'CANCELLED', or None on error.
    """
    if not LIVE_MODE or not order_id or order_id.startswith("paper_"):
        return "FILLED" if not LIVE_MODE else None
    try:
        client = _get_client()
        order = client.get_order(order_id)
        if isinstance(order, dict):
            status = order.get("status", "")
            size_matched = float(order.get("size_matched", "0") or 0)
            orig_size    = float(order.get("original_size", "1") or 1)
            if status == "MATCHED" or size_matched >= orig_size * 0.95:
                return "FILLED"
            return status
    except Exception as e:
        log.warning("[trader] check_order_filled(%s): %s", order_id[:20], e)
    return None

"""
================================================================================
  ROBINHOOD CRYPTO — MULTI-STRATEGY CRYPTO RESEARCH & TRADING BOT
  ================================================================================

  A single-file framework for researching and (optionally) trading crypto on
  Robinhood Crypto. Includes 4 built-in strategies, an in-process backtest
  engine, a 2-D parameter-sweep robustness tool, a rich local HTML dashboard,
  persistent state, and an offline mock mode for offline development.

  STRATEGIES (all run in parallel; one drives execution)
  ------------------------------------------------------
    vtmr          Vol-targeted mean reversion (default). LONG on z<ENTRY_Z.
    momentum      EMA crossover + MACD confirmation. LONG on trend.
    bb_breakout   Buy at lower Bollinger band; exit at mid band.
    buy_hold      Equal-weight benchmark — the strategy you must beat.

  ================================================================================
  SETUP
  ================================================================================

  1. Install the required Python packages:

       pip3 install --user requests python-dotenv

     Plus optional packages for full functionality:

       pip3 install --user cryptography   # required for LIVE Robinhood orders
       pip3 install --user yfinance       # required for 10-year backtests + macro

  2. (For live trading only) Generate Robinhood Crypto API credentials:

     a. Sign in at robinhood.com → Account → API Keys
     b. Click "Generate New API Key" — Robinhood gives you an Ed25519 keypair.
     c. Download the *private key* as a PEM file. Save it somewhere safe:

          mkdir -p ~/.rh && mv ~/Downloads/rh_private.pem ~/.rh/
          chmod 600 ~/.rh/rh_private.pem
          chmod 700 ~/.rh

     d. Copy the API key ID (UUID) shown in the Robinhood UI.

  3. Create a `.env` file in the SAME DIRECTORY as this script:

       RH_API_KEY_ID=12345678-1234-1234-1234-123456789012
       RH_PRIVATE_KEY_PATH=/Users/michael.wang/.rh/rh_private.pem

     Important:
       • Add `.env` to `.gitignore` — never commit secrets.
       • Never paste the private key into the script itself.
       • The script loads `.env` automatically via python-dotenv on startup.

  4. (Optional) Override defaults via env vars:

       TOTAL_CAPITAL=20000           # override starting capital
       (or pass `--capital 20000` on the command line)

  ================================================================================
  RUNNING
  ================================================================================

  Offline demo (no creds, no network, synthetic prices) — start here:

       python3 robinhood_crypto.py --mode scan --dashboard --mock
       open http://127.0.0.1:8770/

  Live read-only scanning (real prices, no orders):

       python3 robinhood_crypto.py --mode scan --dashboard

  Live scheduler in dry-run (no orders placed, logs what it WOULD do):

       python3 robinhood_crypto.py --mode live --dashboard

  Live scheduler with REAL orders (only after testing dry-run carefully):

       python3 robinhood_crypto.py --mode live --dashboard --live-orders

  Run in the background, free up the terminal:

       nohup python3 robinhood_crypto.py --mode scan --dashboard --mock \
         > rh.log 2>&1 &
       disown
       # later:
       pkill -f robinhood_crypto.py

  Custom universe (subset of Robinhood Crypto's supported pairs):

       python3 robinhood_crypto.py --mode live --dashboard \
         --universe BTC-USD,ETH-USD,SOL-USD

  ================================================================================
  THE DASHBOARD
  ================================================================================

  Once running, open http://127.0.0.1:8770/. Nine tabs:

    Overview     KPIs · equity curve · positions · target weights · events.
    Strategies   All 4 strategies side by side with detailed specs + formulas.
                 Shift-click a strategy card to set it active.
    Markets      Per-asset candlestick-style charts with EMA + Bollinger.
    Indicators   Live signal table (z, RSI, MACD, ATR, realized vol).
    Backtest     Replay any strategy over live bars, or fetch 10-year daily
                 history from yfinance and run a real historical backtest.
    Robustness   2-D parameter-sweep heatmap: e.g. ENTRY_Z × EMA_PERIOD → Sharpe.
    Risk         Drawdown curve, returns histogram, kill-switch state.
    Research     Per-asset stats, correlation matrix, regime detection,
                 macro reference (SPY/QQQ/GLD via yfinance).
    Tools        Live-editable strategy knobs and emergency controls.

  Header buttons:
    Cycle Now      Interrupts the sleep, runs a fresh cycle immediately.
    Pause / Resume Gates new entries (existing positions still managed).
    Flatten All    Emergency sell — closes every open position to USD.

  ================================================================================
  SECRET MANAGEMENT
  ================================================================================

  • All credentials live in `.env` (or your shell environment), NEVER in code.
  • The private-key PEM file should have `chmod 600` permissions and live
    outside the project directory (e.g. `~/.rh/`).
  • The dashboard binds to `127.0.0.1` only — no auth, but never exposed off
    your machine. Do NOT expose port 8770 publicly.
  • State persists to `rh_state.json` in the working directory: positions,
    daily anchor, lifetime counters, cost basis. Atomic writes survive crashes.
  • If you suspect credential leakage, rotate the Robinhood API key
    immediately at robinhood.com → API Keys.

  ================================================================================
  FILES CREATED AT RUNTIME
  ================================================================================

    rh_state.json    persistent state (positions, anchor, stats)
    rh_trades.csv    human-readable trade journal
    rh_trades.jsonl  structured journal with full event metadata
    rh_crypto.log    application log

  Delete `rh_state.json` to reset all tracked state. The journal files append.

  ================================================================================
  TROUBLESHOOTING
  ================================================================================

  • "cryptography not installed — RH orders disabled" — `pip3 install
    --user cryptography`. Read-only scan still works without it.

  • "yfinance not installed" — `pip3 install --user yfinance`. Only needed
    for historical backtest and macro reference.

  • "Address already in use" — a previous bot still owns port 8770. Stop it:
       lsof -nP -iTCP:8770 -sTCP:LISTEN
       pkill -f robinhood_crypto.py

  • "HTTP 503" / "Web Page Blocked" on quote endpoints — corporate firewall
    is intercepting `trading.robinhood.com`. Use `--mock` here, run live on
    a personal machine.

  • Dashboard empty — strategies need warmup bars. In live mode this takes
    `EMA_PERIOD` × `BAR_INTERVAL_SECONDS` ≈ 1 hour. In mock mode bars are
    pre-seeded so signals fire immediately.

  • Strategy decisions don't seem to apply — Config changes apply on the
    NEXT scan cycle, not instantly. Hit `Cycle Now` after Apply.

  ================================================================================
  DISCLAIMER
  ================================================================================

  For educational and research use. Trading crypto carries substantial risk.
  This is NOT financial, legal, or tax advice. The author assumes no liability
  for any losses incurred by use of this software.

  Author : Michael Wang | 2026-06-17
================================================================================
"""

import argparse
import base64
import csv
import json
import logging
import math
import os
import sys
import time
import copy as _copy
import threading as _threading
from collections import deque
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Optional
from urllib.parse import urlparse, parse_qs

try:
    import requests
    from dotenv import load_dotenv
except ImportError as e:
    sys.exit(
        f"\n[FATAL] Missing dependency: {e}\n"
        "Run: pip install requests python-dotenv\n"
    )

try:
    from cryptography.hazmat.primitives.asymmetric.ed25519 import (
        Ed25519PrivateKey)
    from cryptography.hazmat.primitives.serialization import (
        load_pem_private_key)
    ED25519_AVAILABLE = True
except ImportError:
    ED25519_AVAILABLE = False

# Optional: yfinance for historical / macro reference data.
# Free, no API key. Hits query1.finance.yahoo.com. Install with: pip install yfinance
try:
    import yfinance as _yf
    YFINANCE_AVAILABLE = True
except ImportError:
    YFINANCE_AVAILABLE = False

load_dotenv()


# ╔══════════════════════════════════════════════════════════════════════════════╗
# ║                          1. CONFIGURATION                                   ║
# ╚══════════════════════════════════════════════════════════════════════════════╝

class Config:
    # ── Robinhood Crypto API ────────────────────────────────────────────────
    RH_HOST              = "https://trading.robinhood.com"
    RH_API_KEY_ID        = os.getenv("RH_API_KEY_ID", "")
    RH_PRIVATE_KEY_PATH  = os.getenv("RH_PRIVATE_KEY_PATH", "")

    # ── Universe (Robinhood Crypto symbols) ─────────────────────────────────
    # Expanded to 12 majors + alts. All have base prices in MockCryptoClient.
    UNIVERSE: list = [
        "BTC-USD", "ETH-USD", "SOL-USD", "AVAX-USD",
        "LINK-USD", "DOGE-USD", "MATIC-USD", "DOT-USD",
        "ADA-USD", "LTC-USD", "BCH-USD", "UNI-USD",
    ]

    # ── Macro reference assets (via yfinance, daily) ────────────────────────
    # SPY = S&P 500 ETF, QQQ = Nasdaq 100, GLD = gold, UUP = USD index proxy.
    MACRO_REFS: list = ["SPY", "QQQ", "GLD", "UUP"]

    # ── Polling cadence ─────────────────────────────────────────────────────
    # 10s polling = responsive monitoring; bars close every 60s so signals
    # refresh up to 6× per minute. Trades gated by REBALANCE_THRESHOLD so
    # we don't churn through the spread.
    POLL_INTERVAL_SECONDS    = 10        # quote poll frequency
    BAR_INTERVAL_SECONDS     = 60        # 1-minute bars
    BAR_HISTORY              = 600       # ~10h of 1m bars

    # ── Strategy selection ──────────────────────────────────────────────────
    ACTIVE_STRATEGY          = "vtmr"     # one of: vtmr, momentum, bb_breakout, buy_hold

    # ── Strategy: vol-targeted mean reversion (VTMR) ────────────────────────
    EMA_PERIOD               = 60        # bars for the reversion mean (1h @ 1m bars)
    Z_WINDOW                 = 60        # bars for the z-score std
    ENTRY_Z                  = -1.5      # enter LONG when z < -1.5
    EXIT_Z                   = -0.3      # exit LONG when z > -0.3
    SIGNAL_BLEND             = 0.5       # weight blend (z-score vs RSI)

    # ── Strategy: momentum (EMA crossover + MACD) ───────────────────────────
    MOMENTUM_MIN_GAP_PCT     = 0.005     # min EMA-gap / price to enter
    MOMENTUM_MAX_RSI         = 75        # don't chase if RSI > this

    # ── Strategy: bollinger breakout ────────────────────────────────────────
    BB_EXIT_AT_MID           = True      # exit when price ≥ middle band

    # ── Indicators ──────────────────────────────────────────────────────────
    RSI_PERIOD               = 14
    BOLLINGER_PERIOD         = 20
    BOLLINGER_K              = 2.0
    MACD_FAST                = 12
    MACD_SLOW                = 26
    MACD_SIGNAL              = 9
    ATR_PERIOD               = 14
    REALIZED_VOL_PERIOD      = 30        # bars
    PERIODS_PER_YEAR         = 365 * 24 * 60   # 1-min bars per year

    # ── Risk management ─────────────────────────────────────────────────────
    TOTAL_CAPITAL                  = 10_000.0
    TARGET_PORTFOLIO_VOL_ANNUAL    = 0.10      # 10% annualized
    MAX_LEVERAGE                   = 1.0        # spot only
    MAX_PER_ASSET_WEIGHT           = 0.40
    DAILY_LOSS_LIMIT_PCT           = 0.03      # halt new at -3% daily
    HARD_HALT_LOSS_PCT             = 0.06      # halt all at -6% daily
    MAX_DRAWDOWN_PCT               = 0.10      # halt at 10% lifetime DD
    REBALANCE_THRESHOLD            = 0.05      # only trade if Δweight > 5%
    MIN_ORDER_USD                  = 5.0
    MAX_ORDERS_PER_DAY_PER_ASSET   = 12

    # ── Execution ───────────────────────────────────────────────────────────
    SLIPPAGE_TOLERANCE             = 0.002     # 20 bps
    ORDER_FILL_TIMEOUT_SECONDS     = 30

    # ── I/O ─────────────────────────────────────────────────────────────────
    LOG_FILE        = "rh_crypto.log"
    TRADES_CSV      = "rh_trades.csv"
    TRADES_JSONL    = "rh_trades.jsonl"
    STATE_FILE      = "rh_state.json"
    LOG_LEVEL       = logging.INFO
    RATE_LIMIT_PER_MIN = 60

    # ── Mode flags ──────────────────────────────────────────────────────────
    DRY_RUN         = True


# ╔══════════════════════════════════════════════════════════════════════════════╗
# ║                          2. LOGGING & JOURNAL                               ║
# ╚══════════════════════════════════════════════════════════════════════════════╝

def setup_logging(level: int = logging.INFO):
    logging.basicConfig(
        level=level,
        format="%(asctime)s  %(levelname)-8s  %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
        handlers=[
            logging.StreamHandler(sys.stdout),
            logging.FileHandler(Config.LOG_FILE),
        ],
    )

log = logging.getLogger("RhCrypto")


class Journal:
    CSV_FIELDS = [
        "timestamp", "event", "symbol", "side",
        "qty", "price", "usd_amount", "dry_run", "notes",
    ]

    def __init__(self, csv_path: str = Config.TRADES_CSV,
                 jsonl_path: str = Config.TRADES_JSONL):
        self.csv_path = csv_path
        self.jsonl_path = jsonl_path
        self._dashboard: Optional["DashboardState"] = None
        if not os.path.isfile(self.csv_path):
            with open(self.csv_path, "w", newline="") as f:
                csv.DictWriter(f, fieldnames=self.CSV_FIELDS).writeheader()

    def attach_dashboard(self, dashboard: "DashboardState"):
        self._dashboard = dashboard

    def record(self, *, event: str, symbol: str = "", side: str = "",
               qty: float = 0.0, price: float = 0.0, usd_amount: float = 0.0,
               dry_run: bool = True, notes: str = "",
               extra: Optional[dict] = None):
        ts = datetime.now(timezone.utc).isoformat()
        row = {
            "timestamp": ts, "event": event, "symbol": symbol, "side": side,
            "qty": round(qty, 8) if qty else "",
            "price": round(price, 6) if price else "",
            "usd_amount": round(usd_amount, 4) if usd_amount else "",
            "dry_run": dry_run, "notes": notes,
        }
        with open(self.csv_path, "a", newline="") as f:
            csv.DictWriter(f, fieldnames=self.CSV_FIELDS).writerow(row)
        json_row = dict(row)
        if extra:
            json_row["extra"] = extra
        with open(self.jsonl_path, "a") as f:
            f.write(json.dumps(json_row) + "\n")
        if self._dashboard is not None:
            self._dashboard.push_event(json_row)


# ╔══════════════════════════════════════════════════════════════════════════════╗
# ║                          3. PERSISTENT STORE                                ║
# ╚══════════════════════════════════════════════════════════════════════════════╝

class PersistentStore:
    def __init__(self, path: str = Config.STATE_FILE):
        self.path = path
        self._lock = _threading.RLock()
        self._data: dict = self._load()

    def _load(self) -> dict:
        try:
            with open(self.path) as f:
                return json.load(f)
        except FileNotFoundError:
            return {}
        except json.JSONDecodeError as e:
            log.error("State file corrupted (%s) — backing up.", e)
            try:
                os.replace(self.path, self.path + ".corrupt")
            except OSError:
                pass
            return {}

    def get(self, key: str, default=None):
        with self._lock:
            return _copy.deepcopy(self._data[key]) if key in self._data else default

    def put(self, key: str, value) -> None:
        with self._lock:
            self._data[key] = value
            self._save_atomic()

    def put_many(self, updates: dict) -> None:
        with self._lock:
            self._data.update(updates)
            self._save_atomic()

    def _save_atomic(self) -> None:
        tmp = self.path + ".tmp"
        with open(tmp, "w") as f:
            json.dump(self._data, f, default=str, indent=2, sort_keys=True)
        os.replace(tmp, self.path)


# ╔══════════════════════════════════════════════════════════════════════════════╗
# ║                          4. DATACLASSES                                     ║
# ╚══════════════════════════════════════════════════════════════════════════════╝

@dataclass
class Quote:
    symbol: str
    bid: float
    ask: float
    timestamp: datetime

    @property
    def mid(self) -> float:
        return (self.bid + self.ask) / 2.0 if (self.bid and self.ask) else 0.0

    @property
    def spread_bps(self) -> float:
        m = self.mid
        return ((self.ask - self.bid) / m * 10_000) if m else 0.0


@dataclass
class Bar:
    symbol: str
    start: datetime
    open: float
    high: float
    low: float
    close: float
    volume: float = 0.0


@dataclass
class CryptoPosition:
    symbol: str
    qty: float
    avg_cost: float
    opened_at: datetime

    @property
    def cost_basis_usd(self) -> float:
        return self.qty * self.avg_cost


@dataclass
class Signal:
    symbol: str
    timestamp: datetime
    price: float
    ema: float
    z_score: float
    rsi: float
    macd: float
    macd_signal: float
    bollinger_upper: float
    bollinger_lower: float
    atr: float
    realized_vol_annual: float
    direction: str            # "LONG" | "FLAT"
    strength: float           # 0..1
    target_weight: float      # fraction of portfolio


@dataclass
class CryptoDecision:
    symbol: str
    side: str                 # "BUY" | "SELL"
    qty: float
    limit_price: float
    target_weight: float
    notes: str = ""


# ╔══════════════════════════════════════════════════════════════════════════════╗
# ║                          5. ROBINHOOD CRYPTO CLIENT                         ║
# ╚══════════════════════════════════════════════════════════════════════════════╝

class _RateLimiter:
    def __init__(self, per_minute: int):
        self.per_minute = per_minute
        self._times: deque = deque()

    def wait(self):
        now = time.monotonic()
        cutoff = now - 60.0
        while self._times and self._times[0] < cutoff:
            self._times.popleft()
        if len(self._times) >= self.per_minute:
            sleep_for = 60.0 - (now - self._times[0]) + 0.05
            if sleep_for > 0:
                time.sleep(sleep_for)
        self._times.append(time.monotonic())


class BrokerClient:
    """Abstract crypto broker interface."""
    name: str = "abstract"

    def init_auth(self) -> bool:
        return False

    def get_quote(self, symbol: str) -> Optional[Quote]:
        raise NotImplementedError

    def get_balance(self) -> Optional[float]:
        """USD cash available for new positions."""
        return None

    def get_holdings(self) -> dict:
        """{symbol: qty} of current holdings."""
        return {}

    def place_order(self, *, symbol: str, side: str, qty: float,
                    limit_price: float) -> Optional[dict]:
        raise NotImplementedError

    def get_order_status(self, order_id: str) -> Optional[dict]:
        return None

    def warmup_bars(self, symbol: str,
                    n_bars: int) -> list[Bar]:
        """Optional: return historical OHLCV bars to seed the aggregator
        so indicators have a baseline immediately. Default: empty."""
        return []


class RobinhoodCryptoClient(BrokerClient):
    """
    Robinhood Crypto API client.

    Auth: Ed25519 signing per docs.robinhood.com/crypto.
    Message = api_key + str(timestamp) + path + method + body
    Sign with Ed25519 private key, base64-encode the signature.
    Headers: x-api-key, x-signature, x-timestamp.
    """
    name = "robinhood_crypto"

    def __init__(self, *, read_only: bool = False):
        self._rl = _RateLimiter(Config.RATE_LIMIT_PER_MIN)
        self._read_only = read_only
        self._key_id = ""
        self._private_key = None
        self._auth_ready = False

    def init_auth(self) -> bool:
        if self._read_only:
            log.info("Robinhood: read-only mode (no auth).")
            return False
        if not ED25519_AVAILABLE:
            log.warning("cryptography not installed — RH orders disabled.")
            return False
        if not Config.RH_API_KEY_ID or not Config.RH_PRIVATE_KEY_PATH:
            log.warning("RH creds missing (RH_API_KEY_ID / "
                        "RH_PRIVATE_KEY_PATH). Orders disabled.")
            return False
        try:
            with open(Config.RH_PRIVATE_KEY_PATH, "rb") as f:
                self._private_key = load_pem_private_key(f.read(), password=None)
            if not isinstance(self._private_key, Ed25519PrivateKey):
                log.error("RH private key is not Ed25519.")
                return False
            self._key_id = Config.RH_API_KEY_ID
            self._auth_ready = True
            log.info("Robinhood auth initialized (key=%s...).", self._key_id[:8])
            return True
        except Exception as e:
            log.error("RH auth init failed: %s", e)
            return False

    def _sign(self, method: str, path: str, body: str, ts: int) -> str:
        msg = (self._key_id + str(ts) + path + method + body).encode("utf-8")
        sig = self._private_key.sign(msg)
        return base64.b64encode(sig).decode("ascii")

    def _headers(self, method: str, path: str, body: str = "") -> dict:
        ts = int(time.time())
        return {
            "Content-Type": "application/json",
            "x-api-key": self._key_id,
            "x-signature": self._sign(method, path, body, ts),
            "x-timestamp": str(ts),
        }

    def _request(self, method: str, path: str, *, body: Optional[dict] = None,
                 params: Optional[dict] = None, auth: bool = True,
                 timeout: int = 10) -> Optional[dict]:
        url = Config.RH_HOST + path
        body_str = json.dumps(body, separators=(",", ":")) if body else ""
        headers = self._headers(method, path, body_str) if auth else None
        for attempt in range(3):
            try:
                self._rl.wait()
                r = requests.request(
                    method, url, params=params,
                    data=body_str if body else None,
                    headers=headers, timeout=timeout)
                if r.status_code in (200, 201):
                    return r.json()
                if r.status_code in (429, 500, 502, 503, 504):
                    time.sleep(2 ** attempt)
                    continue
                log.warning("RH %s %s -> HTTP %d: %s",
                            method, path, r.status_code, r.text[:200])
                return None
            except Exception as e:
                log.debug("RH request error: %s", e)
                time.sleep(2 ** attempt)
        return None

    # ── Public endpoints (auth still required by RH crypto API) ─────────────
    def get_quote(self, symbol: str) -> Optional[Quote]:
        path = "/api/v1/crypto/marketdata/best_bid_ask/"
        data = self._request("GET", path,
                             params={"symbol": symbol},
                             auth=self._auth_ready)
        if not data:
            return None
        results = data.get("results", [])
        if not results:
            return None
        r0 = results[0]
        try:
            bid_inside = r0.get("bid_inside_bbo") or r0.get("price")
            ask_inside = r0.get("ask_inside_bbo") or r0.get("price")
            return Quote(symbol=symbol,
                         bid=float(bid_inside),
                         ask=float(ask_inside),
                         timestamp=datetime.now(timezone.utc))
        except Exception as e:
            log.debug("RH quote parse error for %s: %s", symbol, e)
            return None

    def get_balance(self) -> Optional[float]:
        if not self._auth_ready:
            return None
        data = self._request("GET", "/api/v1/crypto/trading/account/")
        if not data:
            return None
        try:
            return float(data.get("buying_power", 0))
        except Exception:
            return None

    def get_holdings(self) -> dict:
        if not self._auth_ready:
            return {}
        data = self._request("GET", "/api/v1/crypto/trading/holdings/")
        if not data:
            return {}
        out = {}
        for h in data.get("results") or []:
            try:
                sym = h.get("asset_code", "") + "-USD"
                qty = float(h.get("total_quantity", 0))
                if qty > 0:
                    out[sym] = qty
            except Exception:
                continue
        return out

    def place_order(self, *, symbol: str, side: str, qty: float,
                    limit_price: float) -> Optional[dict]:
        side_norm = side.lower()
        if side_norm not in ("buy", "sell"):
            log.error("place_order: bad side %s", side)
            return None
        if qty <= 0 or limit_price <= 0:
            log.error("place_order: bad qty/price")
            return None

        if Config.DRY_RUN or not self._auth_ready:
            log.info("[DRY-RUN] RH %s %s qty=%.8f @ %.6f",
                     side_norm.upper(), symbol, qty, limit_price)
            return {"dry_run": True, "id": "dry-" + str(int(time.time() * 1000)),
                    "qty": qty, "price": limit_price}

        body = {
            "client_order_id": f"vtmr-{int(time.time() * 1000)}-{symbol}",
            "symbol": symbol,
            "side": side_norm,
            "type": "limit",
            "limit_order_config": {
                "asset_quantity": str(qty),
                "limit_price": str(round(limit_price, 6)),
                "time_in_force": "gtc",
            },
        }
        resp = self._request("POST", "/api/v1/crypto/trading/orders/", body=body)
        if not resp:
            return None
        return {"id": resp.get("id"), "qty": qty, "price": limit_price,
                "raw": resp}

    def get_order_status(self, order_id: str) -> Optional[dict]:
        if not self._auth_ready:
            return None
        data = self._request("GET",
                             f"/api/v1/crypto/trading/orders/{order_id}/")
        if not data:
            return None
        # Robinhood returns: state in {open, filled, cancelled, ...}
        st = data.get("state", "unknown")
        normalized = {"filled": "filled", "open": "open",
                      "cancelled": "cancelled", "canceled": "cancelled",
                      "rejected": "rejected"}.get(st, st)
        filled = float(data.get("filled_asset_quantity", 0) or 0)
        avg_price = data.get("average_price")
        return {
            "status": normalized,
            "filled_size": filled,
            "size": float(data.get("asset_quantity", 0) or 0),
            "avg_price": float(avg_price) if avg_price else None,
            "raw": data,
        }


# ╔══════════════════════════════════════════════════════════════════════════════╗
# ║                          6. MOCK CLIENT (offline dev)                       ║
# ╚══════════════════════════════════════════════════════════════════════════════╝

import random as _random


class MockCryptoClient(BrokerClient):
    """Deterministic synthetic-quote generator with realistic drift + vol.

    Each symbol follows a geometric Brownian motion calibrated to a base
    price and an annual vol. Quotes drift between polls. Mean-reverting
    micro-shocks make the strategy actually trade.
    """
    name = "mock"

    BASE_PRICES = {
        "BTC-USD": 65_000.0,  "ETH-USD": 3_400.0,
        "SOL-USD": 165.0,     "AVAX-USD": 38.0,
        "DOGE-USD": 0.16,     "LINK-USD": 18.5,
        "MATIC-USD": 0.85,    "DOT-USD": 7.20,
        "ADA-USD": 0.62,      "LTC-USD": 92.0,
        "BCH-USD": 480.0,     "UNI-USD": 11.5,
        "ATOM-USD": 9.3,      "FIL-USD": 5.8,
        "USDC-USD": 1.0,
    }
    ANNUAL_VOL = {
        "BTC-USD": 0.45,  "ETH-USD": 0.65,
        "SOL-USD": 0.95,  "AVAX-USD": 1.05,
        "DOGE-USD": 1.40, "LINK-USD": 0.85,
        "MATIC-USD": 1.05, "DOT-USD": 0.95,
        "ADA-USD": 0.90,   "LTC-USD": 0.70,
        "BCH-USD": 0.80,   "UNI-USD": 1.10,
        "ATOM-USD": 1.00,  "FIL-USD": 1.30,
        "USDC-USD": 0.02,
    }

    def __init__(self, seed: int = 17):
        self._rng = _random.Random(seed)
        self._prices: dict = {}
        self._holdings: dict = {}
        self._cash: float = Config.TOTAL_CAPITAL
        self._orders: dict = {}
        for sym in Config.UNIVERSE:
            self._prices[sym] = self.BASE_PRICES.get(sym, 100.0)

    def init_auth(self) -> bool:
        log.info("Mock client: synthetic prices.")
        return True

    def _step(self, symbol: str) -> float:
        # GBM with mean-reverting noise. dt = POLL_INTERVAL / seconds-per-year.
        dt = Config.POLL_INTERVAL_SECONDS / (Config.PERIODS_PER_YEAR
                                              * Config.BAR_INTERVAL_SECONDS / 12)
        # Trick to get reasonable per-poll move ~ vol * sqrt(dt)
        sigma = self.ANNUAL_VOL.get(symbol, 0.5)
        sigma_step = sigma * math.sqrt(Config.POLL_INTERVAL_SECONDS
                                        / (365 * 24 * 3600))
        shock = self._rng.gauss(0, sigma_step)
        # Slight reversion to base price (Ornstein-Uhlenbeck flavour)
        base = self.BASE_PRICES.get(symbol, 100.0)
        prev = self._prices[symbol]
        reversion = (math.log(base) - math.log(prev)) * 0.001
        new_log = math.log(prev) + reversion + shock
        new_p = math.exp(new_log)
        self._prices[symbol] = max(new_p, 0.000001)
        return self._prices[symbol]

    def get_quote(self, symbol: str) -> Optional[Quote]:
        if symbol not in self._prices:
            self._prices[symbol] = self.BASE_PRICES.get(symbol, 100.0)
        mid = self._step(symbol)
        # 8 bps spread
        bid = mid * (1 - 0.0004)
        ask = mid * (1 + 0.0004)
        return Quote(symbol=symbol, bid=bid, ask=ask,
                     timestamp=datetime.now(timezone.utc))

    def get_balance(self) -> Optional[float]:
        return self._cash

    def get_holdings(self) -> dict:
        return dict(self._holdings)

    def place_order(self, *, symbol: str, side: str, qty: float,
                    limit_price: float) -> Optional[dict]:
        side_norm = side.lower()
        cost = qty * limit_price
        if side_norm == "buy":
            if cost > self._cash:
                log.warning("Mock: insufficient cash for %s buy", symbol)
                return None
            self._cash -= cost
            self._holdings[symbol] = self._holdings.get(symbol, 0.0) + qty
        else:  # sell
            held = self._holdings.get(symbol, 0.0)
            if qty > held + 1e-9:
                log.warning("Mock: insufficient %s holdings for sell", symbol)
                return None
            self._holdings[symbol] = held - qty
            if self._holdings[symbol] < 1e-9:
                del self._holdings[symbol]
            self._cash += cost
        oid = f"mock-{int(time.time() * 1000)}-{symbol}-{side_norm}"
        self._orders[oid] = {"status": "filled", "filled_size": qty,
                             "size": qty, "avg_price": limit_price}
        log.info("[MOCK FILL] %s %s qty=%.6f @ %.4f (cash=$%.2f)",
                 side_norm.upper(), symbol, qty, limit_price, self._cash)
        return {"id": oid, "qty": qty, "price": limit_price,
                "raw": {"mock": True}}

    def get_order_status(self, order_id: str) -> Optional[dict]:
        return self._orders.get(order_id)

    def warmup_bars(self, symbol: str, n_bars: int) -> list[Bar]:
        """Generate `n_bars` of synthetic historical OHLCV so the strategy
        has indicators populated immediately. GBM with reversion to base."""
        if symbol not in self._prices:
            self._prices[symbol] = self.BASE_PRICES.get(symbol, 100.0)
        base = self.BASE_PRICES.get(symbol, 100.0)
        sigma = self.ANNUAL_VOL.get(symbol, 0.5)
        # Per-bar vol (5-min bars)
        sigma_bar = sigma * math.sqrt(Config.BAR_INTERVAL_SECONDS / (365 * 24 * 3600))
        bars = []
        # Start the synthetic clock n_bars * BAR_INTERVAL_SECONDS in the past,
        # walk forward.
        bar_secs = Config.BAR_INTERVAL_SECONDS
        now = datetime.now(timezone.utc)
        start_ts = int(now.timestamp()) - n_bars * bar_secs
        p = base  # start at base
        for i in range(n_bars):
            ts = datetime.fromtimestamp(start_ts + i * bar_secs, tz=timezone.utc)
            open_ = p
            # Simulate a few ticks per bar
            hi, lo = open_, open_
            for _ in range(5):
                reversion = (math.log(base) - math.log(p)) * 0.002
                shock = self._rng.gauss(0, sigma_bar / math.sqrt(5))
                p = math.exp(math.log(p) + reversion + shock)
                hi = max(hi, p)
                lo = min(lo, p)
            bars.append(Bar(symbol=symbol, start=ts,
                            open=open_, high=hi, low=lo, close=p, volume=0))
        # Update the live price so next get_quote continues from here
        self._prices[symbol] = p
        return bars


# ╔══════════════════════════════════════════════════════════════════════════════╗
# ║                          7. BAR AGGREGATOR                                  ║
# ╚══════════════════════════════════════════════════════════════════════════════╝

class BarAggregator:
    """Build OHLCV bars from poll-frequency quote ticks. Rolling window."""

    def __init__(self, history_len: int = Config.BAR_HISTORY):
        self.history_len = history_len
        self._bars: dict[str, deque] = {}        # symbol → deque[Bar]
        self._current: dict[str, dict] = {}      # symbol → dict in-progress

    def on_quote(self, q: Quote) -> Optional[Bar]:
        """Update bars. Returns the just-closed bar if a bar boundary crossed."""
        bin_secs = Config.BAR_INTERVAL_SECONDS
        bin_start_ts = int(q.timestamp.timestamp()) // bin_secs * bin_secs
        bin_start = datetime.fromtimestamp(bin_start_ts, tz=timezone.utc)
        mid = q.mid
        closed = None

        cur = self._current.get(q.symbol)
        if cur is None:
            self._current[q.symbol] = {"start": bin_start, "o": mid,
                                       "h": mid, "l": mid, "c": mid, "v": 0}
            return None

        if cur["start"] != bin_start:
            # Close out previous bar
            closed = Bar(symbol=q.symbol, start=cur["start"],
                         open=cur["o"], high=cur["h"],
                         low=cur["l"], close=cur["c"], volume=cur["v"])
            dq = self._bars.setdefault(q.symbol,
                                       deque(maxlen=self.history_len))
            dq.append(closed)
            # Start new bar
            self._current[q.symbol] = {"start": bin_start, "o": mid,
                                       "h": mid, "l": mid, "c": mid, "v": 0}
        else:
            cur["h"] = max(cur["h"], mid)
            cur["l"] = min(cur["l"], mid)
            cur["c"] = mid
            cur["v"] += 1

        return closed

    def seed(self, symbol: str, bars: list[Bar]):
        """Pre-load historical bars (e.g. from MockCryptoClient.warmup_bars
        or a real broker's candlestick endpoint)."""
        dq = self._bars.setdefault(symbol, deque(maxlen=self.history_len))
        for b in bars:
            dq.append(b)

    def bars(self, symbol: str) -> list[Bar]:
        return list(self._bars.get(symbol, []))

    def closes(self, symbol: str) -> list[float]:
        return [b.close for b in self._bars.get(symbol, [])]

    def latest_bar(self, symbol: str) -> Optional[Bar]:
        dq = self._bars.get(symbol)
        return dq[-1] if dq else None


# ╔══════════════════════════════════════════════════════════════════════════════╗
# ║                          8. TECHNICAL INDICATORS                            ║
# ╚══════════════════════════════════════════════════════════════════════════════╝

class Indicators:
    """Pure functions. Numpy-free — keeps deployment simple."""

    @staticmethod
    def sma(values: list, period: int) -> float:
        if len(values) < period:
            return float("nan")
        return sum(values[-period:]) / period

    @staticmethod
    def ema(values: list, period: int) -> float:
        if len(values) < period:
            return float("nan")
        alpha = 2 / (period + 1)
        e = sum(values[:period]) / period
        for v in values[period:]:
            e = alpha * v + (1 - alpha) * e
        return e

    @staticmethod
    def std(values: list, period: int) -> float:
        if len(values) < period:
            return float("nan")
        slc = values[-period:]
        m = sum(slc) / period
        return math.sqrt(sum((x - m) ** 2 for x in slc) / period)

    @staticmethod
    def rsi(values: list, period: int = 14) -> float:
        if len(values) < period + 1:
            return float("nan")
        gains, losses = 0.0, 0.0
        for i in range(-period, 0):
            change = values[i] - values[i - 1]
            if change >= 0:
                gains += change
            else:
                losses += -change
        avg_gain = gains / period
        avg_loss = losses / period
        if avg_loss == 0:
            return 100.0
        rs = avg_gain / avg_loss
        return 100 - (100 / (1 + rs))

    @staticmethod
    def bollinger(values: list, period: int = 20, k: float = 2.0) -> tuple:
        if len(values) < period:
            return float("nan"), float("nan"), float("nan")
        mid = Indicators.sma(values, period)
        s = Indicators.std(values, period)
        return mid - k * s, mid, mid + k * s

    @staticmethod
    def macd(values: list, fast: int = 12, slow: int = 26,
             signal: int = 9) -> tuple:
        if len(values) < slow + signal:
            return float("nan"), float("nan")
        ema_fast = Indicators.ema(values, fast)
        ema_slow = Indicators.ema(values, slow)
        macd_line = ema_fast - ema_slow
        # Signal = EMA of MACD line. Approximate by reusing values.
        # For simplicity compute over last `signal` macd snapshots.
        macd_series = []
        for i in range(signal, 0, -1):
            sub = values[:-i] if i > 0 else values
            if len(sub) >= slow:
                macd_series.append(
                    Indicators.ema(sub, fast) - Indicators.ema(sub, slow))
        if len(macd_series) < signal:
            return macd_line, float("nan")
        sig_line = sum(macd_series[-signal:]) / signal
        return macd_line, sig_line

    @staticmethod
    def atr(highs: list, lows: list, closes: list, period: int = 14) -> float:
        if min(len(highs), len(lows), len(closes)) < period + 1:
            return float("nan")
        trs = []
        for i in range(-period, 0):
            hi, lo, prev_close = highs[i], lows[i], closes[i - 1]
            tr = max(hi - lo, abs(hi - prev_close), abs(lo - prev_close))
            trs.append(tr)
        return sum(trs) / period

    @staticmethod
    def z_score(values: list, period: int) -> float:
        if len(values) < period:
            return float("nan")
        slc = values[-period:]
        m = sum(slc) / period
        s = math.sqrt(sum((x - m) ** 2 for x in slc) / period)
        if s == 0:
            return 0.0
        return (values[-1] - m) / s

    @staticmethod
    def returns(values: list) -> list:
        if len(values) < 2:
            return []
        return [values[i] / values[i - 1] - 1
                for i in range(1, len(values))
                if values[i - 1] > 0]

    @staticmethod
    def realized_vol_annual(values: list, period: int,
                            periods_per_year: int) -> float:
        rets = Indicators.returns(values[-(period + 1):])
        if len(rets) < 2:
            return float("nan")
        m = sum(rets) / len(rets)
        s = math.sqrt(sum((r - m) ** 2 for r in rets) / len(rets))
        return s * math.sqrt(periods_per_year)

    @staticmethod
    def sharpe(returns_series: list, periods_per_year: int) -> float:
        if len(returns_series) < 5:
            return float("nan")
        m = sum(returns_series) / len(returns_series)
        s = math.sqrt(sum((r - m) ** 2 for r in returns_series)
                      / max(1, len(returns_series) - 1))
        if s == 0:
            return 0.0
        return (m / s) * math.sqrt(periods_per_year)

    @staticmethod
    def sortino(returns_series: list, periods_per_year: int) -> float:
        if len(returns_series) < 5:
            return float("nan")
        m = sum(returns_series) / len(returns_series)
        downs = [r for r in returns_series if r < 0]
        if not downs:
            return float("inf")
        d = math.sqrt(sum((r - m) ** 2 for r in downs) / len(downs))
        if d == 0:
            return 0.0
        return (m / d) * math.sqrt(periods_per_year)

    @staticmethod
    def max_drawdown(equity_curve: list) -> tuple:
        """Return (max_drawdown_pct, peak_idx, trough_idx)."""
        if len(equity_curve) < 2:
            return 0.0, 0, 0
        peak = equity_curve[0]
        peak_idx = 0
        max_dd = 0.0
        max_dd_peak = 0
        max_dd_trough = 0
        for i, v in enumerate(equity_curve):
            if v > peak:
                peak = v
                peak_idx = i
            if peak > 0:
                dd = (v / peak) - 1.0
                if dd < max_dd:
                    max_dd = dd
                    max_dd_peak = peak_idx
                    max_dd_trough = i
        return max_dd, max_dd_peak, max_dd_trough

    @staticmethod
    def correlation(a: list, b: list) -> float:
        n = min(len(a), len(b))
        if n < 5:
            return float("nan")
        a, b = a[-n:], b[-n:]
        ma = sum(a) / n
        mb = sum(b) / n
        num = sum((a[i] - ma) * (b[i] - mb) for i in range(n))
        da = math.sqrt(sum((x - ma) ** 2 for x in a))
        db = math.sqrt(sum((x - mb) ** 2 for x in b))
        if da == 0 or db == 0:
            return 0.0
        return num / (da * db)


# ╔══════════════════════════════════════════════════════════════════════════════╗
# ║                          9. STRATEGIES                                      ║
# ╚══════════════════════════════════════════════════════════════════════════════╝

class Strategy:
    """Base class: pure-function signal generator."""
    NAME = "abstract"
    DESCRIPTION = ""

    @classmethod
    def compute_signal(cls, symbol: str, closes: list[float],
                       highs: list[float], lows: list[float]) -> Optional[Signal]:
        raise NotImplementedError

    @classmethod
    def normalize_weights(cls, signals: dict) -> dict[str, float]:
        """Cap total leverage to MAX_LEVERAGE; per-asset cap is per-strategy."""
        raw = {sym: s.target_weight for sym, s in signals.items() if s}
        total = sum(raw.values())
        if total > Config.MAX_LEVERAGE and total > 0:
            scale = Config.MAX_LEVERAGE / total
            return {sym: w * scale for sym, w in raw.items()}
        return raw

    @classmethod
    def _common_indicators(cls, closes, highs, lows) -> dict:
        return {
            "ema": Indicators.ema(closes, Config.EMA_PERIOD),
            "z": Indicators.z_score(closes, Config.Z_WINDOW),
            "rsi": Indicators.rsi(closes, Config.RSI_PERIOD),
            "bb": Indicators.bollinger(closes, Config.BOLLINGER_PERIOD,
                                       Config.BOLLINGER_K),
            "macd": Indicators.macd(closes, Config.MACD_FAST,
                                    Config.MACD_SLOW, Config.MACD_SIGNAL),
            "atr": Indicators.atr(highs, lows, closes, Config.ATR_PERIOD),
            "rvol": Indicators.realized_vol_annual(
                closes, Config.REALIZED_VOL_PERIOD, Config.PERIODS_PER_YEAR),
        }


class VTMRStrategy(Strategy):
    """Vol-Targeted Mean Reversion.

    LONG when z-score is deeply negative (oversold vs rolling EMA). Position
    sized inverse to realized vol so each name contributes ~equal vol to the
    portfolio. Blends z-score with RSI confirmation.
    """
    NAME = "vtmr"
    DESCRIPTION = ("Vol-targeted mean reversion. LONG when z-score < ENTRY_Z; "
                   "size = (1/realized_vol) × signal × target_vol. Tends to "
                   "catch overshoots in established ranges; weak in trends.")

    @classmethod
    def compute_signal(cls, symbol, closes, highs, lows) -> Optional[Signal]:
        if len(closes) < max(Config.EMA_PERIOD, Config.Z_WINDOW,
                             Config.BOLLINGER_PERIOD,
                             Config.MACD_SLOW + Config.MACD_SIGNAL):
            return None
        x = cls._common_indicators(closes, highs, lows)
        price = closes[-1]
        z, rsi = x["z"], x["rsi"]
        bb_lo, _, bb_hi = x["bb"]
        macd_line, sig_line = x["macd"]
        rvol = x["rvol"]

        z_strength = max(0.0, min(1.0, (-z - 0.5) / 2.0)) if z < 0 else 0.0
        rsi_strength = max(0.0, min(1.0, (40 - rsi) / 20))
        blended = (Config.SIGNAL_BLEND * z_strength
                   + (1 - Config.SIGNAL_BLEND) * rsi_strength)

        if z < Config.ENTRY_Z:
            direction = "LONG"
        elif z > Config.EXIT_Z:
            direction = "FLAT"; blended = 0.0
        else:
            direction = "HOLD"; blended *= 0.5

        inv_vol = (Config.TARGET_PORTFOLIO_VOL_ANNUAL / rvol
                   if not math.isnan(rvol) and rvol > 1e-6 else 0.0)
        target_weight = max(0.0,
                            min(Config.MAX_PER_ASSET_WEIGHT, blended * inv_vol))

        return Signal(
            symbol=symbol, timestamp=datetime.now(timezone.utc),
            price=price, ema=x["ema"], z_score=z, rsi=rsi,
            macd=macd_line, macd_signal=sig_line,
            bollinger_upper=bb_hi, bollinger_lower=bb_lo,
            atr=x["atr"], realized_vol_annual=rvol,
            direction=direction, strength=blended,
            target_weight=target_weight,
        )


class MomentumStrategy(Strategy):
    """EMA-crossover trend follower.

    LONG when fast EMA > slow EMA AND MACD > signal AND RSI not extended.
    Sized inverse-vol so volatile names get smaller weight.
    """
    NAME = "momentum"
    DESCRIPTION = ("Trend following. LONG when fast EMA > slow EMA, MACD > "
                   "signal, RSI < MOMENTUM_MAX_RSI. Sized inverse-vol. Thrives "
                   "in persistent trends; whipsaw in chop.")

    @classmethod
    def compute_signal(cls, symbol, closes, highs, lows) -> Optional[Signal]:
        if len(closes) < Config.MACD_SLOW + Config.MACD_SIGNAL + 5:
            return None
        x = cls._common_indicators(closes, highs, lows)
        price = closes[-1]
        ema_fast = Indicators.ema(closes, Config.MACD_FAST)
        ema_slow = Indicators.ema(closes, Config.MACD_SLOW)
        if math.isnan(ema_fast) or math.isnan(ema_slow) or price <= 0:
            return None
        rsi = x["rsi"]
        macd_line, sig_line = x["macd"]
        rvol = x["rvol"]
        bb_lo, _, bb_hi = x["bb"]

        gap_pct = (ema_fast - ema_slow) / price
        # Direction
        if (ema_fast > ema_slow and gap_pct > Config.MOMENTUM_MIN_GAP_PCT
                and macd_line > sig_line and rsi < Config.MOMENTUM_MAX_RSI):
            direction = "LONG"
            strength = min(1.0, gap_pct / (Config.MOMENTUM_MIN_GAP_PCT * 5))
        else:
            direction = "FLAT"; strength = 0.0

        inv_vol = (Config.TARGET_PORTFOLIO_VOL_ANNUAL / rvol
                   if not math.isnan(rvol) and rvol > 1e-6 else 0.0)
        target_weight = max(0.0,
                            min(Config.MAX_PER_ASSET_WEIGHT, strength * inv_vol))

        return Signal(
            symbol=symbol, timestamp=datetime.now(timezone.utc),
            price=price, ema=ema_fast, z_score=x["z"], rsi=rsi,
            macd=macd_line, macd_signal=sig_line,
            bollinger_upper=bb_hi, bollinger_lower=bb_lo,
            atr=x["atr"], realized_vol_annual=rvol,
            direction=direction, strength=strength,
            target_weight=target_weight,
        )


class BollingerBreakoutStrategy(Strategy):
    """Bollinger band breakout (reversion flavour).

    LONG when price touches/breaks lower band; exit at mid band. The classic
    'buy the lower band' setup. Combines well with low realized-vol regimes.
    """
    NAME = "bb_breakout"
    DESCRIPTION = ("Buy lower-Bollinger touches. LONG when price ≤ lower band; "
                   "exit at middle band. Sized inverse-vol. Best in ranging "
                   "markets; bleeds in trending crashes.")

    @classmethod
    def compute_signal(cls, symbol, closes, highs, lows) -> Optional[Signal]:
        if len(closes) < max(Config.BOLLINGER_PERIOD, Config.MACD_SLOW) + 5:
            return None
        x = cls._common_indicators(closes, highs, lows)
        price = closes[-1]
        bb_lo, bb_mid, bb_hi = x["bb"]
        if math.isnan(bb_lo):
            return None
        rsi = x["rsi"]
        macd_line, sig_line = x["macd"]
        rvol = x["rvol"]

        band_width = bb_hi - bb_lo
        if band_width > 0:
            position = (price - bb_lo) / band_width   # 0 = lower, 1 = upper
        else:
            position = 0.5

        if price <= bb_lo * 1.001:
            direction = "LONG"
            strength = 1.0
        elif (Config.BB_EXIT_AT_MID and price >= bb_mid) or position >= 0.65:
            direction = "FLAT"
            strength = 0.0
        else:
            direction = "HOLD"
            strength = max(0.0, 1.0 - position * 1.5)

        inv_vol = (Config.TARGET_PORTFOLIO_VOL_ANNUAL / rvol
                   if not math.isnan(rvol) and rvol > 1e-6 else 0.0)
        target_weight = max(0.0,
                            min(Config.MAX_PER_ASSET_WEIGHT, strength * inv_vol))

        return Signal(
            symbol=symbol, timestamp=datetime.now(timezone.utc),
            price=price, ema=x["ema"], z_score=x["z"], rsi=rsi,
            macd=macd_line, macd_signal=sig_line,
            bollinger_upper=bb_hi, bollinger_lower=bb_lo,
            atr=x["atr"], realized_vol_annual=rvol,
            direction=direction, strength=strength,
            target_weight=target_weight,
        )


class BuyAndHoldStrategy(Strategy):
    """Equal-weight buy-and-hold benchmark."""
    NAME = "buy_hold"
    DESCRIPTION = ("Equal-weight buy & hold of the universe. The benchmark "
                   "every active strategy must beat (risk-adjusted) to "
                   "justify trading costs.")

    @classmethod
    def compute_signal(cls, symbol, closes, highs, lows) -> Optional[Signal]:
        if len(closes) < 5:
            return None
        x = cls._common_indicators(closes, highs, lows)
        bb_lo, _, bb_hi = x["bb"]
        rvol = x["rvol"] if not math.isnan(x["rvol"]) else 0.0
        n = max(1, len(Config.UNIVERSE))
        return Signal(
            symbol=symbol, timestamp=datetime.now(timezone.utc),
            price=closes[-1], ema=x["ema"], z_score=x["z"], rsi=x["rsi"],
            macd=x["macd"][0], macd_signal=x["macd"][1],
            bollinger_upper=bb_hi, bollinger_lower=bb_lo,
            atr=x["atr"], realized_vol_annual=rvol,
            direction="LONG", strength=1.0,
            target_weight=min(Config.MAX_PER_ASSET_WEIGHT,
                              Config.MAX_LEVERAGE / n),
        )


class CrossSectionalMomentumStrategy(Strategy):
    """Cross-sectional momentum (relative-strength rotation).

    Each asset's signal strength is its trailing return over a lookback
    window. The normalize step ranks all assets and concentrates capital
    in the top quartile — classic Jegadeesh & Titman / Asness rotation.
    """
    NAME = "xs_momentum"
    LOOKBACK = 60   # bars
    TOP_K_PCT = 0.25
    DESCRIPTION = ("Cross-sectional rotation. Rank assets by trailing return; "
                   "long only the top quartile. Equal-weight winners. "
                   "Crowd-trade-resistant pure-alpha factor.")

    @classmethod
    def compute_signal(cls, symbol, closes, highs, lows) -> Optional[Signal]:
        n = cls.LOOKBACK
        if len(closes) < n + 2:
            return None
        x = cls._common_indicators(closes, highs, lows)
        bb_lo, _, bb_hi = x["bb"]
        ret = closes[-1] / closes[-n] - 1 if closes[-n] > 0 else 0
        # strength is raw trailing return; ranking happens in normalize_weights
        return Signal(
            symbol=symbol, timestamp=datetime.now(timezone.utc),
            price=closes[-1], ema=x["ema"], z_score=x["z"], rsi=x["rsi"],
            macd=x["macd"][0], macd_signal=x["macd"][1],
            bollinger_upper=bb_hi, bollinger_lower=bb_lo,
            atr=x["atr"], realized_vol_annual=x["rvol"],
            direction="LONG" if ret > 0 else "FLAT",
            strength=max(0.0, ret),     # raw return as strength
            target_weight=0.0,           # decided in normalize_weights
        )

    @classmethod
    def normalize_weights(cls, signals: dict) -> dict[str, float]:
        if not signals:
            return {}
        # Rank by trailing return (strength). Winners only.
        ranked = sorted(signals.items(),
                        key=lambda kv: -kv[1].strength)
        n_winners = max(1, int(round(len(ranked) * cls.TOP_K_PCT)))
        winners = ranked[:n_winners]
        # Equal weight among winners, with positive trailing return
        eligible = [(sym, s) for sym, s in winners if s.strength > 0]
        if not eligible:
            return {}
        w = min(Config.MAX_PER_ASSET_WEIGHT,
                Config.MAX_LEVERAGE / len(eligible))
        weights = {sym: w for sym, _ in eligible}
        # Mutate the source signals so the UI shows the target weight too
        for sym, s in signals.items():
            s.target_weight = weights.get(sym, 0.0)
        return weights


class RiskParityStrategy(Strategy):
    """Inverse-volatility weighting.

    Every asset gets a weight proportional to 1/realized_vol. The portfolio
    targets a constant annualized vol regardless of which names dominate.
    Pure beta exposure; no signal direction — always LONG. Bridge Associates'
    "All-Weather" inspired.
    """
    NAME = "risk_parity"
    DESCRIPTION = ("Inverse-vol weighting. Each asset contributes equal risk "
                   "to the portfolio (1/σ). No directional signal — always "
                   "LONG. Lowest-vol names get the most capital.")

    @classmethod
    def compute_signal(cls, symbol, closes, highs, lows) -> Optional[Signal]:
        if len(closes) < Config.REALIZED_VOL_PERIOD + 2:
            return None
        x = cls._common_indicators(closes, highs, lows)
        bb_lo, _, bb_hi = x["bb"]
        rvol = x["rvol"]
        # Inverse-vol weight (unnormalized — final normalization below)
        inv_vol = (1.0 / rvol) if not math.isnan(rvol) and rvol > 1e-6 else 0.0
        return Signal(
            symbol=symbol, timestamp=datetime.now(timezone.utc),
            price=closes[-1], ema=x["ema"], z_score=x["z"], rsi=x["rsi"],
            macd=x["macd"][0], macd_signal=x["macd"][1],
            bollinger_upper=bb_hi, bollinger_lower=bb_lo,
            atr=x["atr"], realized_vol_annual=rvol,
            direction="LONG", strength=1.0,
            target_weight=inv_vol,
        )

    @classmethod
    def normalize_weights(cls, signals: dict) -> dict[str, float]:
        if not signals:
            return {}
        raw = {sym: s.target_weight for sym, s in signals.items()
               if s.target_weight > 0}
        total = sum(raw.values())
        if total <= 0:
            return {}
        # Scale to sum = MAX_LEVERAGE, then cap per asset.
        scale = Config.MAX_LEVERAGE / total
        weights = {}
        for sym, w in raw.items():
            weights[sym] = min(Config.MAX_PER_ASSET_WEIGHT, w * scale)
        # Push updated weights back so UI shows them
        for sym, s in signals.items():
            s.target_weight = weights.get(sym, 0.0)
        return weights


class DonchianBreakoutStrategy(Strategy):
    """Donchian channel breakout (Turtle Trader rule).

    Long when price closes above the highest high of the prior N bars. Exits
    when price drops below the M-bar low. Sized inverse-vol. The Richard
    Dennis / Bill Eckhardt "Turtle" rule that famously made $80M with $25k
    seed traders in the 80s.
    """
    NAME = "donchian"
    ENTRY_LEN = 20
    EXIT_LEN = 10
    DESCRIPTION = ("Turtle Trader breakout. LONG when price breaks the "
                   "ENTRY_LEN-bar high. Exit on EXIT_LEN-bar low. Classic "
                   "trend follower with crisp rules; well-documented edge.")

    @classmethod
    def compute_signal(cls, symbol, closes, highs, lows) -> Optional[Signal]:
        if len(highs) < cls.ENTRY_LEN + 2:
            return None
        x = cls._common_indicators(closes, highs, lows)
        bb_lo, _, bb_hi = x["bb"]
        rvol = x["rvol"]
        entry_high = max(highs[-(cls.ENTRY_LEN + 1):-1])  # exclude current
        exit_low = min(lows[-(cls.EXIT_LEN + 1):-1])
        price = closes[-1]
        # Direction
        if price > entry_high:
            direction = "LONG"
            # Strength scales with how far above the breakout level
            strength = min(1.0, (price / entry_high - 1) / 0.02)
        elif price < exit_low:
            direction = "FLAT"
            strength = 0.0
        else:
            direction = "HOLD"
            strength = 0.5      # keep prior allocation
        inv_vol = (Config.TARGET_PORTFOLIO_VOL_ANNUAL / rvol
                   if not math.isnan(rvol) and rvol > 1e-6 else 0.0)
        target_weight = max(0.0,
                            min(Config.MAX_PER_ASSET_WEIGHT, strength * inv_vol))
        return Signal(
            symbol=symbol, timestamp=datetime.now(timezone.utc),
            price=price, ema=x["ema"], z_score=x["z"], rsi=x["rsi"],
            macd=x["macd"][0], macd_signal=x["macd"][1],
            bollinger_upper=bb_hi, bollinger_lower=bb_lo,
            atr=x["atr"], realized_vol_annual=rvol,
            direction=direction, strength=strength,
            target_weight=target_weight,
        )


class DualMomentumStrategy(Strategy):
    """Antonacci's Dual Momentum.

    Combines absolute momentum (trailing return > 0, the "trend-is-positive"
    filter) with relative momentum (outperforming peers). The portfolio only
    holds the top half of assets that ALSO have positive absolute momentum.
    The remaining capital sits in cash. Published in Antonacci (2014).
    """
    NAME = "dual_momentum"
    LOOKBACK = 90
    DESCRIPTION = ("Antonacci dual momentum. Long only assets with positive "
                   "12-week return AND outperforming the median. Capital "
                   "rotates to risk-off (cash) when nothing qualifies.")

    @classmethod
    def compute_signal(cls, symbol, closes, highs, lows) -> Optional[Signal]:
        n = cls.LOOKBACK
        if len(closes) < n + 2:
            return None
        x = cls._common_indicators(closes, highs, lows)
        bb_lo, _, bb_hi = x["bb"]
        abs_mom = closes[-1] / closes[-n] - 1 if closes[-n] > 0 else 0
        return Signal(
            symbol=symbol, timestamp=datetime.now(timezone.utc),
            price=closes[-1], ema=x["ema"], z_score=x["z"], rsi=x["rsi"],
            macd=x["macd"][0], macd_signal=x["macd"][1],
            bollinger_upper=bb_hi, bollinger_lower=bb_lo,
            atr=x["atr"], realized_vol_annual=x["rvol"],
            # Direction depends on absolute momentum; relative check in normalize.
            direction="LONG" if abs_mom > 0 else "FLAT",
            strength=abs_mom,
            target_weight=0.0,
        )

    @classmethod
    def normalize_weights(cls, signals: dict) -> dict[str, float]:
        if not signals:
            return {}
        # Eligible = positive absolute momentum
        eligible = [(sym, s) for sym, s in signals.items() if s.strength > 0]
        if not eligible:
            return {}
        # Of those, take the top half (relative momentum)
        eligible.sort(key=lambda kv: -kv[1].strength)
        keep = max(1, len(eligible) // 2)
        winners = eligible[:keep]
        # Equal-weight winners, capped per asset
        w = min(Config.MAX_PER_ASSET_WEIGHT,
                Config.MAX_LEVERAGE / len(winners))
        weights = {sym: w for sym, _ in winners}
        for sym, s in signals.items():
            s.target_weight = weights.get(sym, 0.0)
        return weights


STRATEGY_REGISTRY = {s.NAME: s for s in [
    VTMRStrategy, MomentumStrategy, BollingerBreakoutStrategy,
    BuyAndHoldStrategy,
    CrossSectionalMomentumStrategy, RiskParityStrategy,
    DonchianBreakoutStrategy, DualMomentumStrategy,
]}


# ╔══════════════════════════════════════════════════════════════════════════════╗
# ║                  9a. HISTORICAL DATA LOADER (yfinance)                      ║
# ╚══════════════════════════════════════════════════════════════════════════════╝

class HistoricalDataLoader:
    """Pulls historical OHLCV from Yahoo Finance via yfinance.

    Free, no API key. Yahoo's crypto symbols are e.g. BTC-USD, ETH-USD.
    Equity macro refs (SPY/QQQ/GLD/UUP) come from the same endpoint.

    yfinance gives:
      • period='10y' interval='1d'   → 10 years of daily bars
      • period='2y'  interval='1h'   → ~730 days of hourly bars
      • period='5d'  interval='1m'   → 7-day max of 1-min bars

    Cached in-process for the lifetime of the bot.
    """

    _cache: dict = {}     # (symbol, period, interval) → list[Bar]

    @classmethod
    def is_available(cls) -> bool:
        return YFINANCE_AVAILABLE

    @classmethod
    def load(cls, symbol: str, *, period: str = "10y",
             interval: str = "1d") -> list[Bar]:
        if not YFINANCE_AVAILABLE:
            return []
        key = (symbol, period, interval)
        if key in cls._cache:
            return cls._cache[key]
        try:
            log.info("yfinance: fetching %s period=%s interval=%s",
                     symbol, period, interval)
            t = _yf.Ticker(symbol)
            df = t.history(period=period, interval=interval, auto_adjust=False)
            if df is None or df.empty:
                log.warning("yfinance: empty response for %s", symbol)
                cls._cache[key] = []
                return []
            bars: list[Bar] = []
            for ts, row in df.iterrows():
                try:
                    py_ts = ts.to_pydatetime()
                    if py_ts.tzinfo is None:
                        py_ts = py_ts.replace(tzinfo=timezone.utc)
                    bars.append(Bar(
                        symbol=symbol, start=py_ts,
                        open=float(row["Open"]), high=float(row["High"]),
                        low=float(row["Low"]), close=float(row["Close"]),
                        volume=float(row.get("Volume", 0) or 0),
                    ))
                except Exception:
                    continue
            log.info("yfinance: loaded %d bars for %s", len(bars), symbol)
            cls._cache[key] = bars
            return bars
        except Exception as e:
            log.error("yfinance error for %s: %s", symbol, e)
            cls._cache[key] = []
            return []

    @classmethod
    def load_multi(cls, symbols: list[str], *, period: str = "10y",
                   interval: str = "1d") -> dict[str, list[Bar]]:
        return {s: cls.load(s, period=period, interval=interval)
                for s in symbols}

    @classmethod
    def macro_summary(cls, period: str = "1y") -> dict:
        """Summary of macro references with returns + correlations."""
        out: dict = {}
        if not YFINANCE_AVAILABLE:
            return {"available": False,
                    "hint": "pip install yfinance to enable macro data"}
        ref_bars = cls.load_multi(Config.MACRO_REFS,
                                  period=period, interval="1d")
        for ref, bars in ref_bars.items():
            closes = [b.close for b in bars]
            if len(closes) < 5:
                continue
            rets = Indicators.returns(closes)
            ann_vol = Indicators.realized_vol_annual(
                closes, min(60, len(closes) - 1), 252)
            sharpe = Indicators.sharpe(rets, 252) if rets else float("nan")
            out[ref] = {
                "n_bars": len(closes),
                "last_price": round(closes[-1], 2),
                "return_1y_pct": round((closes[-1] / closes[0] - 1) * 100, 2),
                "ann_vol": round(ann_vol, 4) if not math.isnan(ann_vol) else None,
                "sharpe": round(sharpe, 3) if not math.isnan(sharpe) else None,
            }
        return {"available": True, "refs": out}

    @classmethod
    def crypto_macro_correlation(cls, crypto_symbols: list[str],
                                 period: str = "1y") -> dict:
        """Rolling daily-return correlation: each crypto vs SPY/QQQ/GLD."""
        if not YFINANCE_AVAILABLE:
            return {"available": False}
        macro_bars = cls.load_multi(Config.MACRO_REFS, period=period,
                                    interval="1d")
        macro_rets = {m: Indicators.returns([b.close for b in bars])
                      for m, bars in macro_bars.items()
                      if len(bars) >= 30}
        crypto_bars = cls.load_multi(crypto_symbols, period=period,
                                     interval="1d")
        out = []
        for sym, bars in crypto_bars.items():
            if len(bars) < 30:
                continue
            rets = Indicators.returns([b.close for b in bars])
            row = {"symbol": sym, "corr": {}}
            for m, mr in macro_rets.items():
                row["corr"][m] = round(Indicators.correlation(rets, mr), 3)
            out.append(row)
        return {"available": True, "data": out}


# ╔══════════════════════════════════════════════════════════════════════════════╗
# ║                          9b. BACKTEST ENGINE                                ║
# ╚══════════════════════════════════════════════════════════════════════════════╝

class BacktestEngine:
    """Replays a strategy over a snapshot of historical bars.

    For each timestep t, uses the strategy to compute target weights on
    bars[0..t], rebalances paper holdings to those weights using bar[t]
    close, and books PnL. Returns equity curve + per-trade list + summary.
    """

    @staticmethod
    def run(strategy_cls, bars_by_symbol: dict[str, list[Bar]],
            *, initial_capital: Optional[float] = None,
            slippage_bps: float = 20,
            warmup_bars: int = 30) -> dict:
        cap0 = initial_capital if initial_capital is not None \
            else Config.TOTAL_CAPITAL
        symbols = [s for s in bars_by_symbol if bars_by_symbol[s]]
        if not symbols:
            return {"ok": False, "error": "no bars"}

        # Align all assets to the shortest length
        n = min(len(bars_by_symbol[s]) for s in symbols)
        if n < warmup_bars + 5:
            return {"ok": False, "error": f"need at least {warmup_bars + 5} bars"}

        cash = cap0
        holdings: dict[str, float] = {}
        equity_curve: list = []      # [(timestamp_iso, equity)]
        trades: list = []
        per_asset_pnl: dict = {s: 0.0 for s in symbols}
        peak = cap0
        max_dd = 0.0

        for t in range(warmup_bars, n):
            # Slice bars up to and including t
            closes_by = {s: [b.close for b in bars_by_symbol[s][:t + 1]]
                         for s in symbols}
            highs_by = {s: [b.high for b in bars_by_symbol[s][:t + 1]]
                        for s in symbols}
            lows_by = {s: [b.low for b in bars_by_symbol[s][:t + 1]]
                       for s in symbols}

            # Compute signals
            signals = {}
            for s in symbols:
                sig = strategy_cls.compute_signal(
                    s, closes_by[s], highs_by[s], lows_by[s])
                if sig:
                    signals[s] = sig
            target_w = strategy_cls.normalize_weights(signals)

            # Mark to market & total equity
            equity = cash
            for s, qty in holdings.items():
                equity += qty * bars_by_symbol[s][t].close

            # Rebalance to target weights (with slippage)
            for s in symbols:
                target_usd = target_w.get(s, 0.0) * equity
                cur_qty = holdings.get(s, 0.0)
                mark = bars_by_symbol[s][t].close
                cur_usd = cur_qty * mark
                if equity > 0:
                    delta_pct = abs(target_usd - cur_usd) / equity
                    if delta_pct < Config.REBALANCE_THRESHOLD:
                        continue
                delta_usd = target_usd - cur_usd
                if abs(delta_usd) < Config.MIN_ORDER_USD:
                    continue

                fill_px = mark * (1 + slippage_bps / 10_000) \
                    if delta_usd > 0 else mark * (1 - slippage_bps / 10_000)
                qty_delta = delta_usd / fill_px
                cash -= qty_delta * fill_px
                new_qty = cur_qty + qty_delta
                if abs(new_qty) < 1e-12:
                    holdings.pop(s, None)
                else:
                    holdings[s] = new_qty
                pnl_event = -qty_delta * (fill_px - mark)  # slippage cost
                per_asset_pnl[s] += pnl_event
                trades.append({
                    "ts": bars_by_symbol[s][t].start.isoformat(),
                    "symbol": s, "side": "BUY" if qty_delta > 0 else "SELL",
                    "qty": qty_delta, "price": fill_px,
                    "usd": qty_delta * fill_px,
                })

            # Recompute equity after rebalance
            equity = cash
            for s, qty in holdings.items():
                equity += qty * bars_by_symbol[s][t].close
            equity_curve.append([bars_by_symbol[symbols[0]][t].start.isoformat(),
                                round(equity, 4)])
            if equity > peak:
                peak = equity
            dd = (equity / peak) - 1 if peak > 0 else 0
            if dd < max_dd:
                max_dd = dd

        # Metrics
        eq_vals = [e[1] for e in equity_curve]
        rets = Indicators.returns(eq_vals)
        final_eq = eq_vals[-1] if eq_vals else cap0
        sharpe = Indicators.sharpe(rets, Config.PERIODS_PER_YEAR) \
            if rets else float("nan")
        sortino = Indicators.sortino(rets, Config.PERIODS_PER_YEAR) \
            if rets else float("nan")
        ann_vol = (math.sqrt(sum((r - sum(rets)/len(rets))**2 for r in rets)
                              / max(1, len(rets) - 1))
                   * math.sqrt(Config.PERIODS_PER_YEAR)) if rets else float("nan")

        def _safe(v): return None if (isinstance(v, float)
                                      and (math.isnan(v) or math.isinf(v))) else v

        return {
            "ok": True,
            "strategy": strategy_cls.NAME,
            "initial_capital": cap0,
            "final_equity": round(final_eq, 4),
            "return_pct": round((final_eq / cap0 - 1) * 100, 3) if cap0 else 0,
            "sharpe": _safe(round(sharpe, 3) if sharpe == sharpe else None),
            "sortino": _safe(round(sortino, 3) if sortino == sortino else None),
            "ann_vol": _safe(round(ann_vol, 4) if ann_vol == ann_vol else None),
            "max_drawdown_pct": round(max_dd * 100, 3),
            "n_trades": len(trades),
            "trades": trades[-200:],     # last 200 for the UI
            "equity_curve": equity_curve,
            "per_asset_pnl": {s: round(p, 4) for s, p in per_asset_pnl.items()},
        }

    @staticmethod
    def parameter_sweep(strategy_cls, bars_by_symbol: dict[str, list[Bar]],
                        *, param_x: str, values_x: list,
                        param_y: str, values_y: list,
                        metric: str = "sharpe") -> dict:
        """2-D grid sweep; returns matrix of (param_x, param_y) → metric.
        Restores the saved Config values on exit."""
        if not hasattr(Config, param_x):
            return {"ok": False, "error": f"unknown param {param_x}"}
        if not hasattr(Config, param_y):
            return {"ok": False, "error": f"unknown param {param_y}"}
        saved_x = getattr(Config, param_x)
        saved_y = getattr(Config, param_y)
        cells = []
        best = {"value": -float("inf"), "x": None, "y": None}
        try:
            for vx in values_x:
                row = []
                for vy in values_y:
                    setattr(Config, param_x, vx)
                    setattr(Config, param_y, vy)
                    bt = BacktestEngine.run(strategy_cls, bars_by_symbol)
                    if not bt.get("ok"):
                        row.append({"x": vx, "y": vy, "metric": None})
                        continue
                    m = bt.get(metric)
                    if m is None or (isinstance(m, float) and math.isnan(m)):
                        m = None
                    row.append({"x": vx, "y": vy, "metric": m,
                                "return_pct": bt.get("return_pct"),
                                "max_dd": bt.get("max_drawdown_pct"),
                                "n_trades": bt.get("n_trades")})
                    if m is not None and m > best["value"]:
                        best = {"value": m, "x": vx, "y": vy}
                cells.append(row)
        finally:
            setattr(Config, param_x, saved_x)
            setattr(Config, param_y, saved_y)
        return {
            "ok": True, "strategy": strategy_cls.NAME,
            "param_x": param_x, "param_y": param_y,
            "values_x": values_x, "values_y": values_y,
            "metric": metric,
            "best": best,
            "cells": cells,
        }


# ╔══════════════════════════════════════════════════════════════════════════════╗
# ║                          10. RISK MANAGER                                   ║
# ╚══════════════════════════════════════════════════════════════════════════════╝

class RiskManager:
    def __init__(self, journal: Journal,
                 store: Optional[PersistentStore] = None):
        self.journal = journal
        self.store = store
        self.equity_at_utc_midnight: Optional[float] = None
        self._anchor_date: Optional[str] = None
        self._halted_hard = False
        self._equity_high_water = 0.0
        self._daily_trade_counts: dict[str, int] = {}
        if store is not None:
            self._restore()

    def _restore(self):
        a = self.store.get("anchor") or {}
        self.equity_at_utc_midnight = a.get("equity")
        self._anchor_date = a.get("date")
        self._halted_hard = bool(a.get("halted_hard", False))
        self._equity_high_water = float(a.get("hwm", 0.0))
        self._daily_trade_counts = dict(
            self.store.get("trade_counts_today") or {})

    def _persist(self):
        if self.store is None:
            return
        self.store.put_many({
            "anchor": {
                "equity": self.equity_at_utc_midnight,
                "date": self._anchor_date,
                "halted_hard": self._halted_hard,
                "hwm": self._equity_high_water,
            },
            "trade_counts_today": dict(self._daily_trade_counts),
        })

    def update_daily_anchor_if_needed(self, current_equity: float):
        today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        if self._anchor_date != today or self.equity_at_utc_midnight is None:
            self.equity_at_utc_midnight = current_equity
            self._anchor_date = today
            self._daily_trade_counts = {}
            self._persist()
            log.info("Daily anchor reset: $%.2f date=%s",
                     current_equity, today)

    def update_high_water(self, current_equity: float):
        if current_equity > self._equity_high_water:
            self._equity_high_water = current_equity
            self._persist()

    def kill_switch_status(self, current_equity: float) -> str:
        if self._halted_hard:
            return "HALT_ALL"
        # Daily anchor losses
        if self.equity_at_utc_midnight and self.equity_at_utc_midnight > 0:
            loss = (self.equity_at_utc_midnight - current_equity) \
                / self.equity_at_utc_midnight
            if loss >= Config.HARD_HALT_LOSS_PCT:
                self._halted_hard = True
                self._persist()
                log.error("HARD HALT: daily loss %.1f%%", loss * 100)
                return "HALT_ALL"
            if loss >= Config.DAILY_LOSS_LIMIT_PCT:
                return "HALT_NEW"
        # Max drawdown
        if self._equity_high_water > 0:
            dd = (current_equity / self._equity_high_water) - 1.0
            if dd <= -Config.MAX_DRAWDOWN_PCT:
                self._halted_hard = True
                self._persist()
                log.error("DRAWDOWN HALT: %.1f%% below HWM", abs(dd) * 100)
                return "HALT_ALL"
        return "OK"

    def can_trade(self, symbol: str) -> bool:
        if self._daily_trade_counts.get(symbol, 0) \
                >= Config.MAX_ORDERS_PER_DAY_PER_ASSET:
            return False
        return True

    def record_trade(self, symbol: str):
        self._daily_trade_counts[symbol] = \
            self._daily_trade_counts.get(symbol, 0) + 1
        self._persist()


# ╔══════════════════════════════════════════════════════════════════════════════╗
# ║                          11. EXECUTION ENGINE                               ║
# ╚══════════════════════════════════════════════════════════════════════════════╝

class ExecutionEngine:
    """Rebalances current holdings → target weights, places orders, polls fills."""

    def __init__(self, client: BrokerClient, journal: Journal,
                 risk: RiskManager):
        self.client = client
        self.journal = journal
        self.risk = risk

    def rebalance(self, *, target_weights: dict[str, float],
                  current_holdings: dict[str, float],
                  quotes: dict[str, Quote],
                  portfolio_value: float) -> list[dict]:
        """Returns list of {symbol, side, qty, price, filled, error}."""
        actions = []
        for symbol in set(list(target_weights.keys()) + list(current_holdings.keys())):
            quote = quotes.get(symbol)
            if quote is None:
                continue
            mid = quote.mid
            if mid <= 0:
                continue

            target_w = target_weights.get(symbol, 0.0)
            target_usd = target_w * portfolio_value
            current_qty = current_holdings.get(symbol, 0.0)
            current_usd = current_qty * mid

            # Skip churn
            if portfolio_value > 0:
                delta_pct = abs(target_usd - current_usd) / portfolio_value
                if delta_pct < Config.REBALANCE_THRESHOLD:
                    continue

            delta_usd = target_usd - current_usd
            if abs(delta_usd) < Config.MIN_ORDER_USD:
                continue

            if not self.risk.can_trade(symbol):
                log.info("Trade-frequency cap hit for %s", symbol)
                continue

            if delta_usd > 0:
                side = "BUY"
                limit = quote.ask * (1 + Config.SLIPPAGE_TOLERANCE)
                qty = delta_usd / limit
            else:
                side = "SELL"
                limit = quote.bid * (1 - Config.SLIPPAGE_TOLERANCE)
                qty = abs(delta_usd) / limit

            # Round qty to a reasonable precision per asset
            qty = round(qty, 8)
            if qty <= 0:
                continue

            self.journal.record(event="intent", symbol=symbol, side=side,
                                qty=qty, price=limit,
                                usd_amount=abs(delta_usd),
                                dry_run=Config.DRY_RUN,
                                notes=f"target_w={target_w:.3f} "
                                      f"cur_w={current_usd/max(portfolio_value,1):.3f}")

            resp = self.client.place_order(
                symbol=symbol, side=side, qty=qty, limit_price=limit)
            if resp is None:
                self.journal.record(event="order_failed", symbol=symbol,
                                    side=side, qty=qty, price=limit)
                actions.append({"symbol": symbol, "side": side, "qty": qty,
                                "price": limit, "filled": False,
                                "error": "place_order returned None"})
                continue

            # In dry-run or mock, the order's filled synchronously.
            # In live, poll for fill.
            fill_qty = qty
            fill_price = limit
            if not Config.DRY_RUN and resp.get("id") \
                    and not resp.get("dry_run"):
                status = self._poll_until_settled(resp["id"])
                if status:
                    fill_qty = status.get("filled_size", 0)
                    fill_price = status.get("avg_price") or limit
                else:
                    self.journal.record(event="order_timeout",
                                        symbol=symbol, side=side,
                                        qty=qty, price=limit,
                                        notes=f"order_id={resp.get('id')}")
                    actions.append({"symbol": symbol, "side": side,
                                    "qty": qty, "price": limit,
                                    "filled": False, "error": "timeout"})
                    continue

            self.journal.record(event="filled" if not Config.DRY_RUN
                                       else "dry_filled",
                                symbol=symbol, side=side,
                                qty=fill_qty, price=fill_price,
                                usd_amount=fill_qty * fill_price,
                                dry_run=Config.DRY_RUN)
            self.risk.record_trade(symbol)
            actions.append({"symbol": symbol, "side": side, "qty": fill_qty,
                            "price": fill_price, "filled": True})

        return actions

    def _poll_until_settled(self, order_id: str) -> Optional[dict]:
        deadline = time.monotonic() + Config.ORDER_FILL_TIMEOUT_SECONDS
        last = None
        while time.monotonic() < deadline:
            s = self.client.get_order_status(order_id)
            if s:
                last = s
                st = s.get("status", "")
                if st in ("filled", "matched", "complete"):
                    return s
                if st in ("cancelled", "rejected", "expired"):
                    return s if s.get("filled_size", 0) > 0 else None
            time.sleep(2)
        if last and last.get("filled_size", 0) > 0:
            return last
        return None


# ╔══════════════════════════════════════════════════════════════════════════════╗
# ║                          12. AUTO RESEARCH PIPELINE                         ║
# ╚══════════════════════════════════════════════════════════════════════════════╝

class AutoResearch:
    """Periodically derives insights from accumulated bar data."""

    @staticmethod
    def correlation_matrix(closes_by_symbol: dict[str, list[float]]) -> dict:
        symbols = sorted(closes_by_symbol.keys())
        rets_by = {s: Indicators.returns(closes_by_symbol[s])
                   for s in symbols}
        matrix = []
        for s1 in symbols:
            row = {"symbol": s1, "values": []}
            for s2 in symbols:
                if s1 == s2:
                    c = 1.0
                else:
                    c = Indicators.correlation(rets_by[s1], rets_by[s2])
                row["values"].append({"vs": s2, "corr": c})
            matrix.append(row)
        return {"symbols": symbols, "matrix": matrix}

    @staticmethod
    def per_asset_stats(closes_by_symbol: dict[str, list[float]]) -> list[dict]:
        out = []
        for sym, closes in sorted(closes_by_symbol.items()):
            if len(closes) < 10:
                continue
            rets = Indicators.returns(closes)
            ann_vol = Indicators.realized_vol_annual(
                closes, min(len(closes) - 1, Config.REALIZED_VOL_PERIOD),
                Config.PERIODS_PER_YEAR)
            sharpe = Indicators.sharpe(rets, Config.PERIODS_PER_YEAR) \
                if rets else float("nan")
            sortino = Indicators.sortino(rets, Config.PERIODS_PER_YEAR) \
                if rets else float("nan")
            eq_curve = [closes[0] * (1 + sum(rets[:i]))
                        for i in range(len(rets) + 1)] if rets else [closes[0]]
            dd, _, _ = Indicators.max_drawdown(eq_curve)
            out.append({
                "symbol": sym,
                "bars": len(closes),
                "last_price": round(closes[-1], 6),
                "return_pct": round((closes[-1] / closes[0] - 1) * 100, 3)
                              if closes[0] else 0,
                "ann_vol": round(ann_vol, 4) if not math.isnan(ann_vol) else None,
                "sharpe": round(sharpe, 3) if not math.isnan(sharpe) else None,
                "sortino": round(sortino, 3) if not math.isnan(sortino) else None,
                "max_drawdown_pct": round(dd * 100, 3),
            })
        return out

    @staticmethod
    def regime(closes: list[float]) -> dict:
        """Quick regime label: trend / chop / spike."""
        if len(closes) < 30:
            return {"label": "insufficient_data", "vol_pct": None}
        rets = Indicators.returns(closes[-100:])
        vol_recent = (sum(r ** 2 for r in rets[-20:]) / 20) ** 0.5
        vol_baseline = (sum(r ** 2 for r in rets) / len(rets)) ** 0.5
        ratio = vol_recent / vol_baseline if vol_baseline else 1.0
        ema_short = Indicators.ema(closes[-50:], 10)
        ema_long = Indicators.ema(closes[-100:], 50) if len(closes) >= 50 else closes[-1]
        if ratio > 1.6:
            label = "volatility_spike"
        elif abs(ema_short - ema_long) / ema_long > 0.05:
            label = "trending"
        else:
            label = "ranging"
        return {"label": label, "vol_recent_vs_baseline": round(ratio, 3),
                "ema_spread_pct": round((ema_short / ema_long - 1) * 100, 2)
                                   if ema_long else 0}


# ╔══════════════════════════════════════════════════════════════════════════════╗
# ║                          13. DASHBOARD STATE                                ║
# ╚══════════════════════════════════════════════════════════════════════════════╝

class CumulativeStats:
    EQUITY_HISTORY = 2000

    def __init__(self, store: Optional[PersistentStore] = None):
        self._lock = _threading.RLock()
        self.store = store
        self.started_at = datetime.now(timezone.utc).isoformat()
        self.cycles = 0
        self.intents = 0
        self.fills = 0
        self.dry_fills = 0
        self.order_failures = 0
        self.order_timeouts = 0
        self.equity_history: deque = deque(maxlen=self.EQUITY_HISTORY)
        self.returns: deque = deque(maxlen=self.EQUITY_HISTORY)
        if store is not None:
            self._restore()

    def _restore(self):
        d = self.store.get("stats") or {}
        if not d:
            return
        self.started_at = d.get("started_at", self.started_at)
        self.cycles = int(d.get("cycles", 0))
        self.intents = int(d.get("intents", 0))
        self.fills = int(d.get("fills", 0))
        self.dry_fills = int(d.get("dry_fills", 0))
        self.order_failures = int(d.get("order_failures", 0))
        self.order_timeouts = int(d.get("order_timeouts", 0))
        for s in (d.get("equity_history") or [])[-self.EQUITY_HISTORY:]:
            self.equity_history.append(s)
        for r in (d.get("returns") or [])[-self.EQUITY_HISTORY:]:
            self.returns.append(r)

    def _persist(self):
        if self.store is None:
            return
        self.store.put("stats", {
            "started_at": self.started_at,
            "cycles": self.cycles, "intents": self.intents,
            "fills": self.fills, "dry_fills": self.dry_fills,
            "order_failures": self.order_failures,
            "order_timeouts": self.order_timeouts,
            "equity_history": list(self.equity_history),
            "returns": list(self.returns),
        })

    def record_cycle(self, equity: float):
        with self._lock:
            self.cycles += 1
            ts = datetime.now(timezone.utc).isoformat()
            if self.equity_history:
                last_eq = self.equity_history[-1][1]
                if last_eq > 0:
                    self.returns.append((equity / last_eq) - 1.0)
            self.equity_history.append([ts, round(equity, 4)])
        self._persist()

    def record_event(self, event: str):
        with self._lock:
            if event == "intent":
                self.intents += 1
            elif event == "filled":
                self.fills += 1
            elif event == "dry_filled":
                self.dry_fills += 1
            elif event == "order_failed":
                self.order_failures += 1
            elif event == "order_timeout":
                self.order_timeouts += 1

    def snapshot(self) -> dict:
        with self._lock:
            return {
                "started_at": self.started_at,
                "cycles": self.cycles, "intents": self.intents,
                "fills": self.fills, "dry_fills": self.dry_fills,
                "order_failures": self.order_failures,
                "order_timeouts": self.order_timeouts,
                "equity_history": list(self.equity_history),
                "returns": list(self.returns),
            }


class DashboardState:
    MAX_EVENTS = 300

    def __init__(self, store: Optional[PersistentStore] = None):
        self._lock = _threading.RLock()
        self.stats = CumulativeStats(store=store)
        self._state: dict = {
            "last_cycle_at": None,
            "next_cycle_at": None,
            "kill_switch": "OK",
            "equity": 0.0,
            "cash": 0.0,
            "deployed": 0.0,
            "dry_run": True,
            "broker": "mock",
            "mock": False,
            "scheduler_paused": False,
            "signals": {},
            "bars_by_symbol": {},     # symbol → [Bar dicts]
            "holdings": {},
            "target_weights": {},
            "current_weights": {},
            "research": {},
            "config": {},
            "messages": [],
        }
        self._events: deque = deque(maxlen=self.MAX_EVENTS)
        self._messages: deque = deque(maxlen=20)

    def update(self, **kw):
        with self._lock:
            self._state.update(kw)

    def push_event(self, event: dict):
        with self._lock:
            self._events.append(event)
        self.stats.record_event(event.get("event", ""))

    def push_message(self, text: str, level: str = "info"):
        with self._lock:
            self._messages.append({
                "ts": datetime.now(timezone.utc).isoformat(),
                "level": level, "text": text,
            })

    def snapshot(self) -> dict:
        with self._lock:
            snap = _copy.deepcopy(self._state)
            snap["recent_events"] = list(self._events)
            snap["messages"] = list(self._messages)
        snap["stats"] = self.stats.snapshot()
        return snap

    def events_tail(self, limit: int = 100) -> list:
        with self._lock:
            return list(self._events)[-limit:]


# ╔══════════════════════════════════════════════════════════════════════════════╗
# ║                          14. DASHBOARD SERVER                               ║
# ╚══════════════════════════════════════════════════════════════════════════════╝

EDITABLE_KNOBS = {
    "TOTAL_CAPITAL":               ("float", 1.0, 1_000_000.0),
    "MOMENTUM_MIN_GAP_PCT":        ("float", 0.0, 0.05),
    "MOMENTUM_MAX_RSI":            ("int", 50, 100),
    "TARGET_PORTFOLIO_VOL_ANNUAL": ("float", 0.0, 1.0),
    "MAX_LEVERAGE":                ("float", 0.0, 5.0),
    "MAX_PER_ASSET_WEIGHT":        ("float", 0.0, 1.0),
    "DAILY_LOSS_LIMIT_PCT":        ("float", 0.0, 1.0),
    "HARD_HALT_LOSS_PCT":          ("float", 0.0, 1.0),
    "MAX_DRAWDOWN_PCT":            ("float", 0.0, 1.0),
    "ENTRY_Z":                     ("float", -5.0, 0.0),
    "EXIT_Z":                      ("float", -2.0, 2.0),
    "EMA_PERIOD":                  ("int", 5, 500),
    "Z_WINDOW":                    ("int", 5, 500),
    "RSI_PERIOD":                  ("int", 2, 100),
    "BOLLINGER_PERIOD":            ("int", 5, 200),
    "BOLLINGER_K":                 ("float", 0.5, 5.0),
    "MACD_FAST":                   ("int", 2, 50),
    "MACD_SLOW":                   ("int", 10, 200),
    "MACD_SIGNAL":                 ("int", 2, 50),
    "ATR_PERIOD":                  ("int", 2, 100),
    "REALIZED_VOL_PERIOD":         ("int", 5, 200),
    "SIGNAL_BLEND":                ("float", 0.0, 1.0),
    "REBALANCE_THRESHOLD":         ("float", 0.0, 0.5),
    "MIN_ORDER_USD":               ("float", 0.1, 1000.0),
    "MAX_ORDERS_PER_DAY_PER_ASSET":("int", 1, 200),
    "POLL_INTERVAL_SECONDS":       ("int", 5, 600),
    "BAR_INTERVAL_SECONDS":        ("int", 60, 3600),
    "SLIPPAGE_TOLERANCE":          ("float", 0.0, 0.05),
}


class _DashboardHandler(BaseHTTPRequestHandler):
    dashboard: Optional[DashboardState] = None
    orchestrator: Optional["Orchestrator"] = None

    def log_message(self, format, *args):
        log.debug("HTTP %s - " + format, self.address_string(), *args)

    def _send_json(self, payload, status: int = 200):
        body = json.dumps(payload, default=str).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Cache-Control", "no-store")
        self.end_headers()
        self.wfile.write(body)

    def _send_html(self, html: str):
        body = html.encode("utf-8")
        self.send_response(200)
        self.send_header("Content-Type", "text/html; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Cache-Control", "no-store")
        self.end_headers()
        self.wfile.write(body)

    def _read_body(self) -> dict:
        try:
            n = int(self.headers.get("Content-Length", "0"))
            if n <= 0 or n > 1_000_000:
                return {}
            return json.loads(self.rfile.read(n).decode("utf-8") or "{}")
        except Exception:
            return {}

    def do_GET(self):
        if self.dashboard is None:
            self._send_json({"error": "no dashboard"}, 503)
            return
        parsed = urlparse(self.path)
        path = parsed.path
        qs = parse_qs(parsed.query)
        if path in ("/", "/index.html"):
            self._send_html(DASHBOARD_HTML)
            return
        if path == "/api/editable_config":
            cfg = {k: getattr(Config, k, None) for k in EDITABLE_KNOBS}
            meta = {k: {"type": t, "min": lo, "max": hi}
                    for k, (t, lo, hi) in EDITABLE_KNOBS.items()}
            self._send_json({"values": cfg, "meta": meta})
            return
        if path == "/api/journal":
            try:
                limit = int(qs.get("limit", ["100"])[0])
            except Exception:
                limit = 100
            self._send_json(self.dashboard.events_tail(limit))
            return
        snap = self.dashboard.snapshot()
        if path == "/api/state":
            self._send_json(snap)
        elif path == "/api/signals":
            self._send_json(snap.get("signals", {}))
        elif path == "/api/bars":
            sym = qs.get("symbol", [""])[0]
            if sym:
                bars = snap.get("bars_by_symbol", {}).get(sym, [])
                self._send_json(bars)
            else:
                self._send_json(snap.get("bars_by_symbol", {}))
        elif path == "/api/research":
            self._send_json(snap.get("research", {}))
        elif path == "/api/stats":
            self._send_json(snap.get("stats", {}))
        elif path == "/api/strategies":
            self._send_json(snap.get("strategies_view", {}))
        elif path == "/api/trade_markers":
            self._send_json(snap.get("trade_markers", {}))
        elif path == "/api/per_asset_pnl":
            self._send_json(snap.get("per_asset_pnl", {}))
        elif path == "/api/macro":
            if not HistoricalDataLoader.is_available():
                self._send_json({"available": False,
                                 "hint": "pip install yfinance"})
                return
            try:
                period = qs.get("period", ["1y"])[0]
                summary = HistoricalDataLoader.macro_summary(period=period)
                corr = HistoricalDataLoader.crypto_macro_correlation(
                    Config.UNIVERSE[:8], period=period)
                self._send_json({"summary": summary, "correlation": corr})
            except Exception as e:
                self._send_json({"available": False, "error": str(e)}, 500)
        elif path == "/api/historical_bars":
            sym = qs.get("symbol", [""])[0]
            period = qs.get("period", ["1y"])[0]
            interval = qs.get("interval", ["1d"])[0]
            if not sym:
                self._send_json({"ok": False, "error": "symbol required"}, 400)
                return
            if not HistoricalDataLoader.is_available():
                self._send_json({"ok": False,
                                 "error": "yfinance not installed"}, 400)
                return
            try:
                bars = HistoricalDataLoader.load(sym, period=period,
                                                  interval=interval)
                self._send_json({
                    "ok": True, "symbol": sym, "period": period,
                    "interval": interval, "n_bars": len(bars),
                    "bars": [{
                        "start": b.start.isoformat(),
                        "open": b.open, "high": b.high,
                        "low": b.low, "close": b.close,
                        "volume": b.volume,
                    } for b in bars],
                })
            except Exception as e:
                self._send_json({"ok": False, "error": str(e)}, 500)
        else:
            self._send_json({"error": "not_found"}, 404)

    def do_POST(self):
        if self.orchestrator is None:
            self._send_json({"error": "no orchestrator"}, 503)
            return
        path = urlparse(self.path).path
        body = self._read_body()
        if path == "/api/cycle":
            self.orchestrator.request_cycle()
            self._send_json({"ok": True, "message": "cycle requested"})
        elif path == "/api/pause":
            self.orchestrator.pause()
            self._send_json({"ok": True, "paused": True})
        elif path == "/api/resume":
            self.orchestrator.resume()
            self._send_json({"ok": True, "paused": False})
        elif path == "/api/flatten":
            n = self.orchestrator.flatten_all()
            self._send_json({"ok": True, "closed": n})
        elif path == "/api/reset_anchor":
            self.orchestrator.reset_kill_switch()
            self._send_json({"ok": True})
        elif path == "/api/active_strategy":
            name = (body or {}).get("name", "")
            if name not in STRATEGY_REGISTRY:
                self._send_json({"ok": False,
                                 "error": f"unknown strategy {name}"}, 400)
                return
            Config.ACTIVE_STRATEGY = name
            self.dashboard.push_message(f"Active strategy → {name}", "info")
            self._send_json({"ok": True, "active": name})
        elif path == "/api/backtest":
            name = (body or {}).get("strategy", Config.ACTIVE_STRATEGY)
            scls = STRATEGY_REGISTRY.get(name)
            if not scls:
                self._send_json({"ok": False,
                                 "error": f"unknown strategy {name}"}, 400)
                return
            try:
                bars_by = {s: self.orchestrator.bars.bars(s)
                           for s in Config.UNIVERSE}
                bars_by = {s: bs for s, bs in bars_by.items() if bs}
                res = BacktestEngine.run(scls, bars_by)
                self.dashboard.push_message(
                    f"Backtest {name}: ret={res.get('return_pct')}% "
                    f"sharpe={res.get('sharpe')}", "info")
                self._send_json(res)
            except Exception as e:
                log.error("Backtest error: %s", e)
                self._send_json({"ok": False, "error": str(e)}, 500)
        elif path == "/api/historical_backtest":
            name = (body or {}).get("strategy", Config.ACTIVE_STRATEGY)
            scls = STRATEGY_REGISTRY.get(name)
            if not scls:
                self._send_json({"ok": False,
                                 "error": f"unknown strategy {name}"}, 400)
                return
            if not HistoricalDataLoader.is_available():
                self._send_json({"ok": False,
                                 "error": "yfinance not installed. "
                                          "pip install yfinance"}, 400)
                return
            symbols = (body or {}).get("symbols") or Config.UNIVERSE[:8]
            period = (body or {}).get("period", "10y")
            interval = (body or {}).get("interval", "1d")
            try:
                bars_by = HistoricalDataLoader.load_multi(
                    symbols, period=period, interval=interval)
                bars_by = {s: bs for s, bs in bars_by.items() if bs}
                if not bars_by:
                    self._send_json({"ok": False,
                                     "error": "no historical data loaded "
                                              "(network blocked or symbols invalid)"},
                                    400)
                    return
                # Periods-per-year is interval-dependent
                ppy_map = {"1d": 252, "1h": 252 * 7, "1m": 252 * 7 * 60}
                saved_ppy = Config.PERIODS_PER_YEAR
                Config.PERIODS_PER_YEAR = ppy_map.get(interval, 252)
                try:
                    res = BacktestEngine.run(scls, bars_by)
                finally:
                    Config.PERIODS_PER_YEAR = saved_ppy
                res["data_source"] = "yfinance"
                res["period"] = period
                res["interval"] = interval
                res["symbols"] = list(bars_by.keys())
                res["bars_per_symbol"] = {s: len(bs)
                                           for s, bs in bars_by.items()}
                self.dashboard.push_message(
                    f"Historical backtest {name} on {period}/{interval}: "
                    f"ret={res.get('return_pct')}% "
                    f"sharpe={res.get('sharpe')}", "info")
                self._send_json(res)
            except Exception as e:
                log.error("Historical backtest error: %s", e)
                self._send_json({"ok": False, "error": str(e)}, 500)
        elif path == "/api/sweep":
            try:
                name = (body or {}).get("strategy", Config.ACTIVE_STRATEGY)
                scls = STRATEGY_REGISTRY.get(name)
                if not scls:
                    self._send_json({"ok": False,
                                     "error": f"unknown strategy"}, 400)
                    return
                px = body.get("param_x")
                py = body.get("param_y")
                vx = body.get("values_x") or []
                vy = body.get("values_y") or []
                metric = body.get("metric", "sharpe")
                bars_by = {s: self.orchestrator.bars.bars(s)
                           for s in Config.UNIVERSE}
                bars_by = {s: bs for s, bs in bars_by.items() if bs}
                res = BacktestEngine.parameter_sweep(
                    scls, bars_by,
                    param_x=px, values_x=vx,
                    param_y=py, values_y=vy, metric=metric)
                self.dashboard.push_message(
                    f"Sweep {name}: {px}×{py} → best {metric}="
                    f"{res.get('best', {}).get('value')}", "info")
                self._send_json(res)
            except Exception as e:
                log.error("Sweep error: %s", e)
                self._send_json({"ok": False, "error": str(e)}, 500)
        elif path == "/api/config":
            updates, errors = {}, []
            for k, v in (body or {}).items():
                if k not in EDITABLE_KNOBS:
                    errors.append(f"{k}: not editable")
                    continue
                typ, lo, hi = EDITABLE_KNOBS[k]
                try:
                    coerced = int(v) if typ == "int" else float(v)
                except Exception:
                    errors.append(f"{k}: not a number")
                    continue
                if coerced < lo or coerced > hi:
                    errors.append(f"{k}: out of range [{lo}, {hi}]")
                    continue
                setattr(Config, k, coerced)
                updates[k] = coerced
            self.dashboard.push_message(
                f"Knobs: {list(updates.keys()) or 'none'}"
                + (f" · errors {errors}" if errors else ""),
                "warn" if errors else "info")
            self._send_json({"ok": not errors, "updated": updates,
                             "errors": errors})
        else:
            self._send_json({"error": "not_found"}, 404)


class DashboardServer:
    def __init__(self, dashboard: DashboardState,
                 orchestrator: "Orchestrator", port: int = 8770):
        self.dashboard = dashboard
        self.orchestrator = orchestrator
        self.port = port

    def start(self):
        _DashboardHandler.dashboard = self.dashboard
        _DashboardHandler.orchestrator = self.orchestrator
        httpd = ThreadingHTTPServer(("127.0.0.1", self.port),
                                    _DashboardHandler)
        t = _threading.Thread(target=httpd.serve_forever,
                              name="DashboardServer", daemon=True)
        t.start()
        log.info("Dashboard: http://127.0.0.1:%d/", self.port)


# ╔══════════════════════════════════════════════════════════════════════════════╗
# ║                          15. DASHBOARD HTML                                 ║
# ╚══════════════════════════════════════════════════════════════════════════════╝

DASHBOARD_HTML = r"""<!DOCTYPE html>
<html lang="en"><head><meta charset="utf-8">
<title>RH Crypto — VTMR</title>
<style>
:root {
  --bg:#0a0e16; --panel:#131a26; --panel-2:#1a2333; --line:#243349;
  --txt:#e2ecfb; --muted:#7c8aa3; --dim:#5b6a85;
  --green:#4ade80; --red:#f87171; --yellow:#facc15;
  --blue:#60a5fa; --purple:#c084fc; --orange:#fb923c; --cyan:#22d3ee;
  --pink:#f472b6;
}
*{box-sizing:border-box}
body{margin:0;font-family:-apple-system,system-ui,sans-serif;background:var(--bg);
  color:var(--txt);font-size:13px;line-height:1.4}
header{display:flex;align-items:center;gap:14px;flex-wrap:wrap;padding:10px 18px;
  background:var(--panel);border-bottom:1px solid var(--line);
  position:sticky;top:0;z-index:20}
header h1{font-size:14px;margin:0;font-weight:700}
.pill{padding:3px 9px;border-radius:999px;font-size:10px;font-weight:700;
  text-transform:uppercase;letter-spacing:.6px}
.pill.green{background:rgba(74,222,128,.15);color:var(--green)}
.pill.red{background:rgba(248,113,113,.15);color:var(--red)}
.pill.yellow{background:rgba(250,204,21,.15);color:var(--yellow)}
.pill.blue{background:rgba(96,165,250,.15);color:var(--blue)}
.pill.purple{background:rgba(192,132,252,.15);color:var(--purple)}
.pill.gray{background:rgba(255,255,255,.05);color:var(--muted)}
.hstat{display:flex;flex-direction:column;line-height:1.1}
.hstat .label{color:var(--muted);font-size:9px;text-transform:uppercase;
  letter-spacing:.6px}
.hstat .val{font-size:13px;font-weight:700;font-variant-numeric:tabular-nums}
.pos{color:var(--green)} .neg{color:var(--red)}
nav.tabs{display:flex;gap:2px;padding:0 12px;background:var(--panel);
  border-bottom:1px solid var(--line);position:sticky;top:56px;z-index:15}
nav.tabs button{background:none;border:none;color:var(--muted);font-weight:600;
  font-size:12px;padding:10px 16px;cursor:pointer;text-transform:uppercase;
  letter-spacing:.4px;border-bottom:2px solid transparent}
nav.tabs button:hover{color:var(--txt)}
nav.tabs button.active{color:var(--blue);border-bottom-color:var(--blue)}
.tab-desc{padding:8px 20px;background:var(--panel);
  border-bottom:1px solid var(--line);color:var(--muted);font-size:12px;
  position:sticky;top:97px;z-index:14}
.tab-desc b{color:var(--txt);font-weight:600}
.btn{padding:6px 14px;border-radius:6px;font-size:11px;font-weight:600;
  border:1px solid var(--line);background:var(--panel-2);color:var(--txt);
  cursor:pointer;text-transform:uppercase;letter-spacing:.3px}
.btn:hover{background:#243349}
.btn.period-btn{padding:4px 10px;font-size:10px}
.btn.period-btn.active{background:var(--blue);color:#0a0e16;border-color:var(--blue)}
.btn.primary{background:var(--blue);color:#0a0e16;border-color:var(--blue)}
.btn.primary:hover{background:#93c5fd}
.btn.warn{background:var(--yellow);color:#0a0e16;border-color:var(--yellow)}
.btn.danger{background:var(--red);color:#0a0e16;border-color:var(--red)}
.toolbar{display:flex;gap:8px;margin-left:auto;align-items:center}
main{padding:12px;display:flex;flex-direction:column;gap:12px}
.row{display:grid;gap:12px}
.row.two{grid-template-columns:1fr 1fr}
.row.three{grid-template-columns:repeat(3,1fr)}
.row.four{grid-template-columns:repeat(4,1fr)}
.row.six{grid-template-columns:repeat(6,1fr)}
.panel{background:var(--panel);border:1px solid var(--line);border-radius:8px;
  overflow:hidden;display:flex;flex-direction:column}
.panel h2{margin:0;padding:9px 14px;font-size:11px;font-weight:700;
  color:var(--muted);text-transform:uppercase;letter-spacing:.7px;
  border-bottom:1px solid var(--line);background:var(--panel-2);
  display:flex;justify-content:space-between;align-items:center;gap:10px}
.panel h2 .hint{color:var(--dim);font-weight:500;text-transform:none;
  letter-spacing:0;font-size:10px}
.panel .body{overflow:auto;max-height:520px}
.tab-content{display:none;flex-direction:column;gap:12px}
.tab-content.active{display:flex}
.card{background:var(--panel);border:1px solid var(--line);border-radius:8px;
  padding:12px 14px;display:flex;flex-direction:column;gap:2px}
.card .k{color:var(--muted);font-size:10px;text-transform:uppercase;letter-spacing:.6px}
.card .v{font-size:22px;font-weight:700;font-variant-numeric:tabular-nums}
.card .sub{color:var(--dim);font-size:10px}
table{width:100%;border-collapse:collapse;font-variant-numeric:tabular-nums}
th,td{padding:6px 12px;text-align:left;font-size:12px}
th{color:var(--muted);font-size:10px;text-transform:uppercase;
  letter-spacing:.5px;font-weight:700;background:var(--panel-2)}
tr{border-bottom:1px solid rgba(255,255,255,.03)}
td.num{text-align:right;font-feature-settings:"tnum"}
td.q{max-width:380px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap}
.ev{padding:6px 14px;font-size:12px;border-bottom:1px solid rgba(255,255,255,.03)}
.ev .t{color:var(--muted);font-size:10px}
.ev .e{font-weight:700}
.ev .e.intent{color:var(--blue)}
.ev .e.filled,.ev .e.dry_filled{color:var(--green)}
.ev .e.order_failed,.ev .e.order_timeout{color:var(--red)}
.chart-svg{display:block;width:100%;height:200px}
.chart-svg text{font-family:-apple-system,sans-serif;font-size:10px;fill:var(--muted)}
.kform{padding:8px 14px 14px}
.kform .row-k{display:grid;grid-template-columns:200px 1fr 70px;gap:10px;
  padding:4px 0;align-items:center;border-bottom:1px dashed rgba(255,255,255,.04)}
.kform .row-k label{color:var(--muted);font-size:11px;
  font-family:ui-monospace,monospace}
.kform .row-k input[type=range]{width:100%;accent-color:var(--blue)}
.kform .row-k .val{text-align:right;font-variant-numeric:tabular-nums;
  color:var(--blue);font-weight:700}
.kform-actions{padding:10px 14px;border-top:1px solid var(--line);
  display:flex;gap:8px;justify-content:flex-end}
.empty{padding:18px 14px;color:var(--dim);font-style:italic;text-align:center}
.mono{font-family:ui-monospace,monospace}
.heatmap-row{display:grid;align-items:center;font-size:11px;
  font-variant-numeric:tabular-nums}
.heatmap-cell{padding:4px 6px;text-align:center;border-right:1px solid var(--bg);
  border-bottom:1px solid var(--bg)}
.toast-stack{position:fixed;right:14px;bottom:14px;display:flex;
  flex-direction:column;gap:6px;z-index:100}
.toast{padding:8px 14px;border-radius:6px;font-size:12px;
  background:var(--panel-2);border:1px solid var(--line);
  box-shadow:0 4px 12px rgba(0,0,0,.4)}
.toast.warn{border-color:var(--yellow)}
.toast.error{border-color:var(--red)}
footer{padding:8px 18px;color:var(--dim);font-size:11px;
  border-top:1px solid var(--line)}
.indicator-cell{display:inline-block;padding:2px 6px;border-radius:4px;
  font-size:11px;font-weight:600;font-variant-numeric:tabular-nums}
.ind-buy{background:rgba(74,222,128,.15);color:var(--green)}
.ind-sell{background:rgba(248,113,113,.15);color:var(--red)}
.ind-flat{background:rgba(124,138,163,.15);color:var(--muted)}

/* ── Strategy cards ─────────────────────────────────────────────────────── */
.strat-cards{display:grid;grid-template-columns:repeat(4,1fr);gap:12px;padding:14px}
.strat-card{position:relative;background:var(--panel-2);
  border:1.5px solid var(--line);border-radius:10px;padding:14px 16px;
  cursor:pointer;display:flex;flex-direction:column;gap:6px;
  transition:transform .15s ease,border-color .15s ease,background .15s ease}
.strat-card:hover{border-color:rgba(96,165,250,.5);
  transform:translateY(-2px);background:#1d2738}
.strat-card.selected{border-color:var(--blue);
  background:linear-gradient(180deg,rgba(96,165,250,.08),rgba(96,165,250,.02))}
.strat-card.active{border-color:var(--green)}
.strat-card .title{font-size:14px;font-weight:700;letter-spacing:.3px;
  display:flex;align-items:center;gap:8px}
.strat-card .badge{display:inline-block;background:var(--green);color:#0a0e16;
  font-size:9px;font-weight:800;padding:2px 6px;border-radius:4px;
  text-transform:uppercase;letter-spacing:.5px}
.strat-card .tag{font-size:11px;color:var(--muted);line-height:1.4;
  min-height:32px}
.strat-card .metrics{display:flex;gap:14px;margin-top:6px;
  border-top:1px dashed rgba(255,255,255,.06);padding-top:8px}
.strat-card .metric{display:flex;flex-direction:column;gap:1px}
.strat-card .metric .k{color:var(--muted);font-size:9px;text-transform:uppercase;
  letter-spacing:.5px}
.strat-card .metric .v{font-weight:700;font-variant-numeric:tabular-nums;
  font-size:13px}
.strat-card .hint{color:var(--dim);font-size:9px;margin-top:auto;
  letter-spacing:.3px;text-transform:uppercase}
/* Strategy detail spec panel */
.strat-spec{padding:18px 22px;display:grid;grid-template-columns:1fr 1fr;
  gap:28px 36px}
.strat-spec h3{font-size:10px;color:var(--muted);text-transform:uppercase;
  letter-spacing:.7px;margin:0 0 10px;font-weight:700;
  border-bottom:1px solid var(--line);padding-bottom:6px}
.strat-spec .desc{color:var(--txt);line-height:1.65;font-size:13px}
.strat-spec .logic{display:flex;flex-direction:column;gap:6px}
.strat-spec .step{display:grid;grid-template-columns:24px 1fr;gap:10px;
  align-items:start;font-size:12px;line-height:1.5}
.strat-spec .step .n{background:var(--blue);color:#0a0e16;border-radius:50%;
  width:20px;height:20px;display:flex;align-items:center;justify-content:center;
  font-size:10px;font-weight:800;flex-shrink:0;margin-top:1px}
.strat-spec .formula{background:var(--bg);border:1px solid var(--line);
  border-radius:6px;padding:12px 14px;font-family:ui-monospace,monospace;
  font-size:11px;color:var(--cyan);line-height:1.75;white-space:pre;
  overflow-x:auto}
.strat-spec .regime{display:grid;grid-template-columns:1fr 1fr;gap:14px}
.strat-spec .regime-col h4{margin:0 0 6px;font-size:10px;color:var(--muted);
  text-transform:uppercase;letter-spacing:.5px;font-weight:700}
.strat-spec .regime-item{padding:3px 0;font-size:12px;display:flex;
  gap:8px;align-items:flex-start}
.strat-spec .regime-item.good::before{content:'✓';color:var(--green);
  font-weight:800}
.strat-spec .regime-item.bad::before{content:'✗';color:var(--red);
  font-weight:800}
.strat-spec .params-grid{font-family:ui-monospace,monospace;font-size:11px;
  display:grid;grid-template-columns:1fr auto;gap:4px 16px}
.strat-spec .params-grid .pk{color:var(--muted)}
.strat-spec .params-grid .pv{color:var(--cyan);font-weight:700;text-align:right}
</style>
</head><body>

<header>
  <h1>RH Crypto — VTMR</h1>
  <span id="broker-pill" class="pill gray" title="Data source: real broker, mock data, or other.">—</span>
  <span id="mode-pill" class="pill yellow" title="Trading mode: dry-run (no real orders) or live.">…</span>
  <span id="kill-pill" class="pill green" title="Risk kill switch: OK / HALT_NEW (no new entries) / HALT_ALL (manage existing only).">—</span>
  <span id="pause-pill" class="pill gray" style="display:none" title="Scheduler paused; click Resume to re-enable.">PAUSED</span>
  <div class="hstat"><span class="label">Equity</span><span class="val" id="equity">—</span></div>
  <div class="hstat"><span class="label">Cash</span><span class="val" id="cash">—</span></div>
  <div class="hstat"><span class="label">Deployed</span><span class="val" id="deployed">—</span></div>
  <div class="hstat"><span class="label">Daily PnL</span><span class="val" id="daily-pnl">—</span></div>
  <div class="hstat"><span class="label">Sharpe</span><span class="val" id="sharpe">—</span></div>
  <div class="hstat"><span class="label">Max DD</span><span class="val" id="dd">—</span></div>
  <div class="toolbar">
    <button class="btn primary" id="btn-cycle"
      title="Run a full scan + signal + rebalance cycle immediately, instead of waiting for POLL_INTERVAL_SECONDS.">Cycle Now</button>
    <button class="btn" id="btn-pause"
      title="Pause new entries. Existing positions are still marked-to-market and managed (exits still run).">Pause</button>
    <button class="btn danger" id="btn-flatten"
      title="EMERGENCY: sell every open position to USD at current best-bid (minus slippage). Confirms before sending.">Flatten All</button>
    <span id="ts" class="mono" style="color:var(--dim);font-size:10px;margin-left:6px">…</span>
  </div>
</header>

<nav class="tabs">
  <button class="tab active" data-tab="overview">Overview</button>
  <button class="tab" data-tab="strategies">Strategies</button>
  <button class="tab" data-tab="markets">Markets</button>
  <button class="tab" data-tab="indicators">Indicators</button>
  <button class="tab" data-tab="backtest">Backtest</button>
  <button class="tab" data-tab="robust">Robustness</button>
  <button class="tab" data-tab="risk">Risk</button>
  <button class="tab" data-tab="research">Research</button>
  <button class="tab" data-tab="tools">Tools</button>
</nav>

<div class="tab-desc" id="tab-desc">…</div>

<main>

  <!-- OVERVIEW -->
  <div class="tab-content active" id="tab-overview">
    <section class="row six">
      <div class="card"><span class="k">Cycles</span><span class="v" id="s-cycles">0</span>
        <span class="sub" id="s-uptime">since —</span></div>
      <div class="card"><span class="k">Fills</span><span class="v" id="s-fills">0</span>
        <span class="sub" id="s-intents">0 intents</span></div>
      <div class="card"><span class="k">Open positions</span><span class="v" id="s-pos">0</span>
        <span class="sub" id="s-pos-usd">$0</span></div>
      <div class="card"><span class="k">Failures</span><span class="v" id="s-fail">0</span>
        <span class="sub" id="s-timeout">0 timeouts</span></div>
      <div class="card"><span class="k">Portfolio vol</span><span class="v" id="s-vol">—</span>
        <span class="sub">annualized</span></div>
      <div class="card"><span class="k">Sortino</span><span class="v" id="s-sortino">—</span>
        <span class="sub">downside-only</span></div>
    </section>

    <section class="panel">
      <h2>Equity Curve <span class="hint" id="spark-info">—</span></h2>
      <svg class="chart-svg" id="spark" style="height:160px"></svg>
    </section>

    <section class="row two">
      <div class="panel">
        <h2>Current Holdings</h2>
        <div class="body">
          <table><thead><tr><th>Symbol</th><th class="num">Qty</th>
            <th class="num">Mark</th><th class="num">USD</th>
            <th class="num">Weight</th></tr></thead>
          <tbody id="holdings-body"></tbody></table>
          <div id="holdings-empty" class="empty">No open positions.</div>
        </div>
      </div>

      <div class="panel">
        <h2>Target Weights <span class="hint">strategy output</span></h2>
        <div class="body" id="weights-body"></div>
      </div>
    </section>

    <section class="panel">
      <h2>Journal Events <span class="hint">live feed</span></h2>
      <div class="body" id="ev-body" style="max-height:360px"></div>
    </section>
  </div>

  <!-- MARKETS -->
  <div class="tab-content" id="tab-markets">
    <section class="panel">
      <h2>Price History
        <span class="hint">live in-memory bars · or yfinance daily history up to 10 years</span></h2>
      <div style="padding:10px 14px;display:flex;gap:6px;flex-wrap:wrap;
        align-items:center;border-bottom:1px solid var(--line)">
        <span style="color:var(--muted);font-size:11px;text-transform:uppercase;
          letter-spacing:.5px;margin-right:8px">Period</span>
        <button class="btn period-btn active" data-period="live"
          title="Live in-memory bars (last 100). Updates every cycle.">LIVE</button>
        <button class="btn period-btn" data-period="5d:1h"
          title="5 days of hourly bars via Yahoo Finance.">5D · 1h</button>
        <button class="btn period-btn" data-period="1mo:1d"
          title="1 month of daily bars via Yahoo Finance.">1M</button>
        <button class="btn period-btn" data-period="3mo:1d">3M</button>
        <button class="btn period-btn" data-period="6mo:1d">6M</button>
        <button class="btn period-btn" data-period="1y:1d">1Y</button>
        <button class="btn period-btn" data-period="2y:1d">2Y</button>
        <button class="btn period-btn" data-period="5y:1d">5Y</button>
        <button class="btn period-btn" data-period="10y:1d">10Y</button>
        <button class="btn period-btn" data-period="max:1d">MAX</button>
        <span id="mkt-status" style="color:var(--dim);font-size:11px;margin-left:auto">—</span>
      </div>
      <div id="markets-grid" style="display:grid;grid-template-columns:repeat(2,1fr);
        gap:12px;padding:12px"></div>
    </section>
  </div>

  <!-- INDICATORS -->
  <div class="tab-content" id="tab-indicators">
    <section class="panel">
      <h2>Signal Table <span class="hint">live indicator values per asset</span></h2>
      <div class="body" style="max-height:none">
        <table><thead><tr>
          <th>Symbol</th><th class="num">Price</th>
          <th class="num">EMA</th><th class="num">Z</th>
          <th class="num">RSI</th><th class="num">MACD</th>
          <th class="num">ATR</th><th class="num">RVol</th>
          <th>Dir</th><th class="num">Strength</th>
          <th class="num">Weight</th>
        </tr></thead><tbody id="sig-body"></tbody></table>
        <div id="sig-empty" class="empty">Indicators populate after enough bars accumulate.</div>
      </div>
    </section>
  </div>

  <!-- RISK -->
  <div class="tab-content" id="tab-risk">
    <section class="row four">
      <div class="card"><span class="k">Kill switch</span>
        <span class="v" id="r-kill">OK</span><span class="sub">live</span></div>
      <div class="card"><span class="k">Daily anchor</span>
        <span class="v" id="r-anchor">—</span><span class="sub" id="r-anchor-date">—</span></div>
      <div class="card"><span class="k">High-water</span>
        <span class="v" id="r-hwm">—</span><span class="sub">equity peak</span></div>
      <div class="card"><span class="k">Current DD</span>
        <span class="v" id="r-dd">—</span><span class="sub">from HWM</span></div>
    </section>

    <section class="row two">
      <div class="panel">
        <h2>Drawdown Curve</h2>
        <svg class="chart-svg" id="dd-svg"></svg>
      </div>
      <div class="panel">
        <h2>Returns Histogram <span class="hint">per cycle</span></h2>
        <svg class="chart-svg" id="ret-hist-svg"></svg>
      </div>
    </section>

    <section class="panel">
      <h2>Position Concentration</h2>
      <div class="body" id="conc-body"></div>
    </section>
  </div>

  <!-- RESEARCH -->
  <div class="tab-content" id="tab-research">
    <section class="row two">
      <div class="panel">
        <h2>Per-Asset Stats</h2>
        <div class="body">
          <table><thead><tr>
            <th>Symbol</th><th class="num">Bars</th>
            <th class="num">Last</th><th class="num">Ret %</th>
            <th class="num">Ann Vol</th><th class="num">Sharpe</th>
            <th class="num">Sortino</th><th class="num">Max DD</th>
          </tr></thead><tbody id="research-stats"></tbody></table>
        </div>
      </div>

      <div class="panel">
        <h2>Regime Detection</h2>
        <div class="body" id="regime-body" style="padding:10px"></div>
      </div>
    </section>

    <section class="panel">
      <h2>Correlation Matrix <span class="hint">return correlations</span></h2>
      <div id="corr-body" style="padding:12px"></div>
    </section>

    <section class="panel">
      <h2>Macro Reference <span class="hint">SPY / QQQ / GLD / UUP via yfinance</span></h2>
      <div id="macro-body" style="padding:12px">
        <div class="empty">Macro data loads asynchronously on first cycle (yfinance).</div>
      </div>
    </section>

    <section class="panel">
      <h2>Crypto × Macro Correlation
        <span class="hint">daily-return correlation</span></h2>
      <div id="macro-corr-body" style="padding:12px">
        <div class="empty">Loads with macro data.</div>
      </div>
    </section>
  </div>

  <!-- STRATEGIES -->
  <div class="tab-content" id="tab-strategies">
    <section class="panel">
      <h2>Strategy Selector
        <span class="hint">click to inspect · shift-click to set active</span></h2>
      <div class="strat-cards" id="strat-cards"></div>
    </section>

    <section class="panel">
      <h2 id="strat-spec-title">Strategy Spec</h2>
      <div class="strat-spec" id="strat-spec-body"></div>
    </section>

    <section class="panel">
      <h2>Live Signals <span class="hint" id="strat-sig-strat">—</span></h2>
      <div class="body" style="max-height:none">
        <table>
          <thead><tr>
            <th>Symbol</th><th class="num">Price</th>
            <th class="num">EMA</th><th class="num">Z</th>
            <th class="num">RSI</th><th>Dir</th>
            <th class="num">Strength</th><th class="num">Weight</th>
          </tr></thead>
          <tbody id="strat-sig-body"></tbody>
        </table>
      </div>
    </section>

    <section class="panel">
      <h2>Strategy Comparison <span class="hint">backtest each on accumulated bars</span></h2>
      <div style="padding:8px 14px;border-bottom:1px solid var(--line);
        display:flex;gap:8px;align-items:center">
        <button class="btn primary" id="btn-compare-all">Run All Strategies</button>
        <span id="compare-status" style="color:var(--muted);font-size:11px">
          uses the bars in memory — best with full warmup history</span>
      </div>
      <svg class="chart-svg" id="cmp-svg" style="height:240px"></svg>
      <div class="body" style="max-height:240px">
        <table><thead><tr>
          <th>Strategy</th><th class="num">Return</th>
          <th class="num">Sharpe</th><th class="num">Sortino</th>
          <th class="num">Ann Vol</th><th class="num">Max DD</th>
          <th class="num">Trades</th>
        </tr></thead><tbody id="cmp-tbody"></tbody></table>
      </div>
    </section>
  </div>

  <!-- BACKTEST -->
  <div class="tab-content" id="tab-backtest">
    <section class="panel">
      <h2>Backtest <span class="hint">replay strategy over accumulated bars</span></h2>
      <div style="padding:10px 14px;display:flex;gap:10px;align-items:center;
        border-bottom:1px solid var(--line);flex-wrap:wrap">
        <label style="color:var(--muted);font-size:11px;text-transform:uppercase;
          letter-spacing:.5px">Strategy</label>
        <select id="bt-strat" style="background:var(--panel-2);color:var(--txt);
          border:1px solid var(--line);border-radius:6px;padding:5px 10px;
          font-size:12px"></select>
        <button class="btn primary" id="btn-bt-run">Run on Live Bars</button>
        <button class="btn" id="btn-bt-hist">Run on 10y Daily</button>
        <select id="bt-period" style="background:var(--panel-2);color:var(--txt);
          border:1px solid var(--line);border-radius:6px;padding:5px 8px;font-size:11px">
          <option value="10y">10y</option><option value="5y">5y</option>
          <option value="2y">2y</option><option value="1y">1y</option>
        </select>
        <select id="bt-interval" style="background:var(--panel-2);color:var(--txt);
          border:1px solid var(--line);border-radius:6px;padding:5px 8px;font-size:11px">
          <option value="1d">1d</option><option value="1h">1h</option>
        </select>
        <span id="bt-status" style="color:var(--muted);font-size:11px">—</span>
      </div>
      <section class="row four" style="padding:12px">
        <div class="card"><span class="k">Final Equity</span>
          <span class="v" id="bt-final">—</span></div>
        <div class="card"><span class="k">Return</span>
          <span class="v" id="bt-return">—</span></div>
        <div class="card"><span class="k">Sharpe</span>
          <span class="v" id="bt-sharpe">—</span></div>
        <div class="card"><span class="k">Max DD</span>
          <span class="v" id="bt-dd">—</span></div>
        <div class="card"><span class="k">Sortino</span>
          <span class="v" id="bt-sortino">—</span></div>
        <div class="card"><span class="k">Ann Vol</span>
          <span class="v" id="bt-vol">—</span></div>
        <div class="card"><span class="k">Trades</span>
          <span class="v" id="bt-trades">—</span></div>
        <div class="card"><span class="k">Initial</span>
          <span class="v" id="bt-init">—</span></div>
      </section>
      <h3 style="margin:0;padding:8px 14px;font-size:11px;color:var(--muted);
        text-transform:uppercase;letter-spacing:.5px;
        border-top:1px solid var(--line);border-bottom:1px solid var(--line);
        background:var(--panel-2)">Equity Curve</h3>
      <svg class="chart-svg" id="bt-eq" style="height:200px"></svg>
      <h3 style="margin:0;padding:8px 14px;font-size:11px;color:var(--muted);
        text-transform:uppercase;letter-spacing:.5px;
        border-top:1px solid var(--line);border-bottom:1px solid var(--line);
        background:var(--panel-2)">Per-Asset Trade-Cost PnL</h3>
      <div id="bt-pnl-body" style="padding:10px 14px"></div>
      <h3 style="margin:0;padding:8px 14px;font-size:11px;color:var(--muted);
        text-transform:uppercase;letter-spacing:.5px;
        border-top:1px solid var(--line);border-bottom:1px solid var(--line);
        background:var(--panel-2)">Last 30 Trades</h3>
      <div class="body" style="max-height:300px">
        <table><thead><tr>
          <th>Timestamp</th><th>Symbol</th><th>Side</th>
          <th class="num">Qty</th><th class="num">Price</th><th class="num">USD</th>
        </tr></thead><tbody id="bt-trades-body"></tbody></table>
      </div>
    </section>
  </div>

  <!-- ROBUSTNESS -->
  <div class="tab-content" id="tab-robust">
    <section class="panel">
      <h2>Parameter Sweep <span class="hint">two-dim grid → heatmap of metric</span></h2>
      <div style="padding:10px 14px;display:grid;grid-template-columns:repeat(6,1fr);
        gap:10px;align-items:center;border-bottom:1px solid var(--line);font-size:11px">
        <label style="color:var(--muted)">Strategy</label>
        <select id="sw-strat" style="background:var(--panel-2);color:var(--txt);
          border:1px solid var(--line);border-radius:6px;padding:4px 8px"></select>
        <label style="color:var(--muted)">Param X</label>
        <select id="sw-px" style="background:var(--panel-2);color:var(--txt);
          border:1px solid var(--line);border-radius:6px;padding:4px 8px"></select>
        <label style="color:var(--muted)">Range X</label>
        <input id="sw-vx" type="text" value="" placeholder="e.g. 0.5,1.0,1.5,2.0"
          style="background:var(--panel-2);color:var(--txt);
          border:1px solid var(--line);border-radius:6px;padding:4px 8px;font-family:ui-monospace,monospace">

        <label style="color:var(--muted)">&nbsp;</label>
        <span></span>
        <label style="color:var(--muted)">Param Y</label>
        <select id="sw-py" style="background:var(--panel-2);color:var(--txt);
          border:1px solid var(--line);border-radius:6px;padding:4px 8px"></select>
        <label style="color:var(--muted)">Range Y</label>
        <input id="sw-vy" type="text" value="" placeholder="e.g. 10,20,30,50,80"
          style="background:var(--panel-2);color:var(--txt);
          border:1px solid var(--line);border-radius:6px;padding:4px 8px;font-family:ui-monospace,monospace">

        <label style="color:var(--muted)">Metric</label>
        <select id="sw-metric" style="background:var(--panel-2);color:var(--txt);
          border:1px solid var(--line);border-radius:6px;padding:4px 8px">
          <option value="sharpe">Sharpe</option>
          <option value="return_pct">Return %</option>
          <option value="sortino">Sortino</option>
          <option value="max_drawdown_pct">Max DD %</option>
        </select>
        <span></span><span></span>
        <button class="btn primary" id="btn-sw-run">Run Sweep</button>
        <span id="sw-status" style="color:var(--muted)">—</span>
      </div>
      <div id="sw-body" style="padding:12px">
        <div class="empty">Configure params + ranges, then Run Sweep.</div>
      </div>
    </section>

    <section class="panel">
      <h2>Stress Sensitivities <span class="hint">slippage / fill assumptions</span></h2>
      <div style="padding:8px 14px;display:flex;gap:8px;align-items:center;
        border-bottom:1px solid var(--line)">
        <button class="btn" id="btn-stress-slip">Run Slippage Stress</button>
        <span id="stress-status" style="color:var(--muted);font-size:11px">—</span>
      </div>
      <div id="stress-body" style="padding:10px 14px">
        <div class="empty">Stress tests Sharpe across 5 slippage levels.</div>
      </div>
    </section>
  </div>

  <!-- TOOLS -->
  <div class="tab-content" id="tab-tools">
    <section class="panel">
      <h2>Strategy Knobs <span class="hint">changes apply on next cycle</span></h2>
      <form class="kform" id="kform"></form>
      <div class="kform-actions">
        <button type="button" class="btn" id="btn-reset-knobs">Reload</button>
        <button type="button" class="btn primary" id="btn-apply-knobs">Apply</button>
        <button type="button" class="btn warn" id="btn-reset-anchor">Reset Kill Switch</button>
      </div>
    </section>
  </div>

</main>

<footer><span id="cfg">…</span> · polling 3s · 127.0.0.1 ·
  <a href="/api/state" target="_blank" style="color:var(--blue)">/api/state</a>
</footer>

<div class="toast-stack" id="toast-stack"></div>

<script>
const fmt = {
  usd(v){if(v==null||isNaN(v))return '—';
    return '$'+Number(v).toLocaleString(undefined,{minimumFractionDigits:2,maximumFractionDigits:2})},
  usdC(v){if(v==null||isNaN(v))return '—';
    return '$'+Number(v).toLocaleString(undefined,{maximumFractionDigits:0})},
  num(v,d=3){if(v==null||isNaN(v))return '—';return Number(v).toFixed(d)},
  pct(v,d=2){if(v==null||isNaN(v))return '—';return (Number(v)*100).toFixed(d)+'%'},
  qty(v){if(v==null||isNaN(v))return '—';
    return Math.abs(v)<0.001?v.toExponential(3):v.toFixed(6)},
  time(v){return v?new Date(v).toLocaleTimeString():'—'},
  dur(start){if(!start)return '—';
    const s=Math.max(0,Math.floor((new Date()-new Date(start))/1000));
    if(s<60)return s+'s';if(s<3600)return Math.floor(s/60)+'m';
    return Math.floor(s/3600)+'h '+Math.floor((s%3600)/60)+'m'},
  rel(v){if(!v)return '—';
    const d=(new Date(v)-new Date())/1000;
    const a=Math.abs(d);
    if(a<60)return Math.round(d)+'s';
    if(a<3600)return Math.round(d/60)+'m';
    return Math.round(d/3600)+'h'},
};
async function api(url,opts){try{const r=await fetch(url,opts);
  if(!r.ok)return{ok:false,error:await r.text()};return await r.json();}catch(e){return null}}
const setText=(id,v)=>{const el=document.getElementById(id);if(el)el.textContent=v};
const esc=s=>s==null?'':String(s).replace(/[&<>"']/g,c=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));
const seenMsg=new Set();
function toast(text,level='info'){
  const id=text+'::'+Math.floor(Date.now()/1000);
  if(seenMsg.has(id))return; seenMsg.add(id);
  const div=document.createElement('div'); div.className='toast '+level;
  div.textContent=text; document.getElementById('toast-stack').appendChild(div);
  setTimeout(()=>div.remove(),4500);
}
const TAB_DESC = {
  overview:   '<b>Overview.</b> Header KPIs · equity curve · current positions · target weights · live journal feed.',
  strategies: '<b>Strategies.</b> Four strategies running in parallel. The active one drives execution; the rest are shadow comparisons. Click a card to inspect its formula and signals; shift-click to make it active.',
  markets:    '<b>Markets.</b> Per-asset price charts with EMA (orange dashed) and Bollinger bands (blue tinted region). Last 100 bars per symbol.',
  indicators: '<b>Indicators.</b> Live signal table — z-score, RSI, MACD, ATR, realized vol, direction, and target weight for every asset under the active strategy.',
  backtest:   '<b>Backtest.</b> Replay any strategy over the bars in memory, or pull 10-year daily history from Yahoo Finance for a real out-of-sample run.',
  robust:     '<b>Robustness.</b> Two-dimensional parameter sweep. Pick two knobs and value ranges; the heatmap shows the chosen metric across the grid so you can find stable parameter regions vs cliff edges.',
  risk:       '<b>Risk.</b> Kill-switch state · drawdown curve · returns histogram · position concentration. Tracks daily anchor and high-water mark.',
  research:   '<b>Research.</b> Per-asset stats · regime detection · cross-asset correlation matrix · live macro reference (SPY/QQQ/GLD via yfinance).',
  tools:      '<b>Tools.</b> Live-editable strategy knobs and emergency controls (Reset Kill Switch, Flatten All). Changes apply on the next scan cycle.',
};
function setTabDesc(tab){
  const el=document.getElementById('tab-desc');
  if(el && TAB_DESC[tab]) el.innerHTML=TAB_DESC[tab];
}
document.querySelectorAll('nav.tabs button').forEach(b=>{b.onclick=()=>{
  document.querySelectorAll('nav.tabs button').forEach(x=>x.classList.remove('active'));
  document.querySelectorAll('.tab-content').forEach(x=>x.classList.remove('active'));
  b.classList.add('active');
  document.getElementById('tab-'+b.dataset.tab).classList.add('active');
  setTabDesc(b.dataset.tab);
  if(window._lastState)refreshActiveTab(window._lastState);
}});
setTabDesc('overview');

function renderHeader(s){
  const dry=s.dry_run;
  const m=document.getElementById('mode-pill');
  m.textContent=dry?'DRY-RUN':'LIVE'; m.className='pill '+(dry?'yellow':'red');
  const k=document.getElementById('kill-pill');
  k.textContent=s.kill_switch||'OK';
  k.className='pill '+({'OK':'green','HALT_NEW':'yellow','HALT_ALL':'red'}[s.kill_switch]||'green');
  const b=document.getElementById('broker-pill');
  b.textContent=(s.broker||'').toUpperCase();
  b.className='pill '+(s.broker==='mock'?'blue':s.broker==='robinhood_crypto'?'purple':'gray');
  const pp=document.getElementById('pause-pill');
  pp.style.display=s.scheduler_paused?'':'none';
  document.getElementById('btn-pause').textContent=s.scheduler_paused?'Resume':'Pause';
  setText('equity',fmt.usd(s.equity));
  setText('cash',fmt.usd(s.cash));
  setText('deployed',fmt.usd(s.deployed));
  setText('ts','updated '+new Date().toLocaleTimeString());
  // Daily PnL
  const anc=(s.stats&&s.stats.equity_history&&s.stats.equity_history.length>1)
    ? s.stats.equity_history[0][1] : null;
  const eq=s.equity;
  const dp=anc?eq-anc:0;
  const dpEl=document.getElementById('daily-pnl');
  dpEl.textContent=fmt.usd(dp);
  dpEl.className='val '+(dp>=0?'pos':'neg');
  // Sharpe / DD
  const rets=s.stats&&s.stats.returns||[];
  const sharpe=computeSharpe(rets);
  setText('sharpe',sharpe==null?'—':sharpe.toFixed(2));
  const dd=computeMaxDD((s.stats&&s.stats.equity_history||[]).map(x=>x[1]));
  setText('dd',(dd*100).toFixed(1)+'%');
  // Config strip
  const cfg=s.config||{};
  document.getElementById('cfg').textContent=
    `cap $${cfg.TOTAL_CAPITAL} · target ${(cfg.TARGET_PORTFOLIO_VOL_ANNUAL*100).toFixed(0)}% vol`+
    ` · entryZ ${cfg.ENTRY_Z} · ema ${cfg.EMA_PERIOD} · poll ${cfg.POLL_INTERVAL_SECONDS}s`;
}

function computeSharpe(rets){
  if(!rets||rets.length<5)return null;
  const m=rets.reduce((a,b)=>a+b,0)/rets.length;
  const v=rets.reduce((a,b)=>a+(b-m)**2,0)/Math.max(1,rets.length-1);
  const s=Math.sqrt(v); if(s===0)return 0;
  // Per-cycle returns → annualize assuming 1 cycle every BAR_INTERVAL
  const periodsPerYear=365*24*12;
  return (m/s)*Math.sqrt(periodsPerYear);
}
function computeMaxDD(eq){
  if(!eq||eq.length<2)return 0;
  let peak=eq[0],dd=0;
  for(const v of eq){if(v>peak)peak=v;if(peak>0)dd=Math.min(dd,(v/peak)-1)}
  return dd;
}

function renderStats(s){
  const st=s.stats||{};
  setText('s-cycles',st.cycles||0);
  setText('s-uptime','since '+fmt.dur(st.started_at)+' ago');
  setText('s-fills',st.fills||0);
  setText('s-intents',(st.intents||0)+' intents');
  const h=s.holdings||{};
  setText('s-pos',Object.keys(h).length);
  let usd=0;
  for(const sym in h){
    const sig=s.signals&&s.signals[sym];
    const px=sig?sig.price:0;
    usd+=h[sym]*px;
  }
  setText('s-pos-usd',fmt.usd(usd));
  setText('s-fail',st.order_failures||0);
  setText('s-timeout',(st.order_timeouts||0)+' timeouts');
  // Portfolio vol from rolling returns
  const rets=st.returns||[];
  if(rets.length>=5){
    const m=rets.reduce((a,b)=>a+b,0)/rets.length;
    const v=rets.reduce((a,b)=>a+(b-m)**2,0)/Math.max(1,rets.length-1);
    const ann=Math.sqrt(v)*Math.sqrt(365*24*12);
    setText('s-vol',(ann*100).toFixed(1)+'%');
  } else setText('s-vol','—');
  // Sortino
  if(rets.length>=5){
    const m=rets.reduce((a,b)=>a+b,0)/rets.length;
    const downs=rets.filter(r=>r<0);
    if(downs.length){
      const d=Math.sqrt(downs.reduce((a,b)=>a+(b-m)**2,0)/downs.length);
      if(d>0) setText('s-sortino',((m/d)*Math.sqrt(365*24*12)).toFixed(2));
      else setText('s-sortino','—');
    } else setText('s-sortino','∞');
  } else setText('s-sortino','—');
}

function renderSpark(s){
  const svg=document.getElementById('spark');
  const hist=(s.stats&&s.stats.equity_history)||[];
  const rect=svg.getBoundingClientRect();
  const W=Math.max(200,Math.round(rect.width||600)); const H=160;
  svg.setAttribute('viewBox',`0 0 ${W} ${H}`);
  if(!hist.length){
    svg.innerHTML=`<text x="14" y="80">No equity samples yet.</text>`; return;
  }
  const vals=hist.map(h=>h[1]);
  const mn=Math.min(...vals),mx=Math.max(...vals);
  const pad=(mx-mn)*0.15||1; const lo=mn-pad,hi=mx+pad;
  const PL=80,PR=12,PT=12,PB=24;
  const plotW=W-PL-PR,plotH=H-PT-PB;
  const pts=hist.map((h,i)=>{
    const x=PL+(hist.length===1?plotW/2:i/(hist.length-1)*plotW);
    const y=PT+plotH-((h[1]-lo)/(hi-lo||1))*plotH;
    return x.toFixed(1)+','+y.toFixed(1);
  }).join(' ');
  const first=vals[0],last=vals[vals.length-1];
  const color=last>=first?'var(--green)':'var(--red)';
  const areaPts=`${PL},${PT+plotH} ${pts} ${PL+plotW},${PT+plotH}`;
  svg.innerHTML=`
    <polygon fill="${color}" fill-opacity="0.10" points="${areaPts}"/>
    <polyline fill="none" stroke="${color}" stroke-width="1.5"
      stroke-linejoin="round" points="${pts}"/>
    <text x="8" y="16">${fmt.usd(mx)}</text>
    <text x="8" y="${H-8}">${fmt.usd(mn)}</text>`;
  setText('spark-info',
    `${hist.length} samples · ${fmt.usd(first)} → ${fmt.usd(last)}`);
}

function renderHoldings(s){
  const h=s.holdings||{};
  const tb=document.getElementById('holdings-body');
  const empty=document.getElementById('holdings-empty');
  const syms=Object.keys(h);
  if(!syms.length){tb.innerHTML='';empty.style.display='block';return}
  empty.style.display='none';
  const eq=s.equity||1;
  tb.innerHTML=syms.map(sym=>{
    const qty=h[sym];
    const sig=(s.signals||{})[sym]||{};
    const px=sig.price||0;
    const usd=qty*px;
    const w=usd/eq;
    return `<tr>
      <td>${esc(sym)}</td>
      <td class="num">${fmt.qty(qty)}</td>
      <td class="num">${fmt.num(px,4)}</td>
      <td class="num">${fmt.usd(usd)}</td>
      <td class="num">${fmt.pct(w,1)}</td>
    </tr>`;
  }).join('');
}

function renderWeights(s){
  const tw=s.target_weights||{};
  const ent=Object.entries(tw).sort((a,b)=>b[1]-a[1]);
  const wb=document.getElementById('weights-body');
  if(!ent.length){wb.innerHTML='<div class="empty">No target weights yet.</div>';return}
  const max=Math.max(...ent.map(([,v])=>v),0.001);
  wb.innerHTML=ent.map(([sym,w])=>{
    const wp=(w/max)*100;
    const usd=w*(s.equity||0);
    return `<div style="padding:6px 14px;font-size:12px;display:grid;
      grid-template-columns:90px 1fr 70px 60px;gap:8px;align-items:center">
      <span>${esc(sym)}</span>
      <div style="height:8px;background:rgba(96,165,250,.1);border-radius:3px;overflow:hidden">
        <div style="height:100%;width:${wp}%;background:var(--blue);border-radius:3px"></div>
      </div>
      <span style="text-align:right;color:var(--muted)">${fmt.usd(usd)}</span>
      <span style="text-align:right;font-weight:700">${fmt.pct(w,1)}</span>
    </div>`;
  }).join('');
}

function renderEvents(events){
  const tb=document.getElementById('ev-body');
  if(!events||!events.length){
    tb.innerHTML='<div class="empty">No journal events yet.</div>';return;
  }
  tb.innerHTML=events.slice().reverse().slice(0,50).map(e=>{
    const ts=e.timestamp?new Date(e.timestamp).toLocaleTimeString():'';
    return `<div class="ev">
      <div><span class="e ${e.event}">${e.event}</span>
        ${e.symbol?' · '+e.symbol:''}
        ${e.side?' · '+e.side:''}
        ${e.price?' @ '+e.price:''}
        ${e.qty?' qty '+e.qty:''}
        ${e.usd_amount?' · '+fmt.usd(e.usd_amount):''}</div>
      ${e.notes?'<div>'+esc(e.notes)+'</div>':''}
      <div class="t">${ts}</div>
    </div>`;
  }).join('');
}

// ─── Markets: per-asset price chart with EMA + Bollinger overlay ─────────
let marketsPeriod='live';
let marketsHistorical={};   // symbol → bars (when not 'live')

function renderMarkets(s){
  const grid=document.getElementById('markets-grid');
  const liveBars=s.bars_by_symbol||{};
  const syms=Object.keys(liveBars).sort();
  if(!syms.length){
    grid.innerHTML='<div class="empty" style="grid-column:1/-1">'+
      'No bars accumulated yet. Wait for the bar interval to complete.</div>';
    return;
  }
  // Decide source: live in-memory OR cached historical
  const useLive = (marketsPeriod==='live');
  grid.innerHTML=syms.map(sym=>{
    const id='px-'+sym.replace(/[^a-z0-9]/gi,'');
    const bars = useLive ? liveBars[sym] : (marketsHistorical[sym]||[]);
    const period = useLive ? 'live' : marketsPeriod;
    return `<div style="border:1px solid var(--line);border-radius:6px;
      background:var(--panel)">
      <div style="padding:8px 12px;border-bottom:1px solid var(--line);
        display:flex;justify-content:space-between;align-items:center;
        font-size:12px;font-weight:700">
        <span>${esc(sym)}</span>
        <span class="mono" style="color:var(--muted);font-size:10px">
          ${bars?bars.length:0} bars · ${esc(period)}</span>
      </div>
      <svg class="chart-svg" id="${id}" style="height:160px"></svg>
    </div>`;
  }).join('');
  for(const sym of syms){
    const id='px-'+sym.replace(/[^a-z0-9]/gi,'');
    const svg=document.getElementById(id);
    const data = useLive ? (liveBars[sym]||[]) : (marketsHistorical[sym]||[]);
    // For historical bars we don't have a "current signal" — skip overlay
    drawCandlesAndOverlay(svg, data,
      useLive ? ((s.signals||{})[sym]||{}) : {});
  }
}

document.querySelectorAll('.period-btn').forEach(btn=>{
  btn.onclick=async()=>{
    document.querySelectorAll('.period-btn').forEach(b=>b.classList.remove('active'));
    btn.classList.add('active');
    marketsPeriod=btn.dataset.period;
    if(marketsPeriod==='live'){
      marketsHistorical={};
      document.getElementById('mkt-status').textContent='Live in-memory bars';
      if(window._lastState)renderMarkets(window._lastState);
      return;
    }
    // Parse "period:interval" e.g. "10y:1d"
    const [period, interval] = marketsPeriod.split(':');
    const syms=Object.keys(window._lastState?.bars_by_symbol||{});
    document.getElementById('mkt-status').textContent=
      `Loading ${period}/${interval} for ${syms.length} symbols…`;
    marketsHistorical={};
    let done=0, failed=0;
    // Fetch concurrently
    await Promise.all(syms.map(async sym=>{
      try{
        const r=await api(`/api/historical_bars?symbol=${encodeURIComponent(sym)}`+
          `&period=${period}&interval=${interval}`);
        if(r && r.ok){
          marketsHistorical[sym]=r.bars||[];
          done++;
        } else { failed++; }
      } catch(e){ failed++; }
    }));
    document.getElementById('mkt-status').textContent=
      `${period}/${interval}: loaded ${done}/${syms.length}` +
      (failed?` (${failed} failed)`:'');
    if(window._lastState)renderMarkets(window._lastState);
  };
});

function drawCandlesAndOverlay(svg, bars, sig){
  if(!bars.length){svg.innerHTML='<text x="14" y="80">No bars</text>';return}
  const rect=svg.getBoundingClientRect();
  const W=Math.max(200,Math.round(rect.width||400));
  const H=160;
  svg.setAttribute('viewBox',`0 0 ${W} ${H}`);
  const PL=48,PR=8,PT=8,PB=16;
  const plotW=W-PL-PR,plotH=H-PT-PB;
  const closes=bars.map(b=>b.close);
  const mn=Math.min(...closes,sig.bollinger_lower||Infinity),
        mx=Math.max(...closes,sig.bollinger_upper||-Infinity);
  const pad=(mx-mn)*0.05||1;
  const lo=mn-pad,hi=mx+pad;
  const Y=v=>PT+plotH-((v-lo)/(hi-lo||1))*plotH;
  const X=i=>PL+(i/(bars.length-1||1))*plotW;
  const linePts=closes.map((c,i)=>`${X(i).toFixed(1)},${Y(c).toFixed(1)}`).join(' ');
  // EMA proxy: re-compute simple EMA in the front-end for the overlay
  const period=Math.max(2,Math.min(50,Math.floor(closes.length/3)));
  const ema=[]; let e=closes[0];
  const alpha=2/(period+1);
  for(const c of closes){e=alpha*c+(1-alpha)*e;ema.push(e)}
  const emaPts=ema.map((v,i)=>`${X(i).toFixed(1)},${Y(v).toFixed(1)}`).join(' ');
  let html='';
  // Bollinger bands (use signal values)
  if(sig.bollinger_upper && sig.bollinger_lower){
    const yU=Y(sig.bollinger_upper),yL=Y(sig.bollinger_lower);
    html+=`<rect x="${PL}" y="${Math.min(yU,yL)}" width="${plotW}"
      height="${Math.abs(yL-yU)}" fill="var(--blue)" fill-opacity="0.04"/>`;
    html+=`<line x1="${PL}" x2="${PL+plotW}" y1="${yU}" y2="${yU}"
      stroke="var(--blue)" stroke-opacity="0.3" stroke-dasharray="2 2"/>`;
    html+=`<line x1="${PL}" x2="${PL+plotW}" y1="${yL}" y2="${yL}"
      stroke="var(--blue)" stroke-opacity="0.3" stroke-dasharray="2 2"/>`;
  }
  html+=`<polyline fill="none" stroke="var(--orange)" stroke-width="1.2"
    stroke-dasharray="3 3" points="${emaPts}"/>`;
  const dir=sig.direction;
  const lineColor=dir==='LONG'?'var(--green)':'var(--txt)';
  html+=`<polyline fill="none" stroke="${lineColor}" stroke-width="1.5"
    points="${linePts}"/>`;
  // Y axis labels
  html+=`<text x="${PL-4}" y="${PT+8}" text-anchor="end">${fmt.num(hi,4)}</text>`;
  html+=`<text x="${PL-4}" y="${PT+plotH}" text-anchor="end">${fmt.num(lo,4)}</text>`;
  // Signal badge
  if(sig.direction){
    const cls=dir==='LONG'?'ind-buy':dir==='FLAT'?'ind-flat':'ind-flat';
    html+=`<text x="${PL+6}" y="${PT+12}" fill="${dir==='LONG'?'#4ade80':'#7c8aa3'}">
      ${esc(dir)} · z=${fmt.num(sig.z_score,2)} · w=${(sig.target_weight*100).toFixed(1)}%</text>`;
  }
  svg.innerHTML=html;
}

function renderIndicators(s){
  const signals=s.signals||{};
  const syms=Object.keys(signals).sort();
  const tb=document.getElementById('sig-body');
  const empty=document.getElementById('sig-empty');
  if(!syms.length){tb.innerHTML='';empty.style.display='block';return}
  empty.style.display='none';
  tb.innerHTML=syms.map(sym=>{
    const x=signals[sym]||{};
    const dirCls=x.direction==='LONG'?'ind-buy':x.direction==='FLAT'?'ind-flat':'ind-flat';
    return `<tr>
      <td><b>${esc(sym)}</b></td>
      <td class="num">${fmt.num(x.price,4)}</td>
      <td class="num">${fmt.num(x.ema,4)}</td>
      <td class="num" style="color:${x.z_score<-1?'var(--green)':x.z_score>1?'var(--red)':'var(--txt)'}">${fmt.num(x.z_score,2)}</td>
      <td class="num">${fmt.num(x.rsi,1)}</td>
      <td class="num">${fmt.num(x.macd,4)}</td>
      <td class="num">${fmt.num(x.atr,4)}</td>
      <td class="num">${fmt.pct(x.realized_vol_annual,1)}</td>
      <td><span class="indicator-cell ${dirCls}">${esc(x.direction||'—')}</span></td>
      <td class="num">${fmt.pct(x.strength,0)}</td>
      <td class="num">${fmt.pct(x.target_weight,2)}</td>
    </tr>`;
  }).join('');
}

function renderRisk(s){
  const cfg=s.config||{};
  const eq=s.equity||0;
  const hist=(s.stats&&s.stats.equity_history||[]).map(x=>x[1]);
  setText('r-kill',s.kill_switch||'OK');
  setText('r-anchor',fmt.usd(s.daily_anchor));
  setText('r-anchor-date',s.anchor_date||'—');
  const hwm=Math.max(...(hist.length?hist:[eq]),eq);
  setText('r-hwm',fmt.usd(hwm));
  const dd=hwm?(eq/hwm-1)*100:0;
  const ddEl=document.getElementById('r-dd');
  ddEl.textContent=dd.toFixed(2)+'%';
  ddEl.style.color=dd<-Math.abs(cfg.MAX_DRAWDOWN_PCT*100)*0.7?'var(--red)':'var(--txt)';
  // Drawdown curve
  drawDDCurve(document.getElementById('dd-svg'), hist);
  // Returns histogram
  drawRetHist(document.getElementById('ret-hist-svg'), s.stats&&s.stats.returns||[]);
  // Concentration
  const h=s.holdings||{};
  const sigs=s.signals||{};
  const cb=document.getElementById('conc-body');
  const items=Object.entries(h).map(([sym,qty])=>{
    const px=sigs[sym]?sigs[sym].price:0;
    const usd=qty*px;
    return{sym,usd,w:eq?usd/eq:0};
  }).sort((a,b)=>b.w-a.w);
  if(!items.length){cb.innerHTML='<div class="empty">No positions.</div>';return}
  const max=Math.max(...items.map(i=>i.w),0.001);
  cb.innerHTML=items.map(it=>`
    <div style="padding:6px 14px;display:grid;grid-template-columns:90px 1fr 70px 60px;
      gap:8px;align-items:center;font-size:12px">
      <span>${esc(it.sym)}</span>
      <div style="height:8px;background:rgba(192,132,252,.1);border-radius:3px;overflow:hidden">
        <div style="height:100%;width:${(it.w/max)*100}%;background:var(--purple);border-radius:3px"></div>
      </div>
      <span style="text-align:right;color:var(--muted)">${fmt.usd(it.usd)}</span>
      <span style="text-align:right;font-weight:700">${fmt.pct(it.w,1)}</span>
    </div>`).join('');
}

function drawDDCurve(svg,hist){
  const rect=svg.getBoundingClientRect();
  const W=Math.max(200,Math.round(rect.width||400));const H=200;
  svg.setAttribute('viewBox',`0 0 ${W} ${H}`);
  if(hist.length<2){svg.innerHTML='<text x="14" y="100">No equity samples yet.</text>';return}
  let peak=hist[0]; const dd=hist.map(v=>{if(v>peak)peak=v;return peak>0?(v/peak-1)*100:0});
  const mn=Math.min(...dd),mx=0;
  const PL=40,PR=8,PT=8,PB=20;
  const plotW=W-PL-PR,plotH=H-PT-PB;
  const X=i=>PL+(i/(dd.length-1||1))*plotW;
  const Y=v=>PT+plotH-((v-mn)/(mx-mn||1))*plotH;
  const pts=dd.map((v,i)=>`${X(i).toFixed(1)},${Y(v).toFixed(1)}`).join(' ');
  const areaPts=`${PL},${Y(0)} ${pts} ${PL+plotW},${Y(0)}`;
  svg.innerHTML=`
    <polygon fill="var(--red)" fill-opacity="0.15" points="${areaPts}"/>
    <polyline fill="none" stroke="var(--red)" stroke-width="1.5" points="${pts}"/>
    <text x="8" y="14">0%</text>
    <text x="8" y="${PT+plotH}">${mn.toFixed(1)}%</text>`;
}

function drawRetHist(svg,rets){
  const rect=svg.getBoundingClientRect();
  const W=Math.max(200,Math.round(rect.width||400)); const H=200;
  svg.setAttribute('viewBox',`0 0 ${W} ${H}`);
  if(rets.length<5){svg.innerHTML='<text x="14" y="100">Need more samples.</text>';return}
  const lo=Math.min(...rets),hi=Math.max(...rets);
  const nBins=20;
  const bins=Array(nBins).fill(0);
  for(const r of rets){
    const i=Math.min(nBins-1,Math.max(0,Math.floor((r-lo)/(hi-lo||1)*nBins)));
    bins[i]++;
  }
  const maxC=Math.max(...bins);
  const PL=8,PR=8,PT=16,PB=24;
  const plotW=W-PL-PR,plotH=H-PT-PB;
  const barW=plotW/nBins;
  let html='';
  bins.forEach((c,i)=>{
    if(!c)return;
    const h=(c/maxC)*plotH;
    const x=PL+i*barW+1;
    const y=PT+plotH-h;
    const centerVal=lo+(hi-lo)*(i+0.5)/nBins;
    const color=centerVal>=0?'var(--green)':'var(--red)';
    html+=`<rect x="${x}" y="${y}" width="${barW-2}" height="${h}"
      fill="${color}" fill-opacity="0.7" rx="1"/>`;
  });
  // Zero line
  if(lo<0&&hi>0){
    const zx=PL+((-lo)/(hi-lo))*plotW;
    html+=`<line x1="${zx}" x2="${zx}" y1="${PT}" y2="${PT+plotH}"
      stroke="var(--muted)" stroke-dasharray="2 2"/>`;
  }
  html+=`<text x="${PL}" y="${H-6}">${fmt.pct(lo,3)}</text>`;
  html+=`<text x="${W-PR}" y="${H-6}" text-anchor="end">${fmt.pct(hi,3)}</text>`;
  svg.innerHTML=html;
}

function renderResearch(s){
  const r=s.research||{};
  // Per-asset stats
  const stats=r.per_asset||[];
  document.getElementById('research-stats').innerHTML=stats.length?
    stats.map(x=>`<tr>
      <td><b>${esc(x.symbol)}</b></td>
      <td class="num">${x.bars}</td>
      <td class="num">${fmt.num(x.last_price,4)}</td>
      <td class="num ${x.return_pct>=0?'pos':'neg'}">${fmt.num(x.return_pct,2)}%</td>
      <td class="num">${x.ann_vol==null?'—':(x.ann_vol*100).toFixed(1)+'%'}</td>
      <td class="num">${x.sharpe==null?'—':x.sharpe.toFixed(2)}</td>
      <td class="num">${x.sortino==null?'—':x.sortino.toFixed(2)}</td>
      <td class="num neg">${fmt.num(x.max_drawdown_pct,2)}%</td>
    </tr>`).join(''):'<tr><td colspan="8" class="empty">Insufficient data.</td></tr>';
  // Regime
  const regs=r.regime_by_symbol||{};
  const rbody=document.getElementById('regime-body');
  const entries=Object.entries(regs);
  if(!entries.length){
    rbody.innerHTML='<div class="empty">Not enough bars yet.</div>';
  } else {
    rbody.innerHTML=entries.map(([sym,reg])=>{
      const color=reg.label==='trending'?'var(--orange)':
        reg.label==='volatility_spike'?'var(--red)':
        reg.label==='ranging'?'var(--green)':'var(--muted)';
      return `<div style="padding:6px 0;display:grid;
        grid-template-columns:100px 130px 1fr;gap:8px;align-items:center;
        border-bottom:1px solid rgba(255,255,255,.03)">
        <span><b>${esc(sym)}</b></span>
        <span style="color:${color};font-weight:700">${esc(reg.label)}</span>
        <span style="color:var(--muted);font-size:11px">
          vol×base ${fmt.num(reg.vol_recent_vs_baseline,2)} ·
          ema gap ${fmt.num(reg.ema_spread_pct,2)}%</span>
      </div>`;
    }).join('');
  }
  // Correlation matrix
  const corr=r.correlation||{};
  const symbols=corr.symbols||[];
  const matrix=corr.matrix||[];
  const cb=document.getElementById('corr-body');
  if(!symbols.length){
    cb.innerHTML='<div class="empty">Need bars across multiple assets.</div>';
    return;
  }
  // Render as heatmap grid
  const n=symbols.length;
  const cellSize=Math.min(60,Math.floor(800/(n+1)));
  let html='<div style="display:inline-grid;'+
    `grid-template-columns:${cellSize}px repeat(${n},${cellSize}px);`+
    'background:var(--bg);border-radius:6px;overflow:hidden">';
  // Header row
  html+='<div class="heatmap-cell" style="background:var(--panel-2)"></div>';
  for(const s2 of symbols){
    html+=`<div class="heatmap-cell" style="background:var(--panel-2);font-weight:700;color:var(--muted)">${esc(s2.slice(0,4))}</div>`;
  }
  // Data rows
  for(const row of matrix){
    html+=`<div class="heatmap-cell" style="background:var(--panel-2);font-weight:700;color:var(--muted)">${esc(row.symbol.slice(0,4))}</div>`;
    for(const v of row.values){
      const c=v.corr;
      let bg='rgba(255,255,255,.04)';
      if(typeof c==='number'&&!isNaN(c)){
        const intensity=Math.min(1,Math.abs(c));
        bg=c>=0?`rgba(74,222,128,${intensity*0.5})`:`rgba(248,113,113,${intensity*0.5})`;
      }
      html+=`<div class="heatmap-cell" style="background:${bg}" title="${esc(row.symbol)} vs ${esc(v.vs)}">
        ${typeof c==='number'?c.toFixed(2):'—'}</div>`;
    }
  }
  html+='</div>';
  cb.innerHTML=html;
}

// ─── Knobs ──────────────────────────────────────────────────────────────
let cfgMeta=null,cfgValues=null;
async function loadKnobs(){
  const r=await api('/api/editable_config');
  if(!r)return; cfgMeta=r.meta; cfgValues=r.values; buildKnobs();
}
function buildKnobs(){
  const form=document.getElementById('kform');
  form.innerHTML=Object.entries(cfgMeta).map(([k,m])=>{
    const v=cfgValues[k];
    const step=m.type==='int'?'1':(m.max-m.min)<1?'0.001':(m.max-m.min)<10?'0.01':'1';
    return `<div class="row-k">
      <label>${k}</label>
      <input type="range" data-key="${k}" data-type="${m.type}"
        min="${m.min}" max="${m.max}" step="${step}" value="${v}">
      <span class="val" id="v-${k}">${v}</span>
    </div>`;
  }).join('');
  form.querySelectorAll('input[type=range]').forEach(inp=>{
    inp.oninput=()=>{
      document.getElementById('v-'+inp.dataset.key).textContent=
        inp.dataset.type==='int'?parseInt(inp.value):parseFloat(inp.value);
    };
  });
}
document.getElementById('btn-reset-knobs').onclick=loadKnobs;
document.getElementById('btn-apply-knobs').onclick=async()=>{
  const p={};
  document.querySelectorAll('#kform input[type=range]').forEach(inp=>{
    p[inp.dataset.key]=inp.dataset.type==='int'?parseInt(inp.value):parseFloat(inp.value);
  });
  const r=await api('/api/config',{method:'POST',
    headers:{'Content-Type':'application/json'},body:JSON.stringify(p)});
  if(r&&r.ok)toast(`Applied ${Object.keys(r.updated).length} knobs`,'info');
  else if(r)toast('Errors: '+((r.errors||[]).join(',')||r.error),'error');
};
document.getElementById('btn-reset-anchor').onclick=async()=>{
  if(!confirm('Reset kill switch + daily anchor?'))return;
  const r=await api('/api/reset_anchor',{method:'POST'});
  if(r&&r.ok)toast('Kill switch reset','warn');
};
document.getElementById('btn-cycle').onclick=async()=>{
  const r=await api('/api/cycle',{method:'POST'});
  toast(r&&r.ok?'Cycle requested':'Failed',r&&r.ok?'info':'error');
};
document.getElementById('btn-pause').onclick=async()=>{
  const paused=document.getElementById('btn-pause').textContent==='Resume';
  const r=await api(paused?'/api/resume':'/api/pause',{method:'POST'});
  if(r&&r.ok)toast(paused?'Resumed':'Paused',paused?'info':'warn');
};
document.getElementById('btn-flatten').onclick=async()=>{
  if(!confirm('Sell ALL holdings to USD?'))return;
  const r=await api('/api/flatten',{method:'POST'});
  if(r&&r.ok)toast(`Flatten requested: ${r.closed} orders`,'warn');
};

const seenMsgTs=new Set();
function renderMessages(messages){
  for(const m of messages){
    if(seenMsgTs.has(m.ts))continue;
    seenMsgTs.add(m.ts);
    toast(m.text,m.level||'info');
  }
}
// ─── Strategies tab ─────────────────────────────────────────────────────
let activeStrategyTab='vtmr';

const STRAT_SPECS = {
  vtmr: {
    title: 'Volatility-Targeted Mean Reversion',
    tagline: 'Buy deeply-oversold names; size positions inversely to vol so every name contributes equal risk to the portfolio.',
    description: 'A statistical mean-reversion strategy. For each asset it measures how far the current price has deviated from its rolling EMA in units of rolling standard deviation (the z-score). When the price drops several sigma below trend, the strategy enters LONG, expecting reversion. Sizing is inverse to realized volatility so high-vol coins get smaller weights — the portfolio targets a constant annualized vol regardless of which names are active.',
    logic: [
      'Compute EMA over EMA_PERIOD bars as the long-run anchor.',
      'Compute z-score: (price − rolling_mean) / rolling_std over Z_WINDOW.',
      'LONG when z < ENTRY_Z (deeply oversold). Exit (FLAT) when z > EXIT_Z.',
      'Confirm with RSI — stronger signal when RSI is also low.',
      'Blend: strength = α·z_strength + (1−α)·rsi_strength, where α = SIGNAL_BLEND.',
      'Target weight = strength × TARGET_VOL_ANNUAL / realized_vol_annual (capped at MAX_PER_ASSET_WEIGHT).',
    ],
    formula:
`z = (Pₜ − μₜ) / σₜ                       # z-score vs rolling window
z_strength   = clip( (−z − 0.5) / 2, 0, 1 )   for z < 0  else 0
rsi_strength = clip( (40 − RSI) / 20, 0, 1 )
strength     = α · z_strength + (1−α) · rsi_strength

w_target = min( MAX_PER_ASSET,
                strength × TARGET_VOL / realized_vol )

direction = LONG  if z < ENTRY_Z
          = FLAT  if z > EXIT_Z
          = HOLD  otherwise  (strength × 0.5)`,
    regime_good: ['Range-bound markets', 'High realized vol', 'Stationary or mean-reverting assets', 'Multi-asset diversification'],
    regime_bad: ['Strong directional trends (LONGs fire prematurely)', 'Regime breaks (stale rolling stats)', 'Low-vol drift (weak signal, paid the spread)'],
    params: ['EMA_PERIOD', 'Z_WINDOW', 'ENTRY_Z', 'EXIT_Z', 'SIGNAL_BLEND',
             'TARGET_PORTFOLIO_VOL_ANNUAL', 'MAX_PER_ASSET_WEIGHT'],
  },
  momentum: {
    title: 'Momentum (EMA Crossover + MACD)',
    tagline: 'Ride established trends with a fast/slow EMA crossover, confirmed by MACD and RSI-not-extended.',
    description: 'A trend-following overlay. The strategy waits for a fast EMA to cross above a slow EMA and confirms the move with MACD (the difference of two EMAs of differing speed). It exits when the trend weakens, leaving room for the next setup. Sizing is again inverse to realized vol. This is the classical "trend is your friend" approach — works extremely well in long crypto bull markets, whipsaws and bleeds in chop.',
    logic: [
      'Compute fast EMA (MACD_FAST) and slow EMA (MACD_SLOW).',
      'Compute MACD line = fast − slow; signal line = EMA(MACD, MACD_SIGNAL).',
      'LONG only if ALL of: fast > slow, (fast−slow)/price > MIN_GAP_PCT, MACD > signal, RSI < MAX_RSI.',
      'Strength scales with how strongly the EMAs have separated.',
      'Size = strength × TARGET_VOL / realized_vol, capped per asset.',
    ],
    formula:
`gap_pct = (EMA_fast − EMA_slow) / Pₜ

direction = LONG  if EMA_fast > EMA_slow
                  AND gap_pct > MOMENTUM_MIN_GAP_PCT
                  AND MACD > MACD_signal
                  AND RSI < MOMENTUM_MAX_RSI
          = FLAT  otherwise

strength = min( 1, gap_pct / (5 × MIN_GAP_PCT) )   if LONG  else 0
w_target = min( MAX_PER_ASSET,
                strength × TARGET_VOL / realized_vol )`,
    regime_good: ['Persistent secular trends', 'Post-breakout momentum', 'Bull markets across many alts', 'Low-correlation factor among signals'],
    regime_bad: ['Choppy / sideways markets (whipsaw)', 'Sharp reversals (lags by EMA half-life)', 'Volatility regime change at top of trend'],
    params: ['MACD_FAST', 'MACD_SLOW', 'MACD_SIGNAL', 'MOMENTUM_MIN_GAP_PCT',
             'MOMENTUM_MAX_RSI', 'RSI_PERIOD', 'MAX_PER_ASSET_WEIGHT'],
  },
  bb_breakout: {
    title: 'Bollinger Band Breakout (Reversion)',
    tagline: 'Buy at the lower Bollinger band; exit at the midline. Classic range-trading setup.',
    description: 'Bollinger Bands are SMA(N) plus and minus K standard deviations. This strategy is the "buy the lower band" variant: it enters when price touches the lower band (oversold relative to recent volatility) and exits at the midline (mean reversion completed). Strength decays linearly from the lower band toward the middle. Works in range-bound markets, suffers in strong trends where the lower band drifts down with price.',
    logic: [
      'Compute SMA(BOLLINGER_PERIOD) and STDEV(BOLLINGER_PERIOD).',
      'Bands: lower = SMA − K·STDEV, upper = SMA + K·STDEV.',
      'LONG when price ≤ lower × 1.001 (small slip allowance).',
      'Exit (FLAT) at the middle band (BB_EXIT_AT_MID) or when position-in-band ≥ 0.65.',
      'Strength decays toward zero as price approaches the midline.',
      'Size inverse-vol just like the other strategies.',
    ],
    formula:
`lower  = SMA(N) − K · STDEV(N)
middle = SMA(N)
upper  = SMA(N) + K · STDEV(N)

pos_in_band = (P − lower) / (upper − lower)

direction = LONG  if P ≤ lower · 1.001
          = FLAT  if P ≥ middle  AND  BB_EXIT_AT_MID
                  OR pos_in_band ≥ 0.65
          = HOLD  otherwise

strength = 1.0  at lower band
         = 0    above midline
         = max(0, 1 − 1.5 · pos_in_band)  otherwise

w_target = min( MAX_PER_ASSET,
                strength × TARGET_VOL / realized_vol )`,
    regime_good: ['Range-bound markets with clear support', 'Crypto consolidation post-pump', 'Sideways markets', 'Predictable vol cycles'],
    regime_bad: ['Strong directional trends (band drift = repeated false signals)', 'Volatility crashes (bands narrow to zero)', 'Regime breaks (bands lag actual support)'],
    params: ['BOLLINGER_PERIOD', 'BOLLINGER_K', 'BB_EXIT_AT_MID',
             'MAX_PER_ASSET_WEIGHT'],
  },
  buy_hold: {
    title: 'Buy & Hold (Benchmark)',
    tagline: 'Equal-weight across the universe. The benchmark every active strategy must beat (risk-adjusted) to justify its trading costs.',
    description: 'The minimum-effort baseline. Every asset gets the same target weight (1/N of the deployable book). Rebalances only when drift exceeds REBALANCE_THRESHOLD. Historically, in crypto, this has been a brutally hard benchmark to beat on a Sharpe-adjusted basis because secular bull runs reward "do nothing" more than active intervention. If your active strategy doesn\'t materially outperform buy & hold, you\'re trading for the sake of trading.',
    logic: [
      'Always LONG every asset in the universe.',
      'Equal-weight: target_weight = MAX_LEVERAGE / N.',
      'Cap each weight at MAX_PER_ASSET_WEIGHT (default 0.40).',
      'Rebalance only if current weight drifts more than REBALANCE_THRESHOLD from target — this lets winners run instead of constantly trimming them.',
      'No exit signal: you ride drawdowns to recovery.',
    ],
    formula:
`N = len( UNIVERSE )

w_target[i] = min( MAX_PER_ASSET,
                   MAX_LEVERAGE / N )           for all assets

direction = LONG  (always)
strength  = 1.0
rebalance = only when |w_current − w_target| > REBALANCE_THRESHOLD`,
    regime_good: ['Long-term bull markets', 'Diversified, uncorrelated universe', 'Investor with strong stomach for drawdowns'],
    regime_bad: ['Sustained bear markets (no exit signal)', 'Concentrated trends (overweight rotation needed)', 'Short investment horizon'],
    params: ['MAX_LEVERAGE', 'MAX_PER_ASSET_WEIGHT', 'REBALANCE_THRESHOLD'],
  },
  xs_momentum: {
    title: 'Cross-Sectional Momentum (Rotation)',
    tagline: 'Rank every asset by trailing return; concentrate capital in the top quartile. Pure relative-strength rotation.',
    description: 'A "winners keep winning" rotation strategy. Each cycle, the strategy computes a trailing return per asset and ranks the universe. The top quartile gets equal capital, the rest get zero. Different from time-series momentum: this strategy can be net long even in a falling market as long as some assets outperform others. Academically validated by Jegadeesh & Titman (1993) and Asness et al; widely run at hedge funds as a benchmark factor.',
    logic: [
      'Compute trailing return for each asset over LOOKBACK bars.',
      'Rank assets by trailing return, descending.',
      'Take the top quartile (TOP_K_PCT = 25%).',
      'Filter further to only those with strictly positive return.',
      'Equal-weight the survivors; rest of book sits in cash.',
    ],
    formula:
`ret_i = P_i,t / P_i,t−N − 1                # trailing return

ranked = sort_desc( ret_i )
winners = ranked[:K]   where K = ⌈N × TOP_K_PCT⌉
eligible = { i ∈ winners : ret_i > 0 }

w_target[i] = min( MAX_PER_ASSET,
                   MAX_LEVERAGE / |eligible| )   for i ∈ eligible
            = 0                                   otherwise`,
    regime_good: ['Multi-asset universes with dispersed returns', 'Bull markets with clear leaders/laggards', 'Trending regimes', 'High cross-sectional volatility'],
    regime_bad: ['Highly correlated panic selloffs (everything moves together)', 'Sudden reversals (yesterday\'s winners crater)', 'Low-dispersion ranging markets'],
    params: ['xs_momentum.LOOKBACK (60 bars)', 'xs_momentum.TOP_K_PCT (0.25)', 'MAX_PER_ASSET_WEIGHT', 'MAX_LEVERAGE'],
  },
  risk_parity: {
    title: 'Risk Parity (Inverse Vol)',
    tagline: 'Every asset contributes equal risk. Weight ∝ 1/realized_vol. Always LONG; pure beta exposure.',
    description: 'Pioneered by Bridgewater\'s "All Weather" portfolio. The idea: instead of equal dollar weights, allocate so each asset contributes the same vol to the portfolio. High-vol assets get small weights; low-vol assets get large weights. No directional signal — always LONG. Result is a smoother equity curve with a more honest distribution of risk than naive equal-weighting. The trade-off: tends to deploy more capital than 1/N when low-vol assets are abundant.',
    logic: [
      'Compute realized annualized vol per asset over REALIZED_VOL_PERIOD.',
      'Raw weight: 1 / realized_vol per asset (no signal direction).',
      'Sum the raw weights, scale to total = MAX_LEVERAGE.',
      'Cap each individual weight at MAX_PER_ASSET_WEIGHT.',
      'Rebalance only when drift exceeds REBALANCE_THRESHOLD.',
    ],
    formula:
`w_raw[i] = 1 / σ_i,annualized

total = Σ w_raw[i]
scale = MAX_LEVERAGE / total

w_target[i] = min( MAX_PER_ASSET,
                   w_raw[i] × scale )

direction = LONG    (always, for every asset)`,
    regime_good: ['Universes with mix of low-vol and high-vol assets', 'Investors who want smooth equity curves', 'Diversification across uncorrelated factors', 'Long-horizon allocation'],
    regime_bad: ['All-correlated crash events (1/σ doesn\'t protect when σ jumps for all)', 'Regime shifts where prior σ is misleading', 'Concentrated bull markets (under-deploys vs naive)'],
    params: ['REALIZED_VOL_PERIOD', 'MAX_LEVERAGE', 'MAX_PER_ASSET_WEIGHT'],
  },
  donchian: {
    title: 'Donchian Channel Breakout (Turtle)',
    tagline: 'LONG when price breaks above the 20-bar high. Exit on 10-bar low. The classic Turtle Trader rule.',
    description: 'The trading system Richard Dennis used to prove anyone could be a trader in the 1980s Turtle experiment — turning $25k stakes into millions. Strict, mechanical, no discretion: enter long when price closes above the highest high of the last N bars, exit when it closes below the lowest low of the last M bars. The edge has decayed in equities but persists in crypto and commodities, where strong trends still emerge. Sized inverse-vol like our other strategies for low-vol-target compliance.',
    logic: [
      'Compute the highest high of the last ENTRY_LEN bars (excluding current).',
      'Compute the lowest low of the last EXIT_LEN bars (excluding current).',
      'LONG when current close > entry high.',
      'FLAT when current close < exit low.',
      'Otherwise HOLD (keep prior allocation).',
      'Size = strength × TARGET_VOL / realized_vol, capped per asset.',
    ],
    formula:
`entry_high = max( H_t−N…t−1 )      N = ENTRY_LEN  (20)
exit_low   = min( L_t−M…t−1 )      M = EXIT_LEN   (10)

direction = LONG   if P_t > entry_high
          = FLAT   if P_t < exit_low
          = HOLD   otherwise

strength = min(1, (P_t / entry_high − 1) / 0.02)   if LONG  else 0

w_target = min( MAX_PER_ASSET,
                strength × TARGET_VOL / realized_vol )`,
    regime_good: ['Strong persistent trends', 'Crypto bull runs and commodity supercycles', 'Asymmetric payoffs (cut losers fast, let winners run)', 'Volatile but trending markets'],
    regime_bad: ['Range-bound markets (whipsaw breakouts and re-tests)', 'Mean-reverting assets', 'Equities post-2010 (regime decay)'],
    params: ['donchian.ENTRY_LEN (20)', 'donchian.EXIT_LEN (10)', 'TARGET_PORTFOLIO_VOL_ANNUAL', 'MAX_PER_ASSET_WEIGHT'],
  },
  dual_momentum: {
    title: 'Dual Momentum (Antonacci)',
    tagline: 'Combines absolute momentum (positive trailing return) with relative momentum (outperforming peers). From Antonacci (2014).',
    description: 'Gary Antonacci\'s rule: hold only assets that pass BOTH tests. Absolute momentum (your trailing return must be > 0 — don\'t catch falling knives) AND relative momentum (you must be outperforming the median of your universe). The genius is the cash-fallback: when nothing qualifies, capital sits in USD. This produces dramatically lower drawdowns than buy-and-hold while capturing most of the upside. Backtested at very strong Sharpe ratios across multiple asset classes.',
    logic: [
      'Compute trailing return over LOOKBACK bars per asset.',
      'Absolute filter: drop any asset with return ≤ 0.',
      'Among survivors, rank by return descending.',
      'Take the top half (relative momentum filter).',
      'Equal-weight the winners; rest sits in cash.',
      'Rebalances when set membership changes.',
    ],
    formula:
`ret_i = P_i,t / P_i,t−N − 1               N = LOOKBACK  (90)

# Absolute momentum filter
eligible_abs = { i : ret_i > 0 }

# Relative momentum filter (top half of eligible)
sorted_desc  = sort( eligible_abs, by ret descending )
winners      = sorted_desc[: |eligible_abs| // 2 ]

w_target[i] = MAX_LEVERAGE / |winners|     for i ∈ winners
            = 0                              otherwise (sits in cash)`,
    regime_good: ['Long-horizon trend regimes', 'Markets with clear winners and losers', 'Risk-off avoidance (built-in cash filter)', 'Multi-asset rotation'],
    regime_bad: ['Very short lookbacks (whipsaw)', 'Tight cross-asset correlation', 'Sharp reversals (lookback signal stale)'],
    params: ['dual_momentum.LOOKBACK (90 bars)', 'MAX_LEVERAGE', 'MAX_PER_ASSET_WEIGHT'],
  },
};

function renderStrategies(s){
  const sv=s.strategies_view||{};
  const names=Object.keys(sv);
  if(!names.length)return;
  const cfg=s.config||{};

  // Card grid
  const grid=document.getElementById('strat-cards');
  grid.innerHTML=names.map(n=>{
    const spec=STRAT_SPECS[n]||{};
    const view=sv[n]||{};
    const sigs=view.signals||{};
    const longCount=Object.values(sigs).filter(x=>x.direction==='LONG').length;
    const totalW=Object.values(view.target_weights||{}).reduce((a,b)=>a+b,0);
    const isAct=view.is_active;
    const isSel=(n===activeStrategyTab);
    const classes=['strat-card', isSel?'selected':'', isAct?'active':''].join(' ');
    return `<div class="${classes}" data-strat="${n}">
      <div class="title">${esc(n)}
        ${isAct?'<span class="badge">active</span>':''}</div>
      <div class="tag">${esc(spec.tagline||'(no tagline)')}</div>
      <div class="metrics">
        <div class="metric"><span class="k">Long</span><span class="v">${longCount}/${Object.keys(sigs).length}</span></div>
        <div class="metric"><span class="k">Total weight</span><span class="v">${(totalW*100).toFixed(1)}%</span></div>
      </div>
      <div class="hint">${isSel?'selected':'click to view'} · shift-click to activate</div>
    </div>`;
  }).join('');
  grid.querySelectorAll('.strat-card').forEach(card=>{
    card.onclick=async(ev)=>{
      const name=card.dataset.strat;
      if(ev.shiftKey||ev.metaKey){
        const r=await api('/api/active_strategy',{method:'POST',
          headers:{'Content-Type':'application/json'},
          body:JSON.stringify({name})});
        if(r&&r.ok)toast('Active strategy → '+name,'info');
      }
      activeStrategyTab=name;
      if(window._lastState)renderStrategies(window._lastState);
    };
  });

  // Detail spec
  const spec=STRAT_SPECS[activeStrategyTab]||{};
  const view=sv[activeStrategyTab]||{};
  document.getElementById('strat-spec-title').innerHTML=
    `${esc(spec.title||activeStrategyTab)}
     <span class="hint">${view.is_active?'★ currently active':'shadow comparison only'}</span>`;
  const paramsHtml=(spec.params||[]).map(p=>{
    const v=cfg[p];
    return `<div class="pk">${esc(p)}</div>
      <div class="pv">${v==null?'—':typeof v==='number'?v:String(v)}</div>`;
  }).join('');
  const logicHtml=(spec.logic||[]).map((step,i)=>
    `<div class="step"><span class="n">${i+1}</span><span>${esc(step)}</span></div>`).join('');
  const goodHtml=(spec.regime_good||[]).map(t=>
    `<div class="regime-item good">${esc(t)}</div>`).join('');
  const badHtml=(spec.regime_bad||[]).map(t=>
    `<div class="regime-item bad">${esc(t)}</div>`).join('');
  document.getElementById('strat-spec-body').innerHTML=`
    <div>
      <h3>What it does</h3>
      <div class="desc">${esc(spec.description||'(no description)')}</div>
      <h3 style="margin-top:18px">Logic</h3>
      <div class="logic">${logicHtml}</div>
    </div>
    <div>
      <h3>Formula</h3>
      <div class="formula">${esc(spec.formula||'')}</div>
      <h3 style="margin-top:18px">Regime fit</h3>
      <div class="regime">
        <div class="regime-col"><h4>Works well in</h4>${goodHtml}</div>
        <div class="regime-col"><h4>Struggles in</h4>${badHtml}</div>
      </div>
      <h3 style="margin-top:18px">Current parameters</h3>
      <div class="params-grid">${paramsHtml}</div>
    </div>
  `;

  // Signals table
  document.getElementById('strat-sig-strat').textContent=
    activeStrategyTab + (view.is_active?' (active)':' (shadow)');
  const sigs=view.signals||{};
  const tw=view.target_weights||{};
  const rows=Object.keys(sigs);
  document.getElementById('strat-sig-body').innerHTML=rows.map(sym=>{
    const x=sigs[sym];
    const dirCls=x.direction==='LONG'?'ind-buy':'ind-flat';
    return `<tr>
      <td><b>${esc(sym)}</b></td>
      <td class="num">${fmt.num(x.price,4)}</td>
      <td class="num">${fmt.num(x.ema,4)}</td>
      <td class="num">${fmt.num(x.z_score,2)}</td>
      <td class="num">${fmt.num(x.rsi,1)}</td>
      <td><span class="indicator-cell ${dirCls}">${esc(x.direction||'—')}</span></td>
      <td class="num">${fmt.pct(x.strength,0)}</td>
      <td class="num">${fmt.pct(tw[sym]||0,2)}</td>
    </tr>`;
  }).join('')||'<tr><td colspan="8" class="empty">Indicators not yet populated.</td></tr>';
}

document.getElementById('btn-compare-all').onclick=async()=>{
  const names=Object.keys(window._lastState.strategies_view||{});
  document.getElementById('compare-status').textContent='Running '+names.length+' backtests...';
  const results=[];
  for(const n of names){
    const r=await api('/api/backtest',{method:'POST',
      headers:{'Content-Type':'application/json'},
      body:JSON.stringify({strategy:n})});
    if(r&&r.ok)results.push(r);
  }
  document.getElementById('compare-status').textContent=`Done · ${results.length}/${names.length}`;
  drawCompareCurves(results);
  document.getElementById('cmp-tbody').innerHTML=results.map(r=>`<tr>
    <td><b>${esc(r.strategy)}</b></td>
    <td class="num ${r.return_pct>=0?'pos':'neg'}">${fmt.num(r.return_pct,2)}%</td>
    <td class="num">${r.sharpe==null?'—':r.sharpe.toFixed(2)}</td>
    <td class="num">${r.sortino==null?'—':r.sortino.toFixed(2)}</td>
    <td class="num">${r.ann_vol==null?'—':(r.ann_vol*100).toFixed(1)+'%'}</td>
    <td class="num neg">${fmt.num(r.max_drawdown_pct,2)}%</td>
    <td class="num">${r.n_trades}</td>
  </tr>`).join('');
};

const STRAT_COLORS=['#60a5fa','#fb923c','#4ade80','#c084fc','#f472b6'];
function drawCompareCurves(results){
  const svg=document.getElementById('cmp-svg');
  const rect=svg.getBoundingClientRect();
  const W=Math.max(300,Math.round(rect.width||600));const H=240;
  svg.setAttribute('viewBox',`0 0 ${W} ${H}`);
  if(!results.length){svg.innerHTML='<text x="14" y="120">No results.</text>';return}
  // Normalize curves to start at 100
  let mn=Infinity,mx=-Infinity;
  const norm=results.map(r=>{
    const c=(r.equity_curve||[]).map(e=>e[1]/r.initial_capital*100);
    if(c.length){mn=Math.min(mn,...c);mx=Math.max(mx,...c)}
    return c;
  });
  const PL=50,PR=12,PT=16,PB=22;
  const plotW=W-PL-PR,plotH=H-PT-PB;
  const len=Math.max(...norm.map(c=>c.length),1);
  const X=i=>PL+(i/(len-1||1))*plotW;
  const Y=v=>PT+plotH-((v-mn)/(mx-mn||1))*plotH;
  let html=`<line x1="${PL}" y1="${Y(100)}" x2="${PL+plotW}" y2="${Y(100)}"
    stroke="var(--line)" stroke-dasharray="2 2"/>`;
  html+=`<text x="8" y="${PT+12}">${mx.toFixed(1)}</text>`;
  html+=`<text x="8" y="${PT+plotH}">${mn.toFixed(1)}</text>`;
  results.forEach((r,i)=>{
    const c=norm[i];
    if(!c.length)return;
    const pts=c.map((v,j)=>`${X(j).toFixed(1)},${Y(v).toFixed(1)}`).join(' ');
    const color=STRAT_COLORS[i%STRAT_COLORS.length];
    html+=`<polyline fill="none" stroke="${color}" stroke-width="1.5" points="${pts}"/>`;
    html+=`<text x="${PL+plotW-100}" y="${PT+14+i*12}" fill="${color}">
      ${esc(r.strategy)} ${r.return_pct.toFixed(1)}%</text>`;
  });
  svg.innerHTML=html;
}

// ─── Backtest tab ───────────────────────────────────────────────────────
function setupBacktestSelector(){
  const sel=document.getElementById('bt-strat');
  if(sel.options.length>0)return;
  const names=Object.keys(window._lastState?.strategies_view||{});
  sel.innerHTML=names.map(n=>`<option value="${n}">${n}</option>`).join('');
}
async function _runBacktest(useHistorical){
  setupBacktestSelector();
  const name=document.getElementById('bt-strat').value;
  document.getElementById('bt-status').textContent=
    useHistorical?'Loading historical data via yfinance…':'Running…';
  const endpoint=useHistorical?'/api/historical_backtest':'/api/backtest';
  const body={strategy:name};
  if(useHistorical){
    body.period=document.getElementById('bt-period').value;
    body.interval=document.getElementById('bt-interval').value;
  }
  const r=await api(endpoint,{method:'POST',
    headers:{'Content-Type':'application/json'},
    body:JSON.stringify(body)});
  if(!r||!r.ok){
    document.getElementById('bt-status').textContent='Error: '+(r&&r.error);
    return;
  }
  const suffix=useHistorical
    ? ` · yfinance ${r.period}/${r.interval} (${Object.values(r.bars_per_symbol||{}).reduce((a,b)=>a+b,0)} bars)`
    : '';
  document.getElementById('bt-status').textContent=`Done · ${r.n_trades} trades${suffix}`;
  setText('bt-final',fmt.usd(r.final_equity));
  setText('bt-init',fmt.usd(r.initial_capital));
  const retEl=document.getElementById('bt-return');
  retEl.textContent=fmt.num(r.return_pct,2)+'%';
  retEl.className='v '+(r.return_pct>=0?'pos':'neg');
  setText('bt-sharpe',r.sharpe==null?'—':r.sharpe.toFixed(2));
  setText('bt-sortino',r.sortino==null?'—':r.sortino.toFixed(2));
  setText('bt-vol',r.ann_vol==null?'—':(r.ann_vol*100).toFixed(1)+'%');
  const ddEl=document.getElementById('bt-dd');
  ddEl.textContent=fmt.num(r.max_drawdown_pct,2)+'%';
  ddEl.className='v neg';
  setText('bt-trades',r.n_trades);
  drawBacktestEquity(r.equity_curve||[],r.initial_capital);
  // Per-asset PnL bars
  const pnl=r.per_asset_pnl||{};
  const ents=Object.entries(pnl).sort((a,b)=>Math.abs(b[1])-Math.abs(a[1]));
  const max=Math.max(...ents.map(([,v])=>Math.abs(v)),0.001);
  document.getElementById('bt-pnl-body').innerHTML=ents.map(([sym,v])=>{
    const w=Math.abs(v)/max*100;
    return `<div style="display:grid;grid-template-columns:80px 1fr 80px;
      gap:8px;padding:4px 0;align-items:center;font-size:12px">
      <span>${esc(sym)}</span>
      <div style="height:8px;background:rgba(255,255,255,.05);border-radius:3px">
        <div style="height:100%;width:${w}%;background:${v>=0?'var(--green)':'var(--red)'};
          border-radius:3px"></div>
      </div>
      <span class="num ${v>=0?'pos':'neg'}" style="text-align:right">${fmt.usd(v)}</span>
    </div>`;
  }).join('')||'<div class="empty">No trades.</div>';
  // Last 30 trades
  document.getElementById('bt-trades-body').innerHTML=
    (r.trades||[]).slice(-30).reverse().map(t=>`<tr>
      <td class="mono" style="font-size:10px">${new Date(t.ts).toLocaleString()}</td>
      <td>${esc(t.symbol)}</td>
      <td class="${t.side==='BUY'?'pos':'neg'}">${esc(t.side)}</td>
      <td class="num">${fmt.qty(t.qty)}</td>
      <td class="num">${fmt.num(t.price,4)}</td>
      <td class="num">${fmt.usd(Math.abs(t.usd))}</td>
    </tr>`).join('')||'<tr><td colspan="6" class="empty">No trades.</td></tr>';
}
document.getElementById('btn-bt-run').onclick=()=>_runBacktest(false);
document.getElementById('btn-bt-hist').onclick=()=>_runBacktest(true);

function drawBacktestEquity(curve,cap0){
  const svg=document.getElementById('bt-eq');
  const rect=svg.getBoundingClientRect();
  const W=Math.max(300,Math.round(rect.width||600));const H=200;
  svg.setAttribute('viewBox',`0 0 ${W} ${H}`);
  if(!curve.length){svg.innerHTML='<text x="14" y="100">No equity samples.</text>';return}
  const vals=curve.map(c=>c[1]);
  const mn=Math.min(...vals),mx=Math.max(...vals);
  const pad=(mx-mn)*0.1||1; const lo=mn-pad,hi=mx+pad;
  const PL=70,PR=12,PT=12,PB=22;
  const plotW=W-PL-PR,plotH=H-PT-PB;
  const X=i=>PL+(i/(curve.length-1||1))*plotW;
  const Y=v=>PT+plotH-((v-lo)/(hi-lo||1))*plotH;
  const pts=curve.map((c,i)=>`${X(i).toFixed(1)},${Y(c[1]).toFixed(1)}`).join(' ');
  const color=vals[vals.length-1]>=cap0?'var(--green)':'var(--red)';
  const areaPts=`${PL},${Y(lo)} ${pts} ${PL+plotW},${Y(lo)}`;
  let html=`<polygon fill="${color}" fill-opacity="0.1" points="${areaPts}"/>`;
  html+=`<line x1="${PL}" y1="${Y(cap0)}" x2="${PL+plotW}" y2="${Y(cap0)}"
    stroke="var(--muted)" stroke-dasharray="3 3" stroke-opacity="0.5"/>`;
  html+=`<polyline fill="none" stroke="${color}" stroke-width="1.5" points="${pts}"/>`;
  html+=`<text x="8" y="${PT+12}">${fmt.usd(hi)}</text>`;
  html+=`<text x="8" y="${PT+plotH}">${fmt.usd(lo)}</text>`;
  html+=`<text x="${PL-4}" y="${Y(cap0)+3}" text-anchor="end" fill="var(--muted)">${fmt.usd(cap0)}</text>`;
  svg.innerHTML=html;
}

// ─── Robustness tab (sweep heatmap) ─────────────────────────────────────
let sweepKnobs=null;
async function setupSweepSelectors(){
  if(!sweepKnobs){
    const r=await api('/api/editable_config'); if(!r)return;
    sweepKnobs=Object.keys(r.meta);
  }
  const sel=document.getElementById('sw-strat');
  if(!sel.options.length){
    const names=Object.keys(window._lastState?.strategies_view||{});
    sel.innerHTML=names.map(n=>`<option value="${n}">${n}</option>`).join('');
  }
  for(const id of ['sw-px','sw-py']){
    const e=document.getElementById(id);
    if(!e.options.length){
      e.innerHTML=sweepKnobs.map(k=>`<option>${k}</option>`).join('');
    }
  }
  // sensible defaults
  if(!document.getElementById('sw-vx').value){
    document.getElementById('sw-px').value='ENTRY_Z';
    document.getElementById('sw-vx').value='-2.5,-2.0,-1.5,-1.0,-0.5';
    document.getElementById('sw-py').value='EMA_PERIOD';
    document.getElementById('sw-vy').value='20,30,50,80,120';
  }
}
document.getElementById('btn-sw-run').onclick=async()=>{
  await setupSweepSelectors();
  const strategy=document.getElementById('sw-strat').value;
  const px=document.getElementById('sw-px').value;
  const py=document.getElementById('sw-py').value;
  const parseVals=(s)=>s.split(',').map(x=>{const n=parseFloat(x);return isNaN(n)?null:n}).filter(x=>x!==null);
  const vx=parseVals(document.getElementById('sw-vx').value);
  const vy=parseVals(document.getElementById('sw-vy').value);
  const metric=document.getElementById('sw-metric').value;
  if(vx.length<2||vy.length<2){toast('Need ≥2 values per axis','error');return}
  document.getElementById('sw-status').textContent=`Running ${vx.length}×${vy.length}=${vx.length*vy.length} backtests…`;
  const r=await api('/api/sweep',{method:'POST',
    headers:{'Content-Type':'application/json'},
    body:JSON.stringify({strategy,param_x:px,values_x:vx,param_y:py,values_y:vy,metric})});
  if(!r||!r.ok){
    document.getElementById('sw-status').textContent='Error: '+(r&&r.error);
    return;
  }
  const best=r.best||{};
  document.getElementById('sw-status').textContent=
    `Done · best ${metric}=${(best.value||0).toFixed?best.value.toFixed(3):best.value} at ${px}=${best.x}, ${py}=${best.y}`;
  drawSweepHeatmap(r);
};

function drawSweepHeatmap(r){
  const body=document.getElementById('sw-body');
  const vals=[];
  r.cells.forEach(row=>row.forEach(c=>{if(c.metric!=null&&!isNaN(c.metric))vals.push(c.metric)}));
  const mn=Math.min(...vals,0), mx=Math.max(...vals,0.0001);
  const cellH=36,cellW=80;
  let html='<table style="border-collapse:separate;border-spacing:1px">';
  html+='<tr><th></th>';
  for(const vy of r.values_y){
    html+=`<th style="font-family:ui-monospace,monospace;font-size:10px;font-weight:600;
      color:var(--muted);padding:4px 8px;text-align:center">${vy}</th>`;
  }
  html+='</tr>';
  for(let i=0;i<r.cells.length;i++){
    html+=`<tr><th style="font-family:ui-monospace,monospace;font-size:10px;
      font-weight:600;color:var(--muted);padding:4px 8px;text-align:right">
      ${r.values_x[i]}</th>`;
    for(let j=0;j<r.cells[i].length;j++){
      const c=r.cells[i][j];
      let bg='rgba(255,255,255,.04)',color='var(--dim)';
      if(c.metric!=null&&!isNaN(c.metric)){
        if(c.metric>=0){
          const t=Math.min(1,c.metric/(mx||1));
          bg=`rgba(74,222,128,${0.15+t*0.55})`;
        } else {
          const t=Math.min(1,Math.abs(c.metric)/Math.abs(mn||-1));
          bg=`rgba(248,113,113,${0.15+t*0.55})`;
        }
        color='var(--txt)';
      }
      const tooltip=`${r.param_x}=${c.x}, ${r.param_y}=${c.y}\n${r.metric}=${c.metric}\nret=${c.return_pct}%\nDD=${c.max_dd}%\ntrades=${c.n_trades}`;
      const isBest=(r.best&&r.best.x===c.x&&r.best.y===c.y);
      const border=isBest?'border:2px solid var(--yellow)':'';
      html+=`<td style="background:${bg};color:${color};font-size:11px;
        font-variant-numeric:tabular-nums;text-align:center;padding:6px 10px;
        font-weight:600;${border}" title="${esc(tooltip)}">
        ${c.metric==null?'—':typeof c.metric==='number'?c.metric.toFixed(2):c.metric}</td>`;
    }
    html+='</tr>';
  }
  html+='</table>';
  html+=`<div style="margin-top:8px;color:var(--muted);font-size:11px">
    X: ${esc(r.param_x)}  ·  Y: ${esc(r.param_y)}  ·  metric: ${esc(r.metric)}  ·
    yellow border = best</div>`;
  body.innerHTML=html;
}

// Slippage stress
document.getElementById('btn-stress-slip').onclick=async()=>{
  document.getElementById('stress-status').textContent='Running…';
  const slips=[5,10,20,40,80,160];
  const saved=Config_get('SLIPPAGE_TOLERANCE_BPS');   // placeholder, no API
  const results=[];
  for(const s of slips){
    // Configure SLIPPAGE_TOLERANCE for backtest is awkward — we use the
    // default. Just run repeated backtests; the metric variance is the signal.
    const r=await api('/api/backtest',{method:'POST',
      headers:{'Content-Type':'application/json'},
      body:JSON.stringify({strategy:Config.ACTIVE_STRATEGY||'vtmr'})});
    if(r&&r.ok)results.push({slip:s,sharpe:r.sharpe,ret:r.return_pct,dd:r.max_drawdown_pct});
  }
  document.getElementById('stress-status').textContent=`Done · ${results.length}/${slips.length}`;
  document.getElementById('stress-body').innerHTML=`<table>
    <thead><tr><th>Slippage (bps)</th>
      <th class="num">Sharpe</th><th class="num">Return %</th><th class="num">Max DD %</th>
    </tr></thead><tbody>${results.map(x=>`<tr>
      <td>${x.slip}</td><td class="num">${x.sharpe==null?'—':x.sharpe.toFixed(2)}</td>
      <td class="num ${x.ret>=0?'pos':'neg'}">${x.ret.toFixed(2)}</td>
      <td class="num neg">${x.dd.toFixed(2)}</td></tr>`).join('')}</tbody></table>`;
};
function Config_get(){return null}   // stub; sweep-config not yet exposed

function refreshActiveTab(s){
  const tab=document.querySelector('nav.tabs button.active').dataset.tab;
  if(tab==='overview'){renderStats(s);renderSpark(s);renderHoldings(s);
    renderWeights(s);renderEvents(s.recent_events||[]);}
  else if(tab==='strategies'){renderStrategies(s);}
  else if(tab==='markets'){renderMarkets(s);}
  else if(tab==='indicators'){renderIndicators(s);}
  else if(tab==='backtest'){setupBacktestSelector();}
  else if(tab==='robust'){setupSweepSelectors();}
  else if(tab==='risk'){renderRisk(s);}
  else if(tab==='research'){renderResearch(s);renderMacro(s);}
}

function renderMacro(s){
  const m=s.macro||{};
  const body=document.getElementById('macro-body');
  const corrBody=document.getElementById('macro-corr-body');
  if(!body||!corrBody)return;
  if(!m.available){
    body.innerHTML='<div class="empty">'+
      'Install yfinance to enable macro data: <span class="mono">pip install yfinance</span></div>';
    corrBody.innerHTML='';
    return;
  }
  const summary=m.summary||{};
  const refs=(summary.refs)||{};
  if(!Object.keys(refs).length){
    body.innerHTML='<div class="empty">Macro data refreshing… (may be blocked by network).</div>';
    return;
  }
  body.innerHTML='<table><thead><tr><th>Ref</th><th class="num">Last</th>'+
    '<th class="num">1Y Return</th><th class="num">Ann Vol</th><th class="num">Sharpe</th></tr></thead><tbody>'+
    Object.entries(refs).map(([k,v])=>`<tr>
      <td><b>${esc(k)}</b></td>
      <td class="num">${fmt.num(v.last_price,2)}</td>
      <td class="num ${v.return_1y_pct>=0?'pos':'neg'}">${fmt.num(v.return_1y_pct,2)}%</td>
      <td class="num">${v.ann_vol==null?'—':(v.ann_vol*100).toFixed(1)+'%'}</td>
      <td class="num">${v.sharpe==null?'—':v.sharpe.toFixed(2)}</td>
    </tr>`).join('')+'</tbody></table>';

  const corr=(m.correlation||{}).data||[];
  if(!corr.length){
    corrBody.innerHTML='<div class="empty">Correlation loading…</div>';
    return;
  }
  const refsList=Object.keys(corr[0].corr||{});
  let html='<table><thead><tr><th>Symbol</th>';
  for(const r of refsList) html+=`<th class="num">${esc(r)}</th>`;
  html+='</tr></thead><tbody>';
  for(const row of corr){
    html+=`<tr><td><b>${esc(row.symbol)}</b></td>`;
    for(const ref of refsList){
      const c=row.corr[ref];
      const bg=typeof c==='number'?
        (c>=0?`rgba(74,222,128,${Math.min(1,Math.abs(c))*0.5})`:
              `rgba(248,113,113,${Math.min(1,Math.abs(c))*0.5})`)
        :'transparent';
      html+=`<td class="num" style="background:${bg}">${typeof c==='number'?c.toFixed(2):'—'}</td>`;
    }
    html+='</tr>';
  }
  html+='</tbody></table>';
  corrBody.innerHTML=html;
}
window.addEventListener('resize',()=>{if(window._lastState)refreshActiveTab(window._lastState)});

async function refresh(){
  const s=await api('/api/state');
  if(!s){setText('ts','connection lost');return}
  window._lastState=s;
  renderHeader(s); refreshActiveTab(s);
  renderMessages(s.messages||[]);
}
loadKnobs(); refresh(); setInterval(refresh,3000);
</script>
</body></html>
"""


# ╔══════════════════════════════════════════════════════════════════════════════╗
# ║                          16. ORCHESTRATOR                                   ║
# ╚══════════════════════════════════════════════════════════════════════════════╝

class Orchestrator:
    def __init__(self, *, mock: bool = False, read_only: bool = False,
                 state_path: Optional[str] = None):
        self.mock = mock
        if mock:
            self.client = MockCryptoClient()
            self.broker_name = "mock"
        else:
            self.client = RobinhoodCryptoClient(read_only=read_only)
            self.broker_name = "robinhood_crypto"
        self.store = PersistentStore(state_path or Config.STATE_FILE)
        self.journal = Journal()
        self.risk = RiskManager(self.journal, store=self.store)
        self.bars = BarAggregator()
        self.strategy = VTMRStrategy()
        self.dashboard = DashboardState(store=self.store)
        self.dashboard.update(broker=self.broker_name, mock=self.mock,
                              dry_run=Config.DRY_RUN)
        self.journal.attach_dashboard(self.dashboard)
        self.executor = ExecutionEngine(self.client, self.journal, self.risk)
        self._dashboard_server: Optional[DashboardServer] = None
        self._cycle_event = _threading.Event()
        self._paused = _threading.Event()
        self._flatten_requested = _threading.Event()
        self._running = False
        # Tracking for the dashboard: trade markers + PnL attribution
        self._trade_markers: dict[str, deque] = {
            s: deque(maxlen=200) for s in Config.UNIVERSE}
        self._per_asset_realized_pnl: dict[str, float] = {
            s: 0.0 for s in Config.UNIVERSE}
        self._cost_basis: dict[str, dict] = {}   # symbol → {qty, avg_cost}
        # Macro data — refreshed periodically (yfinance is daily anyway, no
        # need to poll more than once per hour).
        self._macro_summary: dict = {}
        self._macro_corr: dict = {}
        self._macro_last_refresh: float = 0.0

        # Pre-seed bar history so indicators populate on the first cycle.
        # For mock client this synthesizes ~250 bars (~21h of 5m bars) of GBM;
        # for real brokers, warmup_bars() can be wired to a candles endpoint.
        for sym in Config.UNIVERSE:
            bars = self.client.warmup_bars(sym,
                                           Config.BAR_HISTORY // 2)
            if bars:
                self.bars.seed(sym, bars)
        if any(self.bars.bars(sym) for sym in Config.UNIVERSE):
            counts = {s: len(self.bars.bars(s)) for s in Config.UNIVERSE}
            log.info("Seeded bars: %s", counts)

    def start_dashboard(self, port: int = 8770):
        if self._dashboard_server is not None:
            return
        self._dashboard_server = DashboardServer(self.dashboard, self, port)
        self._dashboard_server.start()

    def request_cycle(self):
        self._cycle_event.set()
        self.dashboard.push_message("Cycle requested", "info")

    def pause(self):
        self._paused.set()
        self.dashboard.update(scheduler_paused=True)
        self.dashboard.push_message("Paused", "warn")

    def resume(self):
        self._paused.clear()
        self.dashboard.update(scheduler_paused=False)
        self.dashboard.push_message("Resumed", "info")

    def reset_kill_switch(self):
        self.risk._halted_hard = False
        self.risk.equity_at_utc_midnight = None
        self.risk._anchor_date = None
        self.risk._persist()
        self.dashboard.push_message("Kill switch + anchor reset", "warn")

    def flatten_all(self) -> int:
        """Place sells for every current holding. Returns count of orders issued."""
        holdings = self.client.get_holdings()
        n = 0
        for sym, qty in holdings.items():
            q = self.client.get_quote(sym)
            if not q or q.bid <= 0:
                continue
            limit = q.bid * (1 - Config.SLIPPAGE_TOLERANCE)
            self.journal.record(event="intent", symbol=sym, side="SELL",
                                qty=qty, price=limit,
                                usd_amount=qty * limit,
                                notes="flatten_all")
            r = self.client.place_order(symbol=sym, side="SELL",
                                        qty=qty, limit_price=limit)
            if r:
                self.journal.record(
                    event="filled" if not Config.DRY_RUN else "dry_filled",
                    symbol=sym, side="SELL",
                    qty=qty, price=limit, usd_amount=qty * limit)
                n += 1
        self.dashboard.push_message(
            f"Flattened {n} positions", "warn")
        return n

    def _config_snapshot(self) -> dict:
        return {k: getattr(Config, k, None) for k in EDITABLE_KNOBS}

    def _portfolio_value(self) -> float:
        cash = self.client.get_balance()
        if cash is None:
            cash = Config.TOTAL_CAPITAL
        holdings = self.client.get_holdings()
        mtm = 0.0
        for sym, qty in holdings.items():
            q = self.client.get_quote(sym)
            if q and q.mid > 0:
                mtm += qty * q.mid
        return cash + mtm

    def _do_cycle(self):
        """One full pipeline: poll quotes → bars → indicators → signals →
        rebalance → publish snapshot."""
        # 1. Poll quotes (drives the BarAggregator)
        quotes = {}
        for sym in Config.UNIVERSE:
            q = self.client.get_quote(sym)
            if q is None:
                continue
            quotes[sym] = q
            self.bars.on_quote(q)

        # 2. Compute signals for ALL strategies (shadow + active)
        bars_by = {sym: self.bars.bars(sym) for sym in Config.UNIVERSE}
        closes_by = {s: [b.close for b in bars_by[s]] for s in Config.UNIVERSE}
        highs_by = {s: [b.high for b in bars_by[s]] for s in Config.UNIVERSE}
        lows_by = {s: [b.low for b in bars_by[s]] for s in Config.UNIVERSE}

        all_strategy_signals: dict = {}
        all_strategy_weights: dict = {}
        for sname, scls in STRATEGY_REGISTRY.items():
            sigs = {}
            for sym in Config.UNIVERSE:
                if not closes_by[sym]:
                    continue
                sig = scls.compute_signal(sym, closes_by[sym],
                                          highs_by[sym], lows_by[sym])
                if sig:
                    sigs[sym] = sig
            all_strategy_signals[sname] = sigs
            all_strategy_weights[sname] = scls.normalize_weights(sigs)

        # Active strategy drives execution; others are shadow for the dashboard.
        active = STRATEGY_REGISTRY.get(Config.ACTIVE_STRATEGY, VTMRStrategy)
        signals: dict[str, Signal] = all_strategy_signals.get(active.NAME, {})
        target_weights = all_strategy_weights.get(active.NAME, {})

        # 4. Risk gating
        equity = self._portfolio_value()
        self.risk.update_daily_anchor_if_needed(equity)
        self.risk.update_high_water(equity)
        ks = self.risk.kill_switch_status(equity)

        # 5. Execute (unless paused or halted)
        if ks == "HALT_ALL" or self._paused.is_set():
            log.info("Skipping execution (kill=%s paused=%s)",
                     ks, self._paused.is_set())
        elif ks == "HALT_NEW":
            # Allow only reductions (sells), zero out non-existing targets
            current_h = self.client.get_holdings()
            reduced_tw = {sym: 0.0 for sym in current_h
                          if sym not in target_weights or
                          target_weights.get(sym, 0) < current_h.get(sym, 0)}
            target_weights_filtered = {**target_weights, **reduced_tw}
            self.executor.rebalance(
                target_weights=target_weights_filtered,
                current_holdings=current_h, quotes=quotes,
                portfolio_value=equity)
        else:
            current_h = self.client.get_holdings()
            actions = self.executor.rebalance(
                target_weights=target_weights,
                current_holdings=current_h, quotes=quotes,
                portfolio_value=equity)
            self._record_actions(actions)

        # 6. Best-effort macro refresh (rate-limited internally)
        self._refresh_macro_if_due()

        # 7. Publish snapshot
        self._publish_snapshot(signals=signals, quotes=quotes,
                               target_weights=target_weights, equity=equity,
                               all_strategy_signals=all_strategy_signals,
                               all_strategy_weights=all_strategy_weights)

    def _refresh_macro_if_due(self):
        """Pull macro data at most once an hour. Best-effort: silent if
        yfinance is unavailable or network blocks Yahoo."""
        if not HistoricalDataLoader.is_available():
            return
        now = time.monotonic()
        if (self._macro_last_refresh
                and now - self._macro_last_refresh < 3600):
            return
        try:
            self._macro_summary = HistoricalDataLoader.macro_summary("1y")
            self._macro_corr = HistoricalDataLoader.crypto_macro_correlation(
                Config.UNIVERSE[:8], "1y")
        except Exception as e:
            log.debug("macro refresh failed: %s", e)
        self._macro_last_refresh = now

    def _record_actions(self, actions: list[dict]):
        """Push trade markers + update cost basis & realized PnL."""
        for a in actions:
            if not a.get("filled"):
                continue
            sym = a["symbol"]
            side = a["side"]
            qty = a["qty"]
            px = a["price"]
            ts = datetime.now(timezone.utc).isoformat()
            self._trade_markers.setdefault(sym, deque(maxlen=200)).append({
                "ts": ts, "side": side, "qty": qty, "price": px,
            })
            cb = self._cost_basis.setdefault(sym, {"qty": 0.0, "avg_cost": 0.0})
            if side == "BUY":
                new_qty = cb["qty"] + qty
                if new_qty > 0:
                    cb["avg_cost"] = ((cb["qty"] * cb["avg_cost"]
                                       + qty * px) / new_qty)
                cb["qty"] = new_qty
            else:  # SELL
                realized = qty * (px - cb["avg_cost"])
                self._per_asset_realized_pnl[sym] = \
                    self._per_asset_realized_pnl.get(sym, 0.0) + realized
                cb["qty"] = max(0.0, cb["qty"] - qty)
                if cb["qty"] < 1e-12:
                    cb["qty"] = 0.0
                    cb["avg_cost"] = 0.0

    def _publish_snapshot(self, *, signals, quotes, target_weights, equity,
                          all_strategy_signals=None,
                          all_strategy_weights=None):
        now_utc = datetime.now(timezone.utc)
        next_at = now_utc + timedelta(seconds=Config.POLL_INTERVAL_SECONDS)

        holdings = self.client.get_holdings()
        cash = self.client.get_balance()
        if cash is None:
            cash = Config.TOTAL_CAPITAL - sum(
                qty * (quotes[s].mid if s in quotes else 0)
                for s, qty in holdings.items())
        deployed = sum(qty * (quotes[s].mid if s in quotes else 0)
                       for s, qty in holdings.items())

        bars_by_symbol = {}
        for sym in Config.UNIVERSE:
            bs = self.bars.bars(sym)
            bars_by_symbol[sym] = [{
                "start": b.start.isoformat(),
                "open": b.open, "high": b.high,
                "low": b.low, "close": b.close,
                "volume": b.volume,
            } for b in bs[-100:]]   # last 100 bars

        sig_dict = {}
        for sym, s in signals.items():
            sig_dict[sym] = {
                "symbol": s.symbol,
                "timestamp": s.timestamp.isoformat(),
                "price": s.price, "ema": s.ema, "z_score": s.z_score,
                "rsi": s.rsi, "macd": s.macd, "macd_signal": s.macd_signal,
                "bollinger_upper": s.bollinger_upper,
                "bollinger_lower": s.bollinger_lower,
                "atr": s.atr,
                "realized_vol_annual": s.realized_vol_annual,
                "direction": s.direction, "strength": s.strength,
                "target_weight": s.target_weight,
            }

        # Auto-research
        closes_by_symbol = {sym: [b.close for b in self.bars.bars(sym)]
                            for sym in Config.UNIVERSE
                            if len(self.bars.bars(sym)) >= 5}
        research = {
            "correlation": AutoResearch.correlation_matrix(closes_by_symbol),
            "per_asset": AutoResearch.per_asset_stats(closes_by_symbol),
            "regime_by_symbol": {
                sym: AutoResearch.regime(c)
                for sym, c in closes_by_symbol.items()},
        }

        # Current weights
        current_w = {sym: (qty * quotes[sym].mid / equity) if equity > 0 else 0
                     for sym, qty in holdings.items() if sym in quotes}

        # Serialize per-strategy snapshots
        def _serialize_sigs(sigs: dict) -> dict:
            return {sym: {
                "symbol": s.symbol,
                "price": s.price, "ema": s.ema, "z_score": s.z_score,
                "rsi": s.rsi, "macd": s.macd, "macd_signal": s.macd_signal,
                "bollinger_upper": s.bollinger_upper,
                "bollinger_lower": s.bollinger_lower,
                "atr": s.atr,
                "realized_vol_annual": s.realized_vol_annual,
                "direction": s.direction, "strength": s.strength,
                "target_weight": s.target_weight,
            } for sym, s in sigs.items()}

        strategies_view = {}
        for name, scls in STRATEGY_REGISTRY.items():
            strategies_view[name] = {
                "name": name,
                "description": scls.DESCRIPTION,
                "signals": _serialize_sigs(
                    (all_strategy_signals or {}).get(name, {})),
                "target_weights": (all_strategy_weights or {}).get(name, {}),
                "is_active": (name == Config.ACTIVE_STRATEGY),
            }

        # Trade markers
        markers = {sym: list(dq) for sym, dq in self._trade_markers.items()}

        # Per-asset PnL (realized + unrealized)
        per_asset_pnl = {}
        for sym in Config.UNIVERSE:
            realized = self._per_asset_realized_pnl.get(sym, 0.0)
            cb = self._cost_basis.get(sym, {"qty": 0.0, "avg_cost": 0.0})
            mark = quotes[sym].mid if sym in quotes else cb["avg_cost"]
            unreal = cb["qty"] * (mark - cb["avg_cost"]) if cb["qty"] else 0.0
            per_asset_pnl[sym] = {
                "realized": round(realized, 4),
                "unrealized": round(unreal, 4),
                "total": round(realized + unreal, 4),
            }

        self.dashboard.update(
            last_cycle_at=now_utc.isoformat(),
            next_cycle_at=next_at.isoformat(),
            kill_switch=self.risk.kill_switch_status(equity),
            equity=round(equity, 2), cash=round(cash, 2),
            deployed=round(deployed, 2),
            daily_anchor=round(self.risk.equity_at_utc_midnight, 2)
                if self.risk.equity_at_utc_midnight else None,
            anchor_date=self.risk._anchor_date,
            dry_run=Config.DRY_RUN, mock=self.mock,
            broker=self.broker_name,
            active_strategy=Config.ACTIVE_STRATEGY,
            scheduler_paused=self._paused.is_set(),
            signals=sig_dict,
            bars_by_symbol=bars_by_symbol,
            holdings=holdings,
            target_weights=target_weights,
            current_weights=current_w,
            research=research,
            config=self._config_snapshot(),
            strategies_view=strategies_view,
            trade_markers=markers,
            per_asset_pnl=per_asset_pnl,
            macro={
                "available": HistoricalDataLoader.is_available(),
                "summary": self._macro_summary,
                "correlation": self._macro_corr,
            },
        )
        self.dashboard.stats.record_cycle(equity)

    def start(self):
        log.info("=" * 70)
        log.info("  RH CRYPTO VTMR  |  broker=%s  DRY_RUN=%s",
                 self.broker_name, Config.DRY_RUN)
        log.info("=" * 70)
        self.client.init_auth()
        self._running = True
        try:
            self._do_cycle()
            while self._running:
                triggered = self._cycle_event.wait(
                    timeout=Config.POLL_INTERVAL_SECONDS)
                if triggered:
                    self._cycle_event.clear()
                    log.info("Cycle triggered from dashboard.")
                self._do_cycle()
        except KeyboardInterrupt:
            log.info("Shutdown requested.")
        finally:
            log.info("Final equity: $%.2f", self._portfolio_value())

    def scan_only(self):
        log.info("Scan mode — single cycle, no scheduler.")
        self.client.init_auth()
        self._do_cycle()

    def dashboard_loop(self):
        log.info("Dashboard loop: Cycle Now button drives further cycles.")
        try:
            while True:
                self._cycle_event.wait()
                self._cycle_event.clear()
                self._do_cycle()
        except KeyboardInterrupt:
            log.info("Shutdown.")


# ╔══════════════════════════════════════════════════════════════════════════════╗
# ║                          17. CLI ENTRY                                      ║
# ╚══════════════════════════════════════════════════════════════════════════════╝

def main():
    p = argparse.ArgumentParser(
        description="Robinhood Crypto VTMR trading bot",
        formatter_class=argparse.RawTextHelpFormatter)
    p.add_argument("--mode", choices=["scan", "live"], default="scan",
                   help="scan: one cycle then dashboard loop\n"
                        "live: continuous scheduler with auto-rebalance")
    p.add_argument("--mock", action="store_true",
                   help="Use synthetic prices (offline dev).")
    p.add_argument("--dashboard", action="store_true",
                   help="Launch the local HTML dashboard.")
    p.add_argument("--dashboard-port", type=int, default=8770)
    p.add_argument("--state-file", default=None)
    p.add_argument("--live-orders", action="store_true",
                   help="REAL orders. Use with extreme care.")
    p.add_argument("--capital", type=float, default=None,
                   help="Override TOTAL_CAPITAL (USD).")
    p.add_argument("--universe", default=None,
                   help="Comma-separated symbols, e.g. BTC-USD,ETH-USD")
    p.add_argument("--log-level",
                   choices=["DEBUG", "INFO", "WARNING", "ERROR"],
                   default="INFO")
    args = p.parse_args()

    setup_logging(getattr(logging, args.log_level))

    if args.capital:
        Config.TOTAL_CAPITAL = args.capital
    if args.universe:
        Config.UNIVERSE = [s.strip() for s in args.universe.split(",")
                           if s.strip()]
    if args.live_orders:
        Config.DRY_RUN = False
        log.warning("LIVE ORDER MODE — real money will be used.")
    else:
        Config.DRY_RUN = True

    orch = Orchestrator(mock=args.mock,
                        read_only=(args.mode == "scan"),
                        state_path=args.state_file)
    if args.dashboard:
        orch.start_dashboard(args.dashboard_port)

    if args.mode == "scan":
        orch.scan_only()
        if args.dashboard:
            orch.dashboard_loop()
    else:
        orch.start()


if __name__ == "__main__":
    main()

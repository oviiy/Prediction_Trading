"""
================================================================================
  POLYMARKET DECAY TRADING FRAMEWORK
  Strategy : Buy near-certain side of markets close to resolution; capture
             convergence to 1.0 at resolution.
  Capital  : $500-$1,000 USDC on Polygon
  Author   : Michael Wang | 2026-05-05
================================================================================

DISCLAIMER: For educational/research use. Trading prediction markets carries
substantial risk. Polymarket's legal status for US users is uncertain. This is
NOT financial, legal, or tax advice.

SETUP:
  pip install requests python-dotenv py-clob-client

CREDENTIALS (.env file in same directory):
  POLY_API_KEY=your_polymarket_api_key
  POLY_API_SECRET=your_polymarket_api_secret
  POLY_API_PASSPHRASE=your_polymarket_passphrase
  POLY_PRIVATE_KEY=your_wallet_private_key   # for on-chain signing

USAGE:
  python polymarket_decay.py --mode scan                 # discover, no orders
  python polymarket_decay.py --mode live                 # scheduler, dry-run
  python polymarket_decay.py --mode live --live-orders   # real orders (use care)
  python polymarket_decay.py --mode backtest --history history.csv
================================================================================
"""

# ── Standard Library ──────────────────────────────────────────────────────────
import argparse
import csv
import json
import logging
import math
import os
import sys
import time
from collections import deque
from dataclasses import asdict, dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Optional

# ── Third-Party (required) ────────────────────────────────────────────────────
try:
    import requests
    from dotenv import load_dotenv
except ImportError as e:
    sys.exit(
        f"\n[FATAL] Missing dependency: {e}\n"
        "Run: pip install requests python-dotenv py-clob-client\n"
    )

# ── Third-Party (optional CLOB) ───────────────────────────────────────────────
try:
    from py_clob_client.client import ClobClient
    from py_clob_client.clob_types import ApiCreds, OrderArgs, OrderType, Side
    CLOB_AVAILABLE = True
except ImportError:
    CLOB_AVAILABLE = False

load_dotenv()


# ╔══════════════════════════════════════════════════════════════════════════════╗
# ║                          1. CONFIGURATION                                   ║
# ╚══════════════════════════════════════════════════════════════════════════════╝

class Config:
    # ── Polymarket Endpoints / Credentials ──────────────────────────────────
    GAMMA_HOST            = "https://gamma-api.polymarket.com"
    CLOB_HOST             = "https://clob.polymarket.com"
    DATA_HOST             = "https://data-api.polymarket.com"
    POLY_API_KEY          = os.getenv("POLY_API_KEY", "")
    POLY_API_SECRET       = os.getenv("POLY_API_SECRET", "")
    POLY_API_PASSPHRASE   = os.getenv("POLY_API_PASSPHRASE", "")
    POLY_PRIVATE_KEY      = os.getenv("POLY_PRIVATE_KEY", "")
    POLY_CHAIN_ID         = 137                  # Polygon mainnet

    # ── Kalshi (CFTC-regulated, US-legal prediction market) ─────────────────
    KALSHI_HOST            = "https://api.elections.kalshi.com/trade-api/v2"
    KALSHI_API_KEY_ID      = os.getenv("KALSHI_API_KEY_ID", "")
    # Path to a PEM-encoded RSA private key file. Generated in the Kalshi UI
    # → API Keys page. NEVER commit to git.
    KALSHI_PRIVATE_KEY_PATH = os.getenv("KALSHI_PRIVATE_KEY_PATH", "")

    # ── Capital & Risk ──────────────────────────────────────────────────────
    TOTAL_CAPITAL              = 1000.0
    MAX_POSITION_USD           = 200.0
    MAX_CONCURRENT_POSITIONS   = 3
    MAX_TOTAL_DEPLOYED_PCT     = 0.80
    MIN_TRADE_USD              = 5.0
    DAILY_LOSS_LIMIT_PCT       = 0.10           # halt new entries
    HARD_HALT_LOSS_PCT         = 0.20           # halt all activity

    # ── Decay Strategy ──────────────────────────────────────────────────────
    DAYS_TO_RESOLUTION_MIN     = 0.25           # 6 hours
    DAYS_TO_RESOLUTION_MAX     = 7.0
    DOMINANT_PRICE_MIN         = 0.85
    DOMINANT_PRICE_MAX         = 0.97
    MIN_VOLUME_24H_USD         = 5000.0
    MAX_SPREAD                 = 0.03
    MIN_BOOK_DEPTH_USD         = 200.0
    DISPUTE_BUFFER             = 0.02           # 2% probability haircut
    MIN_ANNUALIZED_EDGE        = 0.40           # 40% annualized
    KELLY_FRACTION             = 0.25           # quarter-Kelly
    EMERGENCY_EXIT_DROP        = 0.10           # exit if price drops 10¢ from entry
    SLIPPAGE_TOLERANCE         = 0.01           # abort trade if est fill > limit + 1¢

    # ── Category gates ──────────────────────────────────────────────────────
    CATEGORY_ALLOWLIST = {
        "Sports", "Crypto", "Cryptocurrency", "Numeric",
        "Macro", "Markets", "Stocks",
    }
    CATEGORY_DENYLIST = {
        "Politics", "Elections", "Tweet", "Court", "Celebrity",
    }

    # ── Execution ───────────────────────────────────────────────────────────
    SCAN_INTERVAL_SECONDS      = 900            # 15 minutes
    ORDER_FILL_TIMEOUT_SECONDS = 60
    RATE_LIMIT_PER_MIN         = 60

    # ── I/O ─────────────────────────────────────────────────────────────────
    LOG_FILE        = "framework.log"
    TRADES_CSV      = "trades.csv"
    TRADES_JSONL    = "trades.jsonl"
    STATE_FILE      = "state.json"
    LOG_LEVEL       = logging.INFO

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

log = logging.getLogger("PolymarketDecay")


class Journal:
    """
    Two-format trade journal.

    - CSV is human-readable, fixed schema, never gets surprise fields.
    - JSONL is structured; carries full event metadata for replay/analysis.

    NOTE: explicit field assignment, never **kwargs spread.  Fixes bug #6
    (signal_meta clobbering required keys) from the prior framework.
    """

    CSV_FIELDS = [
        "timestamp", "event", "strategy", "market_question",
        "token_id", "side", "price", "shares", "usdc_amount",
        "dry_run", "notes",
    ]

    def __init__(self, csv_path: str = Config.TRADES_CSV,
                 jsonl_path: str = Config.TRADES_JSONL):
        self.csv_path = csv_path
        self.jsonl_path = jsonl_path
        self._dashboard: Optional["DashboardState"] = None
        self._ensure_csv_header()

    def attach_dashboard(self, dashboard: "DashboardState"):
        self._dashboard = dashboard

    def _ensure_csv_header(self):
        if not os.path.isfile(self.csv_path):
            with open(self.csv_path, "w", newline="") as f:
                csv.DictWriter(f, fieldnames=self.CSV_FIELDS).writeheader()

    def record(self, *, event: str, strategy: str = "",
               market_question: str = "", token_id: str = "",
               side: str = "", price: float = 0.0, shares: float = 0.0,
               usdc_amount: float = 0.0, dry_run: bool = True,
               notes: str = "", extra: Optional[dict] = None):
        ts = datetime.now(timezone.utc).isoformat()

        row = {
            "timestamp": ts,
            "event": event,
            "strategy": strategy,
            "market_question": (market_question or "")[:120],
            "token_id": token_id,
            "side": side,
            "price": round(price, 4) if price else "",
            "shares": round(shares, 4) if shares else "",
            "usdc_amount": round(usdc_amount, 4) if usdc_amount else "",
            "dry_run": dry_run,
            "notes": notes,
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
# ║                       2a. PERSISTENT STATE STORE                            ║
# ╚══════════════════════════════════════════════════════════════════════════════╝

import copy as _copy
import threading as _threading


class PersistentStore:
    """Atomic JSON-backed key/value store.

    A single file holds everything that needs to survive a process restart:
    open positions, the daily anchor, kill-switch state, and lifetime
    counters. Writes go through a tmp-file → rename so a crash mid-write
    can never produce a half-written file.
    """

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
            log.error("State file %s is corrupted (%s) — backing up and "
                      "starting fresh.", self.path, e)
            try:
                os.replace(self.path, self.path + ".corrupt")
            except OSError:
                pass
            return {}

    def get(self, key: str, default=None):
        with self._lock:
            if key in self._data:
                return _copy.deepcopy(self._data[key])
            return default

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
# ║                       2b. DASHBOARD SNAPSHOT STATE                          ║
# ╚══════════════════════════════════════════════════════════════════════════════╝


class CumulativeStats:
    """Lifetime counters across all scan cycles since process start."""

    EQUITY_HISTORY = 240   # ~12h at one sample per cycle (15min)

    def __init__(self, store: Optional["PersistentStore"] = None):
        self._lock = _threading.RLock()
        self.store = store
        self.started_at = datetime.now(timezone.utc).isoformat()
        self.scans = 0
        self.markets_seen = 0
        self.candidates_seen = 0
        self.rejections_total = 0
        self.rejection_counts: dict = {}
        self.intents = 0
        self.fills = 0
        self.slippage_aborts = 0
        self.order_failures = 0
        self.equity_history: deque = deque(maxlen=self.EQUITY_HISTORY)
        if store is not None:
            self._restore()

    def _restore(self):
        d = self.store.get("stats") or {}
        if not d:
            return
        self.started_at = d.get("started_at", self.started_at)
        self.scans = int(d.get("scans", 0))
        self.markets_seen = int(d.get("markets_seen", 0))
        self.candidates_seen = int(d.get("candidates_seen", 0))
        self.rejections_total = int(d.get("rejections_total", 0))
        self.rejection_counts = dict(d.get("rejection_counts") or {})
        self.intents = int(d.get("intents", 0))
        self.fills = int(d.get("fills", 0))
        self.slippage_aborts = int(d.get("slippage_aborts", 0))
        self.order_failures = int(d.get("order_failures", 0))
        for sample in (d.get("equity_history") or [])[-self.EQUITY_HISTORY:]:
            self.equity_history.append(sample)
        log.info("Stats restored: scans=%d fills=%d started=%s",
                 self.scans, self.fills, self.started_at)

    def _persist(self):
        if self.store is None:
            return
        self.store.put("stats", {
            "started_at": self.started_at,
            "scans": self.scans,
            "markets_seen": self.markets_seen,
            "candidates_seen": self.candidates_seen,
            "rejections_total": self.rejections_total,
            "rejection_counts": dict(self.rejection_counts),
            "intents": self.intents,
            "fills": self.fills,
            "slippage_aborts": self.slippage_aborts,
            "order_failures": self.order_failures,
            "equity_history": list(self.equity_history),
        })

    def record_cycle(self, *, markets_count: int, candidates: int,
                     rejections: list):
        with self._lock:
            self.scans += 1
            self.markets_seen += markets_count
            self.candidates_seen += candidates
            self.rejections_total += len(rejections)
            for r in rejections:
                k = r.get("reason", "unknown")
                self.rejection_counts[k] = self.rejection_counts.get(k, 0) + 1
        self._persist()

    def record_equity(self, ts_iso: str, equity: float):
        with self._lock:
            self.equity_history.append([ts_iso, round(equity, 2)])

    def record_event(self, event_name: str):
        with self._lock:
            if event_name == "intent":
                self.intents += 1
            elif event_name in ("filled", "dry_filled"):
                self.fills += 1
            elif event_name == "slippage_abort":
                self.slippage_aborts += 1
            elif event_name == "order_failed":
                self.order_failures += 1

    def snapshot(self) -> dict:
        with self._lock:
            return {
                "started_at": self.started_at,
                "scans": self.scans,
                "markets_seen": self.markets_seen,
                "candidates_seen": self.candidates_seen,
                "rejections_total": self.rejections_total,
                "rejection_counts": dict(self.rejection_counts),
                "intents": self.intents,
                "fills": self.fills,
                "slippage_aborts": self.slippage_aborts,
                "order_failures": self.order_failures,
                "equity_history": list(self.equity_history),
            }


class DashboardState:
    """Thread-safe in-memory snapshot the dashboard server reads from.

    Writers (Orchestrator, Journal) mutate via update() / push_event().
    Readers (HTTP handlers) call snapshot() which returns a deep copy
    under the lock so they can serialize without races.
    """

    MAX_EVENTS = 200
    MAX_REJECTIONS = 500

    def __init__(self, store: Optional["PersistentStore"] = None):
        self._lock = _threading.RLock()
        self.stats = CumulativeStats(store=store)
        self._state: dict = {
            "last_cycle_at": None,
            "next_cycle_at": None,
            "kill_switch": "OK",
            "equity": 0.0,
            "deployed": 0.0,
            "daily_anchor": None,
            "dry_run": True,
            "mock": False,
            "broker": "polymarket",
            "scheduler_paused": False,
            "candidates": [],
            "rejections": [],
            "positions": [],
            "config": {},
            "filter_funnel": {},
            "strategies": {},
            "analytics": {},
            "hot_events": {},
            "total_markets": 0,
            "messages": [],            # last few user-action acks
        }
        self._events: deque = deque(maxlen=self.MAX_EVENTS)
        self._messages: deque = deque(maxlen=20)

    def update(self, **kwargs):
        with self._lock:
            for k, v in kwargs.items():
                self._state[k] = v

    def push_event(self, event: dict):
        with self._lock:
            self._events.append(event)
        self.stats.record_event(event.get("event", ""))

    def push_message(self, text: str, level: str = "info"):
        with self._lock:
            self._messages.append({
                "ts": datetime.now(timezone.utc).isoformat(),
                "level": level,
                "text": text,
            })

    def snapshot(self) -> dict:
        with self._lock:
            snap = _copy.deepcopy(self._state)
            snap["recent_events"] = list(self._events)
            snap["messages"] = list(self._messages)
        snap["stats"] = self.stats.snapshot()
        return snap

    def events_tail(self, limit: int = 50) -> list:
        with self._lock:
            evs = list(self._events)
        return evs[-limit:]


# ╔══════════════════════════════════════════════════════════════════════════════╗
# ║                          3. DATACLASSES                                     ║
# ╚══════════════════════════════════════════════════════════════════════════════╝

@dataclass
class Token:
    token_id: str
    outcome: str          # "Yes" / "No" / outcome label
    price: float          # last traded / mid

@dataclass
class Market:
    condition_id: str
    question: str
    end_date: datetime    # tz-aware, UTC
    category: str
    volume_24h: float
    tokens: list          # list[Token]
    closed: bool = False
    raw: dict = field(default_factory=dict)   # original API payload

@dataclass
class Orderbook:
    token_id: str
    bids: list   # list[(price, size)]
    asks: list   # list[(price, size)]

    @property
    def best_bid(self) -> Optional[float]:
        return self.bids[0][0] if self.bids else None

    @property
    def best_ask(self) -> Optional[float]:
        return self.asks[0][0] if self.asks else None

    @property
    def spread(self) -> Optional[float]:
        if self.best_ask is not None and self.best_bid is not None:
            return self.best_ask - self.best_bid
        return None

    def fillable_usdc_at_or_below(self, max_price: float) -> float:
        """Total USDC of liquidity available on the ask side at or below max_price."""
        total = 0.0
        for price, size in self.asks:
            if price > max_price:
                break
            total += price * size
        return total

@dataclass
class DecayCandidate:
    market: Market
    dominant_token: Token
    edge: float                  # (1 - price) - dispute_buffer
    annualized_edge: float
    days_to_resolution: float
    spread: float
    book_depth_usd: float
    score: float                 # for ranking

@dataclass
class Decision:
    """Strategy → Execution intent."""
    token_id: str
    side: str                    # "BUY" | "CLOSE"
    usdc_amount: float
    limit_price: float
    market_question: str
    notes: str = ""

@dataclass
class PositionState:
    """Tracks per-trade entry — used by RiskManager.
    Fixes bug #4 (lifetime average_buy_price problem)."""
    token_id: str
    market_question: str
    entry_timestamp: datetime
    entry_price: float
    entry_shares: float
    entry_usdc: float
    strategy: str


# ╔══════════════════════════════════════════════════════════════════════════════╗
# ║                          4. POLYMARKET CLIENT                               ║
# ╚══════════════════════════════════════════════════════════════════════════════╝

class _RateLimiter:
    """Simple token-bucket on a per-second basis."""
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
    """Contract every market broker (Polymarket / Kalshi / Mock) must satisfy.

    Methods may return None when data is unavailable (dry-run, missing creds,
    network failure). Callers should check for None — the broker layer never
    raises on absent data.

    Naming note: 'token_id' is the broker-specific instrument identifier
    (Polymarket: CLOB token id hex; Kalshi: market ticker + side).
    """

    name: str = "abstract"   # subclasses override

    def init_auth(self) -> bool:
        """Initialize signing/auth. Return True if live trading is wired up."""
        return False

    def list_markets(self, *, closing_within_days: float = 7.0,
                     min_volume_24h: float = 0.0,
                     limit_per_page: int = 500,
                     max_total: int = 5000) -> list[Market]:
        raise NotImplementedError

    def get_orderbook(self, token_id: str,
                      levels: int = 10) -> Optional[Orderbook]:
        raise NotImplementedError

    def buy_token(self, token_id: str, *, usdc_amount: float,
                  limit_price: float) -> Optional[dict]:
        raise NotImplementedError

    def close_position(self, token_id: str, *, shares: float,
                       limit_price: float) -> Optional[dict]:
        raise NotImplementedError

    def get_my_position(self, token_id: str) -> float:
        return 0.0

    def get_balance(self) -> Optional[float]:
        """USDC (or USD) available for new positions. None if unknown."""
        return None

    def get_order_status(self, order_id: str) -> Optional[dict]:
        """Return {'status': 'open'|'filled'|'cancelled', 'filled_size': N, ...}
        or None if status can't be retrieved."""
        return None


class PolymarketClient(BrokerClient):
    """
    REST + CLOB integration with bug fixes from prior framework:
      Bug #2: order size is in shares, not USDC — converted in buy_token().
      Bug #3: no SELL-on-YES-token semantic. To bet against, BUY the NO token.
              close_position() is a separate verb for selling owned inventory.
      Bug #8: Gamma /markets does not take a free-text 'q' param — uses
              end_date_min / end_date_max instead.
    """
    name = "polymarket"

    def __init__(self, *, read_only: bool = False):
        self._clob: Optional["ClobClient"] = None
        self._rl = _RateLimiter(Config.RATE_LIMIT_PER_MIN)
        self._read_only = read_only

    # ── Auth ────────────────────────────────────────────────────────────────
    def init_auth(self) -> bool:
        return self.init_clob()

    def init_clob(self) -> bool:
        if self._read_only:
            log.info("Polymarket client: read-only mode, skipping CLOB init.")
            return False
        if not CLOB_AVAILABLE:
            log.warning("py-clob-client not installed. Live orders disabled.")
            return False
        if not Config.POLY_API_KEY or not Config.POLY_PRIVATE_KEY:
            log.warning("Polymarket credentials missing. Live orders disabled.")
            return False
        try:
            creds = ApiCreds(
                api_key=Config.POLY_API_KEY,
                api_secret=Config.POLY_API_SECRET,
                api_passphrase=Config.POLY_API_PASSPHRASE,
            )
            self._clob = ClobClient(
                host=Config.CLOB_HOST,
                chain_id=Config.POLY_CHAIN_ID,
                key=Config.POLY_PRIVATE_KEY,
                creds=creds,
            )
            log.info("Polymarket CLOB client initialized.")
            return True
        except Exception as e:
            log.error("Polymarket CLOB init failed: %s", e)
            return False

    # ── HTTP helper with retries ────────────────────────────────────────────
    def _get(self, url: str, params: Optional[dict] = None,
             timeout: int = 10) -> Optional[dict]:
        last_err = None
        for attempt in range(3):
            try:
                self._rl.wait()
                r = requests.get(url, params=params, timeout=timeout)
                if r.status_code == 200:
                    return r.json()
                if r.status_code in (429, 500, 502, 503, 504):
                    time.sleep(2 ** attempt)
                    continue
                log.warning("HTTP %d from %s", r.status_code, url)
                return None
            except Exception as e:
                last_err = e
                time.sleep(2 ** attempt)
        log.error("HTTP retries exhausted for %s: %s", url, last_err)
        return None

    # ── Market discovery ────────────────────────────────────────────────────
    def list_markets(self, *, closing_within_days: float,
                     min_volume_24h: float = 0.0,
                     limit_per_page: int = 500,
                     max_total: int = 5000) -> list[Market]:
        """
        Fetch active markets resolving within `closing_within_days` days.
        Paginates internally. Bug #8 fix: uses end_date_min/end_date_max.
        """
        now_utc = datetime.now(timezone.utc)
        end_min = now_utc.isoformat()
        end_max = (now_utc + timedelta(days=closing_within_days)).isoformat()

        all_markets: list[Market] = []
        offset = 0
        while len(all_markets) < max_total:
            data = self._get(
                f"{Config.GAMMA_HOST}/markets",
                params={
                    "active": "true",
                    "closed": "false",
                    "end_date_min": end_min,
                    "end_date_max": end_max,
                    "limit": limit_per_page,
                    "offset": offset,
                    "order": "endDate",
                    "ascending": "true",
                },
            )
            if not data:
                break

            # Gamma returns a list directly for this endpoint
            page = data if isinstance(data, list) else data.get("data", [])
            if not page:
                break

            for m in page:
                try:
                    mk = self._parse_market(m)
                    if mk and mk.volume_24h >= min_volume_24h:
                        all_markets.append(mk)
                except Exception as e:
                    log.debug("Skip market parse error: %s", e)

            if len(page) < limit_per_page:
                break
            offset += limit_per_page

        return all_markets

    def _parse_market(self, m: dict) -> Optional[Market]:
        end_str = m.get("endDate") or m.get("end_date")
        if not end_str:
            return None
        try:
            end_dt = datetime.fromisoformat(end_str.replace("Z", "+00:00"))
            if end_dt.tzinfo is None:
                end_dt = end_dt.replace(tzinfo=timezone.utc)
        except Exception:
            return None

        # Tokens: gamma returns clobTokenIds as JSON-string + outcomePrices as JSON-string
        tokens: list[Token] = []
        token_ids_raw = m.get("clobTokenIds") or "[]"
        prices_raw = m.get("outcomePrices") or "[]"
        outcomes_raw = m.get("outcomes") or "[]"
        try:
            token_ids = json.loads(token_ids_raw) if isinstance(token_ids_raw, str) else token_ids_raw
            prices = json.loads(prices_raw) if isinstance(prices_raw, str) else prices_raw
            outcomes = json.loads(outcomes_raw) if isinstance(outcomes_raw, str) else outcomes_raw
        except Exception:
            return None

        for i, tid in enumerate(token_ids):
            try:
                price = float(prices[i]) if i < len(prices) else 0.0
                outcome = outcomes[i] if i < len(outcomes) else ""
                tokens.append(Token(token_id=str(tid), outcome=str(outcome), price=price))
            except Exception:
                continue

        if not tokens:
            return None

        return Market(
            condition_id=str(m.get("conditionId") or m.get("id") or ""),
            question=str(m.get("question", ""))[:200],
            end_date=end_dt,
            category=str(m.get("category", "") or m.get("groupItemTitle", "")),
            volume_24h=float(m.get("volume24hr", 0) or m.get("volume24Hr", 0) or 0),
            tokens=tokens,
            closed=bool(m.get("closed", False)),
            raw=m,
        )

    # ── Orderbook ───────────────────────────────────────────────────────────
    def get_orderbook(self, token_id: str, levels: int = 10) -> Optional[Orderbook]:
        data = self._get(f"{Config.CLOB_HOST}/book", params={"token_id": token_id})
        if not data:
            return None
        try:
            bids_raw = data.get("bids") or []
            asks_raw = data.get("asks") or []
            # Polymarket returns bids ascending; sort descending so [0] is best bid
            bids = sorted(
                [(float(b["price"]), float(b["size"])) for b in bids_raw],
                key=lambda x: -x[0],
            )[:levels]
            asks = sorted(
                [(float(a["price"]), float(a["size"])) for a in asks_raw],
                key=lambda x: x[0],
            )[:levels]
            return Orderbook(token_id=token_id, bids=bids, asks=asks)
        except Exception as e:
            log.debug("Orderbook parse error for %s: %s", token_id, e)
            return None

    # ── Order placement ─────────────────────────────────────────────────────
    def buy_token(self, token_id: str, *, usdc_amount: float,
                  limit_price: float) -> Optional[dict]:
        """
        Open a long position by BUYING `usdc_amount` worth of `token_id`
        at `limit_price`.

        BUG #2 FIX: shares = usdc_amount / limit_price.  Polymarket's
        OrderArgs.size is in token shares, not USDC.

        BUG #3 FIX: this is the only "open a position" verb.  To bet against
        an event, call buy_token() with the NO token's id.  Never sell-to-open
        a token you don't hold.
        """
        if limit_price <= 0 or limit_price >= 1:
            log.error("buy_token: invalid limit_price=%s", limit_price)
            return None

        shares = round(usdc_amount / limit_price, 4)
        if shares <= 0:
            log.error("buy_token: computed shares <= 0 (usdc=%s, price=%s)",
                      usdc_amount, limit_price)
            return None

        if Config.DRY_RUN or self._clob is None:
            log.info("[DRY-RUN] BUY token=%s... shares=%.4f @ %.4f ($%.2f)",
                     token_id[:12], shares, limit_price, usdc_amount)
            return {"dry_run": True, "shares": shares, "price": limit_price}

        try:
            order_args = OrderArgs(
                token_id=token_id,
                price=limit_price,
                size=shares,
                side=Side.BUY,
            )
            signed = self._clob.create_order(order_args)
            resp = self._clob.post_order(signed, OrderType.GTC)
            log.info("[POLY] BUY placed: %s", resp)
            return resp
        except Exception as e:
            log.error("[POLY] BUY failed: %s", e)
            return None

    def close_position(self, token_id: str, *, shares: float,
                       limit_price: float) -> Optional[dict]:
        """
        Sell-to-close existing inventory.  This is the only place Side.SELL
        is used and it is only legal when wallet already holds these tokens.
        """
        if limit_price <= 0 or limit_price >= 1 or shares <= 0:
            log.error("close_position: invalid args")
            return None

        if Config.DRY_RUN or self._clob is None:
            log.info("[DRY-RUN] CLOSE token=%s... shares=%.4f @ %.4f",
                     token_id[:12], shares, limit_price)
            return {"dry_run": True, "shares": shares, "price": limit_price}

        try:
            order_args = OrderArgs(
                token_id=token_id,
                price=limit_price,
                size=shares,
                side=Side.SELL,
            )
            signed = self._clob.create_order(order_args)
            resp = self._clob.post_order(signed, OrderType.GTC)
            log.info("[POLY] CLOSE placed: %s", resp)
            return resp
        except Exception as e:
            log.error("[POLY] CLOSE failed: %s", e)
            return None

    def get_my_position(self, token_id: str) -> float:
        """Return wallet share-count for a given token_id (0 if none / read-only)."""
        if Config.DRY_RUN or self._clob is None:
            return 0.0
        try:
            if hasattr(self._clob, "get_balances"):
                balances = self._clob.get_balances()
                for bal in balances or []:
                    if str(bal.get("asset_id", "")) == str(token_id):
                        return float(bal.get("balance", 0))
        except Exception as e:
            log.debug("get_my_position fallback: %s", e)
        return 0.0

    def get_balance(self) -> Optional[float]:
        """USDC.e balance in the Polymarket wallet, or None if unknown."""
        if Config.DRY_RUN or self._clob is None:
            return None
        try:
            # py-clob-client exposes get_balance_allowance(); shape depends on
            # version. Best-effort, return None on any failure so callers fall
            # back to their hardcoded baseline.
            if hasattr(self._clob, "get_balance_allowance"):
                ba = self._clob.get_balance_allowance(
                    params={"asset_type": "COLLATERAL"})
                bal = ba.get("balance") if isinstance(ba, dict) else None
                if bal is not None:
                    # Polymarket returns USDC in 6-decimal int strings
                    return float(bal) / 1_000_000
        except Exception as e:
            log.debug("get_balance fallback: %s", e)
        return None

    def get_order_status(self, order_id: str) -> Optional[dict]:
        if Config.DRY_RUN or self._clob is None:
            return None
        try:
            if hasattr(self._clob, "get_order"):
                o = self._clob.get_order(order_id)
                if isinstance(o, dict):
                    return {
                        "status": o.get("status", "unknown"),
                        "filled_size": float(o.get("size_matched", 0)),
                        "size": float(o.get("original_size", 0)),
                        "raw": o,
                    }
        except Exception as e:
            log.debug("get_order_status error: %s", e)
        return None


# ╔══════════════════════════════════════════════════════════════════════════════╗
# ║                          4a. KALSHI CLIENT                                  ║
# ╚══════════════════════════════════════════════════════════════════════════════╝

# Optional cryptography import — only required for signed (trading) requests.
# Read-only market discovery works without it.
try:
    from cryptography.hazmat.primitives import hashes as _crypto_hashes
    from cryptography.hazmat.primitives.asymmetric import padding as _crypto_padding
    from cryptography.hazmat.primitives.serialization import load_pem_private_key
    CRYPTO_AVAILABLE = True
except ImportError:
    CRYPTO_AVAILABLE = False

import base64 as _b64


class KalshiClient(BrokerClient):
    """Kalshi prediction-market broker.

    Kalshi is a CFTC-regulated US-legal market. Prices are integer cents
    (1-99) on the wire; this class normalizes to floats 0.01-0.99 so the
    rest of the framework (filter, strategies, dashboard) is broker-agnostic.

    token_id convention here: "<ticker>::yes" / "<ticker>::no". Every Kalshi
    market has both sides explicitly.

    Auth: RSA-PSS-SHA256 signing of `<timestamp_ms><METHOD><path>` with the
    user's private key. Read-only endpoints don't need auth.
    """
    name = "kalshi"

    def __init__(self, *, read_only: bool = False):
        self._rl = _RateLimiter(Config.RATE_LIMIT_PER_MIN)
        self._read_only = read_only
        self._private_key = None
        self._auth_ready = False
        self._key_id = ""

    # ── Auth ────────────────────────────────────────────────────────────────
    def init_auth(self) -> bool:
        if self._read_only:
            log.info("Kalshi: read-only mode, skipping auth.")
            return False
        if not CRYPTO_AVAILABLE:
            log.warning("cryptography not installed — Kalshi orders disabled. "
                        "Run: pip install cryptography")
            return False
        if not Config.KALSHI_API_KEY_ID or not Config.KALSHI_PRIVATE_KEY_PATH:
            log.warning("Kalshi creds missing (KALSHI_API_KEY_ID / "
                        "KALSHI_PRIVATE_KEY_PATH). Orders disabled.")
            return False
        try:
            with open(Config.KALSHI_PRIVATE_KEY_PATH, "rb") as f:
                self._private_key = load_pem_private_key(f.read(),
                                                          password=None)
            self._key_id = Config.KALSHI_API_KEY_ID
            self._auth_ready = True
            log.info("Kalshi auth initialized (key_id=%s...).",
                     self._key_id[:8])
            return True
        except Exception as e:
            log.error("Kalshi auth init failed: %s", e)
            return False

    def _sign(self, ts_ms: str, method: str, path: str) -> str:
        if self._private_key is None:
            return ""
        msg = (ts_ms + method + path).encode("utf-8")
        sig = self._private_key.sign(
            msg,
            _crypto_padding.PSS(
                mgf=_crypto_padding.MGF1(_crypto_hashes.SHA256()),
                salt_length=_crypto_padding.PSS.DIGEST_LENGTH,
            ),
            _crypto_hashes.SHA256(),
        )
        return _b64.b64encode(sig).decode("ascii")

    def _auth_headers(self, method: str, path: str) -> dict:
        ts_ms = str(int(time.time() * 1000))
        return {
            "Content-Type": "application/json",
            "KALSHI-ACCESS-KEY": self._key_id,
            "KALSHI-ACCESS-SIGNATURE": self._sign(ts_ms, method, path),
            "KALSHI-ACCESS-TIMESTAMP": ts_ms,
        }

    # ── HTTP ────────────────────────────────────────────────────────────────
    def _get(self, path: str, params: Optional[dict] = None,
             auth: bool = False, timeout: int = 10) -> Optional[dict]:
        url = Config.KALSHI_HOST + path
        headers = (self._auth_headers("GET",
                                      "/trade-api/v2" + path)
                   if auth and self._auth_ready else None)
        for attempt in range(3):
            try:
                self._rl.wait()
                r = requests.get(url, params=params, headers=headers,
                                 timeout=timeout)
                if r.status_code == 200:
                    return r.json()
                if r.status_code in (429, 500, 502, 503, 504):
                    time.sleep(2 ** attempt); continue
                log.warning("Kalshi GET %s -> HTTP %d: %s", path,
                            r.status_code, r.text[:200])
                return None
            except Exception as e:
                log.debug("Kalshi GET error: %s", e)
                time.sleep(2 ** attempt)
        return None

    def _post(self, path: str, body: dict, timeout: int = 10
              ) -> Optional[dict]:
        if not self._auth_ready:
            log.error("Kalshi _post called without auth ready")
            return None
        url = Config.KALSHI_HOST + path
        headers = self._auth_headers("POST", "/trade-api/v2" + path)
        try:
            self._rl.wait()
            r = requests.post(url, json=body, headers=headers, timeout=timeout)
            if r.status_code in (200, 201):
                return r.json()
            log.warning("Kalshi POST %s -> HTTP %d: %s", path,
                        r.status_code, r.text[:200])
            return None
        except Exception as e:
            log.error("Kalshi POST error: %s", e)
            return None

    # ── Markets ─────────────────────────────────────────────────────────────
    def list_markets(self, *, closing_within_days: float = 7.0,
                     min_volume_24h: float = 0.0,
                     limit_per_page: int = 1000,
                     max_total: int = 5000) -> list[Market]:
        now_utc = datetime.now(timezone.utc)
        max_close_ts = int(
            (now_utc + timedelta(days=closing_within_days)).timestamp())
        all_markets: list[Market] = []
        cursor = ""
        while len(all_markets) < max_total:
            params = {
                "status": "open",
                "limit": min(limit_per_page, 1000),
                "max_close_ts": max_close_ts,
            }
            if cursor:
                params["cursor"] = cursor
            data = self._get("/markets", params=params)
            if not data:
                break
            for m in data.get("markets") or []:
                try:
                    mk = self._parse_market(m)
                    if mk and mk.volume_24h >= min_volume_24h:
                        all_markets.append(mk)
                except Exception as e:
                    log.debug("Skip Kalshi market: %s", e)
            cursor = data.get("cursor") or ""
            if not cursor:
                break
        return all_markets

    def _parse_market(self, m: dict) -> Optional[Market]:
        end_str = m.get("close_time")
        if not end_str:
            return None
        try:
            end_dt = datetime.fromisoformat(end_str.replace("Z", "+00:00"))
            if end_dt.tzinfo is None:
                end_dt = end_dt.replace(tzinfo=timezone.utc)
        except Exception:
            return None

        ticker = str(m.get("ticker") or "")
        if not ticker:
            return None

        yes_ask = m.get("yes_ask")
        no_ask = m.get("no_ask")
        if yes_ask is None or no_ask is None:
            return None

        tokens = [
            Token(token_id=f"{ticker}::yes", outcome="Yes",
                  price=float(yes_ask) / 100.0),
            Token(token_id=f"{ticker}::no", outcome="No",
                  price=float(no_ask) / 100.0),
        ]

        title = m.get("title") or ""
        subtitle = m.get("subtitle") or ""
        question = (title + (" — " + subtitle if subtitle else ""))[:200]

        return Market(
            condition_id=ticker,
            question=question,
            end_date=end_dt,
            category=str(m.get("category", "")),
            volume_24h=float(m.get("volume_24h") or m.get("volume") or 0),
            tokens=tokens,
            closed=(m.get("status") != "open"),
            raw=m,
        )

    # ── Orderbook ───────────────────────────────────────────────────────────
    def get_orderbook(self, token_id: str, levels: int = 10
                      ) -> Optional[Orderbook]:
        ticker, _, side = token_id.partition("::")
        if not ticker or side not in ("yes", "no"):
            return None
        data = self._get(f"/markets/{ticker}/orderbook",
                         params={"depth": levels})
        if not data:
            return None
        ob = data.get("orderbook") or {}
        yes_book = ob.get("yes") or []   # [[price_cents, count], ...]
        no_book = ob.get("no") or []
        if side == "yes":
            bids = [(float(p) / 100.0, float(s)) for p, s in yes_book]
            # Yes asks come from complement of No bids
            asks = [((100 - float(p)) / 100.0, float(s)) for p, s in no_book]
        else:  # no
            bids = [(float(p) / 100.0, float(s)) for p, s in no_book]
            asks = [((100 - float(p)) / 100.0, float(s)) for p, s in yes_book]
        bids.sort(key=lambda x: -x[0])
        asks.sort(key=lambda x: x[0])
        return Orderbook(token_id=token_id,
                         bids=bids[:levels], asks=asks[:levels])

    # ── Order placement ─────────────────────────────────────────────────────
    def buy_token(self, token_id: str, *, usdc_amount: float,
                  limit_price: float) -> Optional[dict]:
        ticker, _, side = token_id.partition("::")
        if not ticker or side not in ("yes", "no"):
            log.error("Kalshi buy_token: bad token_id %s", token_id)
            return None
        if limit_price <= 0 or limit_price >= 1:
            log.error("Kalshi buy_token: invalid limit_price=%s", limit_price)
            return None
        price_cents = int(round(limit_price * 100))
        if not (1 <= price_cents <= 99):
            log.error("Kalshi buy_token: price_cents=%d out of range", price_cents)
            return None
        # Kalshi orders are integer contract counts.
        count = int(usdc_amount / limit_price)
        if count <= 0:
            log.error("Kalshi buy_token: count <= 0")
            return None

        if Config.DRY_RUN or not self._auth_ready:
            log.info("[DRY-RUN] Kalshi BUY %s/%s count=%d @ %d¢ ($%.2f)",
                     ticker, side, count, price_cents, usdc_amount)
            return {"dry_run": True, "shares": count, "price": limit_price}

        payload = {
            "ticker": ticker,
            "client_order_id": f"decay-{int(time.time() * 1000)}-{ticker[:12]}",
            "type": "limit",
            "action": "buy",
            "side": side,
            "count": count,
            f"{side}_price": price_cents,
        }
        resp = self._post("/portfolio/orders", payload)
        if not resp:
            return None
        order = resp.get("order") or {}
        log.info("Kalshi BUY placed: %s", order.get("order_id"))
        return {
            "orderID": order.get("order_id"),
            "shares": count,
            "price": limit_price,
            "raw": order,
        }

    def close_position(self, token_id: str, *, shares: float,
                       limit_price: float) -> Optional[dict]:
        ticker, _, side = token_id.partition("::")
        if not ticker or side not in ("yes", "no"):
            return None
        if limit_price <= 0 or limit_price >= 1 or shares <= 0:
            return None
        price_cents = int(round(limit_price * 100))
        count = int(shares)
        if count <= 0:
            return None

        if Config.DRY_RUN or not self._auth_ready:
            log.info("[DRY-RUN] Kalshi CLOSE %s/%s count=%d @ %d¢",
                     ticker, side, count, price_cents)
            return {"dry_run": True, "shares": count, "price": limit_price}

        payload = {
            "ticker": ticker,
            "client_order_id": f"close-{int(time.time() * 1000)}-{ticker[:12]}",
            "type": "limit",
            "action": "sell",
            "side": side,
            "count": count,
            f"{side}_price": price_cents,
        }
        resp = self._post("/portfolio/orders", payload)
        if not resp:
            return None
        order = resp.get("order") or {}
        return {
            "orderID": order.get("order_id"),
            "shares": count,
            "price": limit_price,
            "raw": order,
        }

    # ── Account ─────────────────────────────────────────────────────────────
    def get_balance(self) -> Optional[float]:
        if not self._auth_ready:
            return None
        data = self._get("/portfolio/balance", auth=True)
        if not data:
            return None
        # Kalshi returns cents
        bal_cents = data.get("balance")
        if bal_cents is None:
            return None
        return float(bal_cents) / 100.0

    def get_my_position(self, token_id: str) -> float:
        if not self._auth_ready:
            return 0.0
        ticker, _, side = token_id.partition("::")
        data = self._get("/portfolio/positions", auth=True,
                         params={"ticker": ticker})
        if not data:
            return 0.0
        for pos in data.get("market_positions") or []:
            if pos.get("ticker") == ticker:
                pos_count = pos.get("position", 0)
                # Positive = long Yes; negative = long No
                if side == "yes" and pos_count > 0:
                    return float(pos_count)
                if side == "no" and pos_count < 0:
                    return float(-pos_count)
        return 0.0

    def get_order_status(self, order_id: str) -> Optional[dict]:
        if not self._auth_ready:
            return None
        data = self._get(f"/portfolio/orders/{order_id}", auth=True)
        if not data:
            return None
        order = data.get("order") or {}
        st = order.get("status", "unknown")
        # Kalshi statuses: resting, canceled, executed, expired
        normalized = {
            "executed": "filled", "resting": "open",
            "canceled": "cancelled", "expired": "expired",
        }.get(st, st)
        return {
            "status": normalized,
            "filled_size": float(order.get("filled_count", 0)),
            "size": float(order.get("count", 0)),
            "avg_price": (float(order.get("filled_avg_price", 0)) / 100.0
                          if order.get("filled_avg_price") else None),
            "raw": order,
        }


# ╔══════════════════════════════════════════════════════════════════════════════╗
# ║                     4b. MOCK CLIENT (offline / dev mode)                    ║
# ╚══════════════════════════════════════════════════════════════════════════════╝

import random as _random


class MockPolymarketClient(PolymarketClient):
    """Synthetic data source that mimics PolymarketClient's interface.

    Useful when the real Polymarket API is unreachable — corporate firewall,
    geofence, or local development with no network. Generates a deterministic
    catalog of markets spanning every filter outcome (some rejected at each
    gate, ~10-20 surviving as candidates) so the dashboard funnel,
    counterfactual hints, and candidate detail panel all populate.
    """

    QUESTION_TEMPLATES = [
        ("Sports",  "Will the {team_a} beat the {team_b} on {date_label}?"),
        ("Sports",  "Will {team_a} win the {league} championship?"),
        ("Crypto",  "Will Bitcoin close above ${price_k}k by {date_label}?"),
        ("Crypto",  "Will ETH/BTC ratio exceed {ratio} this week?"),
        ("Markets", "Will the S&P 500 close above {price_k} on {date_label}?"),
        ("Markets", "Will 10Y Treasury yield exceed {ratio}% on {date_label}?"),
        ("Stocks",  "Will {team_a} beat earnings expectations Q{q}?"),
        ("Numeric", "Will US unemployment rate be below {ratio}% next print?"),
        # denied categories — should be filtered
        ("Politics","Will Senator {team_a} vote yes on the {league} bill?"),
        ("Tweet",   "Will {team_a} tweet about {team_b} this week?"),
        ("Court",   "Will the {team_a} v {team_b} ruling come this week?"),
    ]
    TEAMS = ["Lakers", "Warriors", "Celtics", "Heat", "Yankees", "Dodgers",
             "Patriots", "Chiefs", "Manchester United", "Arsenal", "Apple",
             "Nvidia", "Tesla", "Amazon", "Microsoft", "Google", "Meta"]
    LEAGUES = ["NBA", "NFL", "MLB", "Premier League", "Champions League"]
    DATES = ["Friday", "Sunday", "Monday", "July 1", "next week", "EOM"]

    def __init__(self, *, seed: int = 42, n_markets: int = 60):
        # NOTE: do not call super().__init__ with read_only handling beyond
        # what we need; we still want the rate limiter to exist so .wait()
        # calls are no-ops.
        super().__init__(read_only=True)
        self._rng = _random.Random(seed)
        self._n_markets = n_markets
        self._markets: Optional[list[Market]] = None
        self._books: dict[str, Orderbook] = {}
        self._call_count = 0

    # ── No-op auth ─────────────────────────────────────────────────────────
    def init_clob(self) -> bool:
        log.info("Mock client: synthetic data, no CLOB needed.")
        return False

    # ── Synthetic market catalog (cached after first call) ─────────────────
    def list_markets(self, *, closing_within_days: float = 7.0,
                     min_volume_24h: float = 0.0,
                     limit_per_page: int = 500,
                     max_total: int = 5000) -> list[Market]:
        self._call_count += 1
        if self._markets is not None:
            # Perturb a fraction of token prices each cycle to create
            # "hot event" movers, and intentionally seed a few arbitrage
            # opportunities (Yes + No != 1.0) on later cycles.
            for i, m in enumerate(self._markets):
                if len(m.tokens) != 2:
                    continue
                # ~25% of markets see a noticeable price move per cycle
                if self._rng.random() < 0.25:
                    delta = (self._rng.random() - 0.5) * 0.08
                    new_p = max(0.02, min(0.98, m.tokens[0].price + delta))
                    # Slight arbitrage gap on ~10% of moved markets
                    if self._rng.random() < 0.10:
                        gap = (self._rng.random() - 0.5) * 0.06
                        m.tokens[0] = Token(
                            token_id=m.tokens[0].token_id,
                            outcome=m.tokens[0].outcome,
                            price=round(new_p, 4))
                        m.tokens[1] = Token(
                            token_id=m.tokens[1].token_id,
                            outcome=m.tokens[1].outcome,
                            price=round(1.0 - new_p + gap, 4))
                    else:
                        m.tokens[0] = Token(
                            token_id=m.tokens[0].token_id,
                            outcome=m.tokens[0].outcome,
                            price=round(new_p, 4))
                        m.tokens[1] = Token(
                            token_id=m.tokens[1].token_id,
                            outcome=m.tokens[1].outcome,
                            price=round(1.0 - new_p, 4))
                    # Refresh orderbooks to reflect new mid
                    for t in m.tokens:
                        self._books[t.token_id] = self._make_book(
                            t.price, self._rng)
                # Volume fluctuates a bit too
                if self._rng.random() < 0.30:
                    m.volume_24h = max(0.0, m.volume_24h *
                        self._rng.uniform(0.7, 1.5))
            return list(self._markets)

        now_utc = datetime.now(timezone.utc)
        markets: list[Market] = []
        rng = self._rng

        for i in range(self._n_markets):
            category, template = rng.choice(self.QUESTION_TEMPLATES)
            question = template.format(
                team_a=rng.choice(self.TEAMS),
                team_b=rng.choice(self.TEAMS),
                league=rng.choice(self.LEAGUES),
                date_label=rng.choice(self.DATES),
                price_k=rng.randint(40, 200),
                ratio=round(rng.uniform(1.0, 6.0), 2),
                q=rng.randint(1, 4),
            )

            # Days-to-resolution distribution:
            # 70% inside window, 15% too close, 15% too far
            bucket = rng.random()
            if bucket < 0.70:
                days = rng.uniform(Config.DAYS_TO_RESOLUTION_MIN + 0.1,
                                   Config.DAYS_TO_RESOLUTION_MAX - 0.1)
            elif bucket < 0.85:
                days = rng.uniform(0.01, Config.DAYS_TO_RESOLUTION_MIN - 0.01)
            else:
                days = rng.uniform(Config.DAYS_TO_RESOLUTION_MAX + 0.5,
                                   Config.DAYS_TO_RESOLUTION_MAX + 14)
            end_date = now_utc + timedelta(days=days)

            # Dominant price distribution:
            # 55% in band, 25% above (too certain), 20% below (not certain)
            r = rng.random()
            if r < 0.55:
                dom_price = round(rng.uniform(
                    Config.DOMINANT_PRICE_MIN + 0.005,
                    Config.DOMINANT_PRICE_MAX - 0.005), 4)
            elif r < 0.80:
                dom_price = round(rng.uniform(0.975, 0.998), 4)
            else:
                dom_price = round(rng.uniform(0.50, 0.84), 4)

            # Other token = complement (binary market)
            other_price = round(1.0 - dom_price, 4)

            yes_first = rng.random() < 0.5
            tokens = [
                Token(token_id=f"mock_{i}_yes",
                      outcome="Yes",
                      price=dom_price if yes_first else other_price),
                Token(token_id=f"mock_{i}_no",
                      outcome="No",
                      price=other_price if yes_first else dom_price),
            ]

            volume_24h = (rng.uniform(Config.MIN_VOLUME_24H_USD, 250_000)
                          if rng.random() < 0.80
                          else rng.uniform(0, Config.MIN_VOLUME_24H_USD * 0.8))

            closed = rng.random() < 0.05

            m = Market(
                condition_id=f"mock_cond_{i}",
                question=question,
                end_date=end_date,
                category=category,
                volume_24h=volume_24h,
                tokens=tokens,
                closed=closed,
                raw={"mock": True, "i": i},
            )
            markets.append(m)

            # Pre-generate orderbooks for the dominant tokens
            for t in tokens:
                self._books[t.token_id] = self._make_book(t.price, rng)

        self._markets = markets
        log.info("Mock client: generated %d synthetic markets.", len(markets))
        return list(markets)

    def _make_book(self, mid: float, rng: _random.Random) -> Orderbook:
        """Synthesize an orderbook around `mid` with varied spread/depth."""
        # Spread distribution:
        # 70% tight (within MAX_SPREAD), 30% wide
        if rng.random() < 0.70:
            half_spread = rng.uniform(0.001, Config.MAX_SPREAD / 2)
        else:
            half_spread = rng.uniform(Config.MAX_SPREAD * 0.7,
                                      Config.MAX_SPREAD * 3)

        best_ask = round(min(0.999, mid + half_spread), 4)
        best_bid = round(max(0.001, mid - half_spread), 4)

        # Depth distribution:
        # 70% deep, 30% thin
        if rng.random() < 0.70:
            base_size = rng.uniform(150, 2000)   # USDC notional units
        else:
            base_size = rng.uniform(5, 100)

        # Build 6 ask levels stepping up, 6 bid levels stepping down
        asks = []
        for k in range(6):
            p = round(min(0.999, best_ask + k * 0.005), 4)
            s = round(base_size / (best_ask or 1) * rng.uniform(0.6, 1.4), 2)
            asks.append((p, s))
        bids = []
        for k in range(6):
            p = round(max(0.001, best_bid - k * 0.005), 4)
            s = round(base_size / (best_bid or 1) * rng.uniform(0.6, 1.4), 2)
            bids.append((p, s))

        return Orderbook(token_id="", bids=bids, asks=asks)

    def get_orderbook(self, token_id: str, levels: int = 10
                      ) -> Optional[Orderbook]:
        # ~5% of tokens have no orderbook (so we exercise that rejection)
        if self._rng.random() < 0.05:
            return None
        ob = self._books.get(token_id)
        if ob is None:
            return None
        # Stamp token_id on a fresh copy
        return Orderbook(token_id=token_id, bids=list(ob.bids),
                         asks=list(ob.asks))

    # ── No real order placement in mock mode ───────────────────────────────
    def get_my_position(self, token_id: str) -> float:
        return 0.0


# ╔══════════════════════════════════════════════════════════════════════════════╗
# ║                          5. MARKET FILTER                                   ║
# ╚══════════════════════════════════════════════════════════════════════════════╝

class MarketFilter:
    """
    Selects decay-candidate markets.  Filters in order:
      1. end_date in [now+min, now+max]
      2. dominant token price in band
      3. volume threshold
      4. spread threshold
      5. book depth threshold
      6. category in allowlist, NOT in denylist
    Returns ranked list of DecayCandidate.
    """

    def __init__(self, client: PolymarketClient):
        self.client = client
        self.last_markets: list[Market] = []

    @staticmethod
    def _category_ok(cat: str) -> bool:
        if not cat:
            return False  # be conservative: no category → skip
        cat_lower = cat.lower()
        for deny in Config.CATEGORY_DENYLIST:
            if deny.lower() in cat_lower:
                return False
        for allow in Config.CATEGORY_ALLOWLIST:
            if allow.lower() in cat_lower:
                return True
        return False

    def find_candidates(self) -> tuple[list[DecayCandidate], list[dict], dict]:
        """Return (ranked candidates, rejection rows, filter_funnel).

        Rejection rows describe markets that failed a gate so the
        dashboard can show *why* the universe shrinks. filter_funnel
        captures the count of markets surviving each gate in sequence.
        """
        log.info("Fetching markets resolving within %.1f days...",
                 Config.DAYS_TO_RESOLUTION_MAX)
        markets = self.client.list_markets(
            closing_within_days=Config.DAYS_TO_RESOLUTION_MAX,
            min_volume_24h=Config.MIN_VOLUME_24H_USD,
        )
        self.last_markets = markets
        log.info("Pulled %d candidate markets from Gamma.", len(markets))

        now_utc = datetime.now(timezone.utc)
        candidates: list[DecayCandidate] = []
        rejections: list[dict] = []
        funnel = {
            "fetched": len(markets),
            "open": 0,
            "category_ok": 0,
            "days_window": 0,
            "price_band": 0,
            "orderbook_ok": 0,
            "spread_ok": 0,
            "depth_ok": 0,
            "edge_positive": 0,
            "ann_edge_ok": 0,
        }

        def reject(q: str, reason: str, value=None, limit=None):
            if len(rejections) >= DashboardState.MAX_REJECTIONS:
                return
            rejections.append({
                "question": (q or "")[:120],
                "reason": reason,
                "value": value,
                "limit": limit,
            })

        for m in markets:
            if m.closed:
                reject(m.question, "closed")
                continue
            funnel["open"] += 1

            if not self._category_ok(m.category):
                reject(m.question, "category_blocked", value=m.category)
                continue
            funnel["category_ok"] += 1

            days = (m.end_date - now_utc).total_seconds() / 86400.0
            if days < Config.DAYS_TO_RESOLUTION_MIN:
                reject(m.question, "too_close_to_resolution",
                       value=round(days, 3), limit=Config.DAYS_TO_RESOLUTION_MIN)
                continue
            if days > Config.DAYS_TO_RESOLUTION_MAX:
                reject(m.question, "too_far_from_resolution",
                       value=round(days, 3), limit=Config.DAYS_TO_RESOLUTION_MAX)
                continue
            funnel["days_window"] += 1

            # Identify dominant token (highest current price)
            if not m.tokens:
                reject(m.question, "no_tokens")
                continue
            dominant = max(m.tokens, key=lambda t: t.price)
            if not (Config.DOMINANT_PRICE_MIN <= dominant.price
                    <= Config.DOMINANT_PRICE_MAX):
                reject(m.question, "price_out_of_band",
                       value=round(dominant.price, 4),
                       limit=[Config.DOMINANT_PRICE_MIN, Config.DOMINANT_PRICE_MAX])
                continue
            funnel["price_band"] += 1

            # Pull orderbook for liquidity & spread checks
            ob = self.client.get_orderbook(dominant.token_id)
            if ob is None or ob.spread is None:
                reject(m.question, "no_orderbook")
                continue
            if ob.best_ask is None:
                reject(m.question, "no_ask")
                continue
            funnel["orderbook_ok"] += 1

            if ob.spread > Config.MAX_SPREAD:
                reject(m.question, "spread_too_wide",
                       value=round(ob.spread, 4), limit=Config.MAX_SPREAD)
                continue
            funnel["spread_ok"] += 1

            depth = ob.fillable_usdc_at_or_below(
                ob.best_ask + Config.SLIPPAGE_TOLERANCE
            )
            if depth < Config.MIN_BOOK_DEPTH_USD:
                reject(m.question, "book_too_thin",
                       value=round(depth, 2), limit=Config.MIN_BOOK_DEPTH_USD)
                continue
            funnel["depth_ok"] += 1

            edge = (1.0 - ob.best_ask) - Config.DISPUTE_BUFFER
            if edge <= 0:
                reject(m.question, "no_edge_after_buffer",
                       value=round(edge, 4))
                continue
            funnel["edge_positive"] += 1

            ann = (edge / ob.best_ask) * (365.0 / max(days, 0.01))
            if ann < Config.MIN_ANNUALIZED_EDGE:
                reject(m.question, "ann_edge_too_low",
                       value=round(ann, 3), limit=Config.MIN_ANNUALIZED_EDGE)
                continue
            funnel["ann_edge_ok"] += 1

            score = ann  # rank purely by annualized edge for v1
            candidates.append(DecayCandidate(
                market=m,
                dominant_token=Token(
                    token_id=dominant.token_id,
                    outcome=dominant.outcome,
                    price=ob.best_ask,
                ),
                edge=edge,
                annualized_edge=ann,
                days_to_resolution=days,
                spread=ob.spread,
                book_depth_usd=depth,
                score=score,
            ))

        candidates.sort(key=lambda c: -c.score)
        log.info("Found %d decay candidates after filtering (%d rejections).",
                 len(candidates), len(rejections))
        return candidates, rejections, funnel


# ╔══════════════════════════════════════════════════════════════════════════════╗
# ║                          6. DECAY STRATEGY                                  ║
# ╚══════════════════════════════════════════════════════════════════════════════╝

class DecayStrategy:
    """
    Pure-function entry/exit decisions.
    No I/O.  Inputs in, decisions out.
    """

    NAME = "polymarket_decay"

    @staticmethod
    def evaluate_entry(candidate: DecayCandidate,
                       portfolio_value: float) -> Optional[Decision]:
        """Return a BUY Decision or None."""
        # Quarter-Kelly sizing on edge
        kelly_size = candidate.edge * Config.KELLY_FRACTION * portfolio_value
        size_usdc = min(
            Config.MAX_POSITION_USD,
            kelly_size,
            candidate.book_depth_usd,
        )
        if size_usdc < Config.MIN_TRADE_USD:
            return None

        return Decision(
            token_id=candidate.dominant_token.token_id,
            side="BUY",
            usdc_amount=round(size_usdc, 2),
            limit_price=round(candidate.dominant_token.price, 4),
            market_question=candidate.market.question,
            notes=(
                f"edge={candidate.edge:.3f} "
                f"ann={candidate.annualized_edge:.2f} "
                f"days={candidate.days_to_resolution:.2f}"
            ),
        )

    @staticmethod
    def evaluate_exit(state: PositionState,
                      current_price: float,
                      now_utc: datetime,
                      market_end_date: datetime) -> Optional[Decision]:
        """Return a CLOSE Decision or None."""
        # Emergency exit: regime change indicator
        price_drop = state.entry_price - current_price
        if price_drop >= Config.EMERGENCY_EXIT_DROP:
            return Decision(
                token_id=state.token_id,
                side="CLOSE",
                usdc_amount=state.entry_shares * current_price,
                limit_price=round(current_price, 4),
                market_question=state.market_question,
                notes=f"emergency_exit: drop={price_drop:.3f}",
            )

        # Pre-resolution sweep: if very close to resolution, hold (oracle resolves)
        # If price is essentially 1.0, we already won — wait for resolution payout.
        return None


# ╔══════════════════════════════════════════════════════════════════════════════╗
# ║                          7. RISK MANAGER                                    ║
# ╚══════════════════════════════════════════════════════════════════════════════╝

class RiskManager:
    """
    Strategy-agnostic.  Enforces:
      - Per-trade entry tracking (fixes bug #4)
      - Rolling daily anchor at 00:00 UTC (fixes bug #5)
      - Position caps & deployment limits
      - Two-tier kill switch (10% halt new, 20% halt all)
    """

    def __init__(self, journal: Journal,
                 store: Optional["PersistentStore"] = None):
        self.journal = journal
        self.store = store
        self.positions: dict[str, PositionState] = {}
        self.equity_at_utc_midnight: Optional[float] = None
        self._anchor_date: Optional[str] = None
        self._halted_hard: bool = False
        if store is not None:
            self._restore()

    # ── Persistence ─────────────────────────────────────────────────────────
    def _restore(self) -> None:
        positions_raw = self.store.get("positions", {}) or {}
        restored = 0
        for token_id, p in positions_raw.items():
            try:
                self.positions[token_id] = PositionState(
                    token_id=p["token_id"],
                    market_question=p.get("market_question", ""),
                    entry_timestamp=datetime.fromisoformat(p["entry_timestamp"]),
                    entry_price=float(p["entry_price"]),
                    entry_shares=float(p["entry_shares"]),
                    entry_usdc=float(p["entry_usdc"]),
                    strategy=p.get("strategy", ""),
                )
                restored += 1
            except Exception as e:
                log.error("Skipping corrupt position %s: %s", token_id, e)
        anchor = self.store.get("anchor") or {}
        self.equity_at_utc_midnight = anchor.get("equity")
        self._anchor_date = anchor.get("date")
        self._halted_hard = bool(anchor.get("halted_hard", False))
        if restored or self._anchor_date:
            log.info("RiskManager restored: %d positions, anchor=%s "
                     "halted_hard=%s", restored, self._anchor_date,
                     self._halted_hard)

    def _persist(self) -> None:
        if self.store is None:
            return
        self.store.put_many({
            "positions": {
                tid: {
                    "token_id": p.token_id,
                    "market_question": p.market_question,
                    "entry_timestamp": p.entry_timestamp.isoformat(),
                    "entry_price": p.entry_price,
                    "entry_shares": p.entry_shares,
                    "entry_usdc": p.entry_usdc,
                    "strategy": p.strategy,
                } for tid, p in self.positions.items()
            },
            "anchor": {
                "equity": self.equity_at_utc_midnight,
                "date": self._anchor_date,
                "halted_hard": self._halted_hard,
            },
        })

    # ── Daily anchor ────────────────────────────────────────────────────────
    def update_daily_anchor_if_needed(self, current_equity: float):
        today_str = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        if self._anchor_date != today_str or self.equity_at_utc_midnight is None:
            self.equity_at_utc_midnight = current_equity
            self._anchor_date = today_str
            log.info("Daily anchor reset: equity=$%.2f date=%s",
                     current_equity, today_str)
            self._persist()

    # ── Kill switch ─────────────────────────────────────────────────────────
    def kill_switch_status(self, current_equity: float) -> str:
        """Returns 'OK', 'HALT_NEW', or 'HALT_ALL'."""
        if self._halted_hard:
            return "HALT_ALL"
        if self.equity_at_utc_midnight is None or self.equity_at_utc_midnight <= 0:
            return "OK"
        loss_pct = (self.equity_at_utc_midnight - current_equity) / self.equity_at_utc_midnight
        if loss_pct >= Config.HARD_HALT_LOSS_PCT:
            if not self._halted_hard:
                self._halted_hard = True
                self._persist()
            log.error("HARD HALT: daily loss %.1f%% >= %.1f%%. Manual intervention needed.",
                      loss_pct * 100, Config.HARD_HALT_LOSS_PCT * 100)
            return "HALT_ALL"
        if loss_pct >= Config.DAILY_LOSS_LIMIT_PCT:
            log.warning("HALT_NEW: daily loss %.1f%% >= %.1f%%. Existing positions still managed.",
                        loss_pct * 100, Config.DAILY_LOSS_LIMIT_PCT * 100)
            return "HALT_NEW"
        return "OK"

    # ── Position tracking ───────────────────────────────────────────────────
    def record_entry(self, *, decision: Decision, fill_price: float,
                     fill_shares: float, strategy_name: str):
        state = PositionState(
            token_id=decision.token_id,
            market_question=decision.market_question,
            entry_timestamp=datetime.now(timezone.utc),
            entry_price=fill_price,
            entry_shares=fill_shares,
            entry_usdc=fill_price * fill_shares,
            strategy=strategy_name,
        )
        self.positions[decision.token_id] = state
        self._persist()
        log.info("Recorded entry: token=%s... shares=%.4f @ %.4f",
                 decision.token_id[:12], fill_shares, fill_price)

    def record_exit(self, token_id: str, exit_price: float):
        st = self.positions.pop(token_id, None)
        if st:
            self._persist()
            pnl = (exit_price - st.entry_price) * st.entry_shares
            log.info("Closed position: token=%s... pnl=$%.2f (%.1f%%)",
                     token_id[:12], pnl,
                     (exit_price / st.entry_price - 1) * 100 if st.entry_price else 0)

    # ── Caps ────────────────────────────────────────────────────────────────
    def can_open_new(self, current_deployed_usdc: float,
                     portfolio_value: float) -> bool:
        if len(self.positions) >= Config.MAX_CONCURRENT_POSITIONS:
            return False
        if current_deployed_usdc / max(portfolio_value, 1.0) >= Config.MAX_TOTAL_DEPLOYED_PCT:
            return False
        return True

    def cap_position_size(self, requested_usdc: float,
                          portfolio_value: float,
                          current_deployed: float) -> float:
        remaining_budget = (
            portfolio_value * Config.MAX_TOTAL_DEPLOYED_PCT - current_deployed
        )
        return max(
            0.0,
            min(requested_usdc, Config.MAX_POSITION_USD, remaining_budget),
        )

    def deployed_usdc(self) -> float:
        return sum(p.entry_usdc for p in self.positions.values())


# ╔══════════════════════════════════════════════════════════════════════════════╗
# ║                          8. EXECUTION ENGINE                                ║
# ╚══════════════════════════════════════════════════════════════════════════════╝

class ExecutionEngine:
    """Strategy-agnostic order placement with slippage guard."""

    def __init__(self, client: BrokerClient, journal: Journal,
                 risk: RiskManager):
        self.client = client
        self.journal = journal
        self.risk = risk

    def _poll_until_settled(self, order_id: str, decision: "Decision",
                            strategy_name: str) -> Optional[dict]:
        """Poll order status until the broker reports it filled or cancelled,
        or ORDER_FILL_TIMEOUT_SECONDS elapses. Returns a dict with filled_size
        and avg_price on a (possibly partial) fill; None on cancellation,
        timeout, or unrecoverable status."""
        deadline = time.monotonic() + Config.ORDER_FILL_TIMEOUT_SECONDS
        last_status = None
        while time.monotonic() < deadline:
            status = self.client.get_order_status(order_id)
            if status:
                last_status = status
                st = status.get("status", "")
                filled = float(status.get("filled_size", 0))
                if st in ("filled", "matched", "complete"):
                    return {"filled_size": filled,
                            "avg_price": status.get("avg_price",
                                                    decision.limit_price)}
                if st in ("cancelled", "canceled", "rejected", "expired"):
                    log.warning("Order %s ended in status=%s (filled=%.2f)",
                                order_id, st, filled)
                    self.journal.record(
                        event="order_failed",
                        strategy=strategy_name,
                        market_question=decision.market_question,
                        token_id=decision.token_id,
                        side=decision.side,
                        price=decision.limit_price,
                        usdc_amount=decision.usdc_amount,
                        notes=f"order ended status={st} filled={filled}",
                    )
                    # If we got a partial fill, return what we got
                    if filled > 0:
                        return {"filled_size": filled,
                                "avg_price": status.get("avg_price",
                                                        decision.limit_price)}
                    return None
            time.sleep(2)
        log.warning("Order %s did not settle within %ds (last=%s)",
                    order_id, Config.ORDER_FILL_TIMEOUT_SECONDS, last_status)
        self.journal.record(
            event="order_timeout",
            strategy=strategy_name,
            market_question=decision.market_question,
            token_id=decision.token_id,
            side=decision.side,
            price=decision.limit_price,
            usdc_amount=decision.usdc_amount,
            notes=f"order_id={order_id}",
        )
        # Return any partial fill we may have observed
        if last_status and float(last_status.get("filled_size", 0)) > 0:
            return {"filled_size": float(last_status["filled_size"]),
                    "avg_price": last_status.get("avg_price",
                                                 decision.limit_price)}
        return None

    def place(self, decision: Decision, strategy_name: str) -> bool:
        # Intent log first
        self.journal.record(
            event="intent",
            strategy=strategy_name,
            market_question=decision.market_question,
            token_id=decision.token_id,
            side=decision.side,
            price=decision.limit_price,
            usdc_amount=decision.usdc_amount,
            dry_run=Config.DRY_RUN,
            notes=decision.notes,
        )

        # Slippage guard for BUY
        if decision.side == "BUY":
            ob = self.client.get_orderbook(decision.token_id)
            if ob is None or ob.best_ask is None:
                log.warning("No orderbook for %s, aborting trade",
                            decision.token_id[:12])
                return False
            if ob.best_ask > decision.limit_price + Config.SLIPPAGE_TOLERANCE:
                log.warning(
                    "Slippage abort: ask=%.4f > limit=%.4f + tol",
                    ob.best_ask, decision.limit_price,
                )
                self.journal.record(
                    event="slippage_abort",
                    strategy=strategy_name,
                    market_question=decision.market_question,
                    token_id=decision.token_id,
                    side=decision.side,
                    price=ob.best_ask,
                    notes=f"limit={decision.limit_price}",
                )
                return False

            resp = self.client.buy_token(
                decision.token_id,
                usdc_amount=decision.usdc_amount,
                limit_price=decision.limit_price,
            )
        elif decision.side == "CLOSE":
            state = self.risk.positions.get(decision.token_id)
            if not state:
                log.warning("CLOSE requested but no position for %s",
                            decision.token_id[:12])
                return False
            resp = self.client.close_position(
                decision.token_id,
                shares=state.entry_shares,
                limit_price=decision.limit_price,
            )
        else:
            log.error("Unknown decision side: %s", decision.side)
            return False

        if resp is None:
            self.journal.record(
                event="order_failed",
                strategy=strategy_name,
                market_question=decision.market_question,
                token_id=decision.token_id,
                side=decision.side,
                price=decision.limit_price,
                usdc_amount=decision.usdc_amount,
                dry_run=Config.DRY_RUN,
                notes=decision.notes,
            )
            return False

        # Confirm the fill — limit orders are GTC and may rest unfilled.
        # Only the live (non-dry-run) path needs polling; dry-run resp is
        # synthetic and already represents the intended fill.
        if not Config.DRY_RUN and isinstance(resp, dict) and resp.get("orderID"):
            fill_status = self._poll_until_settled(
                resp["orderID"], decision, strategy_name)
            if fill_status is None:
                return False
            fill_shares = fill_status["filled_size"]
            fill_price = fill_status.get("avg_price", decision.limit_price)
        else:
            # Dry-run or no order-id: trust the synthetic response.
            fill_shares = float(resp.get("shares", 0)) or (
                decision.usdc_amount / decision.limit_price
            )
            fill_price = float(resp.get("price", decision.limit_price))

        self.journal.record(
            event="filled" if not Config.DRY_RUN else "dry_filled",
            strategy=strategy_name,
            market_question=decision.market_question,
            token_id=decision.token_id,
            side=decision.side,
            price=fill_price,
            shares=fill_shares,
            usdc_amount=fill_price * fill_shares,
            dry_run=Config.DRY_RUN,
            notes=decision.notes,
            extra={"resp": resp},
        )

        # Update risk state
        if decision.side == "BUY":
            self.risk.record_entry(
                decision=decision,
                fill_price=fill_price,
                fill_shares=fill_shares,
                strategy_name=strategy_name,
            )
        else:
            self.risk.record_exit(decision.token_id, fill_price)

        return True


# ╔══════════════════════════════════════════════════════════════════════════════╗
# ║                          8b. DASHBOARD HTTP SERVER                          ║
# ╚══════════════════════════════════════════════════════════════════════════════╝

from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import urlparse, parse_qs


DASHBOARD_HTML = r"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8">
<title>Polymarket Decay — Dashboard</title>
<style>
  :root {
    --bg: #0a0e16; --panel: #131a26; --panel-2: #1a2333;
    --line: #243349; --txt: #e2ecfb; --muted: #7c8aa3; --dim: #5b6a85;
    --green: #4ade80; --red: #f87171; --yellow: #facc15;
    --blue: #60a5fa; --purple: #c084fc; --orange: #fb923c; --pink: #f472b6;
    --cyan: #22d3ee;
  }
  * { box-sizing: border-box; }
  body {
    margin: 0; font-family: -apple-system, BlinkMacSystemFont, "SF Pro Text",
      Inter, system-ui, sans-serif;
    background: var(--bg); color: var(--txt); font-size: 13px; line-height: 1.4;
  }
  /* Header */
  header {
    display: flex; align-items: center; gap: 14px; flex-wrap: wrap;
    padding: 10px 18px; background: var(--panel);
    border-bottom: 1px solid var(--line);
    position: sticky; top: 0; z-index: 20;
  }
  header h1 { font-size: 14px; margin: 0; font-weight: 700; letter-spacing: .4px; }
  .pill {
    padding: 3px 9px; border-radius: 999px; font-size: 10px;
    font-weight: 700; text-transform: uppercase; letter-spacing: .6px;
  }
  .pill.green { background: rgba(74,222,128,.15); color: var(--green); }
  .pill.red   { background: rgba(248,113,113,.15); color: var(--red); }
  .pill.yellow{ background: rgba(250,204,21,.15); color: var(--yellow); }
  .pill.blue  { background: rgba(96,165,250,.15); color: var(--blue); }
  .pill.purple{ background: rgba(192,132,252,.15); color: var(--purple); }
  .pill.gray  { background: rgba(255,255,255,.05); color: var(--muted); }
  .hstat { display: flex; flex-direction: column; line-height: 1.1; }
  .hstat .label { color: var(--muted); font-size: 9px; text-transform: uppercase;
    letter-spacing: .6px; }
  .hstat .val { font-size: 13px; font-weight: 700; font-variant-numeric: tabular-nums; }
  /* Tabs */
  nav.tabs {
    display: flex; gap: 2px; padding: 0 12px; background: var(--panel);
    border-bottom: 1px solid var(--line);
    position: sticky; top: 56px; z-index: 15;
  }
  nav.tabs button {
    background: none; border: none; color: var(--muted); font-weight: 600;
    font-size: 12px; padding: 10px 16px; cursor: pointer; letter-spacing: .4px;
    text-transform: uppercase; border-bottom: 2px solid transparent;
  }
  nav.tabs button:hover { color: var(--txt); }
  nav.tabs button.active { color: var(--blue);
    border-bottom-color: var(--blue); }
  nav.tabs .counter { color: var(--dim); font-weight: 500; font-size: 10px;
    margin-left: 6px; }
  /* Sub-tabs (for Strategies) */
  .subtabs {
    display: flex; gap: 2px; padding: 6px 10px;
    background: var(--panel-2); border-bottom: 1px solid var(--line);
  }
  .subtabs button {
    background: rgba(255,255,255,.03); border: 1px solid var(--line);
    color: var(--muted); font-weight: 600; font-size: 11px;
    padding: 5px 12px; cursor: pointer; border-radius: 6px;
    text-transform: uppercase; letter-spacing: .4px;
  }
  .subtabs button:hover { background: rgba(255,255,255,.06); color: var(--txt); }
  .subtabs button.active { background: var(--blue); color: #0a0e16;
    border-color: var(--blue); }
  /* Buttons */
  .btn {
    padding: 6px 14px; border-radius: 6px; font-size: 11px; font-weight: 600;
    border: 1px solid var(--line); background: var(--panel-2); color: var(--txt);
    cursor: pointer; letter-spacing: .3px; text-transform: uppercase;
  }
  .btn:hover { background: #243349; }
  .btn.primary { background: var(--blue); color: #0a0e16; border-color: var(--blue); }
  .btn.primary:hover { background: #93c5fd; }
  .btn.warn { background: var(--yellow); color: #0a0e16; border-color: var(--yellow); }
  .btn:disabled { opacity: .4; cursor: not-allowed; }
  .toolbar { display: flex; gap: 8px; margin-left: auto; align-items: center; }
  /* Layout */
  main { padding: 12px; display: flex; flex-direction: column; gap: 12px; }
  .row { display: grid; gap: 12px; }
  .row.two   { grid-template-columns: 1fr 1fr; }
  .row.three { grid-template-columns: repeat(3, 1fr); }
  .row.four  { grid-template-columns: repeat(4, 1fr); }
  .row.six   { grid-template-columns: repeat(6, 1fr); }
  .panel {
    background: var(--panel); border: 1px solid var(--line); border-radius: 8px;
    overflow: hidden; display: flex; flex-direction: column;
  }
  .panel h2 {
    margin: 0; padding: 9px 14px; font-size: 11px; font-weight: 700;
    color: var(--muted); text-transform: uppercase; letter-spacing: .7px;
    border-bottom: 1px solid var(--line); background: var(--panel-2);
    display: flex; justify-content: space-between; align-items: center; gap: 10px;
  }
  .panel h2 .count { color: var(--blue); font-weight: 800; }
  .panel h2 .hint { color: var(--dim); font-weight: 500;
    text-transform: none; letter-spacing: 0; font-size: 10px; }
  .panel .body { overflow-y: auto; max-height: 520px; }
  /* Tab visibility */
  .tab-content { display: none; flex-direction: column; gap: 12px; }
  .tab-content.active { display: flex; }
  /* Stat cards */
  .card {
    background: var(--panel); border: 1px solid var(--line); border-radius: 8px;
    padding: 12px 14px; display: flex; flex-direction: column; gap: 2px;
  }
  .card .k { color: var(--muted); font-size: 10px; text-transform: uppercase;
    letter-spacing: .6px; }
  .card .v { font-size: 22px; font-weight: 700; font-variant-numeric: tabular-nums; }
  .card .sub { color: var(--dim); font-size: 10px; }
  /* Tables */
  table { width: 100%; border-collapse: collapse;
    font-variant-numeric: tabular-nums; }
  th, td { padding: 6px 12px; text-align: left; font-size: 12px; }
  th { color: var(--muted); font-size: 10px; text-transform: uppercase;
       letter-spacing: .5px; font-weight: 700; background: var(--panel-2);
       position: sticky; top: 0; }
  tr.row-click { cursor: pointer; }
  tr.row-click:hover { background: rgba(96,165,250,.06); }
  tr.row-click.sel { background: rgba(96,165,250,.12); }
  tr { border-bottom: 1px solid rgba(255,255,255,.03); }
  td.num { text-align: right; font-feature-settings: "tnum"; }
  td.q { max-width: 380px; overflow: hidden; text-overflow: ellipsis;
    white-space: nowrap; }
  .pos { color: var(--green); }
  .neg { color: var(--red); }
  /* Events */
  .ev { padding: 6px 14px; font-size: 12px;
    border-bottom: 1px solid rgba(255,255,255,.03); }
  .ev .t { color: var(--muted); font-size: 10px; }
  .ev .e { font-weight: 700; }
  .ev .e.intent { color: var(--blue); }
  .ev .e.filled, .ev .e.dry_filled { color: var(--green); }
  .ev .e.order_failed, .ev .e.slippage_abort { color: var(--red); }
  /* Rejection chips */
  .reject-summary { display: flex; flex-wrap: wrap; gap: 6px;
    padding: 10px 14px; }
  .chip { background: rgba(255,255,255,.04); border: 1px solid var(--line);
    padding: 3px 9px; border-radius: 999px; font-size: 11px;
    cursor: pointer; user-select: none; }
  .chip:hover { background: rgba(255,255,255,.08); }
  .chip.sel { background: rgba(250,204,21,.15); color: var(--yellow);
    border-color: rgba(250,204,21,.4); }
  .chip b { color: var(--yellow); margin-left: 4px; }
  .reject-row { display: grid; grid-template-columns: 1fr auto;
    padding: 4px 14px; font-size: 12px; gap: 8px;
    border-bottom: 1px solid rgba(255,255,255,.03); }
  .reject-row .reason { color: var(--yellow); font-size: 11px;
    font-variant-numeric: tabular-nums; }
  /* Funnel */
  .funnel-row { display: grid; grid-template-columns: 140px 1fr 60px;
    gap: 10px; padding: 5px 14px; align-items: center; font-size: 12px; }
  .funnel-row .label { color: var(--muted); font-size: 11px; }
  .funnel-row .bar { height: 8px; background: rgba(96,165,250,.1);
    border-radius: 3px; overflow: hidden; }
  .funnel-row .bar .fill { height: 100%; background: var(--blue);
    border-radius: 3px; transition: width .3s; }
  .funnel-row .num { text-align: right; font-variant-numeric: tabular-nums; }
  /* Horizontal bar chart */
  .hbar-row { display: grid; grid-template-columns: 1fr 60px;
    gap: 8px; padding: 4px 14px; align-items: center; font-size: 12px; }
  .hbar-row .lbl { color: var(--txt); font-size: 11px; }
  .hbar-row .num { text-align: right; font-variant-numeric: tabular-nums;
    color: var(--muted); }
  .hbar-row .bg {
    grid-column: 1 / -1; height: 8px; background: rgba(96,165,250,.08);
    border-radius: 3px; overflow: hidden; margin-top: 2px;
  }
  .hbar-row .bg .fill { height: 100%; background: var(--blue); border-radius: 3px; }
  .hbar-row.color-purple .bg .fill { background: var(--purple); }
  .hbar-row.color-orange .bg .fill { background: var(--orange); }
  .hbar-row.color-green  .bg .fill { background: var(--green); }
  .hbar-row.color-cyan   .bg .fill { background: var(--cyan); }
  /* Config form */
  .kform { padding: 8px 14px 14px; }
  .kform .row-k {
    display: grid; grid-template-columns: 200px 1fr 70px;
    gap: 10px; padding: 4px 0; align-items: center;
    border-bottom: 1px dashed rgba(255,255,255,.04);
  }
  .kform .row-k label { color: var(--muted); font-size: 11px;
    font-family: ui-monospace, SFMono-Regular, monospace; }
  .kform .row-k input[type=range] { width: 100%; accent-color: var(--blue); }
  .kform .row-k .val { text-align: right; font-variant-numeric: tabular-nums;
    color: var(--blue); font-weight: 700; }
  .kform-actions { padding: 10px 14px; border-top: 1px solid var(--line);
    display: flex; gap: 8px; justify-content: flex-end; }
  /* Detail */
  .detail-grid { display: grid; grid-template-columns: 1fr 1fr 1fr;
    gap: 12px; padding: 14px; }
  .detail-grid h4 { margin: 0 0 6px; font-size: 11px; color: var(--muted);
    text-transform: uppercase; letter-spacing: .5px; font-weight: 700; }
  .ob-table { font-size: 11px; }
  .ob-table td { padding: 2px 8px; }
  .ob-table .ask { color: var(--red); }
  .ob-table .bid { color: var(--green); }
  .kv { display: grid; grid-template-columns: 1fr auto; gap: 4px 12px;
    font-size: 12px; }
  .kv .k { color: var(--muted); }
  .kv .v { text-align: right; font-variant-numeric: tabular-nums;
    font-weight: 600; }
  /* SVG charts */
  .spark { display: block; width: 100%; height: 64px; }
  .chart-svg { display: block; width: 100%; height: 200px; }
  .chart-svg text { font-family: -apple-system, sans-serif; font-size: 10px;
    fill: var(--muted); }
  /* Strategy description */
  .strat-desc { padding: 10px 14px; font-size: 12px; color: var(--muted);
    background: rgba(96,165,250,.04); border-bottom: 1px solid var(--line); }
  /* Backtest */
  .bt-form { padding: 10px 14px; display: flex; gap: 8px; align-items: center;
    border-bottom: 1px solid var(--line); }
  .bt-form input[type=text] {
    flex: 1; padding: 6px 10px; border-radius: 6px; font-size: 12px;
    border: 1px solid var(--line); background: var(--panel-2); color: var(--txt);
    font-family: ui-monospace, SFMono-Regular, monospace;
  }
  .bt-results { padding: 10px 14px; font-size: 12px; }
  /* Toasts */
  .toast-stack { position: fixed; right: 14px; bottom: 14px;
    display: flex; flex-direction: column; gap: 6px; z-index: 100; }
  .toast { padding: 8px 14px; border-radius: 6px; font-size: 12px;
    background: var(--panel-2); border: 1px solid var(--line);
    box-shadow: 0 4px 12px rgba(0,0,0,.4); }
  .toast.warn { border-color: var(--yellow); }
  .toast.error { border-color: var(--red); }
  .empty { padding: 18px 14px; color: var(--dim); font-style: italic;
    text-align: center; }
  .stale { color: var(--yellow); }
  footer { padding: 8px 18px; color: var(--dim); font-size: 11px;
    border-top: 1px solid var(--line); display: flex;
    justify-content: space-between; gap: 12px; flex-wrap: wrap; }
  .mono { font-family: ui-monospace, SFMono-Regular, monospace; }
  details > summary { padding: 9px 14px; cursor: pointer; user-select: none;
    background: var(--panel-2); border-bottom: 1px solid var(--line);
    font-size: 11px; text-transform: uppercase; letter-spacing: .7px;
    color: var(--muted); font-weight: 700; }
  details > summary::-webkit-details-marker { color: var(--blue); }
</style>
</head>
<body>

<header>
  <h1>Polymarket Decay</h1>
  <span id="broker-pill" class="pill gray">—</span>
  <span id="mode-pill" class="pill yellow">…</span>
  <span id="kill-pill" class="pill green">…</span>
  <span id="mock-pill" class="pill blue" style="display:none">MOCK</span>
  <span id="pause-pill" class="pill gray" style="display:none">PAUSED</span>
  <div class="hstat"><span class="label">Equity</span>
    <span class="val" id="equity">—</span></div>
  <div class="hstat"><span class="label">Deployed</span>
    <span class="val" id="deployed">—</span></div>
  <div class="hstat"><span class="label">Anchor</span>
    <span class="val" id="anchor">—</span></div>
  <div class="hstat"><span class="label">Last cycle</span>
    <span class="val" id="last-cycle">—</span></div>
  <div class="hstat"><span class="label">Next cycle</span>
    <span class="val" id="next-cycle">—</span></div>
  <div class="toolbar">
    <button class="btn primary" id="btn-scan">Scan Now</button>
    <button class="btn" id="btn-pause">Pause</button>
    <span id="ts" class="mono" style="color:var(--dim);font-size:10px;
      margin-left:6px">…</span>
  </div>
</header>

<nav class="tabs">
  <button class="tab active" data-tab="overview">Overview</button>
  <button class="tab" data-tab="strategies">Strategies
    <span class="counter" id="cnt-strats">—</span></button>
  <button class="tab" data-tab="markets">Hot Events
    <span class="counter" id="cnt-hot">—</span></button>
  <button class="tab" data-tab="analytics">Analytics</button>
  <button class="tab" data-tab="rejections">Rejections
    <span class="counter" id="cnt-rej">—</span></button>
  <button class="tab" data-tab="tools">Tools</button>
</nav>

<main>

  <!-- ═══════════════════════════ OVERVIEW TAB ═══════════════════════════ -->
  <div class="tab-content active" id="tab-overview">

    <section class="row six">
      <div class="card"><span class="k">Scans</span>
        <span class="v" id="s-scans">0</span>
        <span class="sub" id="s-uptime">since —</span></div>
      <div class="card"><span class="k">Markets seen</span>
        <span class="v" id="s-markets">0</span>
        <span class="sub" id="s-mkt-now">— this cycle</span></div>
      <div class="card"><span class="k">Candidates (cum.)</span>
        <span class="v" id="s-cands">0</span>
        <span class="sub" id="s-cands-now">— this cycle</span></div>
      <div class="card"><span class="k">Fills</span>
        <span class="v" id="s-fills">0</span>
        <span class="sub" id="s-intents">0 intents</span></div>
      <div class="card"><span class="k">Slippage aborts</span>
        <span class="v" id="s-slip">0</span>
        <span class="sub" id="s-fail">0 failed</span></div>
      <div class="card"><span class="k">Open positions</span>
        <span class="v" id="s-pos">0</span>
        <span class="sub" id="s-pos-pnl">— PnL</span></div>
    </section>

    <section class="panel">
      <h2>Equity (last samples)
        <span class="hint" id="spark-info">—</span></h2>
      <svg class="spark" id="spark"></svg>
    </section>

    <section class="row two">
      <div class="panel">
        <h2>Open Positions <span class="count" id="pos-count">0</span></h2>
        <div class="body">
          <table>
            <thead><tr>
              <th>Question</th><th class="num">Entry</th>
              <th class="num">Mark</th><th class="num">PnL</th>
              <th class="num">%</th>
            </tr></thead>
            <tbody id="pos-body"></tbody>
          </table>
          <div id="pos-empty" class="empty">No open positions.</div>
        </div>
      </div>

      <div class="panel">
        <h2>Top Decay Candidates <span class="count" id="cand-count">0</span>
          <span class="hint">click a row for orderbook</span></h2>
        <div class="body">
          <table>
            <thead><tr>
              <th>Question</th><th>Side</th>
              <th class="num">Px</th><th class="num">Edge</th>
              <th class="num">Ann%</th><th class="num">Days</th>
              <th class="num">Size</th>
            </tr></thead>
            <tbody id="cand-body"></tbody>
          </table>
          <div id="cand-empty" class="empty">No candidates this cycle.</div>
        </div>
      </div>
    </section>

    <section class="panel" id="detail-panel" style="display:none">
      <h2 id="detail-title">Candidate Detail
        <span class="hint" id="detail-close" style="cursor:pointer">close ×</span></h2>
      <div class="detail-grid">
        <div>
          <h4>Yes / No prices</h4>
          <table class="ob-table" id="detail-tokens"></table>
          <div style="height:10px"></div>
          <h4>Edge breakdown</h4>
          <div class="kv" id="detail-edge"></div>
        </div>
        <div>
          <h4>Orderbook — Asks (sell side)</h4>
          <table class="ob-table" id="detail-asks"></table>
          <div style="height:10px"></div>
          <h4>Orderbook — Bids (buy side)</h4>
          <table class="ob-table" id="detail-bids"></table>
        </div>
        <div>
          <h4>Proposed trade</h4>
          <div class="kv" id="detail-trade"></div>
          <div style="height:10px"></div>
          <h4>Market</h4>
          <div class="kv" id="detail-market"></div>
        </div>
      </div>
    </section>

    <section class="panel">
      <h2>Journal Events <span class="count" id="ev-count">0</span>
        <span class="hint">live feed of intents, fills, aborts</span></h2>
      <div class="body" id="ev-body" style="max-height: 360px"></div>
    </section>

  </div>

  <!-- ═══════════════════════════ STRATEGIES TAB ═══════════════════════════ -->
  <div class="tab-content" id="tab-strategies">
    <section class="panel">
      <nav class="subtabs" id="strat-subtabs">
        <button class="active" data-strat="decay">Decay <span id="strat-n-decay" class="counter">0</span></button>
        <button data-strat="mean_reversion">Mean Reversion <span id="strat-n-mean_reversion" class="counter">0</span></button>
        <button data-strat="arbitrage">Arbitrage <span id="strat-n-arbitrage" class="counter">0</span></button>
        <button data-strat="volume_leaders">Volume Leaders <span id="strat-n-volume_leaders" class="counter">0</span></button>
        <button data-strat="closing_soon">Closing Soon <span id="strat-n-closing_soon" class="counter">0</span></button>
      </nav>
      <div class="strat-desc" id="strat-desc">…</div>
      <div class="body" style="max-height: 70vh">
        <table id="strat-table">
          <thead id="strat-thead"></thead>
          <tbody id="strat-tbody"></tbody>
        </table>
        <div id="strat-empty" class="empty" style="display:none">
          No matches for this strategy.
        </div>
      </div>
    </section>
  </div>

  <!-- ═══════════════════════════ HOT EVENTS TAB ═══════════════════════════ -->
  <div class="tab-content" id="tab-markets">
    <section class="row two">
      <div class="panel">
        <h2>Biggest Price Movers
          <span class="hint">change since last cycle</span></h2>
        <div class="body" style="max-height: 420px">
          <table>
            <thead><tr>
              <th>Question</th><th class="num">From</th>
              <th class="num">To</th><th class="num">Δ</th>
              <th class="num">Δ%</th>
            </tr></thead>
            <tbody id="movers-body"></tbody>
          </table>
          <div id="movers-empty" class="empty">No movers yet. Run another scan to see changes.</div>
        </div>
      </div>

      <div class="panel">
        <h2>Volume Movers
          <span class="hint">≥25% change in 24h volume</span></h2>
        <div class="body" style="max-height: 420px">
          <table>
            <thead><tr>
              <th>Question</th><th class="num">From</th>
              <th class="num">To</th><th class="num">Δ%</th>
            </tr></thead>
            <tbody id="vmov-body"></tbody>
          </table>
          <div id="vmov-empty" class="empty">No volume movers yet.</div>
        </div>
      </div>
    </section>

    <section class="row two">
      <div class="panel">
        <h2>New Candidates this cycle <span class="count" id="new-count">0</span></h2>
        <div class="body" style="max-height: 280px" id="new-body"></div>
      </div>
      <div class="panel">
        <h2>Dropped from candidates <span class="count" id="drop-count">0</span></h2>
        <div class="body" style="max-height: 280px" id="drop-body"></div>
      </div>
    </section>
  </div>

  <!-- ═══════════════════════════ ANALYTICS TAB ═══════════════════════════ -->
  <div class="tab-content" id="tab-analytics">
    <section class="row four">
      <div class="card"><span class="k">Total markets</span>
        <span class="v" id="a-total">0</span>
        <span class="sub" id="a-open">— open</span></div>
      <div class="card"><span class="k">Categories</span>
        <span class="v" id="a-cats">0</span>
        <span class="sub">distinct in allowlist + denylist</span></div>
      <div class="card"><span class="k">Candidates</span>
        <span class="v" id="a-cands">0</span>
        <span class="sub" id="a-cand-pct">— hit rate</span></div>
      <div class="card"><span class="k">Tracked markets</span>
        <span class="v" id="a-tracked">0</span>
        <span class="sub">across cycles</span></div>
    </section>

    <section class="row two">
      <div class="panel">
        <h2>Category breakdown
          <span class="hint">all open markets</span></h2>
        <div class="body" id="cat-body" style="max-height: 320px"></div>
      </div>
      <div class="panel">
        <h2>Rejection donut
          <span class="hint">reasons markets failed gates</span></h2>
        <svg class="chart-svg" id="donut-svg" viewBox="0 0 400 220"></svg>
        <div id="donut-legend" class="reject-summary"></div>
      </div>
    </section>

    <section class="row two">
      <div class="panel">
        <h2>Annualized edge histogram
          <span class="hint">candidates only</span></h2>
        <svg class="chart-svg" id="edge-hist-svg"></svg>
      </div>
      <div class="panel">
        <h2>Days-to-resolution histogram
          <span class="hint">all open markets</span></h2>
        <svg class="chart-svg" id="days-hist-svg"></svg>
      </div>
    </section>

    <section class="row two">
      <div class="panel">
        <h2>Dominant price distribution
          <span class="hint">all open markets</span></h2>
        <svg class="chart-svg" id="price-hist-svg"></svg>
      </div>
      <div class="panel">
        <h2>Spread vs Depth (scatter)
          <span class="hint">color = ann edge ·  size = book depth</span></h2>
        <svg class="chart-svg" id="scatter-svg" viewBox="0 0 600 240"></svg>
      </div>
    </section>

    <section class="panel">
      <h2>Filter Funnel
        <span class="hint">how many markets survive each gate</span></h2>
      <div class="body" id="funnel-body"></div>
    </section>
  </div>

  <!-- ═══════════════════════════ REJECTIONS TAB ═══════════════════════════ -->
  <div class="tab-content" id="tab-rejections">
    <section class="panel">
      <h2>Rejection Trace <span class="count" id="rej-count">0</span>
        <span class="hint">click a chip to filter detail list</span></h2>
      <div class="reject-summary" id="rej-summary"></div>
      <div class="body" id="rej-body" style="max-height: 60vh"></div>
    </section>
  </div>

  <!-- ═══════════════════════════ TOOLS TAB ═══════════════════════════ -->
  <div class="tab-content" id="tab-tools">
    <section class="panel">
      <h2>Strategy Knobs — live editable
        <span class="hint">changes apply on next scan</span></h2>
      <form class="kform" id="kform"></form>
      <div class="kform-actions">
        <button type="button" class="btn" id="btn-reset-knobs">Reload Current</button>
        <button type="button" class="btn primary" id="btn-apply-knobs">Apply Changes</button>
      </div>
    </section>

    <section class="panel">
      <h2>Backtest — replay strategy over historical CSV</h2>
      <div class="bt-form">
        <input type="text" id="bt-path"
          placeholder="historical_markets.csv"
          value="historical_markets.csv">
        <button type="button" class="btn primary" id="btn-bt-run">Run Backtest</button>
      </div>
      <div class="bt-results" id="bt-results">
        <span class="empty">No backtest run yet. Enter a CSV path and click Run.</span>
      </div>
    </section>
  </div>

</main>

<footer>
  <span id="cfg">…</span>
  <span>polling 3s · 127.0.0.1 ·
    <a id="api-link" href="/api/state" target="_blank"
       style="color:var(--blue)">/api/state</a></span>
</footer>

<div class="toast-stack" id="toast-stack"></div>

<script>
// ─── Utilities ─────────────────────────────────────────────────────────────
const fmt = {
  usd(v) { if (v == null || isNaN(v)) return '—';
    return '$' + Number(v).toLocaleString(undefined,
      {minimumFractionDigits: 2, maximumFractionDigits: 2}); },
  usdC(v) { if (v == null || isNaN(v)) return '—';
    return '$' + Number(v).toLocaleString(undefined,
      {maximumFractionDigits: 0}); },
  num(v, d = 3) { if (v == null || isNaN(v)) return '—';
    return Number(v).toFixed(d); },
  pct(v, d = 1) { if (v == null || isNaN(v)) return '—';
    return (Number(v) * 100).toFixed(d) + '%'; },
  rel(v) { if (!v) return '—';
    const diff = (new Date(v) - new Date()) / 1000;
    const a = Math.abs(diff);
    if (a < 60) return Math.round(diff) + 's';
    if (a < 3600) return Math.round(diff / 60) + 'm';
    return Math.round(diff / 3600) + 'h'; },
  time(v) { return v ? new Date(v).toLocaleTimeString() : '—'; },
  dur(start) { if (!start) return '—';
    const s = Math.max(0, Math.floor((new Date() - new Date(start)) / 1000));
    if (s < 60) return s + 's';
    if (s < 3600) return Math.floor(s / 60) + 'm';
    return Math.floor(s / 3600) + 'h ' + Math.floor((s % 3600) / 60) + 'm'; },
};

async function api(url, opts) {
  try {
    const r = await fetch(url, opts);
    if (!r.ok) return {ok: false, status: r.status, error: await r.text()};
    return await r.json();
  } catch (e) { return null; }
}
const setText = (id, v) => { const el = document.getElementById(id);
  if (el) el.textContent = v; };
function esc(s) {
  if (s == null) return '';
  return String(s).replace(/[&<>"']/g, c => ({
    '&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));
}

const seenMsgs = new Set();
function toast(text, level = 'info') {
  const id = text + '::' + level + '::' + Math.floor(Date.now() / 1000);
  if (seenMsgs.has(id)) return;
  seenMsgs.add(id);
  const div = document.createElement('div');
  div.className = 'toast ' + level;
  div.textContent = text;
  document.getElementById('toast-stack').appendChild(div);
  setTimeout(() => div.remove(), 4500);
}

// ─── Tab switching ─────────────────────────────────────────────────────────
document.querySelectorAll('nav.tabs button').forEach(btn => {
  btn.onclick = () => {
    document.querySelectorAll('nav.tabs button').forEach(b =>
      b.classList.remove('active'));
    document.querySelectorAll('.tab-content').forEach(c =>
      c.classList.remove('active'));
    btn.classList.add('active');
    document.getElementById('tab-' + btn.dataset.tab).classList.add('active');
    if (window._lastState) refreshActiveTab(window._lastState);
  };
});

// ─── Header ────────────────────────────────────────────────────────────────
function renderHeader(s) {
  const dry = s.dry_run;
  const pill = document.getElementById('mode-pill');
  pill.textContent = dry ? 'DRY-RUN' : 'LIVE ORDERS';
  pill.className = 'pill ' + (dry ? 'yellow' : 'red');

  const kp = document.getElementById('kill-pill');
  kp.textContent = s.kill_switch || 'OK';
  kp.className = 'pill ' + ({'OK':'green','HALT_NEW':'yellow','HALT_ALL':'red'}
    [s.kill_switch] || 'green');

  const bp = document.getElementById('broker-pill');
  bp.textContent = (s.broker || 'polymarket').toUpperCase();
  bp.className = 'pill ' + (s.broker === 'kalshi' ? 'purple' :
    s.broker === 'mock' ? 'blue' : 'gray');

  document.getElementById('mock-pill').style.display = s.mock ? '' : 'none';
  const pp = document.getElementById('pause-pill');
  pp.style.display = s.scheduler_paused ? '' : 'none';
  const btnPause = document.getElementById('btn-pause');
  btnPause.textContent = s.scheduler_paused ? 'Resume' : 'Pause';
  btnPause.classList.toggle('warn', !!s.scheduler_paused);

  setText('equity',  fmt.usd(s.equity));
  setText('deployed', fmt.usd(s.deployed));
  setText('anchor',   fmt.usd(s.daily_anchor));
  setText('last-cycle', fmt.rel(s.last_cycle_at) + ' ago');
  setText('next-cycle', s.next_cycle_at ? 'in ' + fmt.rel(s.next_cycle_at) : '—');
  setText('ts', 'updated ' + new Date().toLocaleTimeString());

  const cfg = s.config || {};
  document.getElementById('cfg').textContent =
    `cap $${cfg.TOTAL_CAPITAL} · band ${cfg.DOMINANT_PRICE_MIN}-${cfg.DOMINANT_PRICE_MAX}` +
    ` · min-ann ${(cfg.MIN_ANNUALIZED_EDGE*100).toFixed(0)}% · kelly ${cfg.KELLY_FRACTION}` +
    ` · max-pos $${cfg.MAX_POSITION_USD}`;

  // Tab counters
  const strats = s.strategies || {};
  const stCount = Object.values(strats).reduce((a, v) =>
    a + ((v && v.matches) || []).length, 0);
  setText('cnt-strats', stCount);
  const hot = s.hot_events || {};
  setText('cnt-hot',
    ((hot.price_movers || []).length) + ((hot.new_candidates || []).length));
  setText('cnt-rej', (s.rejections || []).length);
}

// ─── Stats strip ───────────────────────────────────────────────────────────
function renderStats(s) {
  const st = s.stats || {};
  setText('s-scans', st.scans || 0);
  setText('s-uptime', 'since ' + fmt.dur(st.started_at) + ' ago');
  setText('s-markets', st.markets_seen || 0);
  setText('s-mkt-now', (s.total_markets || 0) + ' this cycle');
  setText('s-cands', st.candidates_seen || 0);
  setText('s-cands-now', (s.candidates || []).length + ' this cycle');
  setText('s-fills', st.fills || 0);
  setText('s-intents', (st.intents || 0) + ' intents');
  setText('s-slip', st.slippage_aborts || 0);
  setText('s-fail', (st.order_failures || 0) + ' failed');

  const positions = s.positions || [];
  setText('s-pos', positions.length);
  const pnl = positions.reduce((a, p) => a + (p.pnl_usdc || 0), 0);
  const el = document.getElementById('s-pos-pnl');
  el.textContent = fmt.usd(pnl) + ' PnL';
  el.style.color = pnl >= 0 ? 'var(--green)' : 'var(--red)';
}

// ─── Equity sparkline ──────────────────────────────────────────────────────
function renderSpark(s) {
  const svg = document.getElementById('spark');
  const hist = (s.stats && s.stats.equity_history) || [];
  const rect = svg.getBoundingClientRect();
  const W = Math.max(60, Math.round(rect.width || 600));
  const H = 64;
  svg.setAttribute('viewBox', `0 0 ${W} ${H}`);
  svg.setAttribute('width', W); svg.setAttribute('height', H);
  if (!hist.length) {
    svg.innerHTML = `<text x="14" y="36" fill="#5b6a85" font-size="11">` +
      `No equity samples yet.</text>`;
    setText('spark-info', '');
    return;
  }
  const vals = hist.map(h => h[1]);
  const mn = Math.min(...vals), mx = Math.max(...vals);
  const pad = (mx - mn) * 0.15 || 1;
  const lo = mn - pad, hi = mx + pad;
  const PAD_L = 64, PAD_R = 8, PAD_T = 8, PAD_B = 12;
  const plotW = Math.max(10, W - PAD_L - PAD_R);
  const plotH = Math.max(10, H - PAD_T - PAD_B);
  const pts = hist.map((h, i) => {
    const x = PAD_L + (hist.length === 1 ? plotW / 2
      : (i / (hist.length - 1)) * plotW);
    const y = PAD_T + plotH - ((h[1] - lo) / (hi - lo || 1)) * plotH;
    return x.toFixed(1) + ',' + y.toFixed(1);
  }).join(' ');
  const lastV = vals[vals.length - 1], firstV = vals[0];
  const color = lastV >= firstV ? 'var(--green)' : 'var(--red)';
  const areaPts = `${PAD_L},${PAD_T + plotH} ${pts} ${PAD_L + plotW},${PAD_T + plotH}`;
  svg.innerHTML = `
    <polygon fill="${color}" fill-opacity="0.08" points="${areaPts}"/>
    <polyline fill="none" stroke="${color}" stroke-width="1.5"
      stroke-linejoin="round" stroke-linecap="round" points="${pts}"/>
    <text x="8" y="14" fill="#7c8aa3">${fmt.usd(mx)}</text>
    <text x="8" y="${H - 4}" fill="#7c8aa3">${fmt.usd(mn)}</text>`;
  setText('spark-info',
    `${hist.length} samples · ${fmt.usd(firstV)} → ${fmt.usd(lastV)}`);
}
window.addEventListener('resize', () => {
  if (window._lastState) {
    renderSpark(window._lastState);
    renderAnalytics(window._lastState);
  }
});

// ─── Positions ─────────────────────────────────────────────────────────────
function renderPositions(positions) {
  const body = document.getElementById('pos-body');
  const empty = document.getElementById('pos-empty');
  setText('pos-count', positions.length);
  if (!positions.length) { body.innerHTML = ''; empty.style.display = 'block'; return; }
  empty.style.display = 'none';
  body.innerHTML = positions.map(p => `
    <tr>
      <td class="q" title="${esc(p.market_question)}">${esc(p.market_question)}</td>
      <td class="num">${fmt.num(p.entry_price, 4)}</td>
      <td class="num">${fmt.num(p.mark_price, 4)}</td>
      <td class="num ${p.pnl_usdc >= 0 ? 'pos' : 'neg'}">${fmt.usd(p.pnl_usdc)}</td>
      <td class="num ${p.pnl_pct >= 0 ? 'pos' : 'neg'}">${fmt.num(p.pnl_pct, 1)}%</td>
    </tr>`).join('');
}

// ─── Candidates + Detail ───────────────────────────────────────────────────
let selectedCandidateIdx = -1;
let lastCandidates = [];

function renderCandidates(cs) {
  lastCandidates = cs;
  const body = document.getElementById('cand-body');
  const empty = document.getElementById('cand-empty');
  setText('cand-count', cs.length);
  if (!cs.length) { body.innerHTML = ''; empty.style.display = 'block'; return; }
  empty.style.display = 'none';
  body.innerHTML = cs.map((c, i) => `
    <tr class="row-click ${i === selectedCandidateIdx ? 'sel' : ''}"
        data-idx="${i}">
      <td class="q" title="${esc(c.question)}">${esc(c.question)}</td>
      <td>${esc(c.outcome)}</td>
      <td class="num">${fmt.num(c.price, 3)}</td>
      <td class="num">${fmt.num(c.edge, 3)}</td>
      <td class="num">${(c.annualized_edge*100).toFixed(0)}%</td>
      <td class="num">${fmt.num(c.days_to_resolution, 2)}</td>
      <td class="num">${fmt.usd(c.proposed_usdc)}</td>
    </tr>`).join('');
  body.querySelectorAll('tr.row-click').forEach(tr => {
    tr.onclick = () => selectCandidate(parseInt(tr.dataset.idx, 10));
  });
  if (selectedCandidateIdx >= 0 && cs[selectedCandidateIdx]) {
    renderDetail(cs[selectedCandidateIdx]);
  }
}

function selectCandidate(idx) {
  selectedCandidateIdx = idx;
  document.querySelectorAll('#cand-body tr').forEach((tr, i) =>
    tr.classList.toggle('sel', i === idx));
  if (lastCandidates[idx]) renderDetail(lastCandidates[idx]);
}

document.getElementById('detail-close').onclick = () => {
  selectedCandidateIdx = -1;
  document.getElementById('detail-panel').style.display = 'none';
  document.querySelectorAll('#cand-body tr').forEach(tr => tr.classList.remove('sel'));
};

function renderDetail(c) {
  document.getElementById('detail-panel').style.display = '';
  setText('detail-title', 'Detail — ' + (c.question || ''));
  document.getElementById('detail-tokens').innerHTML = (c.all_tokens || []).map(t => `
    <tr><td>${esc(t.outcome)}</td>
        <td class="num">${fmt.num(t.price, 4)}</td></tr>`).join('');
  document.getElementById('detail-edge').innerHTML = `
    <span class="k">1 − Price</span><span class="v">${fmt.num(1 - c.price, 4)}</span>
    <span class="k">Dispute buffer</span><span class="v">−0.0200</span>
    <span class="k">Edge</span><span class="v" style="color:var(--blue)">${fmt.num(c.edge, 4)}</span>
    <span class="k">Days to res.</span><span class="v">${fmt.num(c.days_to_resolution, 2)}</span>
    <span class="k">Annualized</span><span class="v" style="color:var(--green)">${(c.annualized_edge*100).toFixed(1)}%</span>
    <span class="k">Spread</span><span class="v">${fmt.num(c.spread, 4)}</span>`;
  const asks = (c.orderbook && c.orderbook.asks) || [];
  const bids = (c.orderbook && c.orderbook.bids) || [];
  document.getElementById('detail-asks').innerHTML = asks.length
    ? asks.slice().reverse().map(a => `<tr><td class="ask">${fmt.num(a[0], 4)}</td>
        <td class="num">${fmt.num(a[1], 2)}</td>
        <td class="num">${fmt.usd(a[0] * a[1])}</td></tr>`).join('')
    : '<tr><td colspan="3" style="color:var(--dim)">No asks (top-5 only)</td></tr>';
  document.getElementById('detail-bids').innerHTML = bids.length
    ? bids.map(b => `<tr><td class="bid">${fmt.num(b[0], 4)}</td>
        <td class="num">${fmt.num(b[1], 2)}</td>
        <td class="num">${fmt.usd(b[0] * b[1])}</td></tr>`).join('')
    : '<tr><td colspan="3" style="color:var(--dim)">No bids</td></tr>';
  document.getElementById('detail-trade').innerHTML = `
    <span class="k">Proposed USDC</span><span class="v">${fmt.usd(c.proposed_usdc)}</span>
    <span class="k">Proposed shares</span><span class="v">${fmt.num(c.proposed_shares, 2)}</span>
    <span class="k">Limit price</span><span class="v">${fmt.num(c.price, 4)}</span>
    <span class="k">Depth @ limit</span><span class="v">${fmt.usd(c.book_depth_usd)}</span>`;
  document.getElementById('detail-market').innerHTML = `
    <span class="k">Category</span><span class="v">${esc(c.category || '—')}</span>
    <span class="k">Volume 24h</span><span class="v">${fmt.usd(c.volume_24h)}</span>
    <span class="k">End date</span><span class="v">${fmt.time(c.end_date)}</span>
    <span class="k">Token id</span><span class="v mono" style="font-size:10px">${(c.token_id||'').slice(0,14)}…</span>`;
}

// ─── Events ────────────────────────────────────────────────────────────────
function renderEvents(events) {
  const body = document.getElementById('ev-body');
  setText('ev-count', events.length);
  if (!events.length) {
    body.innerHTML = '<div class="empty">No journal events yet.</div>'; return;
  }
  body.innerHTML = events.slice().reverse().map(e => {
    const ts = e.timestamp ? new Date(e.timestamp).toLocaleTimeString() : '';
    const note = e.notes ? ' — ' + esc(e.notes) : '';
    return `<div class="ev">
      <div><span class="e ${e.event}">${e.event}</span>
        ${e.side ? ' · ' + e.side : ''}
        ${e.price ? ' @ ' + e.price : ''}
        ${e.usdc_amount ? ' · ' + fmt.usd(e.usdc_amount) : ''}</div>
      <div>${esc(e.market_question || '')}${note}</div>
      <div class="t">${ts}</div>
    </div>`;
  }).join('');
}

// ─── Strategies (tab with sub-tabs) ────────────────────────────────────────
let activeStrat = 'decay';
document.querySelectorAll('#strat-subtabs button').forEach(b => {
  b.onclick = () => { activeStrat = b.dataset.strat;
    if (window._lastState) renderStrategies(window._lastState); };
});

const STRAT_COLUMNS = {
  decay: [
    ['Question', 'question', 'q'],
    ['Side', 'dominant_outcome', null],
    ['Price', 'dominant_price', 'num4'],
    ['Edge', 'edge', 'num3'],
    ['Ann%', 'annualized_edge', 'pct'],
    ['Days', 'days_to_resolution', 'num2'],
    ['Depth', 'book_depth_usd', 'usdC'],
  ],
  mean_reversion: [
    ['Question', 'question', 'q'],
    ['Top Outcome', 'dominant_outcome', null],
    ['Price', 'dominant_price', 'num4'],
    ['Volume 24h', 'volume_24h', 'usdC'],
    ['Days', 'days_to_resolution', 'num2'],
    ['Category', 'category', null],
  ],
  arbitrage: [
    ['Question', 'question', 'q'],
    ['Sum', 'sum', 'num4'],
    ['Edge %', 'edge_pct', 'num2'],
    ['Direction', 'direction', null],
    ['Volume 24h', 'volume_24h', 'usdC'],
    ['Days', 'days_to_resolution', 'num2'],
  ],
  volume_leaders: [
    ['Question', 'question', 'q'],
    ['Category', 'category', null],
    ['Volume 24h', 'volume_24h', 'usdC'],
    ['Top Price', 'dominant_price', 'num4'],
    ['Days', 'days_to_resolution', 'num2'],
  ],
  closing_soon: [
    ['Question', 'question', 'q'],
    ['Days', 'days_to_resolution', 'num2'],
    ['Top Price', 'dominant_price', 'num4'],
    ['Top Outcome', 'dominant_outcome', null],
    ['Volume 24h', 'volume_24h', 'usdC'],
    ['Category', 'category', null],
  ],
};

function renderStrategies(s) {
  const strats = s.strategies || {};
  // Counters
  for (const key of Object.keys(STRAT_COLUMNS)) {
    setText('strat-n-' + key, ((strats[key] && strats[key].matches) || []).length);
  }
  const data = strats[activeStrat] || {};
  document.querySelectorAll('#strat-subtabs button').forEach(b =>
    b.classList.toggle('active', b.dataset.strat === activeStrat));
  setText('strat-desc', data.description || '—');

  const cols = STRAT_COLUMNS[activeStrat] || [];
  document.getElementById('strat-thead').innerHTML = '<tr>' +
    cols.map(([label, , type]) => `<th class="${type === 'q' ? '' :
      ['num4','num3','num2','pct','usdC','usd'].includes(type) ? 'num' : ''}">${label}</th>`
    ).join('') + '</tr>';

  const rows = data.matches || [];
  if (!rows.length) {
    document.getElementById('strat-tbody').innerHTML = '';
    document.getElementById('strat-empty').style.display = 'block';
  } else {
    document.getElementById('strat-empty').style.display = 'none';
    document.getElementById('strat-tbody').innerHTML = rows.map(r => '<tr>' +
      cols.map(([, field, type]) => {
        const v = r[field];
        let cell, cls = '';
        if (type === 'q') { cell = esc(v || ''); cls = 'q'; }
        else if (type === 'num4') { cell = fmt.num(v, 4); cls = 'num'; }
        else if (type === 'num3') { cell = fmt.num(v, 3); cls = 'num'; }
        else if (type === 'num2') { cell = fmt.num(v, 2); cls = 'num'; }
        else if (type === 'pct')  { cell = (v*100).toFixed(0) + '%'; cls = 'num'; }
        else if (type === 'usdC') { cell = fmt.usdC(v); cls = 'num'; }
        else { cell = esc(v == null ? '—' : String(v)); }
        return `<td class="${cls}">${cell}</td>`;
      }).join('') + '</tr>').join('');
  }
}

// ─── Hot Events ────────────────────────────────────────────────────────────
function renderHotEvents(s) {
  const h = s.hot_events || {};
  const movers = h.price_movers || [];
  const mbody = document.getElementById('movers-body');
  document.getElementById('movers-empty').style.display = movers.length ? 'none' : 'block';
  mbody.innerHTML = movers.map(m => `
    <tr>
      <td class="q" title="${esc(m.question)}">${esc(m.question)}</td>
      <td class="num">${fmt.num(m.from_price, 4)}</td>
      <td class="num">${fmt.num(m.to_price, 4)}</td>
      <td class="num ${m.delta >= 0 ? 'pos' : 'neg'}">${m.delta >= 0 ? '+' : ''}${fmt.num(m.delta, 4)}</td>
      <td class="num ${m.delta_pct >= 0 ? 'pos' : 'neg'}">${m.delta_pct >= 0 ? '+' : ''}${fmt.num(m.delta_pct, 1)}%</td>
    </tr>`).join('');

  const vm = h.volume_movers || [];
  const vbody = document.getElementById('vmov-body');
  document.getElementById('vmov-empty').style.display = vm.length ? 'none' : 'block';
  vbody.innerHTML = vm.map(m => `
    <tr>
      <td class="q" title="${esc(m.question)}">${esc(m.question)}</td>
      <td class="num">${fmt.usdC(m.from_volume)}</td>
      <td class="num">${fmt.usdC(m.to_volume)}</td>
      <td class="num ${m.delta_pct >= 0 ? 'pos' : 'neg'}">${m.delta_pct >= 0 ? '+' : ''}${fmt.num(m.delta_pct, 1)}%</td>
    </tr>`).join('');

  const newC = h.new_candidates || [];
  setText('new-count', newC.length);
  document.getElementById('new-body').innerHTML = newC.length
    ? newC.map(c => `<div class="ev">
        <div><span class="e filled">NEW</span>
          price ${fmt.num(c.price, 4)} · ann ${(c.ann*100).toFixed(0)}%</div>
        <div>${esc(c.question)}</div></div>`).join('')
    : '<div class="empty">No new candidates this cycle.</div>';

  const dropC = h.dropped_candidates || [];
  setText('drop-count', dropC.length);
  document.getElementById('drop-body').innerHTML = dropC.length
    ? dropC.map(c => `<div class="ev">
        <div><span class="e order_failed">DROPPED</span></div>
        <div>${esc(c.question)}</div></div>`).join('')
    : '<div class="empty">No candidates dropped this cycle.</div>';
}

// ─── Analytics charts ──────────────────────────────────────────────────────
function renderAnalytics(s) {
  const a = s.analytics || {};

  // Stat cards
  setText('a-total', (a.total_open_markets || 0) + (a.total_closed_markets || 0));
  setText('a-open', (a.total_open_markets || 0) + ' open · ' +
    (a.total_closed_markets || 0) + ' closed');
  setText('a-cats', (a.by_category || []).length);
  setText('a-cands', (s.candidates || []).length);
  const total = a.total_open_markets || 0;
  const hit = total ? ((s.candidates || []).length / total) * 100 : 0;
  setText('a-cand-pct', fmt.num(hit, 1) + '% hit rate');
  setText('a-tracked', (s.hot_events || {}).tracked_markets || 0);

  // Category breakdown (horizontal bars)
  renderHBars('cat-body', a.by_category || [], 'count', 'color-purple');

  // Rejection donut
  renderDonut('donut-svg', 'donut-legend',
    rejectionsToBins(s.rejections || []));

  // Histograms
  renderHist('edge-hist-svg', a.edge_histogram || [], 'count', 'var(--green)');
  renderHist('days-hist-svg', a.days_histogram || [], 'count', 'var(--orange)');
  renderHist('price-hist-svg', a.price_distribution || [], 'count', 'var(--cyan)');

  // Scatter
  renderScatter('scatter-svg', a.spread_depth_scatter || []);

  // Funnel
  renderFunnel(s);
}

function rejectionsToBins(rejections) {
  const counts = {};
  for (const r of rejections) counts[r.reason] = (counts[r.reason] || 0) + 1;
  return Object.entries(counts).sort((a, b) => b[1] - a[1])
    .map(([label, count]) => ({label, count}));
}

function renderHBars(containerId, items, key, colorClass) {
  const c = document.getElementById(containerId);
  if (!items.length) {
    c.innerHTML = '<div class="empty">No data yet.</div>'; return;
  }
  const max = Math.max(...items.map(x => x[key] || 0)) || 1;
  c.innerHTML = items.map(it => {
    const w = ((it[key] || 0) / max) * 100;
    return `<div class="hbar-row ${colorClass || ''}">
      <span class="lbl">${esc(it.label || it.category)}</span>
      <span class="num">${it[key] || 0}</span>
      <div class="bg"><div class="fill" style="width:${w}%"></div></div>
    </div>`;
  }).join('');
}

function renderHist(svgId, items, key, color) {
  const svg = document.getElementById(svgId);
  const rect = svg.getBoundingClientRect();
  const W = Math.max(200, Math.round(rect.width || 400));
  const H = 200;
  svg.setAttribute('viewBox', `0 0 ${W} ${H}`);
  svg.setAttribute('width', W); svg.setAttribute('height', H);
  if (!items.length) {
    svg.innerHTML = `<text x="${W/2}" y="${H/2}" text-anchor="middle">No data</text>`;
    return;
  }
  const max = Math.max(...items.map(x => x[key] || 0)) || 1;
  const PAD_L = 8, PAD_R = 8, PAD_T = 16, PAD_B = 32;
  const plotW = W - PAD_L - PAD_R, plotH = H - PAD_T - PAD_B;
  const barW = plotW / items.length;
  let html = '';
  items.forEach((it, i) => {
    const h = ((it[key] || 0) / max) * plotH;
    const x = PAD_L + i * barW + 2;
    const y = PAD_T + plotH - h;
    html += `<rect x="${x}" y="${y}" width="${barW - 4}" height="${h}"
      fill="${color}" fill-opacity="0.8" rx="2"/>`;
    if (it[key] > 0) {
      html += `<text x="${x + (barW - 4) / 2}" y="${y - 4}"
        text-anchor="middle" fill="#e2ecfb" font-size="10"
        font-weight="700">${it[key]}</text>`;
    }
    html += `<text x="${x + (barW - 4) / 2}" y="${H - 8}"
      text-anchor="middle">${esc(it.label)}</text>`;
  });
  svg.innerHTML = html;
}

const DONUT_COLORS = ['#60a5fa','#facc15','#f87171','#4ade80','#c084fc',
  '#fb923c','#22d3ee','#f472b6','#a3e635','#94a3b8'];

function renderDonut(svgId, legendId, items) {
  const svg = document.getElementById(svgId);
  const legend = document.getElementById(legendId);
  if (!items.length) {
    svg.innerHTML = '<text x="200" y="110" text-anchor="middle">No data</text>';
    legend.innerHTML = '';
    return;
  }
  const total = items.reduce((a, x) => a + x.count, 0);
  const cx = 110, cy = 110, r = 80, sw = 28;
  let acc = 0;
  let html = `<circle cx="${cx}" cy="${cy}" r="${r}" fill="none"
    stroke="#1a2333" stroke-width="${sw}"/>`;
  items.forEach((it, i) => {
    const frac = it.count / total;
    const dasharray = (frac * 2 * Math.PI * r).toFixed(2) + ' ' +
                      (2 * Math.PI * r).toFixed(2);
    const dashoffset = (-acc * 2 * Math.PI * r).toFixed(2);
    html += `<circle cx="${cx}" cy="${cy}" r="${r}" fill="none"
      stroke="${DONUT_COLORS[i % DONUT_COLORS.length]}" stroke-width="${sw}"
      stroke-dasharray="${dasharray}" stroke-dashoffset="${dashoffset}"
      transform="rotate(-90 ${cx} ${cy})"/>`;
    acc += frac;
  });
  html += `<text x="${cx}" y="${cy - 4}" text-anchor="middle"
    fill="#e2ecfb" font-size="22" font-weight="700">${total}</text>`;
  html += `<text x="${cx}" y="${cy + 14}" text-anchor="middle">rejections</text>`;
  svg.innerHTML = html;
  legend.innerHTML = items.map((it, i) =>
    `<span class="chip" style="border-color:${DONUT_COLORS[i % DONUT_COLORS.length]}">
      <span style="display:inline-block;width:8px;height:8px;background:${DONUT_COLORS[i % DONUT_COLORS.length]};
        border-radius:2px;margin-right:5px"></span>
      ${esc(it.label)} <b style="color:${DONUT_COLORS[i % DONUT_COLORS.length]}">${it.count}</b>
    </span>`).join('');
}

function renderScatter(svgId, points) {
  const svg = document.getElementById(svgId);
  const W = 600, H = 240;
  svg.setAttribute('viewBox', `0 0 ${W} ${H}`);
  if (!points.length) {
    svg.innerHTML = '<text x="300" y="120" text-anchor="middle">No candidates yet</text>';
    return;
  }
  const spreads = points.map(p => p.spread);
  const depths = points.map(p => p.depth);
  const sMin = 0, sMax = Math.max(...spreads, 0.01);
  const dMin = 0, dMax = Math.max(...depths, 100);
  const PAD_L = 50, PAD_R = 16, PAD_T = 16, PAD_B = 36;
  const plotW = W - PAD_L - PAD_R, plotH = H - PAD_T - PAD_B;
  let html = '';
  // Axes
  html += `<line x1="${PAD_L}" y1="${PAD_T + plotH}" x2="${PAD_L + plotW}"
    y2="${PAD_T + plotH}" stroke="#243349"/>`;
  html += `<line x1="${PAD_L}" y1="${PAD_T}" x2="${PAD_L}"
    y2="${PAD_T + plotH}" stroke="#243349"/>`;
  // Axis labels
  html += `<text x="${PAD_L + plotW/2}" y="${H - 8}" text-anchor="middle">Spread →</text>`;
  html += `<text x="14" y="${PAD_T + plotH/2}" text-anchor="middle"
    transform="rotate(-90 14 ${PAD_T + plotH/2})">Depth (USDC) →</text>`;
  // Tick labels (corners)
  html += `<text x="${PAD_L}" y="${H - 24}" text-anchor="middle">0</text>`;
  html += `<text x="${PAD_L + plotW}" y="${H - 24}" text-anchor="middle">${fmt.num(sMax, 3)}</text>`;
  html += `<text x="42" y="${PAD_T + plotH}" text-anchor="end">${fmt.usdC(0)}</text>`;
  html += `<text x="42" y="${PAD_T + 6}" text-anchor="end">${fmt.usdC(dMax)}</text>`;
  // Points
  for (const p of points) {
    const x = PAD_L + ((p.spread - sMin) / (sMax - sMin || 1)) * plotW;
    const y = PAD_T + plotH - ((p.depth - dMin) / (dMax - dMin || 1)) * plotH;
    const radius = Math.min(8, 3 + Math.log10(Math.max(1, p.depth)) * 0.9);
    const c = Math.min(1, (p.ann || 0) / 5);
    const color = `hsl(${(120 - c * 120).toFixed(0)},80%,55%)`;
    html += `<circle cx="${x.toFixed(1)}" cy="${y.toFixed(1)}" r="${radius.toFixed(1)}"
      fill="${color}" fill-opacity="0.55" stroke="${color}">
      <title>${esc(p.question)}: spread=${p.spread}, depth=${fmt.usdC(p.depth)}, ann=${(p.ann*100).toFixed(0)}%</title>
    </circle>`;
  }
  svg.innerHTML = html;
}

// ─── Funnel ────────────────────────────────────────────────────────────────
function renderFunnel(s) {
  const f = s.filter_funnel || {};
  const stages = [
    ['Fetched',          'fetched'],
    ['Open',             'open'],
    ['Category OK',      'category_ok'],
    ['Days window',      'days_window'],
    ['Price band',       'price_band'],
    ['Orderbook OK',     'orderbook_ok'],
    ['Spread OK',        'spread_ok'],
    ['Depth OK',         'depth_ok'],
    ['Edge positive',    'edge_positive'],
    ['Ann edge OK ✓',    'ann_edge_ok'],
  ];
  const total = f.fetched || 0;
  const body = document.getElementById('funnel-body');
  if (!total) {
    body.innerHTML = '<div class="empty">No funnel data yet.</div>'; return;
  }
  body.innerHTML = stages.map(([label, key]) => {
    const v = f[key] ?? 0;
    const w = total ? (v / total) * 100 : 0;
    return `<div class="funnel-row">
      <span class="label">${label}</span>
      <div class="bar"><div class="fill" style="width:${w}%"></div></div>
      <span class="num">${v}</span>
    </div>`;
  }).join('');
}

// ─── Rejections (full tab) ─────────────────────────────────────────────────
let rejFilter = null;
function renderRejections(rejections, cfg) {
  setText('rej-count', rejections.length);
  const counts = {};
  for (const r of rejections) counts[r.reason] = (counts[r.reason] || 0) + 1;
  const summary = document.getElementById('rej-summary');
  summary.innerHTML = Object.entries(counts).sort((a, b) => b[1] - a[1])
    .map(([k, v]) => `<span class="chip ${rejFilter === k ? 'sel' : ''}"
      data-reason="${k}">${k}<b>${v}</b></span>`).join('');
  summary.querySelectorAll('.chip').forEach(ch => {
    ch.onclick = () => { const r = ch.dataset.reason;
      rejFilter = rejFilter === r ? null : r;
      renderRejections(rejections, cfg); };
  });
  const hints = computeCounterfactuals(rejections, cfg);
  const filtered = rejFilter ? rejections.filter(r => r.reason === rejFilter)
    : rejections;
  const body = document.getElementById('rej-body');
  const hintHTML = hints.length
    ? '<div style="padding:8px 14px;border-bottom:1px solid var(--line);' +
      'background:rgba(96,165,250,.04);font-size:11px;color:var(--blue)">' +
      hints.map(h => '• ' + h).join('<br>') + '</div>'
    : '';
  body.innerHTML = hintHTML + (filtered.slice(0, 200).map(r => {
    const val = r.value != null ? ' <span class="mono">(' +
      (typeof r.value === 'number' ? r.value : JSON.stringify(r.value)) + ')</span>' : '';
    const lim = r.limit != null ? ' vs <span class="mono">' +
      JSON.stringify(r.limit) + '</span>' : '';
    return `<div class="reject-row">
      <div class="q" title="${esc(r.question)}">${esc(r.question)}</div>
      <div class="reason">${r.reason}${val}${lim}</div>
    </div>`;
  }).join('') || '<div class="empty">No rejections recorded.</div>');
}

function computeCounterfactuals(rejections, cfg) {
  const hints = [];
  const annLow = rejections.filter(r => r.reason === 'ann_edge_too_low' && typeof r.value === 'number');
  if (annLow.length && cfg.MIN_ANNUALIZED_EDGE) {
    const half = cfg.MIN_ANNUALIZED_EDGE / 2;
    const recover = annLow.filter(r => r.value >= half).length;
    if (recover > 0) hints.push(`Lowering MIN_ANNUALIZED_EDGE from ${(cfg.MIN_ANNUALIZED_EDGE*100).toFixed(0)}% → ${(half*100).toFixed(0)}% would recover ${recover} candidates.`);
  }
  const priceOOB = rejections.filter(r => r.reason === 'price_out_of_band' && typeof r.value === 'number');
  if (priceOOB.length && cfg.DOMINANT_PRICE_MIN != null) {
    const lo = cfg.DOMINANT_PRICE_MIN - 0.05, hi = cfg.DOMINANT_PRICE_MAX + 0.02;
    const recover = priceOOB.filter(r => r.value >= lo && r.value <= hi).length;
    if (recover > 0) hints.push(`Widening DOMINANT_PRICE band to [${lo.toFixed(2)}, ${hi.toFixed(2)}] would recover ${recover} markets.`);
  }
  const spreadW = rejections.filter(r => r.reason === 'spread_too_wide' && typeof r.value === 'number');
  if (spreadW.length && cfg.MAX_SPREAD) {
    const newSpread = cfg.MAX_SPREAD * 2;
    const recover = spreadW.filter(r => r.value <= newSpread).length;
    if (recover > 0) hints.push(`Doubling MAX_SPREAD to ${newSpread.toFixed(3)} would recover ${recover} markets.`);
  }
  const thin = rejections.filter(r => r.reason === 'book_too_thin' && typeof r.value === 'number');
  if (thin.length && cfg.MIN_BOOK_DEPTH_USD) {
    const half = cfg.MIN_BOOK_DEPTH_USD / 2;
    const recover = thin.filter(r => r.value >= half).length;
    if (recover > 0) hints.push(`Halving MIN_BOOK_DEPTH_USD to $${half.toFixed(0)} would recover ${recover} markets.`);
  }
  return hints;
}

// ─── Knobs (Tools tab) ─────────────────────────────────────────────────────
let cfgMeta = null;
let cfgValues = null;

async function loadKnobs() {
  const r = await api('/api/editable_config');
  if (!r) return;
  cfgMeta = r.meta; cfgValues = r.values; buildKnobs();
}

function buildKnobs() {
  const form = document.getElementById('kform');
  form.innerHTML = Object.entries(cfgMeta).map(([k, m]) => {
    const v = cfgValues[k];
    const step = m.type === 'int' ? '1'
      : (m.max - m.min) < 1 ? '0.001'
      : (m.max - m.min) < 10 ? '0.01' : '1';
    return `<div class="row-k">
      <label for="k-${k}">${k}</label>
      <input type="range" id="k-${k}" data-key="${k}" data-type="${m.type}"
        min="${m.min}" max="${m.max}" step="${step}" value="${v}">
      <span class="val" id="v-${k}">${v}</span>
    </div>`;
  }).join('');
  form.querySelectorAll('input[type=range]').forEach(inp => {
    inp.oninput = () => {
      document.getElementById('v-' + inp.dataset.key).textContent =
        inp.dataset.type === 'int' ? parseInt(inp.value)
        : parseFloat(inp.value);
    };
  });
}

document.getElementById('btn-reset-knobs').onclick = loadKnobs;
document.getElementById('btn-apply-knobs').onclick = async () => {
  const payload = {};
  document.querySelectorAll('#kform input[type=range]').forEach(inp => {
    payload[inp.dataset.key] = inp.dataset.type === 'int'
      ? parseInt(inp.value) : parseFloat(inp.value);
  });
  const r = await api('/api/config', {
    method: 'POST', headers: {'Content-Type': 'application/json'},
    body: JSON.stringify(payload),
  });
  if (r && r.ok) toast(`Applied ${Object.keys(r.updated).length} knob(s).`, 'info');
  else if (r) toast(`Errors: ${(r.errors || []).join(', ') || r.error}`, 'error');
  else toast('Apply failed', 'error');
};

// ─── Backtest ──────────────────────────────────────────────────────────────
document.getElementById('btn-bt-run').onclick = async () => {
  const path = document.getElementById('bt-path').value.trim() || 'historical_markets.csv';
  document.getElementById('bt-results').innerHTML = '<span class="empty">Running…</span>';
  const r = await api('/api/backtest', {
    method: 'POST', headers: {'Content-Type': 'application/json'},
    body: JSON.stringify({path}),
  });
  if (!r) { document.getElementById('bt-results').innerHTML =
    '<span style="color:var(--red)">Network error.</span>'; return; }
  if (!r.ok) { document.getElementById('bt-results').innerHTML =
    `<span style="color:var(--red)">Error: ${esc(r.error || 'unknown')}</span>`; return; }
  const res = r.results;
  const trades = (res.trades || []).slice(-15);
  document.getElementById('bt-results').innerHTML = `
    <div style="display:grid;grid-template-columns:repeat(6,1fr);gap:12px;margin-bottom:10px">
      <div class="kv"><span class="k">Initial</span><span class="v">${fmt.usd(res.initial_capital)}</span></div>
      <div class="kv"><span class="k">Final</span><span class="v">${fmt.usd(res.final_cash)}</span></div>
      <div class="kv"><span class="k">Return</span>
        <span class="v ${res.total_return_pct >= 0 ? 'pos' : 'neg'}">${fmt.num(res.total_return_pct, 2)}%</span></div>
      <div class="kv"><span class="k">Trades</span><span class="v">${res.total_trades}</span></div>
      <div class="kv"><span class="k">Win rate</span><span class="v">${fmt.num(res.win_rate_pct, 1)}%</span></div>
      <div class="kv"><span class="k">Avg PnL</span><span class="v">${fmt.usd(res.avg_pnl)}</span></div>
    </div>
    <table style="font-size:11px">
      <thead><tr><th>Question</th><th class="num">Entry</th><th class="num">Size</th>
        <th>Result</th><th class="num">PnL</th></tr></thead>
      <tbody>${trades.map(t => `<tr>
        <td class="q">${esc(t.question)}</td>
        <td class="num">${fmt.num(t.entry_price, 4)}</td>
        <td class="num">${fmt.usd(t.size_usdc)}</td>
        <td class="${t.outcome ? 'pos' : 'neg'}">${t.outcome ? 'WIN' : 'LOSS'}</td>
        <td class="num ${t.pnl >= 0 ? 'pos' : 'neg'}">${fmt.usd(t.pnl)}</td></tr>`).join('')}</tbody>
    </table>`;
};

// ─── Action buttons ────────────────────────────────────────────────────────
document.getElementById('btn-scan').onclick = async () => {
  const r = await api('/api/scan', {method: 'POST'});
  toast(r && r.ok ? 'Scan requested' : 'Scan request failed',
    r && r.ok ? 'info' : 'error');
};
document.getElementById('btn-pause').onclick = async () => {
  const isPaused = document.getElementById('btn-pause').textContent === 'Resume';
  const r = await api(isPaused ? '/api/resume' : '/api/pause', {method: 'POST'});
  if (r && r.ok) toast(isPaused ? 'Scheduler resumed' : 'Scheduler paused',
    isPaused ? 'info' : 'warn');
};

// ─── Message stream ────────────────────────────────────────────────────────
const seenMsgTs = new Set();
function renderMessages(messages) {
  for (const m of messages) {
    if (seenMsgTs.has(m.ts)) continue;
    seenMsgTs.add(m.ts);
    toast(m.text, m.level || 'info');
  }
}

// ─── Per-tab refresh ───────────────────────────────────────────────────────
function refreshActiveTab(s) {
  const tab = document.querySelector('nav.tabs button.active').dataset.tab;
  if (tab === 'overview')   {
    renderStats(s); renderSpark(s);
    renderPositions(s.positions || []);
    renderCandidates(s.candidates || []);
    renderEvents(s.recent_events || []);
  }
  else if (tab === 'strategies') renderStrategies(s);
  else if (tab === 'markets')    renderHotEvents(s);
  else if (tab === 'analytics')  renderAnalytics(s);
  else if (tab === 'rejections') renderRejections(s.rejections || [], s.config || {});
}

// ─── Main poll ─────────────────────────────────────────────────────────────
async function refresh() {
  const s = await api('/api/state');
  if (!s) {
    setText('ts', 'connection lost');
    document.getElementById('ts').className = 'stale mono';
    return;
  }
  document.getElementById('ts').className = 'mono';
  window._lastState = s;
  renderHeader(s);
  refreshActiveTab(s);
  renderMessages(s.messages || []);
}

loadKnobs();
refresh();
setInterval(refresh, 3000);
</script>
</body>
</html>
"""


# Subset of Config attributes safe to mutate from the dashboard.
# Type tag tells the front end how to render the input AND tells the
# POST handler how to coerce the incoming string.
EDITABLE_CONFIG_KEYS = {
    "TOTAL_CAPITAL":            ("float", 1.0, 1_000_000.0),
    "MAX_POSITION_USD":         ("float", 1.0, 100_000.0),
    "MAX_CONCURRENT_POSITIONS": ("int", 1, 50),
    "MAX_TOTAL_DEPLOYED_PCT":   ("float", 0.0, 1.0),
    "MIN_TRADE_USD":            ("float", 0.1, 10_000.0),
    "DAILY_LOSS_LIMIT_PCT":     ("float", 0.0, 1.0),
    "HARD_HALT_LOSS_PCT":       ("float", 0.0, 1.0),
    "DAYS_TO_RESOLUTION_MIN":   ("float", 0.0, 30.0),
    "DAYS_TO_RESOLUTION_MAX":   ("float", 0.1, 60.0),
    "DOMINANT_PRICE_MIN":       ("float", 0.5, 0.999),
    "DOMINANT_PRICE_MAX":       ("float", 0.5, 0.999),
    "MIN_VOLUME_24H_USD":       ("float", 0.0, 10_000_000.0),
    "MAX_SPREAD":               ("float", 0.0, 0.5),
    "MIN_BOOK_DEPTH_USD":       ("float", 0.0, 1_000_000.0),
    "DISPUTE_BUFFER":           ("float", 0.0, 0.5),
    "MIN_ANNUALIZED_EDGE":      ("float", 0.0, 5.0),
    "KELLY_FRACTION":           ("float", 0.0, 1.0),
    "EMERGENCY_EXIT_DROP":      ("float", 0.0, 1.0),
    "SLIPPAGE_TOLERANCE":       ("float", 0.0, 0.5),
    "SCAN_INTERVAL_SECONDS":    ("int", 30, 86400),
}


class _DashboardHandler(BaseHTTPRequestHandler):
    # Class-level refs; set by DashboardServer.start()
    dashboard: Optional[DashboardState] = None
    orchestrator: Optional["Orchestrator"] = None

    def log_message(self, format, *args):
        # Silence default access-log noise; pipe to debug instead
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

    def _read_json_body(self) -> dict:
        try:
            n = int(self.headers.get("Content-Length", "0"))
            if n <= 0 or n > 1_000_000:
                return {}
            raw = self.rfile.read(n)
            return json.loads(raw.decode("utf-8") or "{}")
        except Exception as e:
            log.debug("POST body parse error: %s", e)
            return {}

    def do_GET(self):
        if self.dashboard is None:
            self._send_json({"error": "dashboard not attached"}, 503)
            return

        parsed = urlparse(self.path)
        path = parsed.path
        qs = parse_qs(parsed.query)

        if path in ("/", "/index.html"):
            self._send_html(DASHBOARD_HTML)
            return

        if path == "/api/journal":
            try:
                limit = int(qs.get("limit", ["50"])[0])
            except Exception:
                limit = 50
            self._send_json(self.dashboard.events_tail(limit))
            return

        if path == "/api/editable_config":
            cfg = {k: getattr(Config, k, None)
                   for k in EDITABLE_CONFIG_KEYS}
            meta = {k: {"type": t, "min": lo, "max": hi}
                    for k, (t, lo, hi) in EDITABLE_CONFIG_KEYS.items()}
            self._send_json({"values": cfg, "meta": meta})
            return

        snap = self.dashboard.snapshot()
        if path == "/api/state":
            self._send_json(snap)
        elif path == "/api/candidates":
            self._send_json(snap.get("candidates", []))
        elif path == "/api/rejections":
            self._send_json(snap.get("rejections", []))
        elif path == "/api/positions":
            self._send_json(snap.get("positions", []))
        elif path == "/api/funnel":
            self._send_json(snap.get("filter_funnel", {}))
        elif path == "/api/stats":
            self._send_json(snap.get("stats", {}))
        elif path == "/api/strategies":
            self._send_json(snap.get("strategies", {}))
        elif path == "/api/analytics":
            self._send_json(snap.get("analytics", {}))
        elif path == "/api/hot_events":
            self._send_json(snap.get("hot_events", {}))
        elif path == "/api/config":
            self._send_json(snap.get("config", {}))
        else:
            self._send_json({"error": "not_found", "path": path}, 404)

    def do_POST(self):
        if self.orchestrator is None or self.dashboard is None:
            self._send_json({"error": "orchestrator not attached"}, 503)
            return
        parsed = urlparse(self.path)
        path = parsed.path
        body = self._read_json_body()

        if path == "/api/scan":
            self.orchestrator.request_scan_now()
            self._send_json({"ok": True, "message": "scan requested"})
        elif path == "/api/pause":
            self.orchestrator.pause_scheduler()
            self._send_json({"ok": True, "paused": True})
        elif path == "/api/resume":
            self.orchestrator.resume_scheduler()
            self._send_json({"ok": True, "paused": False})
        elif path == "/api/config":
            updates = {}
            errors = []
            for k, v in (body or {}).items():
                if k not in EDITABLE_CONFIG_KEYS:
                    errors.append(f"{k}: not editable")
                    continue
                typ, lo, hi = EDITABLE_CONFIG_KEYS[k]
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
                f"Config updated: {list(updates.keys()) or 'none'}"
                + (f" · errors: {errors}" if errors else ""),
                "warn" if errors else "info",
            )
            self._send_json({"ok": not errors, "updated": updates,
                             "errors": errors})
        elif path == "/api/backtest":
            csv_path = (body or {}).get("path", "historical_markets.csv")
            if not os.path.isfile(csv_path):
                self._send_json({"ok": False,
                                 "error": f"file not found: {csv_path}"}, 400)
                return
            try:
                results = self.orchestrator.run_backtest(csv_path)
                self._send_json({"ok": True, "results": results})
            except Exception as e:
                log.error("Backtest error: %s", e)
                self._send_json({"ok": False, "error": str(e)}, 500)
        else:
            self._send_json({"error": "not_found", "path": path}, 404)


class DashboardServer:
    """Tiny stdlib HTTP server serving the dashboard.

    Binds to 127.0.0.1 only — never exposed to the network.
    Runs in a daemon thread so it never blocks process exit.
    """

    def __init__(self, dashboard: DashboardState,
                 orchestrator: "Orchestrator", port: int = 8765):
        self.dashboard = dashboard
        self.orchestrator = orchestrator
        self.port = port
        self._httpd: Optional[ThreadingHTTPServer] = None
        self._thread: Optional[_threading.Thread] = None

    def start(self):
        _DashboardHandler.dashboard = self.dashboard
        _DashboardHandler.orchestrator = self.orchestrator
        self._httpd = ThreadingHTTPServer(("127.0.0.1", self.port),
                                          _DashboardHandler)
        self._thread = _threading.Thread(
            target=self._httpd.serve_forever,
            name="DashboardServer",
            daemon=True,
        )
        self._thread.start()
        log.info("Dashboard: http://127.0.0.1:%d/", self.port)


# ╔══════════════════════════════════════════════════════════════════════════════╗
# ║                          9. ORCHESTRATOR                                    ║
# ╚══════════════════════════════════════════════════════════════════════════════╝

class Orchestrator:
    """Top-level scheduler loop."""

    def __init__(self, *, read_only: bool = False, mock: bool = False,
                 broker: str = "polymarket",
                 state_path: Optional[str] = None):
        self.mock = mock
        self.broker_name = "mock" if mock else broker
        if mock:
            self.client = MockPolymarketClient()
        elif broker == "kalshi":
            self.client = KalshiClient(read_only=read_only)
        else:
            self.client = PolymarketClient(read_only=read_only)
        # Persistent store — positions, anchor, kill-switch, stats all survive
        # restarts via atomic JSON writes.
        self.store = PersistentStore(state_path or Config.STATE_FILE)
        self.journal = Journal()
        self.risk = RiskManager(self.journal, store=self.store)
        self.filter = MarketFilter(self.client)
        self.executor = ExecutionEngine(self.client, self.journal, self.risk)
        self.strategy = DecayStrategy()
        self.dashboard = DashboardState(store=self.store)
        # Seed broker/mock immediately so the UI reports correctly even
        # before the first scan completes.
        self.dashboard.update(broker=self.broker_name, mock=self.mock,
                              dry_run=Config.DRY_RUN)
        self.journal.attach_dashboard(self.dashboard)
        self._dashboard_server: Optional[DashboardServer] = None
        self._scan_now_event = _threading.Event()
        self._paused = _threading.Event()
        self._running = False
        # Cross-cycle state for hot-events tracking
        self._prev_market_snap: dict = {}     # condition_id → {price, volume, question}
        self._prev_candidate_ids: set = set() # condition_ids of last cycle's candidates

    def request_scan_now(self):
        self._scan_now_event.set()
        self.dashboard.push_message("Scan requested", "info")

    def pause_scheduler(self):
        self._paused.set()
        self.dashboard.update(scheduler_paused=True)
        self.dashboard.push_message("Scheduler paused", "warn")

    def resume_scheduler(self):
        self._paused.clear()
        self.dashboard.update(scheduler_paused=False)
        self.dashboard.push_message("Scheduler resumed", "info")

    def run_backtest(self, csv_path: str) -> dict:
        bt = Backtester()
        results = bt.run(csv_path)
        self.dashboard.update(last_backtest=results)
        self.dashboard.push_message(
            f"Backtest done: trades={results['total_trades']} "
            f"ret={results['total_return_pct']}%", "info")
        return results

    def start_dashboard(self, port: int = 8765):
        if self._dashboard_server is not None:
            return
        self._dashboard_server = DashboardServer(
            self.dashboard, orchestrator=self, port=port,
        )
        self._dashboard_server.start()

    def _config_snapshot(self) -> dict:
        return {
            "TOTAL_CAPITAL": Config.TOTAL_CAPITAL,
            "MAX_POSITION_USD": Config.MAX_POSITION_USD,
            "MAX_CONCURRENT_POSITIONS": Config.MAX_CONCURRENT_POSITIONS,
            "DAYS_TO_RESOLUTION_MIN": Config.DAYS_TO_RESOLUTION_MIN,
            "DAYS_TO_RESOLUTION_MAX": Config.DAYS_TO_RESOLUTION_MAX,
            "DOMINANT_PRICE_MIN": Config.DOMINANT_PRICE_MIN,
            "DOMINANT_PRICE_MAX": Config.DOMINANT_PRICE_MAX,
            "MIN_ANNUALIZED_EDGE": Config.MIN_ANNUALIZED_EDGE,
            "MIN_VOLUME_24H_USD": Config.MIN_VOLUME_24H_USD,
            "MAX_SPREAD": Config.MAX_SPREAD,
            "MIN_BOOK_DEPTH_USD": Config.MIN_BOOK_DEPTH_USD,
            "KELLY_FRACTION": Config.KELLY_FRACTION,
            "DISPUTE_BUFFER": Config.DISPUTE_BUFFER,
            "SCAN_INTERVAL_SECONDS": Config.SCAN_INTERVAL_SECONDS,
        }

    # ── Strategy views / analytics / hot-events helpers ──────────────────

    @staticmethod
    def _market_summary(m: Market, now_utc: datetime) -> dict:
        if not m.tokens:
            return {}
        top = max(m.tokens, key=lambda t: t.price)
        days = (m.end_date - now_utc).total_seconds() / 86400.0
        return {
            "condition_id": m.condition_id,
            "question": m.question,
            "category": m.category,
            "volume_24h": round(m.volume_24h, 2),
            "end_date": m.end_date.isoformat(),
            "days_to_resolution": round(days, 3),
            "dominant_outcome": top.outcome,
            "dominant_price": round(top.price, 4),
            "tokens": [{"outcome": t.outcome,
                        "price": round(t.price, 4),
                        "token_id": t.token_id} for t in m.tokens],
        }

    def _compute_strategy_views(self, markets: list, candidates: list,
                                now_utc: datetime) -> dict:
        """Build multiple candidate lists by applying different filters
        over the same market universe. Pure read-only views."""
        views: dict = {}
        ms = lambda m: self._market_summary(m, now_utc)

        # Decay: just re-publish the primary filter's output
        views["decay"] = {
            "name": "Decay",
            "description": ("Buy the dominant token of near-certain markets "
                            "close to resolution and capture convergence to 1.0."),
            "matches": [ms(c.market) | {
                "edge": round(c.edge, 4),
                "annualized_edge": round(c.annualized_edge, 4),
                "spread": round(c.spread, 4),
                "book_depth_usd": round(c.book_depth_usd, 2),
            } for c in candidates[:25]],
        }

        # Mean Reversion: uncertain markets (45-55¢) where coin-flip pricing
        # might be off, especially with high volume.
        mr = []
        for m in markets:
            if m.closed or not m.tokens:
                continue
            top = max(m.tokens, key=lambda t: t.price)
            if 0.45 <= top.price <= 0.55 and m.volume_24h >= 1000:
                mr.append((m, abs(0.50 - top.price), m.volume_24h))
        mr.sort(key=lambda x: -x[2])  # rank by volume
        views["mean_reversion"] = {
            "name": "Mean Reversion",
            "description": ("Highly uncertain markets where coin-flip pricing "
                            "might be exploitable. Higher volume = more signal."),
            "matches": [ms(m) | {"volume_rank": i + 1}
                        for i, (m, _, _) in enumerate(mr[:25])],
        }

        # Arbitrage: Yes + No != 1.0 — sum mismatch on binary markets.
        arb = []
        for m in markets:
            if m.closed or len(m.tokens) != 2:
                continue
            total = sum(t.price for t in m.tokens)
            if abs(total - 1.0) > 0.015:
                arb.append((m, total))
        arb.sort(key=lambda x: -abs(x[1] - 1.0))
        views["arbitrage"] = {
            "name": "Arbitrage",
            "description": ("Binary markets where Yes + No ≠ 1.0. "
                            "If sum < 1: buy both for guaranteed profit. "
                            "If sum > 1: sell both."),
            "matches": [ms(m) | {
                "sum": round(t, 4),
                "edge_pct": round((1.0 - t) * 100 if t < 1 else (t - 1) * 100, 2),
                "direction": "buy_both" if t < 1 else "sell_both",
            } for m, t in arb[:25]],
        }

        # Volume Leaders: top-N by 24h volume
        vol_sorted = sorted([m for m in markets if not m.closed],
                            key=lambda m: -m.volume_24h)
        views["volume_leaders"] = {
            "name": "Volume Leaders",
            "description": "Markets with the highest 24-hour trading volume.",
            "matches": [ms(m) for m in vol_sorted[:25]],
        }

        # Closing Soon: markets resolving within 24h
        closing = []
        for m in markets:
            if m.closed:
                continue
            days = (m.end_date - now_utc).total_seconds() / 86400.0
            if 0 <= days <= 1.0:
                closing.append((m, days))
        closing.sort(key=lambda x: x[1])
        views["closing_soon"] = {
            "name": "Closing Soon",
            "description": "Markets resolving within the next 24 hours.",
            "matches": [ms(m) for m, _ in closing[:25]],
        }

        return views

    def _compute_analytics(self, markets: list, candidates: list,
                           now_utc: datetime) -> dict:
        """Aggregate market data for the Analytics tab charts."""
        from collections import Counter

        # Category breakdown (open markets only)
        cat_counter = Counter()
        for m in markets:
            if not m.closed:
                cat_counter[m.category or "Uncategorized"] += 1
        by_category = [{"label": k, "count": v}
                       for k, v in cat_counter.most_common(10)]

        # Annualized edge histogram (candidates only)
        ann_bins = [
            (0.0, 0.5, "0-50%"),
            (0.5, 1.0, "50-100%"),
            (1.0, 2.0, "100-200%"),
            (2.0, 5.0, "200-500%"),
            (5.0, 10.0, "500-1000%"),
            (10.0, 50.0, "1k-5k%"),
            (50.0, 10_000, ">5000%"),
        ]
        edge_hist = []
        for lo, hi, label in ann_bins:
            n = sum(1 for c in candidates if lo <= c.annualized_edge < hi)
            edge_hist.append({"label": label, "count": n})

        # Days-to-resolution histogram (all open markets)
        day_bins = [
            (0, 0.25, "<6h"),
            (0.25, 1, "6h–1d"),
            (1, 3, "1–3d"),
            (3, 7, "3–7d"),
            (7, 14, "1–2w"),
            (14, 30, "2w–1m"),
            (30, 9999, "1m+"),
        ]
        days_hist = []
        for lo, hi, label in day_bins:
            n = 0
            for m in markets:
                if m.closed:
                    continue
                d = (m.end_date - now_utc).total_seconds() / 86400.0
                if lo <= d < hi:
                    n += 1
            days_hist.append({"label": label, "count": n})

        # Dominant price distribution (all open markets)
        price_bins = [
            (0, 0.30, "0–30¢"),
            (0.30, 0.50, "30–50¢"),
            (0.50, 0.70, "50–70¢"),
            (0.70, 0.85, "70–85¢"),
            (0.85, 0.92, "85–92¢"),
            (0.92, 0.97, "92–97¢"),
            (0.97, 1.01, "97¢+"),
        ]
        price_hist = []
        for lo, hi, label in price_bins:
            n = 0
            for m in markets:
                if m.closed or not m.tokens:
                    continue
                top = max(m.tokens, key=lambda t: t.price)
                if lo <= top.price < hi:
                    n += 1
            price_hist.append({"label": label, "count": n})

        # Spread vs Depth scatter — one point per candidate
        scatter = [{
            "spread": round(c.spread, 4),
            "depth": round(c.book_depth_usd, 2),
            "ann": round(c.annualized_edge, 4),
            "question": c.market.question[:60],
        } for c in candidates[:50]]

        return {
            "by_category": by_category,
            "edge_histogram": edge_hist,
            "days_histogram": days_hist,
            "price_distribution": price_hist,
            "spread_depth_scatter": scatter,
            "total_open_markets": sum(1 for m in markets if not m.closed),
            "total_closed_markets": sum(1 for m in markets if m.closed),
        }

    def _compute_hot_events(self, markets: list, candidates: list) -> dict:
        """Diff this cycle's market state against the previous cycle."""
        prev = self._prev_market_snap
        current: dict = {}
        movers: list = []
        vol_movers: list = []

        for m in markets:
            if not m.tokens:
                continue
            top = max(m.tokens, key=lambda t: t.price)
            current[m.condition_id] = {
                "question": m.question,
                "category": m.category,
                "price": top.price,
                "volume_24h": m.volume_24h,
            }
            if m.condition_id in prev:
                p_prev = prev[m.condition_id]["price"]
                v_prev = prev[m.condition_id]["volume_24h"]
                d_price = top.price - p_prev
                if abs(d_price) >= 0.015:
                    movers.append({
                        "condition_id": m.condition_id,
                        "question": m.question,
                        "from_price": round(p_prev, 4),
                        "to_price": round(top.price, 4),
                        "delta": round(d_price, 4),
                        "delta_pct": (round((top.price / p_prev - 1) * 100, 2)
                                      if p_prev else 0),
                    })
                d_vol = m.volume_24h - v_prev
                if v_prev > 0 and abs(d_vol) / v_prev >= 0.25:
                    vol_movers.append({
                        "condition_id": m.condition_id,
                        "question": m.question,
                        "from_volume": round(v_prev, 2),
                        "to_volume": round(m.volume_24h, 2),
                        "delta_pct": round((m.volume_24h / v_prev - 1) * 100, 2),
                    })

        movers.sort(key=lambda x: -abs(x["delta"]))
        vol_movers.sort(key=lambda x: -abs(x["delta_pct"]))

        # New / dropped candidates (by condition_id)
        cur_cand_ids = {c.market.condition_id for c in candidates}
        prev_cand_ids = self._prev_candidate_ids
        new_ids = cur_cand_ids - prev_cand_ids
        dropped_ids = prev_cand_ids - cur_cand_ids
        cand_lookup = {c.market.condition_id: c for c in candidates}
        new_cands = []
        for cid in list(new_ids)[:10]:
            c = cand_lookup.get(cid)
            if c:
                new_cands.append({
                    "condition_id": cid,
                    "question": c.market.question,
                    "price": round(c.dominant_token.price, 4),
                    "ann": round(c.annualized_edge, 4),
                })
        dropped_cands = [{
            "condition_id": cid,
            "question": prev.get(cid, {}).get("question", "(unknown)"),
        } for cid in list(dropped_ids)[:10]]

        # Persist for next cycle
        self._prev_market_snap = current
        self._prev_candidate_ids = cur_cand_ids

        return {
            "price_movers": movers[:15],
            "volume_movers": vol_movers[:15],
            "new_candidates": new_cands,
            "dropped_candidates": dropped_cands,
            "tracked_markets": len(current),
        }

    def _publish_snapshot(self, *, candidates: list, rejections: list,
                          equity: float, funnel: Optional[dict] = None):
        now_utc = datetime.now(timezone.utc)
        next_at = now_utc + timedelta(seconds=Config.SCAN_INTERVAL_SECONDS)

        candidate_rows = []
        for i, c in enumerate(candidates[:50]):
            # All tokens in market (so UI can show Yes/No pair)
            all_tokens = [{
                "token_id": t.token_id,
                "outcome": t.outcome,
                "price": round(t.price, 4),
            } for t in c.market.tokens]
            row = {
                "question": c.market.question,
                "category": c.market.category,
                "condition_id": c.market.condition_id,
                "end_date": c.market.end_date.isoformat(),
                "volume_24h": round(c.market.volume_24h, 2),
                "outcome": c.dominant_token.outcome,
                "token_id": c.dominant_token.token_id,
                "price": round(c.dominant_token.price, 4),
                "edge": round(c.edge, 4),
                "annualized_edge": round(c.annualized_edge, 4),
                "days_to_resolution": round(c.days_to_resolution, 3),
                "spread": round(c.spread, 4),
                "book_depth_usd": round(c.book_depth_usd, 2),
                "score": round(c.score, 4),
                "all_tokens": all_tokens,
            }
            # Enrich top 5 with orderbook for drill-down
            if i < 5:
                ob = self.client.get_orderbook(c.dominant_token.token_id)
                if ob:
                    row["orderbook"] = {
                        "bids": ob.bids[:8],
                        "asks": ob.asks[:8],
                        "best_bid": ob.best_bid,
                        "best_ask": ob.best_ask,
                    }
            # Sizing preview
            kelly_size = c.edge * Config.KELLY_FRACTION * equity
            size_usdc = max(0.0, min(Config.MAX_POSITION_USD, kelly_size,
                                     c.book_depth_usd))
            row["proposed_usdc"] = round(size_usdc, 2)
            row["proposed_shares"] = (round(size_usdc / c.dominant_token.price, 4)
                                      if c.dominant_token.price else 0.0)
            candidate_rows.append(row)

        position_rows = []
        for p in self.risk.positions.values():
            # Mark-to-market with current best-bid if available
            ob = self.client.get_orderbook(p.token_id)
            mark = ob.best_bid if (ob and ob.best_bid) else p.entry_price
            pnl_usdc = (mark - p.entry_price) * p.entry_shares
            pnl_pct = ((mark / p.entry_price) - 1) * 100 if p.entry_price else 0
            position_rows.append({
                "token_id": p.token_id,
                "market_question": p.market_question,
                "entry_timestamp": p.entry_timestamp.isoformat(),
                "entry_price": round(p.entry_price, 4),
                "entry_shares": round(p.entry_shares, 4),
                "entry_usdc": round(p.entry_usdc, 2),
                "mark_price": round(mark, 4),
                "mark_usdc": round(mark * p.entry_shares, 2),
                "pnl_usdc": round(pnl_usdc, 2),
                "pnl_pct": round(pnl_pct, 2),
                "strategy": p.strategy,
            })

        # Compute multi-strategy views, analytics, and hot events from the
        # full markets universe (not just the post-filter candidates).
        markets_all = self.filter.last_markets or []
        strategies = self._compute_strategy_views(
            markets_all, candidates, now_utc)
        analytics = self._compute_analytics(markets_all, candidates, now_utc)
        hot = self._compute_hot_events(markets_all, candidates)

        self.dashboard.update(
            last_cycle_at=now_utc.isoformat(),
            next_cycle_at=next_at.isoformat(),
            kill_switch=self.risk.kill_switch_status(equity),
            equity=round(equity, 2),
            deployed=round(self.risk.deployed_usdc(), 2),
            daily_anchor=(round(self.risk.equity_at_utc_midnight, 2)
                          if self.risk.equity_at_utc_midnight else None),
            dry_run=Config.DRY_RUN,
            mock=self.mock,
            broker=self.broker_name,
            scheduler_paused=self._paused.is_set(),
            candidates=candidate_rows,
            rejections=rejections,
            positions=position_rows,
            config=self._config_snapshot(),
            filter_funnel=funnel or {},
            strategies=strategies,
            analytics=analytics,
            hot_events=hot,
            total_markets=len(markets_all),
        )
        self.dashboard.stats.record_cycle(
            markets_count=(funnel or {}).get("fetched", 0),
            candidates=len(candidate_rows),
            rejections=rejections,
        )
        self.dashboard.stats.record_equity(now_utc.isoformat(), equity)

    def _portfolio_value(self) -> float:
        """Total equity = free cash (real if available, else Config baseline)
        + mark-to-market of every open position."""
        # Mark-to-market positions at current best-bid
        mtm = 0.0
        for token_id, st in self.risk.positions.items():
            ob = self.client.get_orderbook(token_id)
            if ob and ob.best_bid is not None:
                mtm += st.entry_shares * ob.best_bid
            else:
                mtm += st.entry_usdc

        # Prefer real wallet balance when the broker can give it to us.
        # Fall back to the hard-coded baseline minus deployed USDC.
        real_cash = self.client.get_balance()
        if real_cash is not None:
            return real_cash + mtm
        deployed = self.risk.deployed_usdc()
        cash = max(0.0, Config.TOTAL_CAPITAL - deployed)
        return cash + mtm

    def start(self):
        log.info("=" * 70)
        log.info("  POLYMARKET DECAY FRAMEWORK  |  DRY_RUN=%s", Config.DRY_RUN)
        log.info("=" * 70)

        self.client.init_auth()
        equity = self._portfolio_value()
        self.risk.update_daily_anchor_if_needed(equity)
        self._running = True

        # Run one cycle immediately
        try:
            self._scan_cycle()
            while self._running:
                # Wait for scan interval OR a dashboard-triggered scan-now
                triggered = self._scan_now_event.wait(
                    timeout=Config.SCAN_INTERVAL_SECONDS
                )
                if triggered:
                    self._scan_now_event.clear()
                    log.info("Manual scan triggered from dashboard.")
                self._scan_cycle()
        except KeyboardInterrupt:
            log.info("Shutdown requested.")
        finally:
            log.info("Final equity: $%.2f", self._portfolio_value())

    def scan_only(self):
        """One-shot: list candidates, don't trade."""
        log.info("Running scan-only mode...")
        candidates, rejections, funnel = self.filter.find_candidates()
        equity = self._portfolio_value()
        self.risk.update_daily_anchor_if_needed(equity)
        self._publish_snapshot(candidates=candidates, rejections=rejections,
                               equity=equity, funnel=funnel)
        if not candidates:
            log.info("No candidates found.")
            return
        log.info("─── TOP DECAY CANDIDATES ───")
        for i, c in enumerate(candidates[:20], 1):
            log.info(
                "[%d] %s | %s | price=%.3f edge=%.3f ann=%.1f%% days=%.2f depth=$%.0f",
                i, c.market.question[:60], c.dominant_token.outcome,
                c.dominant_token.price, c.edge, c.annualized_edge * 100,
                c.days_to_resolution, c.book_depth_usd,
            )

    def dashboard_loop(self):
        """Keep the scan-mode dashboard responsive: wait for scan-now
        requests indefinitely, re-running a scan each time."""
        log.info("Dashboard left running. Click 'Scan Now' or Ctrl-C to exit.")
        try:
            while True:
                self._scan_now_event.wait()
                self._scan_now_event.clear()
                log.info("Manual scan triggered from dashboard.")
                self.scan_only()
        except KeyboardInterrupt:
            log.info("Shutdown requested.")

    def _scan_cycle(self):
        log.info("─── Scan cycle: %s ───",
                 datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC"))

        equity = self._portfolio_value()
        self.risk.update_daily_anchor_if_needed(equity)

        ks = self.risk.kill_switch_status(equity)
        if ks == "HALT_ALL":
            log.error("HALT_ALL active — skipping cycle entirely.")
            return

        # 1. Manage exits on existing positions
        for token_id in list(self.risk.positions.keys()):
            state = self.risk.positions[token_id]
            ob = self.client.get_orderbook(token_id)
            current_price = ob.best_bid if (ob and ob.best_bid) else state.entry_price
            # We don't have market end_date here without re-fetching; pass entry+max
            decision = self.strategy.evaluate_exit(
                state=state,
                current_price=current_price,
                now_utc=datetime.now(timezone.utc),
                market_end_date=state.entry_timestamp + timedelta(
                    days=Config.DAYS_TO_RESOLUTION_MAX
                ),
            )
            if decision:
                self.executor.place(decision, DecayStrategy.NAME)

        # 2. New entries (gated by kill switch)
        candidates, rejections, funnel = self.filter.find_candidates()
        # Publish snapshot regardless of HALT state so the dashboard
        # still shows what the filter saw this cycle.
        equity = self._portfolio_value()
        self._publish_snapshot(candidates=candidates, rejections=rejections,
                               equity=equity, funnel=funnel)
        if ks == "HALT_NEW":
            log.warning("HALT_NEW active — skipping new entries this cycle.")
            return
        if self._paused.is_set():
            log.info("Scheduler paused — skipping new entries this cycle.")
            return

        for c in candidates:
            equity = self._portfolio_value()
            deployed = self.risk.deployed_usdc()
            if not self.risk.can_open_new(deployed, equity):
                break
            if c.dominant_token.token_id in self.risk.positions:
                continue
            decision = self.strategy.evaluate_entry(c, equity)
            if not decision:
                continue
            decision.usdc_amount = self.risk.cap_position_size(
                decision.usdc_amount, equity, deployed
            )
            if decision.usdc_amount < Config.MIN_TRADE_USD:
                continue
            self.executor.place(decision, DecayStrategy.NAME)
            time.sleep(0.3)

        log.info(
            "Cycle done. positions=%d deployed=$%.2f equity=$%.2f",
            len(self.risk.positions),
            self.risk.deployed_usdc(),
            self._portfolio_value(),
        )


# ╔══════════════════════════════════════════════════════════════════════════════╗
# ║                          10. BACKTESTER                                     ║
# ╚══════════════════════════════════════════════════════════════════════════════╝

class Backtester:
    """
    Replay decay strategy over historical resolved markets.

    Expected CSV columns (header required):
      condition_id, question, end_date_iso, category,
      final_outcome (0|1 for the dominant token),
      price_history_json (list of {ts_iso, best_ask, best_bid, vol_24h}),
      dominant_token_id

    Produces a summary + trade list to stdout/log.
    """

    def run(self, csv_path: str) -> dict:
        rows = []
        with open(csv_path, "r") as f:
            for row in csv.DictReader(f):
                rows.append(row)
        log.info("Backtest: loaded %d historical markets", len(rows))

        capital = Config.TOTAL_CAPITAL
        cash = capital
        trades: list[dict] = []
        equity_curve: list[float] = []

        for row in rows:
            try:
                end_date = datetime.fromisoformat(
                    row["end_date_iso"].replace("Z", "+00:00")
                )
                final = int(row["final_outcome"])
                history = json.loads(row["price_history_json"])
                category = row.get("category", "")
                if not history:
                    continue
                if not MarketFilter._category_ok(category):
                    continue
            except Exception as e:
                log.debug("Skip backtest row: %s", e)
                continue

            # Walk forward; first sample where filters pass = entry
            entry = None
            for sample in history:
                ts = datetime.fromisoformat(sample["ts_iso"].replace("Z", "+00:00"))
                days = (end_date - ts).total_seconds() / 86400
                if days < Config.DAYS_TO_RESOLUTION_MIN:
                    break
                if days > Config.DAYS_TO_RESOLUTION_MAX:
                    continue
                ask = float(sample.get("best_ask", 0))
                if not (Config.DOMINANT_PRICE_MIN <= ask <= Config.DOMINANT_PRICE_MAX):
                    continue
                if float(sample.get("vol_24h", 0)) < Config.MIN_VOLUME_24H_USD:
                    continue
                edge = (1.0 - ask) - Config.DISPUTE_BUFFER
                if edge <= 0:
                    continue
                ann = (edge / ask) * (365.0 / max(days, 0.01))
                if ann < Config.MIN_ANNUALIZED_EDGE:
                    continue

                size_usdc = min(
                    Config.MAX_POSITION_USD,
                    edge * Config.KELLY_FRACTION * capital,
                    cash,
                )
                if size_usdc < Config.MIN_TRADE_USD:
                    continue

                # Apply 1¢ entry-spread haircut
                effective_ask = ask + 0.005
                shares = size_usdc / effective_ask
                cash -= size_usdc
                entry = {
                    "ts": ts, "price": effective_ask, "shares": shares,
                    "size_usdc": size_usdc, "ann": ann,
                }
                break

            if entry is None:
                continue

            # Settle at resolution
            payout = entry["shares"] * (1.0 if final == 1 else 0.0)
            cash += payout
            pnl = payout - entry["size_usdc"]
            trades.append({
                "question": row.get("question", "")[:60],
                "entry_price": round(entry["price"], 4),
                "shares": round(entry["shares"], 4),
                "size_usdc": round(entry["size_usdc"], 2),
                "outcome": final,
                "pnl": round(pnl, 2),
                "ann_edge": round(entry["ann"], 3),
            })
            equity_curve.append(cash)

        wins = [t for t in trades if t["pnl"] > 0]
        total_ret = (cash - capital) / capital * 100 if capital else 0
        return {
            "initial_capital": capital,
            "final_cash": round(cash, 2),
            "total_return_pct": round(total_ret, 2),
            "total_trades": len(trades),
            "win_rate_pct": round(len(wins) / max(len(trades), 1) * 100, 1),
            "avg_pnl": round(sum(t["pnl"] for t in trades) / max(len(trades), 1), 2),
            "trades": trades,
        }

    def print_results(self, results: dict):
        log.info("━" * 60)
        log.info("  BACKTEST RESULTS")
        log.info("━" * 60)
        log.info("  Initial Capital  : $%.2f", results["initial_capital"])
        log.info("  Final Cash       : $%.2f", results["final_cash"])
        log.info("  Total Return     : %.2f%%", results["total_return_pct"])
        log.info("  Total Trades     : %d", results["total_trades"])
        log.info("  Win Rate         : %.1f%%", results["win_rate_pct"])
        log.info("  Avg PnL/Trade    : $%.2f", results["avg_pnl"])
        log.info("━" * 60)
        for t in results["trades"][-20:]:
            mark = "WIN " if t["pnl"] > 0 else "LOSS"
            log.info(
                "  %s  %s | entry=%.3f size=$%.2f → pnl=$%.2f",
                mark, t["question"], t["entry_price"], t["size_usdc"], t["pnl"],
            )


# ╔══════════════════════════════════════════════════════════════════════════════╗
# ║                          11. CLI ENTRY POINT                                ║
# ╚══════════════════════════════════════════════════════════════════════════════╝

def main():
    parser = argparse.ArgumentParser(
        description="Polymarket Decay Trading Framework",
        formatter_class=argparse.RawTextHelpFormatter,
    )
    parser.add_argument(
        "--mode", choices=["live", "scan", "backtest"], default="scan",
        help=(
            "live     : run scheduler loop, place trades (dry-run unless --live-orders)\n"
            "scan     : one-shot: discover and rank decay candidates, no orders\n"
            "backtest : replay strategy over a historical CSV (requires --history)"
        ),
    )
    parser.add_argument("--capital", type=float, default=None,
                        help="Override TOTAL_CAPITAL (USDC)")
    parser.add_argument("--live-orders", action="store_true",
                        help="REAL order execution (disables dry-run). USE WITH CAUTION.")
    parser.add_argument("--history", default="historical_markets.csv",
                        help="CSV path for backtest mode")
    parser.add_argument("--log-level",
                        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
                        default="INFO")
    parser.add_argument("--dashboard", action="store_true",
                        help="Launch the local HTML dashboard at "
                             "http://127.0.0.1:<port>/ (read-only)")
    parser.add_argument("--dashboard-port", type=int, default=8765,
                        help="Port for the dashboard server (default: 8765)")
    parser.add_argument("--mock", action="store_true",
                        help="Use synthetic market data instead of the real "
                             "API. Useful when the API is unreachable "
                             "(corporate firewall, geofence, offline dev).")
    parser.add_argument("--broker", choices=["polymarket", "kalshi"],
                        default="polymarket",
                        help="Which broker to trade on. Ignored when --mock.")
    parser.add_argument("--state-file", default=None,
                        help=f"Path to JSON state file "
                             f"(default: {Config.STATE_FILE}).")
    args = parser.parse_args()

    setup_logging(getattr(logging, args.log_level))

    if args.capital:
        Config.TOTAL_CAPITAL = args.capital
    if args.live_orders:
        Config.DRY_RUN = False
        log.warning("LIVE ORDER MODE ENABLED — real money will be used.")
    else:
        Config.DRY_RUN = True

    if args.mode == "scan":
        orch = Orchestrator(read_only=True, mock=args.mock,
                            broker=args.broker, state_path=args.state_file)
        if args.dashboard:
            orch.start_dashboard(args.dashboard_port)
        orch.scan_only()
        if args.dashboard:
            orch.dashboard_loop()
    elif args.mode == "backtest":
        bt = Backtester()
        results = bt.run(args.history)
        bt.print_results(results)
    else:  # live
        orch = Orchestrator(mock=args.mock, broker=args.broker,
                            state_path=args.state_file)
        if args.dashboard:
            orch.start_dashboard(args.dashboard_port)
        orch.start()


if __name__ == "__main__":
    main()

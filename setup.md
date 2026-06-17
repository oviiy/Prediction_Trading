# Polymarket Decay Trading Framework

Single-file Python trading bot for prediction-market **decay** trades: buy the
near-certain side of markets close to resolution, capture convergence to $1 on
resolution. Comes with a rich local HTML dashboard, multiple "strategy lenses"
(decay, mean reversion, arbitrage, volume leaders, closing soon), live-editable
strategy knobs, an in-UI backtest panel, hot-events tracking, and persistent
state across restarts.

Supports **Polymarket** and **Kalshi** as brokers, plus a **mock** mode that
generates synthetic markets for offline development and demoing.

> **Status:** research / framework. The live-trading code paths exist but
> haven't been exercised against real exchanges from this codebase. Read the
> "Trading caveats" section before flipping `--live-orders` on.

---

## Quick start (mock mode, no creds, no network)

The fastest way to see the dashboard light up:

```bash
cd /Users/michael.wang/MW/Personal/trade
pip3 install --user requests python-dotenv
python3 polymarket_decay.py --mode scan --dashboard --mock
# Open http://127.0.0.1:8765/
```

This generates ~60 synthetic markets every cycle, runs them through the real
filter pipeline, and populates everything: funnel, candidates, rejection trace,
strategy tabs, analytics charts, hot events.

---

## Prerequisites

- **Python 3.9+** (this repo was tested on macOS Python 3.9.6).
- Required packages (read-only / mock / dashboard):
  ```bash
  pip3 install --user requests python-dotenv
  ```
- For **live Polymarket** trading:
  ```bash
  pip3 install --user py-clob-client
  ```
- For **live Kalshi** trading:
  ```bash
  pip3 install --user cryptography
  ```
- No npm / build step. The dashboard HTML is embedded in the Python file.

---

## CLI reference

```text
python3 polymarket_decay.py [OPTIONS]

--mode {scan,live,backtest}
    scan      One-shot: discover and rank candidates, no orders.
              With --dashboard, the bot stays running so you can
              trigger more scans from the UI.
    live      Scheduler loop. Re-scans every SCAN_INTERVAL_SECONDS
              and places orders (dry-run unless --live-orders).
    backtest  Replay strategy over a historical CSV (--history).

--broker {polymarket,kalshi}      Default: polymarket.
                                  Ignored when --mock is set.
--mock                            Use synthetic markets (offline dev).
--dashboard                       Launch the local HTML dashboard.
--dashboard-port N                Default: 8765.
--state-file PATH                 JSON state file (default: state.json
                                  in cwd).
--live-orders                     Disable dry-run. REAL orders.
                                  Use with extreme care.
--capital N                       Override TOTAL_CAPITAL (USDC).
--history PATH                    CSV path for --mode backtest.
--log-level {DEBUG,INFO,WARNING,ERROR}    Default: INFO.
```

---

## Common run patterns

### Research / demo (no network, no creds)
```bash
python3 polymarket_decay.py --mode scan --dashboard --mock
```

### Scan live Polymarket markets (read-only — most of the dashboard works)
```bash
python3 polymarket_decay.py --mode scan --dashboard
```
> Note: from a US IP, Polymarket geofences some endpoints. From a Clearwater
> office network, both Polymarket and Kalshi are blocked by the web filter.

### Live Polymarket scheduler in dry-run (every 15min, logs intended trades)
```bash
python3 polymarket_decay.py --mode live --dashboard
```

### Live Polymarket scheduler with **real orders** (read warnings below first)
```bash
python3 polymarket_decay.py --mode live --dashboard --live-orders
```

### Scan Kalshi (read-only)
```bash
python3 polymarket_decay.py --mode scan --dashboard --broker kalshi
```

### Live Kalshi (dry-run, then live)
```bash
python3 polymarket_decay.py --mode live --dashboard --broker kalshi
python3 polymarket_decay.py --mode live --dashboard --broker kalshi --live-orders
```

### Backtest
```bash
python3 polymarket_decay.py --mode backtest --history historical_markets.csv
```

---

## Configuring credentials

Create a `.env` file in the same directory as `polymarket_decay.py`. It is
loaded automatically by `python-dotenv`. **Never commit this file** — add
`.env` to your `.gitignore` if you use git.

### Polymarket

```bash
# .env
POLY_API_KEY=...           # from polymarket.com → API
POLY_API_SECRET=...
POLY_API_PASSPHRASE=...
POLY_PRIVATE_KEY=0x...     # wallet private key, for on-chain signing
```

Setup steps:

1. Create a wallet (e.g., MetaMask) on the Polygon network.
2. Bridge USDC.e (bridged USDC, the version Polymarket uses) to your wallet
   via a bridge like Polygon's official one or via Binance withdrawal directly
   to Polygon.
3. Send a small amount of MATIC (a few cents) for gas.
4. On polymarket.com, generate API key/secret/passphrase under Account → API.
5. Drop all four into `.env`.

### Kalshi

```bash
# .env
KALSHI_API_KEY_ID=00000000-0000-0000-0000-000000000000
KALSHI_PRIVATE_KEY_PATH=/Users/michael.wang/.kalshi/private.pem
```

Setup steps:

1. Sign up at [kalshi.com](https://kalshi.com).
2. Fund your account via bank transfer (USD, not crypto).
3. Go to Account → API Keys, generate a new key. Save the **private key**
   `.pem` file somewhere safe (e.g. `~/.kalshi/private.pem`, `chmod 600`).
4. Copy the **key ID** (UUID) and the **path** into `.env`.

---

## Files the bot creates

| File | Purpose |
|------|---------|
| `state.json` | Open positions, daily anchor, kill-switch state, cumulative stats. Atomic writes. Override with `--state-file`. |
| `trades.csv` | Human-readable trade journal. |
| `trades.jsonl` | Structured journal with full event metadata. |
| `framework.log` | Bot log. |

Delete `state.json` to reset everything (positions, daily anchor, lifetime
stats). The other files append over time.

---

## The dashboard

After launching with `--dashboard`, open <http://127.0.0.1:8765/>. Six tabs:

- **Overview** — stats cards, equity sparkline, open positions with mark/PnL,
  top decay candidates (click a row to see the orderbook), recent journal
  events.
- **Strategies** — sub-tabs for five different lenses over the same market
  universe: Decay, Mean Reversion, Arbitrage, Volume Leaders, Closing Soon.
- **Hot Events** — biggest price movers and volume movers since the last
  cycle, plus new/dropped candidates.
- **Analytics** — category breakdown bar chart, rejection donut, edge /
  days-to-resolution / dominant-price histograms, spread-vs-depth scatter,
  filter funnel.
- **Rejections** — full rejection trace with reason chips (click to filter)
  and counterfactual hints ("Lowering MIN_ANNUALIZED_EDGE from 40% to 20%
  would recover N candidates").
- **Tools** — live-editable strategy knobs (20 sliders) and a backtest panel
  (specify a CSV path, click Run).

The two header buttons:
- **Scan Now** — interrupts the sleep and runs a fresh cycle.
- **Pause / Resume** — gates new entries; still manages existing positions.

The dashboard binds to `127.0.0.1` only and is not exposed to the network.
Polling interval is 3 seconds.

---

## Running in the background

Three options, easiest first:

### 1. `nohup` — simplest, dies on reboot
```bash
cd /Users/michael.wang/MW/Personal/trade
nohup python3 polymarket_decay.py --mode live --dashboard --mock \
  > decay.log 2>&1 &
disown
# Later:
tail -f decay.log
open http://127.0.0.1:8765/
pkill -f polymarket_decay.py   # to stop
```

### 2. `tmux` — interactive, survives logout, dies on reboot
```bash
tmux new -s decay
python3 polymarket_decay.py --mode live --dashboard --mock
# Detach: Ctrl-b then d
# Reattach later: tmux attach -t decay
```

### 3. `launchd` — survives reboot, auto-restarts on crash

Save this to `~/Library/LaunchAgents/com.mw.decay.plist`:

```xml
<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN"
  "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
  <key>Label</key>
  <string>com.mw.decay</string>
  <key>WorkingDirectory</key>
  <string>/Users/michael.wang/MW/Personal/trade</string>
  <key>ProgramArguments</key>
  <array>
    <string>/usr/bin/python3</string>
    <string>polymarket_decay.py</string>
    <string>--mode</string><string>live</string>
    <string>--dashboard</string>
    <string>--mock</string>
  </array>
  <key>RunAtLoad</key>
  <true/>
  <key>KeepAlive</key>
  <true/>
  <key>StandardOutPath</key>
  <string>/Users/michael.wang/MW/Personal/trade/decay.log</string>
  <key>StandardErrorPath</key>
  <string>/Users/michael.wang/MW/Personal/trade/decay.err</string>
</dict>
</plist>
```

Then:
```bash
launchctl load ~/Library/LaunchAgents/com.mw.decay.plist
# Verify:
launchctl list | grep decay
# Stop and remove:
launchctl unload ~/Library/LaunchAgents/com.mw.decay.plist
```

---

## Strategy tuning

The `Config` class in `polymarket_decay.py` holds all the knobs. The most
important ones, with defaults:

| Knob | Default | Meaning |
|------|---------|---------|
| `DAYS_TO_RESOLUTION_MIN` | 0.25 | Skip markets less than 6h from resolution |
| `DAYS_TO_RESOLUTION_MAX` | 7.0 | Skip markets more than 7d out |
| `DOMINANT_PRICE_MIN` | 0.85 | Price band lower bound |
| `DOMINANT_PRICE_MAX` | 0.97 | Price band upper bound |
| `MIN_ANNUALIZED_EDGE` | 0.40 | Required annualized edge (40%) |
| `MIN_VOLUME_24H_USD` | 5000 | Volume floor |
| `MAX_SPREAD` | 0.03 | Spread ceiling (3¢) |
| `MIN_BOOK_DEPTH_USD` | 200 | Depth floor at the limit price |
| `DISPUTE_BUFFER` | 0.02 | Haircut for dispute risk |
| `KELLY_FRACTION` | 0.25 | Quarter-Kelly sizing |
| `MAX_POSITION_USD` | 200 | Per-position cap |
| `MAX_CONCURRENT_POSITIONS` | 3 | Position count cap |
| `DAILY_LOSS_LIMIT_PCT` | 0.10 | Halt new entries at -10% daily |
| `HARD_HALT_LOSS_PCT` | 0.20 | Halt everything at -20% daily |
| `SCAN_INTERVAL_SECONDS` | 900 | Cycle every 15 minutes |

All of these are editable live from the **Tools** tab in the dashboard. The
changes apply on the next scan.

---

## Trading caveats — read before `--live-orders`

The framework has been tested in dry-run and mock. Real-money trading has
real risk. Specific gotchas:

1. **Polymarket geofence.** From a US IP and/or a wallet flagged as US,
   Polymarket's CLOB will reject orders. They settled with the CFTC in 2022
   over US user access. If you are a US person, **don't trade Polymarket;
   use Kalshi instead** (CFTC-regulated, US-legal).
2. **Kalshi is the practical option for US users.** It has a public API,
   similar market shape, and is fully legal. This bot supports it via
   `--broker kalshi`.
3. **Test on dry-run first.** `--mode live --dashboard` without
   `--live-orders` will run the full pipeline and log all intended trades to
   `trades.csv` without placing real orders. Read the journal carefully.
4. **The `cryptography` library is required for Kalshi orders** (RSA signing).
   Read-only scanning works without it.
5. **Order fill polling is implemented but not exercised against real
   responses.** When you do go live, expect at least minor surprises in the
   `get_order_status` field names; treat the first day as a debug session
   and watch the log.
6. **State persistence is real now.** Open positions, the daily anchor,
   kill-switch state, and lifetime counters all survive process restarts via
   `state.json`. But: don't manually edit `state.json` while the bot is
   running.
7. **Kill switches are advisory.** The bot tracks a daily anchor and halts new
   entries at -10% daily loss, halts everything at -20%. These are sanity
   limits, not a hedge — a market can blow up faster than the next cycle.

The disclaimer at the top of `polymarket_decay.py` says it well: for
educational / research use, not financial / legal / tax advice.

---

## Troubleshooting

### "HTTP 503" or block page in logs
Your network is intercepting outbound traffic. Clearwater's office network
blocks both `gamma-api.polymarket.com` and `api.elections.kalshi.com`. Run
the bot from a personal network / hotspot, or use `--mock`.

### "Address already in use" when launching
A previous bot is still bound to port 8765. Stop it first:
```bash
lsof -nP -iTCP:8765 -sTCP:LISTEN
pkill -f polymarket_decay.py
```

### Dashboard is empty / "no candidates"
With real markets, common reasons:
- `MIN_ANNUALIZED_EDGE` too high — lower it on the Tools tab.
- `DOMINANT_PRICE` band too narrow.
- `MIN_VOLUME_24H_USD` too high.
Check the **Filter Funnel** in the Analytics tab to see at which gate
markets are being dropped. The **Rejection Trace** has counterfactual hints.

### Stats reset every restart
You're not passing `--state-file` and the bot isn't persisting because the
CWD changed. Check that `state.json` exists in the cwd after a run, or
specify `--state-file /absolute/path/state.json`.

### "cryptography not installed — Kalshi orders disabled"
Run `pip3 install --user cryptography`. Required only for live Kalshi orders;
read-only scan/dashboard work without it.

### Editing config from the UI doesn't seem to do anything
Config changes apply **on the next scan**, not immediately. Hit `Scan Now`
after `Apply Changes`.

---

## API endpoints (for scripting / curl)

The dashboard exposes JSON endpoints on `http://127.0.0.1:8765`:

```text
GET  /                       HTML dashboard
GET  /api/state              full snapshot
GET  /api/candidates         current candidates
GET  /api/strategies         all 5 strategy views
GET  /api/analytics          chart aggregates
GET  /api/hot_events         price/volume movers, new/dropped
GET  /api/positions          open positions
GET  /api/rejections         all rejection rows
GET  /api/funnel             filter funnel
GET  /api/stats              lifetime counters
GET  /api/journal?limit=N    last N journal events
GET  /api/config             current Config values
GET  /api/editable_config    which knobs the UI can edit + ranges

POST /api/scan               trigger a scan cycle immediately
POST /api/pause              pause new entries
POST /api/resume             resume new entries
POST /api/config             {KEY: value, ...} — update Config
POST /api/backtest           {"path": "file.csv"} — run backtest
```

All endpoints return JSON. Localhost-only; no auth.

---

## Layout

```
trade/
├── polymarket_decay.py         single-file framework
├── README.md                   this file
├── docs/
│   └── plans/                  implementation plan docs
├── DEPLOYMENT_NOTES.md         deploy/setup notes
├── state.json                  (created at runtime) persistent state
├── trades.csv                  (created at runtime) trade journal
├── trades.jsonl                (created at runtime) structured journal
└── framework.log               (created at runtime) log
```

---

## License / disclaimer

For educational and research use. Trading prediction markets carries
substantial risk. This is not financial, legal, or tax advice.

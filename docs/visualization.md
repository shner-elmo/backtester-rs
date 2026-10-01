# Visualization & UI

The workspace has two web crates:

| Crate | Port | Purpose |
|-------|------|---------|
| `data-viz` | `:3000` | Parquet explorer: OHLCV candles + indicators |
| `ui` | `:3001` | Backtest-results dashboard |

They bind different ports, so both can run at once.

## data-viz — the Parquet chart explorer

A server that reads the canonical minute Parquet dataset through the
backtesting engine and serves candles plus computed indicators to a
[TradingView Lightweight Charts](https://github.com/tradingview/lightweight-charts)
frontend.

### Run it

```bash
export BACKTEST_DATA_DIR=/path/to/data
cargo run -p data-viz
# open http://localhost:3000   (override the port with PORT=8080)
```

The chart state is part of the page URL, so a chart can be opened directly or
bookmarked with its symbol, date range, timeframe, and indicators:

```text
http://localhost:3000/?symbol=AAPL&start=2023-01-01&end=2023-01-05&tf=min1&ind=ema:20,rsi:14
```

Loading from the toolbar updates the browser URL to match the current controls.
The accepted page parameters are the same as the `/api/bars` parameters below.

The server expects the data root to contain:

```
<data_root>/encoded_tickers.json
<data_root>/year=YYYY/month=M/part-0.parquet
```

(See [data-setup.md](./data-setup.md) for details.)

Every request is logged to stderr with its method, path, status and how long it
took — handy for seeing the cost of a scan:

```
GET /api/bars?symbol=AAPL&tf=daily -> 200 in 22.4s
```

Logging defaults to `info`; set `RUST_LOG` to override (e.g. `RUST_LOG=warn` to
quiet it, or `RUST_LOG=debug` for more).

### Everything is US/Eastern

This is the one thing to internalise before reading the API below.

`time` on every bar is **Eastern wall-clock seconds**: the bar's US/Eastern local
time reinterpreted as a Unix timestamp. It is *not* an instant — a 09:30 ET bar
comes back as the epoch value for `09:30Z`. Lightweight Charts renders numeric
times in UTC, so this shift makes the time axis read market-local without any
`tickMarkFormatter` override, and it keeps day separators on session boundaries.
DST is applied per bar via `chrono-tz`, so the session starts at 09:30 in both
EST and EDT.

To read one back, format it as UTC: `date -u -d @1609752600` → `09:30`.

The same rule drives bucketing. `daily` and `weekly` buckets are stamped at
Eastern midnight. `start`/`end` are inclusive ET calendar dates.

`min1` and `min5` retain pre-market and after-market bars. `daily` and `weekly`
use the engine's NYSE calendar filter and aggregate only regular-session bars,
including the actual 13:00 close on official half days.

### Features

- **Timeframes**: `min1` (default), `min5`, `daily`, `weekly`. Higher
  timeframes use the backtest engine's consolidators.
- **One request per load**: `GET /api/bars` returns bars and indicators in a
  single JSON body, and the frontend does one `setData()` per series. There is no
  streaming.
- **Extended hours**: shown for `min1` and `min5`, in a single candlestick
  series with per-bar colour overrides so pre/after-market candles read as
  dimmed but continuous. Daily and weekly bars are regular-session only.
- **Indicators**: multiple indicators with configurable parameters, added via
  chip UI, each with its own colour. Overlays (EMA, SMA, BBands) go on the price
  chart; oscillators (RSI, MACD) open a pane below.
- **Calendar navigation**: ◀/▶ buttons shift start/end dates by ±1 month.
- **Shareable URLs**: page query parameters initialize the controls and load the
  chart immediately; each toolbar load updates the URL for copying or bookmarking.
- **Partition pruning**: only the Hive-partitioned month-files covering the
  requested date range are opened, and Parquet filter pushdown is enabled so
  OHLCV pages for other tickers are never decoded.

### HTTP API

| Route | Description |
|-------|-------------|
| `GET /` | The chart HTML page |
| `GET /api/bars?symbol=AAPL&start=…&end=…&tf=min1&ind=ema:20,rsi:14` | Bars + indicators |

`tf` values: `min1` (default), `min5`, `daily`, `weekly`. `ind` is a
comma-separated list of indicator specs — repeated query keys are not supported,
because axum's `Query` extractor is `serde_urlencoded`, which has no sequence
support.

```json
{
  "symbol": "AAPL",
  "timeframe": "min1",
  "bars": [
    {"time": 1672736400, "open": 130.28, "high": 131.0, "low": 130.28,
     "close": 131.0, "volume": 8174.0, "is_extended": true}
  ],
  "indicators": {
    "ema:20":      {"ema": [130.2, 130.4]},
    "bbands:20:2": {"upper": [], "middle": [], "lower": []}
  }
}
```

Indicator lines are bare value arrays **index-aligned with `bars`** — zip them
against `bars[i].time`. They are keyed by the requested spec, so `ema:20` and
`ema:50` stay distinct. `is_extended` is always `false` for `daily`/`weekly`.

Indicator specs are `<type>[:<param>…]`: `ema:20`, `sma:20`, `rsi:14`,
`macd:12:26:9` (fast/slow/signal), `bbands:20:2.0` (period/σ). Params are
optional and fall back to those defaults; an unparseable spec is dropped from
the response rather than failing the request.

```bash
curl 'http://localhost:3000/api/bars?symbol=AAPL&start=2023-01-01&end=2023-01-05&tf=min1'
curl 'http://localhost:3000/api/bars?symbol=AAPL&tf=daily&ind=macd:12:26:9'
```

A one-month `min1` query for one symbol takes well under a second against the
full dataset. Multi-year ranges are a linear scan of every month-file (the files
are time-sorted, so row-group stats can't prune on `ticker`) and take minutes —
the toolbar defaults to a one-month window for that reason.

Integration tests run against the committed fixture (no external data required):

```bash
cargo test -p data-viz
```

### Library use

`data-viz` also exposes a library API
([`src/lib.rs`](../data-viz/src/lib.rs)) if you want the data without the HTTP
layer:

- `create_app(data_dir: impl AsRef<Path>) -> Router` — the axum app.
- `load_bars(data_dir: impl AsRef<Path>, symbol, start, end, Timeframe) -> Result<Vec<OhlcBar>, String>`
  — bars straight from Parquet via the backtest engine. An unknown symbol is `Ok(vec![])`;
  a query failure is `Err`.

## ui — the results dashboard

Renders a `backtest_result_<timestamp>.json` (what every `run` writes — see
[results.md](./results.md)) as a dark-theme dashboard: stat cards (final
equity, return, PnL, win rate, profit factor, drawdown, Sharpe, commission),
the daily equity curve with the starting-capital line, a drawdown chart, a
trade-PnL histogram, monthly returns, open positions, a per-symbol summary,
and the full trade log. A header picker switches between all the result files
in the directory without restarting the server.

### Run it

```bash
# 1. Produce a result file:
export BACKTEST_DATA_DIR=test-data
cargo run --example ema_cross

# 2. Serve every backtest_result_*.json in the current dir (newest selected):
cargo run -p ui
# open http://localhost:3001 — use the header picker to switch between them

# ...or pin it to a specific file / change the port:
cargo run -p ui -- path/to/backtest_result.json
PORT=8080 cargo run -p ui
```

### HTTP API

| Route | Description |
|-------|-------------|
| `GET /` | The dashboard ([`ui/src/index.html`](../ui/src/index.html)) |
| `GET /api/results` | The selectable result filenames (newest first) and which is default |
| `GET /api/result?file=` | One result's JSON, plus a `source_file` field (defaults to newest) |

All charts and tables are computed client-side from `/api/result`, so the
server is just a static file plus two JSON endpoints. Charts use
[TradingView Lightweight Charts](https://github.com/tradingview/lightweight-charts)
(same as `data-viz`) for the time series and a small canvas renderer for the
bar charts.

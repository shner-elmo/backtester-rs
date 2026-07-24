# Visualization & UI

The workspace has two web crates:

| Crate | Port | Purpose |
|-------|------|---------|
| `data-viz` | `:3000` | Parquet explorer: OHLCV candles + indicators |
| `ui` | `:3001` | Backtest-results dashboard |

They bind different ports, so both can run at once.

## data-viz — the Parquet chart explorer

A [DataFusion](https://datafusion.apache.org/)-backed server that reads the
minute Parquet dataset and serves candles plus computed indicators to a
[TradingView Lightweight Charts](https://github.com/tradingview/lightweight-charts)
frontend.

### Run it

```bash
# CLI arg takes precedence over DATA_DIR env var (default: ../../data/output):
cargo run -p data-viz -- /path/to/data/output
# or:
DATA_DIR=/path/to/data/output cargo run -p data-viz
# open http://localhost:3000   (override the port with PORT=8080)
```

The server expects the data root to contain:

```
<data_root>/minute/encoded_tickers.json
<data_root>/minute/year=YYYY/month=M/part-0.parquet
```

(See [data-setup.md](./data-setup.md) for details.)

### Features

- **Timeframes**: 1m (minute, default), 5m, 15m, 1h, 4h, 1D. Sub-daily
  timeframes aggregate via `date_bin` SQL; daily uses `DATE_TRUNC`. Aggregation
  is pushed into DataFusion — no BTree/HashMap in application code.
- **Streaming**: bars are delivered via SSE (`/api/stream/bars`). The frontend
  shows "Sent! → Loading… N bars → Done" status as data arrives.
- **Extended hours**: sub-daily bars are split into two candlestick series
  (regular green/red vs. muted pre/after-market). Highlighted using US/Eastern
  timezone conversion via `chrono-tz`.
- **Indicators**: multiple indicators with configurable parameters, added via
  chip UI. Overlays (EMA, SMA, BBands) go on the main chart; oscillators
  (RSI, MACD) open a pane below.
- **Calendar navigation**: ◀/▶ buttons shift start/end dates by ±1 month.
- **Partition pruning**: only the Hive-partitioned month-files covering the
  requested date range are opened.

### HTTP API

| Route | Description |
|-------|-------------|
| `GET /` | The chart HTML page |
| `GET /api/stream/bars?symbol=AAPL&start=…&end=…&timeframe=minute` | SSE stream of bar batches (used by the frontend) |
| `GET /api/bars?symbol=AAPL&start=…&end=…&timeframe=minute` | Same data, JSON array (for scripts/tests) |
| `GET /api/indicators?symbol=AAPL&type=ema&period=20&timeframe=minute` | One indicator series |

`timeframe` values: `minute` (default), `min5`, `min15`, `hour1`, `hour4`, `daily`.

Each bar in `/api/bars` and the SSE stream has the shape:
```json
{"time": 1672736400, "open": 130.28, "high": 131.0, "low": 130.28, "close": 131.0, "volume": 8174.0, "is_extended": true}
```
`time` is a Unix timestamp in seconds. `is_extended` is `false` for daily bars.

Supported indicator `type` values: `ema`, `sma`, `rsi`, `macd`, `bbands`.
Optional params: `period` (default 20 / 14 for RSI), `fast`/`slow`/`signal` for
MACD (12/26/9), `mult` for BBands width in σ (2.0). All accept `start`/`end`/`timeframe`.

```bash
# Non-streaming JSON (e.g. scripts):
curl 'http://localhost:3000/api/bars?symbol=AAPL&start=2023-01-01&end=2023-01-05&timeframe=minute'
curl 'http://localhost:3000/api/indicators?symbol=AAPL&type=macd&timeframe=daily'
```

Integration tests run against the committed fixture (no external data required):

```bash
cargo test -p data-viz
```

### Library use

`data-viz` also exposes a library API
([`src/lib.rs`](../data-viz/src/lib.rs)) if you want the data without the HTTP
layer:

- `create_app(data_root) -> Router` — the axum app.
- `load_bars(data_root, symbol, start, end, Timeframe) -> Vec<OhlcBar>` — bars
  straight from Parquet via DataFusion.

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
cargo run --example ema_cross -- backtester/tests/fixtures

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

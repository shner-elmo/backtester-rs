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
# Point DATA_PATH at the directory that contains encoded_tickers.json and a
# minute/ subdirectory (defaults to ../../data/output):
DATA_PATH=/path/to/data/output cargo run -p data-viz
# open http://localhost:3000
```

The server expects `DATA_PATH` to be a **data root** laid out as:

```
<DATA_PATH>/encoded_tickers.json
<DATA_PATH>/minute/year=YYYY/month=M/part-0.parquet
```

(See [data-setup.md](./data-setup.md) for details.)

### HTTP API

The frontend is driven by three endpoints, which you can also hit directly:

| Route | Description |
|-------|-------------|
| `GET /` | The chart HTML page ([`src/index.html`](../data-viz/src/index.html)) |
| `GET /api/bars?symbol=AAPL&start=2023-01-01&end=2023-12-31` | OHLCV as JSON (`start`/`end` optional) |
| `GET /api/indicators?symbol=AAPL&type=ema&period=20` | One indicator series aligned to the bars |

Supported `type` values: `ema`, `sma`, `rsi`, `macd`, `bbands`. `macd` returns
`macd`/`signal`/`histogram` series; `bbands` returns `upper`/`middle`/`lower`;
the rest return a single series under a key named after the type.

```bash
curl 'http://localhost:3000/api/bars?symbol=AAPL&start=2023-01-01&end=2023-01-05'
curl 'http://localhost:3000/api/indicators?symbol=AAPL&type=macd'
```

The request/response behavior is covered by
[`data-viz/tests/integration.rs`](../data-viz/tests/integration.rs), which runs
against the committed fixture (no external data required):

```bash
cargo test -p data-viz
```

### Library use

`data-viz` also exposes a library API
([`src/lib.rs`](../data-viz/src/lib.rs)) if you want the data without the HTTP
layer:

- `create_app(data_root) -> Router` — the axum app.
- `load_daily_bars(data_root, symbol, start, end) -> Vec<OhlcBar>` — bars
  straight from Parquet via DataFusion.

## ui — the results dashboard

Renders a `backtest_result_<timestamp>.json` (what every `run` writes — see
[results.md](./results.md)) as a dark-theme dashboard: stat cards (final
equity, return, PnL, win rate, profit factor, drawdown, Sharpe, commission),
the daily equity curve with the starting-capital line, a drawdown chart, a
trade-PnL histogram, monthly returns, open positions, a per-symbol summary,
and the full trade log.

### Run it

```bash
# 1. Produce a result file:
cargo run --example ema_cross -- backtester/tests/fixtures

# 2. Serve it (picks the newest backtest_result_*.json in the current dir):
cargo run -p ui
# open http://localhost:3001

# ...or point it at a specific file / different port:
cargo run -p ui -- path/to/backtest_result.json
PORT=8080 cargo run -p ui
```

### HTTP API

| Route | Description |
|-------|-------------|
| `GET /` | The dashboard ([`ui/src/index.html`](../ui/src/index.html)) |
| `GET /api/result` | The loaded result JSON, plus a `source_file` field |

All charts and tables are computed client-side from `/api/result`, so the
server is just a static file plus one JSON endpoint. Charts use
[TradingView Lightweight Charts](https://github.com/tradingview/lightweight-charts)
(same as `data-viz`) for the time series and a small canvas renderer for the
bar charts.

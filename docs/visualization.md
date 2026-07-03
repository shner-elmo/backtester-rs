# Visualization & UI

The workspace has two web crates. Their current status differs — read this
before expecting a dashboard.

| Crate | Status | Purpose |
|-------|--------|---------|
| `data-viz` | **Working** | Parquet explorer: OHLCV candles + indicators |
| `ui` | **Stub** | Planned backtest-results dashboard |

> Both bind `0.0.0.0:3000`, so you can only run one at a time.

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

## ui — results dashboard (not yet implemented)

```bash
cargo run -p ui   # serves http://localhost:3000
```

Right now [`ui/src/main.rs`](../ui/src/main.rs) is a placeholder that responds
with `"backtester UI — not yet implemented"`. The intended dashboard (equity
curve, drawdown, PnL histogram, monthly/daily bars, per-symbol table, trade
log, wired to `backtester::stats`) is described in [`TODO.md`](../TODO.md).
Until it lands, use the [trades JSON + jq](./results.md) for results and
`data-viz` for price charts.

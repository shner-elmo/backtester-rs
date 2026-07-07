# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Commands

```bash
cargo build                     # Build
cargo build --release           # Release build
cargo test                      # Run tests
cargo fmt                       # Format (max width 100, groups: std > external > crate)
cargo clippy                    # Lint
cargo run --example ema_cross -- backtester/tests/fixtures  # Run the EMA cross example on the committed fixture
cargo run -p ui                 # Results dashboard at :3001 (newest backtest_result_*.json in CWD)
cargo bench -p backtester --bench engine  # Throughput benchmarks (loader + a full backtest)
```

CI fails the `bench` job if a benchmark is >2x slower than
`backtester/benches/baseline.bencher.txt`. Refresh that baseline after an
intended perf change: `cargo bench -p backtester --bench engine -- --output-format bencher`
and paste the `test ... bench:` lines into the file (numbers should come from a
CI run, not local hardware). Point the benches at the real dataset with
`BENCH_DATA_ROOT=/path/to/data` (and `BENCH_SYMBOLS=AAPL,MSFT`).

## Architecture

Event-driven backtesting engine. The flow is:

```
Parquet files → Bar stream → Engine loop → Consolidators → Algorithm callbacks
                                  ↓
                              Algorithm (on_data, on_end_of_day)
                                  ↓
                              Orders → Broker/Portfolio → Trade records (JSON)
```

**Modules:**

- `engine.rs` — Orchestrates the backtest: streams data one month-file at a time (skipping months outside the date range), manages warm-up, calls algorithm callbacks, processes orders, nets fills into position-lifetime trades, builds the daily equity curve. Applies stock splits on execution date (position qty × ratio, basis ÷ ratio, history rescaled; bar prices stay raw), credits cash dividends on the ex-date (attributed to position PnL as total return), transfers positions across ticker renames, accrues optional short-borrow/margin-interest financing at each day boundary, fills resting limit/stop orders (with volume-participation partial fills) intrabar, and force-liquidates symbols silent for N trading days (delisting, with an optional fill haircut). `run_backtest()` returns a `BacktestResult`; `run()` additionally prints a summary and writes `backtest_result_<ts>.json`
- `algorithm.rs` — `Algorithm` trait users implement: `initialize()`, `on_data()`, `on_end_of_day()`, plus optional `on_split()` / `on_delisted()` / `on_dividend()`
- `context.rs` — Execution context passed to algorithm: subscribe symbols, place market orders (`Market`, `SetHoldings`, `Liquidate`) and resting `limit_order`/`stop_order`s, access bar history (rolling deque, default 500 bars via `set_max_history`), register consolidators, set slippage/commission/margin/lot-size/financing/volume-participation/risk-free-rate models and toggles
- `broker.rs` — `Portfolio` (cash + positions map); `Position` tracks qty + avg price; `apply_fill()` updates positions
- `slippage.rs` / `commission.rs` — Pluggable fill friction: trait + built-ins + closure blanket impls, both keyed off `FillContext`
- `margin.rs` — Pluggable buying-power limit (same trait + built-ins + closure pattern, keyed off `MarginContext`): `NoMargin` (default, unlimited) or `MaxLeverage` (gross exposure ≤ n × equity; over-cap fills trimmed to the lot or rejected, reducing fills always pass)
- `consolidator.rs` — Aggregates minute bars into `Minutes(n)`, `Hours(n)`, or `Daily` periods (OHLCV aggregated); fires an `FnMut` callback when each period closes
- `data.rs` — Loads bars from Parquet files (arrow/parquet crates); maps encoded ticker IDs to symbols via JSON; Hive-partitioned files sorted by (year, month); `read_bars_from_file` is the per-file streaming entry point
- `bar.rs` — `Bar`: time, OHLCV, `MarketSession` (PreMarket/Main/AfterMarket)
- `slice.rs` — `Slice`: point-in-time snapshot of bars for all subscribed symbols
- `stats.rs` — `Trade`, `EquityPoint`, `BacktestStats`; `compute_stats` derives drawdown/Sharpe from the daily mark-to-market equity curve
- `indicators.rs` — Re-exports from the `ta` crate: `Ema`, `Sma`, `Macd`, `Rsi`, `BollingerBands`

**Writing a strategy:** implement `Algorithm`, call `ctx.add_equity()` and set dates/cash in `initialize()`, then trade in `on_data()`. See `examples/ema_cross.rs` for a complete example.

**Order processing** happens after `on_data()` returns. `SetHoldings(pct)` targets a portfolio-weight allocation (rounded to the lot size); `Liquidate` closes all positions; `Market(qty)` trades a fixed quantity. A "trade" is one position lifetime (flat → flat); intermediate rebalance fills are netted into it.

**Output:** engine writes `backtest_result_<timestamp>.json` (stats, daily equity curve, open positions, trades) and prints summary stats. The `ui` crate renders that file as a dashboard.

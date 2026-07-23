# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Reference docs — read these before exploring the source

`docs/` answers most schema/API questions without spelunking the code:

- `docs/backtesting.md` — the full user-facing API: `Algorithm` trait, every `Context` method (a table), fill timing, order types, slippage/commission/margin models, financing, corporate-action semantics, consolidators, the example strategies
- `docs/data-setup.md` — Parquet dataset layout (Hive `year=/month=` partitioning), column schema, the metadata JSON files (`encoded_tickers.json`, splits/dividends/renames) and their formats, `STONKS_DATA_ROOT`, the committed test fixture, helper examples
- `docs/results.md` — the `backtest_result_*.json` shape (full example), stat definitions, trade-netting semantics, `jq` recipes
- `docs/visualization.md` — the `data-viz` (:3000) and `ui` (:3001) servers and their HTTP APIs

Keep these in sync with code changes — they are the canonical reference; this file only summarizes.

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

- `engine/` — Orchestrates the backtest. `mod.rs` holds the entry points (`run_backtest()` returns a `BacktestResult`; `run()` additionally prints a summary and writes `backtest_result_<ts>.json`) and metadata loading; `run.rs` holds the `Engine` state struct and the event loop (`run_prepared` streams data one month-file at a time skipping months outside the date range, `process_tick` sequences each tick's phases, warm-up, daily equity curve); `orders.rs` executes orders (sizing, volume-participation and margin caps, slippage/commission, deferred next-bar-open and resting limit/stop fills); `corporate_actions.rs` does day-boundary work (splits on execution date — position qty × ratio, basis ÷ ratio, history rescaled, bar prices stay raw; dividends credited on the ex-date and attributed to position PnL as total return; ticker renames; short-borrow/margin-interest financing accrual; force-liquidation of symbols silent for N trading days with an optional fill haircut); `ledger.rs` nets fills into position-lifetime trades. Validates the configured date range up front (an inverted range is an error; a range the data doesn't cover warns and runs over the overlap). Data files must be sorted by timestamp: the engine streams each file tick-by-tick (`data::TickReader`) without buffering or re-sorting, and a timestamp regression — within a file or across month files — fails the run with `OutOfOrderData`
- `algorithm.rs` — `Algorithm` trait users implement: `initialize()`, `on_data()`, `on_end_of_day()`, plus optional `on_split()` / `on_delisted()` / `on_dividend()` / `on_rename()`
- `context.rs` — Execution context passed to algorithm: subscribe symbols, place market orders (`Market`, `SetHoldings`, `Liquidate`) and resting `limit_order`/`stop_order`s, access bar history (rolling deque, default 500 bars via `set_max_history`), register consolidators, set slippage/commission/margin/lot-size/financing/volume-participation/risk-free-rate/output-dir/log-config models and toggles
- `broker.rs` — `Portfolio` (cash + positions map); `Position` tracks qty + avg price; `apply_fill()` updates positions
- `slippage.rs` / `commission.rs` — Pluggable fill friction: trait + built-ins + closure blanket impls, both keyed off `FillContext`
- `margin.rs` — Pluggable buying-power limit (same trait + built-ins + closure pattern, keyed off `MarginContext`): `NoMargin` (default, unlimited) or `MaxLeverage` (gross exposure ≤ n × equity; over-cap fills trimmed to the lot or rejected, reducing fills always pass)
- `consolidator.rs` — Aggregates minute bars into `Minutes(n)`, `Hours(n)`, or `Daily` periods (OHLCV aggregated); fires an `FnMut` callback when each period closes
- `data.rs` — Loads bars from Parquet files (arrow/parquet crates); maps encoded ticker IDs to symbols via JSON; Hive-partitioned files sorted by (year, month), rows time-sorted within each file; `TickReader` streams a file grouped into per-timestamp ticks (the engine's entry point), `read_bars_from_file` yields raw bars per file. Metadata filenames (ticker map, splits, dividends, renames) default to consts but are overridable per-run via `Context::set_*_file` setters (relative to the data root, or absolute)
- `bar.rs` — `Bar`: time, OHLCV. `bar.session()` derives the `MarketSession` (PreMarket/Main/AfterMarket) from the timestamp's US Eastern time-of-day — there is no session column in the data
- `slice.rs` — `Slice`: point-in-time snapshot of bars for all subscribed symbols
- `stats.rs` — `Trade`, `EquityPoint`, `BacktestStats`; `compute_stats` derives drawdown/Sharpe from the daily mark-to-market equity curve
- `indicators.rs` — Re-exports from the `ta` crate: `Ema`, `Sma`, `Macd`, `Rsi`, `BollingerBands`
- `logging.rs` — `LogConfig`: per-category flags for what the engine logs to stderr as it runs (run summary, every fill/trade, daily recap, corporate events, data-quality warnings). Defaults to warnings only; set via `Context::set_log_config` (`LogConfig::all()` / `::none()` shortcuts)

**Writing a strategy:** implement `Algorithm`, call `ctx.add_equity()` and set dates/cash in `initialize()`, then trade in `on_data()`. See `examples/ema_cross.rs` for a complete example.

**Order processing** happens after `on_data()` returns. `SetHoldings(pct)` targets a portfolio-weight allocation (rounded to the lot size); `Liquidate` closes all positions; `Market(qty)` trades a fixed quantity. A "trade" is one position lifetime (flat → flat); intermediate rebalance fills are netted into it.

**Output:** engine writes `backtest_result_<timestamp>.json` (stats, daily equity curve, open positions, trades) and prints summary stats. The `ui` crate renders that file as a dashboard.

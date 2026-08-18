# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Reference docs — read these before exploring the source

`docs/` answers most schema/API questions without spelunking the code:

- `docs/backtesting.md` — the full user-facing API: `Algorithm` trait, every `Context` method (a table), fill timing, order types, slippage/commission/margin models, financing, corporate-action semantics, consolidators, the example strategies
- `docs/data-setup.md` — Parquet dataset layout (Hive `year=/month=` partitioning), column schema, the metadata JSON files (`encoded_tickers.json`, splits/dividends/renames) and their formats, `STONKS_DATA_ROOT`, regenerating the dataset from raw CSVs (`scripts/ingest_arrow.rs`), the committed test fixture, helper examples
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
cargo bench -p backtester --bench consolidators  # Consolidator dispatch across a wide (100/500-symbol) universe
```

`[profile.release]` sets `lto = "thin"` and `codegen-units = 1` — worth ~9% on a
full-universe scan, but it makes a release build take minutes. Iterate with the
dev profile (`cargo build`, `cargo test`) and only reach for `--release` when
measuring.

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

- `engine/` — Orchestrates the backtest. `mod.rs` holds the entry points (`run_backtest()` returns a `BacktestResult`; `run()` additionally prints a summary and writes `backtest_result_<ts>.json`; `*_with_ticker_map` variants take a non-default ticker-map path) — the ticker map is loaded here, before `initialize`, and metadata loading resolves tickers against it; `run.rs` holds the `Engine` state struct and the event loop (`run_prepared` drops month-files outside the date range, then streams the whole run through a `TickStream`, `process_tick` sequences each tick's phases, warm-up, daily equity curve); `orders.rs` executes orders (sizing, volume-participation and margin caps, slippage/commission, deferred next-bar-open and resting limit/stop fills); `corporate_actions.rs` does day-boundary work (splits on execution date — position qty × ratio, basis ÷ ratio, bar prices stay raw; dividends credited on the ex-date and attributed to position PnL as total return; ticker renames; short-borrow/margin-interest financing accrual; force-liquidation of symbols silent for N trading days with an optional fill haircut); `ledger.rs` nets fills into position-lifetime trades. Validates the configured date range up front (an inverted range is an error; a range the data doesn't cover warns and runs over the overlap). Data files must be sorted by timestamp: the engine streams ticks through `tick_stream::TickStream` without buffering or re-sorting, and a timestamp regression — within a file or across month files — fails the run with `OutOfOrderData`
- `symbol.rs` — `Symbol` (a `Copy` newtype over the dataset's encoded ticker id) plus `SymbolMap` (hash map on symbols, multiply-hashed), `SymbolSet`, and `SymbolVec` (dense per-symbol array indexed by ticker id, used for the per-bar state: marks, last-seen day). Knows nothing about tickers — naming lives in `data::TickerMap`. Ticker strings are read only at the edges (subscribing, corporate-action matching, results/logs), so nothing per-bar allocates or hashes text
- `algorithm.rs` — `Algorithm` trait users implement: `initialize()`, `on_data()`, `on_end_of_day()`, plus optional `on_split()` / `on_delisted()` / `on_dividend()` / `on_rename()`
- `context.rs` — Execution context passed to algorithm: subscribe symbols (`add_equity(ticker) -> Symbol`, panics on a ticker the dataset lacks; `try_add_equity` / `add_all_equities`; `symbol()` / `symbol_name()` convert between id and text), place market orders (`Market`, `SetHoldings`, `Liquidate`) and resting `limit_order`/`stop_order`s, register consolidators, set slippage/commission/margin/lot-size/financing/volume-participation/risk-free-rate/output-dir/log-config models and toggles
- `broker.rs` — `Portfolio` (cash + positions, a `SymbolMap`); `Position` tracks qty + avg price; `apply_fill()` updates positions; `PriceTable` (dense last-known marks) values them
- `slippage.rs` / `commission.rs` — Pluggable fill friction: trait + built-ins + closure blanket impls, both keyed off `FillContext`
- `margin.rs` — Pluggable buying-power limit (same trait + built-ins + closure pattern, keyed off `MarginContext`): `NoMargin` (default, unlimited) or `MaxLeverage` (gross exposure ≤ n × equity; over-cap fills trimmed to the lot or rejected, reducing fills always pass)
- `consolidator.rs` — Aggregates minute bars into `Minutes(n)`, `Hours(n)`, `Daily`, or `Weekly` periods (OHLCV aggregated); fires an `FnMut` callback when each period closes. `Minutes`/`Hours` floor onto an epoch grid; `Daily`/`Weekly` floor to US Eastern midnight (of the bar's day, or of its week's Monday). Registered consolidators are indexed by symbol in `Context::consolidators_by_symbol`, so the tick loop dispatches a bar without scanning the registry
- `data.rs` — Loads bars from Parquet files (arrow/parquet crates); `TickerMap` holds the dataset's id ↔ ticker naming (loaded before `initialize` so `add_equity` can return the dataset's id) and `SubscriptionMask` is the per-row filter the reader uses — a selective one (under half the dataset's ids) is pushed into the Parquet reader as a `RowFilter` on `ticker`, so OHLCV pages for unsubscribed rows are never decoded; Hive-partitioned files sorted by (year, month), rows time-sorted within each file; `TickReader` streams one file grouped into per-timestamp ticks (used by the helper examples; the engine goes through `tick_stream::TickStream` instead), `read_bars_from_file` yields raw bars per file. Metadata filenames default to consts; splits/dividends/renames are overridable from `initialize` via `Context::set_*_file`, the ticker map via the `*_with_ticker_map` entry points (relative to the data root, or absolute)
- `tick_stream.rs` — `TickStream`: the engine's reader. Decodes Parquet row groups (one work unit each, ~30 per month file) on a thread pool and hands the tick loop the same strictly ordered stream a sequential read would produce. Units are dealt round-robin and the consumer reads its workers' channels in the same rotation, so file order is restored without a reorder buffer; ticks straddling a chunk or row-group boundary are merged by the consumer; an out-of-order row is reported at the first offending row *in file order* whichever thread found it; a worker that dies without signalling completion is `BacktestError::ReaderThreadDied`, never a silent end-of-data. Thread count via `Context::set_read_threads`
- `bar.rs` — `Bar`: time, OHLCV. `bar.session()` derives the `MarketSession` (PreMarket/Main/AfterMarket) from the timestamp's US Eastern time-of-day — there is no session column in the data
- `slice.rs` — `Slice`: point-in-time snapshot of bars for all subscribed symbols, keyed by `Symbol`
- `stats.rs` — `Trade`, `EquityPoint`, `BacktestStats`; `compute_stats` derives drawdown/Sharpe from the daily mark-to-market equity curve
- `indicators.rs` — Re-exports from the `ta` crate: `Ema`, `Sma`, `Macd`, `Rsi`, `BollingerBands`
- `logging.rs` — `LogConfig`: per-category flags for what the engine logs to stderr as it runs (run summary, every fill/trade, daily recap, corporate events, data-quality warnings). Defaults to warnings only; set via `Context::set_log_config` (`LogConfig::all()` / `::none()` shortcuts)

**Writing a strategy:** implement `Algorithm`, call `ctx.add_equity()` (keep the `Symbol` it returns) and set dates/cash in `initialize()`, then trade in `on_data()`. See `examples/ema_cross.rs` for a complete example.

**Order processing** happens after `on_data()` returns. `SetHoldings(pct)` targets a portfolio-weight allocation (rounded to the lot size); `Liquidate` closes all positions; `Market(qty)` trades a fixed quantity. A "trade" is one position lifetime (flat → flat); intermediate rebalance fills are netted into it.

**Output:** engine writes `backtest_result_<timestamp>.json` (stats, daily equity curve, open positions, trades) and prints summary stats. The `ui` crate renders that file as a dashboard.

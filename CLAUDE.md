# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Commands

```bash
cargo build                     # Build
cargo build --release           # Release build
cargo test                      # Run tests
cargo fmt                       # Format (max width 100, groups: std > external > crate)
cargo clippy                    # Lint
cargo run --example ema_cross   # Run the EMA cross example strategy
```

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

- `engine.rs` — Orchestrates the full backtest: loads data, sorts bars chronologically, manages warm-up, calls algorithm callbacks, processes orders, computes final metrics
- `algorithm.rs` — `Algorithm` trait users implement: `initialize()`, `on_data()`, `on_end_of_day()`
- `context.rs` — Execution context passed to algorithm: subscribe symbols, place orders (`Market`, `SetHoldings`, `Liquidate`), access bar history (rolling 500-bar deque), register consolidators
- `broker.rs` — `Portfolio` (cash + positions map); `Position` tracks qty + avg price; `apply_fill()` updates positions
- `consolidator.rs` — Aggregates minute bars into `Minutes(n)`, `Hours(n)`, or `Daily` periods; fires a callback when each period closes
- `data.rs` — Loads bars from Parquet files (arrow/parquet crates); maps encoded ticker IDs to symbols via JSON; files sorted by (year, month)
- `bar.rs` — `Bar`: time, open, close, `MarketSession` (PreMarket/Main/AfterMarket)
- `slice.rs` — `Slice`: point-in-time snapshot of bars for all subscribed symbols
- `indicators.rs` — Re-exports from the `ta` crate: `Ema`, `Sma`, `Macd`, `Rsi`, `BollingerBands`

**Writing a strategy:** implement `Algorithm`, call `ctx.add_equity()` and set dates/cash in `initialize()`, then trade in `on_data()`. See `examples/ema_cross.rs` for a complete example.

**Order processing** happens after `on_data()` returns. `SetHoldings(pct)` targets a portfolio-weight allocation; `Liquidate` closes all positions; `Market(qty)` trades a fixed quantity.

**Output:** engine writes `backtest_trades.json` (serde_json) and prints summary stats (trade count, win rate, total PnL, final equity).

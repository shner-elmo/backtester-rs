# backtester-rs

A fast, event-driven backtesting engine written in Rust. Strategies are defined
by implementing a single trait; the engine handles data loading, order
execution, and performance reporting. Think QuantConnect-style ergonomics, but
local and fast.

The repo is a Cargo workspace with three crates:

- **`backtester`** — the core engine and strategy API.
- **`data-viz`** — a DataFusion-backed Parquet explorer (OHLCV + indicator charts).
- **`ui`** — the backtest-results dashboard (equity curve, drawdown, trade log).

## Quick start

Run the built-in EMA-crossover strategy against the committed test fixture
(AAPL, Jan 2023) — no external data needed — then open the results dashboard:

```bash
cargo run --example ema_cross -- backtester/tests/fixtures
cargo run -p ui        # http://localhost:3001
```

```
=== Backtest Complete ===
Result written to: backtest_result_2026-07-04T22-13-30.json  (view it with `cargo run -p ui`)
Trades: 92  |  Win Rate: 24%  |  Total PnL: $-1402  |  Final Equity: $98726
Profit Factor: 0.88  |  Max Drawdown: 3.6%  |  Sharpe: -2.01  |  Commission: $0.00
```

To run against a full dataset, pass a directory containing
`encoded_tickers.json` (see [docs/data-setup.md](docs/data-setup.md)):

```bash
cargo run --release --example ema_cross -- /path/to/data/output
```

## Documentation

| Guide | Contents |
|-------|----------|
| [docs/backtesting.md](docs/backtesting.md) | Write and run a strategy: the `Algorithm` trait, `Context` API, indicators, consolidation |
| [docs/results.md](docs/results.md) | The console summary, the result JSON, trade netting, exploring with `jq` |
| [docs/visualization.md](docs/visualization.md) | The `ui` results dashboard and the `data-viz` chart explorer |
| [docs/data-setup.md](docs/data-setup.md) | Dataset layout & schema, `STONKS_DATA_ROOT`, the test fixture, helper examples |

## Writing a strategy (at a glance)

```rust
use backtester::{run, Algorithm, Context, Slice};

struct MyStrategy;

impl Algorithm for MyStrategy {
    fn initialize(&mut self, ctx: &mut Context) {
        ctx.set_start_date(2023, 1, 1);
        ctx.set_end_date(2023, 12, 31);
        ctx.set_cash(100_000.0);
        ctx.set_warm_up(20);        // bars to skip before on_data fires
        ctx.add_equity("AAPL");
    }

    fn on_data(&mut self, ctx: &mut Context, data: &Slice) {
        let Some(bar) = data.bars.get("AAPL") else { return };
        // bar.open/high/low/close/volume, bar.time, bar.market_session
        ctx.set_holdings("AAPL", 1.0);   // go 100% long
    }
}

fn main() {
    run(MyStrategy, "backtester/tests/fixtures");
}
```

Full API reference in [docs/backtesting.md](docs/backtesting.md).

## Common commands

```bash
cargo build                          # build the workspace
cargo test                           # run all tests (uses the committed fixture)
cargo fmt                            # format (max width 100)
cargo clippy                         # lint
cargo run --example ema_cross -- backtester/tests/fixtures   # run a backtest
cargo run -p ui                                              # results dashboard at :3001
DATA_PATH=/path/to/data/output cargo run -p data-viz         # chart explorer at :3000
```

## Data format

Hive-partitioned minute Parquet plus a ticker-encoding JSON:

```
<data root>/
  encoded_tickers.json                    # {"47": "AAPL", ...}
  minute/year=YYYY/month=M/part-0.parquet
```

Parquet columns: `ticker` (encoded id), `volume`, `open`, `close`, `high`,
`low`, `window_start` (timestamp), `transactions`, `market_session`, `day`.
Full details and the `STONKS_DATA_ROOT` env var are in
[docs/data-setup.md](docs/data-setup.md).

## Output

- **`backtest_result_<timestamp>.json`** — the full result: stats, daily
  equity curve, open positions, and completed trades (rebalance fills netted
  per position lifetime). Written to the working directory; render it with
  `cargo run -p ui`.
- **Stdout** — trade count, win rate, total PnL, final equity, profit factor,
  max drawdown, Sharpe, commission. See [docs/results.md](docs/results.md).
- **In code** — `run` returns the `BacktestResult`; `run_backtest` returns it
  without printing or writing anything.

## Status & roadmap

Slippage and commission are customizable (`ctx.set_slippage(..)` /
`ctx.set_commission(..)`); stock splits adjust positions and history on their
execution date, and silent (delisted/renamed) symbols are force-liquidated —
see [docs/backtesting.md](docs/backtesting.md#corporate-actions). Data streams
one month-file at a time. See [TODO.md](TODO.md) for remaining rough edges
(dividends are not applied yet).

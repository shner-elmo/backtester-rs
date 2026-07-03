# backtester-rs

A fast, event-driven backtesting engine written in Rust. Strategies are defined
by implementing a single trait; the engine handles data loading, order
execution, and performance reporting. Think QuantConnect-style ergonomics, but
local and fast.

The repo is a Cargo workspace with three crates:

- **`backtester`** — the core engine and strategy API.
- **`data-viz`** — a DataFusion-backed Parquet explorer (OHLCV + indicator charts).
- **`ui`** — a planned results dashboard (currently a stub).

## Quick start

Run the built-in EMA-crossover strategy against the committed test fixture
(AAPL, Jan 2023) — no external data needed:

```bash
cargo run --example ema_cross -- backtester/tests/fixtures
```

```
=== Backtest Complete ===
Trades written to: backtest_trades_2026-07-03T18-07-59.json
Trades: 92  |  Win Rate: 24%  |  Total PnL: $-1404  |  Final Equity: $98725
Profit Factor: 0.88  |  Max Drawdown: 4.8%  |  Sharpe: -0.63
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
| [docs/results.md](docs/results.md) | The console summary, the trades JSON, and exploring results with `jq` |
| [docs/visualization.md](docs/visualization.md) | The `data-viz` chart explorer (run command, HTTP API) and `ui` status |
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
        // bar.open, bar.high, bar.low, bar.close, bar.time, bar.market_session
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

- **`backtest_trades_<timestamp>.json`** — array of closed-trade records
  (symbol, entry/exit price & time, quantity, PnL), written to the working
  directory.
- **Stdout** — trade count, win rate, total PnL, final equity, profit factor,
  max drawdown, Sharpe. See [docs/results.md](docs/results.md).

## Status & roadmap

Slippage is customizable (`ctx.set_slippage(..)`, see
[docs/backtesting.md](docs/backtesting.md#slippage)). See [TODO.md](TODO.md) for
known bugs, remaining features (commission model, `ui` dashboard, streaming
data load), and rough edges.

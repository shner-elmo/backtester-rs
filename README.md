# backtester-rs

A fast, event-driven backtesting engine for US equity minute data. Write a
strategy as a Rust trait; the engine streams Parquet data, executes orders, and
produces performance results.

The workspace contains:

- `backtester` — the engine and strategy API
- `ui` — a results dashboard on port 3001
- `data-viz` — an OHLCV and indicator explorer on port 3000

## Quick start

Run the EMA crossover example against the included AAPL fixture, then open the
generated result:

```bash
cargo run --example ema_cross -- backtester/tests/fixtures
cargo run -p ui
```

Visit <http://localhost:3001>. No external data is required.

To run against a full dataset:

```bash
cargo run --release --example ema_cross -- /path/to/dataset/minute
```

## Write a strategy

Implement `Algorithm`, configure the run in `initialize`, and place orders in
`on_data`:

```rust
use backtester::{run, Algorithm, Context, Slice, Symbol};

struct BuyAndHold {
    symbol: Option<Symbol>,
    invested: bool,
}

impl Algorithm for BuyAndHold {
    fn initialize(&mut self, ctx: &mut Context) {
        ctx.set_start_date(2023, 1, 1);
        ctx.set_end_date(2023, 1, 31);
        ctx.set_cash(100_000.0);
        self.symbol = Some(ctx.add_equity("AAPL"));
    }

    fn on_data(&mut self, ctx: &mut Context, data: &Slice) {
        let Some(symbol) = self.symbol else { return };
        if !self.invested && data.bars.contains_key(&symbol) {
            ctx.set_holdings(symbol, 1.0);
            self.invested = true;
        }
    }
}

fn main() {
    let strategy = BuyAndHold { symbol: None, invested: false };
    run(strategy, "backtester/tests/fixtures").unwrap();
}
```

See [Running a Backtest](docs/backtesting.md) for the complete API, including
indicators, consolidators, order types, fill models, and corporate actions.

## Data

The engine reads time-sorted, Hive-partitioned Parquet files and an encoded
ticker map:

```text
<dataset>/
  minute/
    encoded_tickers.json
    year=2023/month=1/part-0.parquet
```

Pass `<dataset>/minute` to a backtest. Pass `<dataset>` to the chart explorer:

```bash
cargo run -p data-viz -- /path/to/dataset
```

See [Data Setup](docs/data-setup.md) for the schema, metadata files, ingestion
script, and SEC Form 4 data.

## Results

`run` prints a summary and writes `backtest_result_<timestamp>.json` with the
equity curve, statistics, open positions, and completed trades. `run_backtest`
returns the same data without printing or writing a file.

View result files with `cargo run -p ui`. See
[Results & Statistics](docs/results.md) for the JSON schema and metric
definitions.

## Modeling defaults

- Market orders fill at the current bar's close by default. Use
  `FillTiming::NextBarOpen` to avoid same-bar look-ahead.
- Slippage and commissions default to zero, and buying power is unlimited until
  a margin model is configured.
- Pre-market and after-market bars are included.
- Prices are raw and unadjusted. Optional metadata files provide splits,
  dividends, and ticker renames; stale held symbols are treated as delisted.

Read the [modeling guide](docs/backtesting.md) before relying on a result.

## Performance

The reference no-op scan processes about 1.835 billion bars in 80 seconds
(~23 million bars/s) on an 8-core Ryzen 7 5700U with NVMe storage. Strategy
logic and hardware determine real-world throughput.

## Development

```bash
cargo build
cargo test
cargo fmt
cargo clippy
```

More documentation:

- [Visualization](docs/visualization.md)
- [Performance sweep](docs/perf-sweep-task.md)

Licensed under [GPL-3.0](LICENSE).

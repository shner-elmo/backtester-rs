# backtester-rs

A fast, event-driven backtesting engine written in Rust. Strategies are defined by implementing a single trait; the engine handles data loading, order execution, and performance reporting.

## Quick Start

```bash
cargo run --example ema_cross -- data/output/minute
```

This runs the built-in EMA crossover strategy on AAPL for 2023. The engine writes `backtest_trades.json` and prints a summary:

```
Trades: 42 | Win Rate: 61.9% | Total PnL: $3,241.50 | Final Equity: $103,241.50
```

## Writing a Strategy

Implement the `Algorithm` trait:

```rust
use backtester_rs::{run, Algorithm, Context, Slice};

struct MyStrategy;

impl Algorithm for MyStrategy {
    fn initialize(&mut self, ctx: &mut Context) {
        ctx.set_start_date(2023, 1, 1);
        ctx.set_end_date(2023, 12, 31);
        ctx.set_cash(100_000.0);
        ctx.set_warm_up(20);       // bars to skip before on_data is called
        ctx.add_equity("AAPL");
    }

    fn on_data(&mut self, ctx: &mut Context, data: &Slice) {
        let Some(bar) = data.bars.get("AAPL") else { return };
        // bar.open, bar.close, bar.time, bar.market_session
        ctx.set_holdings("AAPL", 1.0); // go 100% long
    }
}

fn main() {
    run(MyStrategy, "data/output/minute");
}
```

### Context API

| Method | Description |
|--------|-------------|
| `ctx.add_equity(symbol)` | Subscribe to a symbol |
| `ctx.market_order(symbol, qty)` | Buy/sell a fixed quantity (negative to sell) |
| `ctx.set_holdings(symbol, pct)` | Target a portfolio-weight (1.0 = 100%) |
| `ctx.liquidate(symbol)` | Close the entire position |
| `ctx.history(symbol, n)` | Last `n` bars (up to 500) |
| `ctx.consolidate(symbol, period, cb)` | Aggregate bars into a larger timeframe |
| `ctx.portfolio` | Access cash, positions, and equity |

### Built-in Indicators

Thin wrappers over the [`ta`](https://crates.io/crates/ta) crate:

```rust
use backtester_rs::indicators::{Ema, Sma, Macd, Rsi, BollingerBands, Next};

let mut ema = Ema::new(14).unwrap();
let value = ema.next(bar.close);
```

### Bar Consolidation

Aggregate minute bars into larger timeframes:

```rust
use backtester_rs::ConsolidatorPeriod;

ctx.consolidate("AAPL", ConsolidatorPeriod::Hours(1), |bar| {
    println!("Hourly bar close: {}", bar.close);
});
// Also: ConsolidatorPeriod::Minutes(5), ConsolidatorPeriod::Daily
```

## Data Format

The engine reads Parquet files from a directory structured as:

```
data/output/minute/
  YYYY-MM/
    *.parquet
```

Each file contains columns: `ticker` (encoded ID), `open`, `close`, `window_start` (timestamp), `market_session`. A companion JSON file maps ticker IDs to symbols.

## Output

- **`backtest_trades.json`** — Array of trade records with symbol, side, quantity, price, PnL
- **Stdout** — Trade count, win rate, total PnL, final equity

## Dependencies

- `arrow` / `parquet` — Parquet data loading
- `ta` — Technical indicators
- `chrono` / `chrono-tz` — Timestamp handling
- `serde` / `serde_json` — Trade record serialization
- `walkdir` — Parquet file discovery

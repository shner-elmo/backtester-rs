# Running a Backtest

The core crate is **`backtester`**. A strategy is any type that implements the
[`Algorithm`](../backtester/src/algorithm.rs) trait; the engine handles data
loading, chronological ordering, warm-up, order execution, and reporting.

## The `Algorithm` trait

```rust
pub trait Algorithm {
    fn initialize(&mut self, ctx: &mut Context);          // configure the run
    fn on_data(&mut self, ctx: &mut Context, data: &Slice); // called once per timestamp
    fn on_end_of_day(&mut self, _ctx: &mut Context) {}     // optional, default no-op
}
```

- `initialize` runs once before any data. Set the date range, starting cash,
  warm-up, and subscribe to symbols here.
- `on_data` runs once per distinct bar timestamp, after warm-up, for every
  subscribed symbol that has a bar at that instant. Place orders here.
- `on_end_of_day` is optional. (Known rough edge: it currently fires on the
  first bar of the *next* day — see [Known limitations](#known-limitations).)

## A complete strategy

See [`backtester/examples/ema_cross.rs`](../backtester/examples/ema_cross.rs)
for the full version. The essentials:

```rust
use backtester::{indicators::{Ema, Next}, run, Algorithm, Context, Slice};

struct EmaCross { symbol: String, fast: Ema, slow: Ema }

impl Algorithm for EmaCross {
    fn initialize(&mut self, ctx: &mut Context) {
        ctx.set_start_date(2023, 1, 1);
        ctx.set_end_date(2023, 12, 31);
        ctx.set_cash(100_000.0);
        ctx.set_warm_up(30);              // skip the first 30 bars
        ctx.add_equity(&self.symbol.clone());
    }

    fn on_data(&mut self, ctx: &mut Context, data: &Slice) {
        let Some(bar) = data.bars.get(&self.symbol) else { return };
        let fast = self.fast.next(bar.close);
        let slow = self.slow.next(bar.close);
        if fast > slow {
            ctx.set_holdings(&self.symbol.clone(), 1.0);  // go 100% long
        } else {
            ctx.liquidate(&self.symbol.clone());
        }
    }
}

fn main() {
    let algo = EmaCross {
        symbol: "AAPL".into(),
        fast: Ema::new(10).unwrap(),
        slow: Ema::new(30).unwrap(),
    };
    run(algo, "backtester/tests/fixtures");   // data_path, see below
}
```

## `Context` API

Configure the run and interact with the portfolio through `ctx`:

| Method | Purpose |
|--------|---------|
| `set_start_date(y, m, d)` / `set_end_date(y, m, d)` | Inclusive backtest window |
| `set_cash(amount)` | Starting cash |
| `set_warm_up(bars)` | Bars to consume before `on_data` starts firing |
| `add_equity(symbol)` | Subscribe to a symbol |
| `market_order(symbol, qty)` | Trade a fixed quantity (negative = sell) |
| `set_holdings(symbol, pct)` | Target a portfolio weight (`1.0` = 100% long) |
| `liquidate(symbol)` | Close the entire position |
| `history(symbol, n)` | Last `n` bars for a symbol (rolling 500-bar window) |
| `consolidate(symbol, period, cb)` | Aggregate bars into a larger timeframe |
| `on_time(...)` | Schedule a callback at a time of day |
| `ctx.portfolio` | Cash, positions, and equity |

Orders are queued during `on_data` and filled at the bar's **close** price
after the callback returns, adjusted by the [slippage model](#slippage) (none
by default). There is currently **no commission** model.

## Slippage

Set a slippage model in `initialize` to make fills execute away from the
reference (close) price — buys higher, sells lower:

```rust
use backtester::slippage::PercentSlippage;

ctx.set_slippage(PercentSlippage::bps(10.0)); // 0.1% against the aggressor
```

Built-in models (`backtester::slippage`):

| Model | Behavior |
|-------|----------|
| `NoSlippage` | Fills at the reference price (the default) |
| `PercentSlippage { rate }` / `PercentSlippage::bps(n)` | Moves price by a fraction / basis points |
| `FixedSlippage { per_share }` | Moves price by a fixed cash amount per share |

It's fully customizable — pass your own type implementing `SlippageModel`, or
just a closure `Fn(&FillContext) -> f64`. The `FillContext` gives you the
signed `quantity`, the reference `price`, a `direction()` helper (+1 buy / −1
sell), and the full `bar`, so you can key slippage off the bar's range or
volume:

```rust
use backtester::slippage::FillContext;

// Wider slippage on volatile bars: a quarter of the bar's high–low range.
ctx.set_slippage(|fill: &FillContext| {
    let range = (fill.bar.high - fill.bar.low).max(0.0);
    fill.price + 0.25 * range * fill.direction()
});
```

Slippage affects both realized PnL and the cash paid/received; order *sizing*
(e.g. `set_holdings`) still uses the reference price. See
[`examples/slippage_demo.rs`](../backtester/examples/slippage_demo.rs) for a
runnable before/after comparison against `ema_cross`.

## Bars, slices, and sessions

- A `Slice` (`data`) exposes `data.bars: HashMap<String, Bar>`; use
  `data.bars.get(symbol)`.
- A [`Bar`](../backtester/src/bar.rs) has `time` (`DateTime<Utc>`), `open`,
  `high`, `low`, `close`, and `market_session`
  (`PreMarket` / `Main` / `AfterMarket`).

## Indicators

Thin wrappers over the [`ta`](https://crates.io/crates/ta) crate, re-exported
from `backtester::indicators`: `Ema`, `Sma`, `Macd`, `Rsi`, `BollingerBands`,
and the `Next` trait.

```rust
use backtester::indicators::{Ema, Next};
let mut ema = Ema::new(14).unwrap();
let v = ema.next(bar.close);
```

## Bar consolidation

Aggregate minute bars into larger timeframes:

```rust
use backtester::consolidator::ConsolidatorPeriod;

ctx.consolidate("AAPL", ConsolidatorPeriod::Hours(1), |bar| {
    println!("hourly close: {}", bar.close);
});
// Also: ConsolidatorPeriod::Minutes(5), ConsolidatorPeriod::Daily
```

Note: `consolidate` takes an `Fn` (not `FnMut`), so mutating strategy state
from the callback needs a `RefCell`.

## Running it

```bash
# Against the committed fixture (AAPL, Jan 2023) — no external data needed:
cargo run --example ema_cross -- backtester/tests/fixtures

# Against your full dataset (a directory containing encoded_tickers.json):
cargo run --release --example ema_cross -- /path/to/data/output
```

The `data_path` argument must be a directory that contains
`encoded_tickers.json` and has the Parquet files somewhere beneath it. See
[data-setup.md](./data-setup.md) for the expected layout, and
[results.md](./results.md) for what the run produces.

## Known limitations

These are tracked in [`TODO.md`](../TODO.md):

- `on_end_of_day` fires one tick late (first bar of the following day).
- Trade recording is only correct for simple open→close round trips, not
  partial fills or direction flips.
- No commission model (slippage is supported — see [Slippage](#slippage)).
- `set_holdings` can produce fractional shares.

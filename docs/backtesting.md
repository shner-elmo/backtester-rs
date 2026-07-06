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
    fn on_split(&mut self, _ctx: &mut Context, _symbol: &str, _ratio: f64) {} // optional
    fn on_delisted(&mut self, _ctx: &mut Context, _symbol: &str) {}           // optional
}
```

- `initialize` runs once before any data. Set the date range, starting cash,
  warm-up, and subscribe to symbols here.
- `on_data` runs once per distinct bar timestamp, after warm-up, for every
  subscribed symbol that has a bar at that instant. Place orders here.
- `on_end_of_day` is optional. It fires when the first bar of a new trading
  day (US Eastern) arrives, *before* that bar touches history or prices — so
  it sees the world exactly as it was at the previous day's last bar. Orders
  placed in it fill at the new day's first bar.

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
| `set_holdings(symbol, pct)` | Target a portfolio weight (`1.0` = 100% long), rounded to the lot size |
| `liquidate(symbol)` | Close the entire position |
| `set_slippage(model)` | Fill-price friction — see [Slippage](#slippage) |
| `set_commission(model)` | Cash charge per fill — see [Commission](#commission) |
| `set_fill_timing(timing)` | When orders fill — see [Fill timing](#fill-timing) |
| `set_lot_size(lot)` | Share rounding for `set_holdings` (default `1.0` = whole shares) |
| `set_delist_after_days(n)` | Force-close positions in symbols silent for `n` trading days (default 5, `0` = off) |
| `history(symbol, n)` | Last `n` bars for a symbol (rolling 500-bar window) |
| `consolidate(symbol, period, cb)` | Aggregate bars into a larger timeframe |
| `on_time(...)` | Schedule a callback at a time of day |
| `ctx.portfolio` | Cash, positions, and equity |

Orders are queued during `on_data` and filled after the callback returns,
adjusted by the [slippage model](#slippage) and charged the
[commission model](#commission) (both default to zero friction). By default the
fill price is the **close of the same bar**; see [Fill timing](#fill-timing) to
fill at the next bar's open instead.

## Fill timing

By default an order fills at the **close of the bar it was placed on**
(`FillTiming::CurrentBarClose`). That is simple but optimistic: the strategy
transacts at a price it has already seen when it decided to trade. To remove
that same-bar look-ahead, fill at the **open of the symbol's next bar**:

```rust
use backtester::FillTiming;

ctx.set_fill_timing(FillTiming::NextBarOpen);
```

Under `NextBarOpen`, an order decided on bar *t* fills at the open of bar *t+1*
for that symbol; an order placed on a symbol's final bar never fills (there is
no next bar), and pending orders are dropped if the symbol is delisted first.
`set_holdings` sizes against prices known when the order was placed, so no
information from the fill bar leaks into the decision. Slippage and commission
apply to the open fill exactly as they do to a close fill.

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

## Commission

Commission works the same way (`backtester::commission`): a cash charge per
fill, deducted from cash and attributed to the trade's PnL.

```rust
use backtester::commission::{PerShareCommission, PercentCommission};

// Broker-style: half a cent per share with a $1 per-order minimum.
ctx.set_commission(PerShareCommission { per_share: 0.005, minimum: 1.0 });
// ...or 2 bps of traded notional:
ctx.set_commission(PercentCommission::bps(2.0));
// ...or any closure over the same FillContext slippage uses:
ctx.set_commission(|_fill: &backtester::FillContext| 1.0); // flat $1/order
```

| Model | Behavior |
|-------|----------|
| `NoCommission` | Free fills (the default) |
| `PerShareCommission { per_share, minimum }` | Cash per share, floored at a per-order minimum |
| `PercentCommission { rate }` / `PercentCommission::bps(n)` | Fraction of traded notional |

Total commission paid is reported in the run summary and
`BacktestResult::total_commission`.

## Corporate actions

### Splits

If a `get_splits.json` (Polygon format) sits next to `encoded_tickers.json`,
the engine applies stock splits for subscribed symbols on their execution
date. The dataset's prices are **raw/unadjusted** (CELH really does go from
$158 to $52 overnight on its 1→3 split), and the engine keeps them that way —
what changes is your *account*, exactly like at a real broker:

- **Bar prices in slices are never touched.** If your strategy checks
  `price > x` in the morning, it sees the true post-split price — the same
  number it would have seen trading live. Back-adjusting would make it test
  against prices nobody could ever have traded.
- **Held positions are adjusted**: quantity × ratio, cost basis ÷ ratio —
  position value is unchanged and no trade is emitted. The trade ledger is
  rescaled the same way, so entry/exit averages of an eventual round trip
  come out in consistent post-split terms.
- **`ctx.history()` is rescaled** into post-split terms (prices ÷ ratio,
  volume × ratio), so lookbacks and indicators fed from history don't see a
  phantom ±ratio move. Indicators *you* own can't be rescaled for you — reset
  them in `on_split(ctx, symbol, ratio)`.
- **Fractional remainders are cashed out in lieu**: a reverse split that
  leaves a fraction against the lot size credits the cash at the post-split
  price. If that cashes out the whole position, the closing trade carries
  `exit_reason: "split"`.

The daily equity curve stays continuous across a split (only real market
moves show), which is locked in by tests and was validated on the real
dataset across CELH's 2023-11-15 split.

### Delistings & ticker changes

The dataset can't distinguish a delisting from a ticker rename or a buyout —
in all three the old symbol just stops producing bars. The engine treats them
uniformly: a **held** symbol with no bars for `set_delist_after_days` (default
5) consecutive trading days is force-liquidated at its last known price, with
no commission. The closing trade carries `exit_reason: "delisted"` and
`on_delisted(ctx, symbol)` fires. This is approximately right for cash
buyouts, realizes the correct PnL for renames (only the position lifetime
resets), and closes the book on true delistings — note the last traded price
of a bankruptcy delisting is usually optimistic.

Cash dividends are **not** applied yet (see `TODO.md`).

## Bars, slices, and sessions

- A `Slice` (`data`) exposes `data.bars: HashMap<String, Bar>`; use
  `data.bars.get(symbol)`.
- A [`Bar`](../backtester/src/bar.rs) has `time` (`DateTime<Utc>`), `open`,
  `high`, `low`, `close`, `volume`, and `market_session`
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
    println!("hourly close: {} (volume {})", bar.close, bar.volume);
});
// Also: ConsolidatorPeriod::Minutes(5), ConsolidatorPeriod::Daily
```

Consolidated bars aggregate high/low/volume across the period. The callback is
an `FnMut`, so it can own and mutate captured state (e.g. an indicator).

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

- Fills happen at the bar close only — no intrabar execution, limit/stop
  orders, or partial fills.
- No margin/borrow accounting: shorts and >100% allocations are allowed and
  simply drive cash negative, cost-free.
- Data is streamed one month-file at a time; subscribing to a very large
  symbol set still holds one month of their bars in memory.

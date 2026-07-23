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
    fn on_dividend(&mut self, _ctx: &mut Context, _symbol: &str, _amount: f64) {} // optional
    fn on_rename(&mut self, _ctx: &mut Context, _old: &str, _new: &str) {}    // optional
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
| `limit_order(symbol, qty, price)` | Resting limit order — see [Order types](#order-types) |
| `stop_order(symbol, qty, price)` | Resting stop order — see [Order types](#order-types) |
| `set_slippage(model)` | Fill-price friction — see [Slippage](#slippage) |
| `set_commission(model)` | Cash charge per fill — see [Commission](#commission) |
| `set_margin_model(model)` | Buying-power limit — see [Margin](#margin--buying-power) |
| `set_fill_timing(timing)` | When market orders fill — see [Fill timing](#fill-timing) |
| `set_max_volume_participation(f)` | Cap each fill at fraction `f` of bar volume (partial fills; default `0.0` = off) |
| `set_lot_size(lot)` | Share rounding for `set_holdings` (default `1.0` = whole shares) |
| `set_margin_interest_rate(annual)` | Interest on a negative cash balance — see [Financing](#financing) |
| `set_short_borrow_rate(annual)` | Borrow fee on short market value — see [Financing](#financing) |
| `set_delist_after_days(n)` | Force-close positions in symbols silent for `n` trading days (default 5, `0` = off) |
| `set_delist_haircut(fraction)` | Write-down applied to the forced-liquidation price (default `0.0`) |
| `set_risk_free_rate(annual)` | Annual rate the Sharpe ratio is computed in excess of (default `0.0`) |
| `set_track_intraday_equity(b)` | Record a per-bar equity mark into `intraday_equity` (default off) |
| `set_output_dir(dir)` | Where `run` writes the result JSON (default CWD; beats `$BACKTEST_OUTPUT_DIR`) |
| `set_ticker_map_file(path)` | Ticker-encoding map location (default `encoded_tickers.json` in the data root) |
| `set_splits_file(path)` | Splits JSON location (default `get_splits.json`; explicit path must exist) |
| `set_dividends_file(path)` | Dividends JSON location (default `get_dividends.json`; explicit path must exist) |
| `set_renames_file(path)` | Renames JSON location (default `ticker_renames.json`; explicit path must exist) |
| `set_max_history(n)` | Bars per symbol `history()` retains (default 500) |
| `history(symbol, n)` | Last `n` bars for a symbol (rolling window) |
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

## Order types

Alongside the market orders (`market_order`, `set_holdings`, `liquidate`) that
follow the [fill-timing](#fill-timing) model, two order types rest across bars
and fill **intrabar** off the bar's range, independent of `set_fill_timing`:

```rust
ctx.limit_order("AAPL", 100.0, 180.0);  // buy 100 at 180 or better
ctx.stop_order("AAPL", -100.0, 170.0);  // stop-loss: sell 100 if it trades down to 170
```

- **Limit** — a buy (`qty > 0`) fills only when `bar.low <= price`, a sell when
  `bar.high >= price`, at the limit price (or the better bar open if it gapped
  through). It rests until touched or the backtest ends.
- **Stop** — a buy triggers to a market fill when `bar.high >= price`, a sell
  when `bar.low <= price`, filling at the stop (or the worse bar open on a gap).

### Partial fills

`set_max_volume_participation(fraction)` caps every fill at that fraction of the
filling bar's volume. A resting order's unfilled remainder carries to the next
bar; a market order's remainder is dropped. The default `0.0` means unlimited
(fills ignore bar volume).

## Financing

Two optional financing costs accrue at `annual_rate / 252` per trading day on
positions carried into a new day, deducted from cash and attributed to position
PnL (so the accounting identity holds):

```rust
ctx.set_short_borrow_rate(0.03);      // 3%/yr borrow fee on short market value
ctx.set_margin_interest_rate(0.06);   // 6%/yr interest on a negative cash balance
```

The borrow fee is charged per short position; margin interest is charged on any
negative cash and spread across the long book by market value (falling back to
the short book when there are no longs). Both default to `0.0` — shorts and
leverage are free unless you set a rate.

## Margin / buying power

By default buying power is **unlimited**: any order fills in full and cash may
go arbitrarily negative (financing above only prices that leverage, it doesn't
limit it). Set a margin model to constrain what the account can carry:

```rust
use backtester::margin::MaxLeverage;

ctx.set_margin_model(MaxLeverage::new(1.0)); // cash account
ctx.set_margin_model(MaxLeverage::new(2.0)); // Reg-T-style 2x gross exposure
```

`MaxLeverage(n)` caps gross exposure (Σ |position market value|) at `n` ×
equity. An order that would exceed the cap is **trimmed** to the quantity that
fits (rounded down to the lot size) and rejected outright once nothing fits.
Orders that *reduce* exposure always pass, so a book that became over-levered
through a losing move can still close out — there is no forced margin call;
de-risking is the strategy's job.

Like slippage and commission, the model is pluggable: implement `MarginModel`
or pass a closure over `MarginContext` (the proposed signed quantity and
price, the current position, cash, equity, and the rest of the book's gross
exposure) returning the allowed signed quantity:

```rust
use backtester::margin::MarginContext;

// A long-only account: sells may only reduce an existing long.
ctx.set_margin_model(|m: &MarginContext| {
    if m.quantity < 0.0 {
        m.quantity.max(-m.current_qty.max(0.0))
    } else {
        m.quantity
    }
});
```

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

### Delistings

By default the price feed can't distinguish a delisting from a buyout — the
symbol just stops producing bars. A **held** symbol with no bars for
`set_delist_after_days` (default 5) consecutive trading days is force-liquidated
at its last known price, with no commission. The closing trade carries
`exit_reason: "delisted"` and `on_delisted(ctx, symbol)` fires. This is
approximately right for cash buyouts and closes the book on true delistings.

The last traded price of a bankruptcy delisting is usually optimistic, so
`set_delist_haircut(fraction)` knocks a fraction off the forced-liquidation
price (`0.0` by default, `1.0` writes the position off entirely) — the fill
prints at `last_price * (1 - fraction)`.

### Ticker renames

A rename (FB → META) also looks like a delisting in the raw feed. Provide a
`ticker_renames.json` next to `encoded_tickers.json` — a JSON array of
`{"date": "YYYY-MM-DD", "old": "FB", "new": "META"}` — and the engine transfers
the position, PnL ledger, history, resting orders, and last price from the old
symbol to the new one on the effective date, with **no trade emitted** (a
rename is a relabeling, not a round trip). The successor is subscribed up front
so its bars stream, and `on_rename(ctx, old, new)` fires so you can update the
symbols and indicators your strategy keys on.

### Cash dividends

If a `get_dividends.json` (Polygon format) sits next to `encoded_tickers.json`,
the engine credits cash dividends for subscribed symbols on their
**ex-dividend date**, using the same day-boundary timing as splits (after the
prior day's equity mark, before the day's bars move prices):

- A position held on the ex-date is credited `quantity * cash_amount`; a
  **short** is debited the same (you owe the dividend). Symbols you don't hold
  on the ex-date pay nothing.
- The income is added to cash **and** attributed to the open position's PnL, so
  an eventual round trip reports its **total return** (price change plus
  dividends), and the equity curve stays continuous through the ex-date drop.
- `on_dividend(ctx, symbol, amount)` fires after the credit, for logging or a
  DRIP-style reinvestment.

The dividends file is large across the whole market, so it is streamed and
filtered to the subscribed symbols as it parses, rather than loaded whole.

## Bars, slices, and sessions

- A `Slice` (`data`) exposes `data.bars: HashMap<String, Bar>`; use
  `data.bars.get(symbol)`.
- A [`Bar`](../backtester/src/bar.rs) has `time` (`DateTime<Utc>`), `open`,
  `high`, `low`, `close`, and `volume`; `bar.session()` derives the session
  (`PreMarket` / `Main` / `AfterMarket`) from the US Eastern time-of-day.

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

## Example strategies

The [`backtester/examples/`](../backtester/examples) directory has runnable
strategies, smallest first. Run any of them against the committed fixture
(AAPL, Jan 2023) — no external data needed:

```bash
cargo run --example <name> -- backtester/tests/fixtures
```

| Example | What it shows |
|---------|---------------|
| [`buy_and_hold`](../backtester/examples/buy_and_hold.rs) | The baseline: buy on the first bar, hold to the end |
| [`ema_cross`](../backtester/examples/ema_cross.rs) | Fast/slow EMA crossover — the canonical trend strategy |
| [`rsi_mean_reversion`](../backtester/examples/rsi_mean_reversion.rs) | Buy an oversold RSI, exit on reversion toward neutral |
| [`bollinger_bands`](../backtester/examples/bollinger_bands.rs) | Buy below the lower band, exit at the middle band |
| [`macd_trend`](../backtester/examples/macd_trend.rs) | Hold long while the MACD histogram is positive |
| [`slippage_demo`](../backtester/examples/slippage_demo.rs) | `ema_cross` with slippage and commission applied |
| [`kitchen_sink`](../backtester/examples/kitchen_sink.rs) | Every configurable knob at once |

`kitchen_sink` is the guided tour of the whole API: warm-up, a slippage and a
commission model, next-bar-open fills, a custom lot size and delist threshold,
an hourly consolidator feeding a trend SMA, a scheduled pre-close flatten,
`ctx.history()` lookbacks, and the `on_split` / `on_delisted` / `on_end_of_day`
hooks.

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

## Modeling notes

- Market fills print at a single price (bar close or next open); resting
  limit/stop orders and volume-participation partial fills add intrabar
  execution, but there is no order-book depth or queue modeling.
- Data streams one tick at a time: each month file is consumed through a
  `TickReader` that keeps only the current timestamp's bars resident (no
  month-sized buffering, no re-sorting — an out-of-order timestamp fails the
  run). Memory scales with the subscribed universe's per-tick bar count plus
  the rolling `history()` windows, not with the dataset.

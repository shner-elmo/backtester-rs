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
Profit Factor: 0.88  |  Max Drawdown: 3.6%  |  Sharpe: -1.84  |  Commission: $0.00
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
cargo run --example kitchen_sink -- backtester/tests/fixtures  # every feature at once
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
  per position lifetime). Written to the working directory (or
  `$BACKTEST_OUTPUT_DIR` when set); render it with `cargo run -p ui`.
- **Stdout** — trade count, win rate, total PnL, final equity, profit factor,
  max drawdown, Sharpe, commission. See [docs/results.md](docs/results.md).
- **In code** — `run` returns the `BacktestResult`; `run_backtest` returns it
  without printing or writing anything.

## Simulation semantics — read this before trusting a number

How the engine handles the things that silently corrupt backtests, and the
things it deliberately does **not** model.

### Execution

- Orders are queued during `on_data` / `on_end_of_day` and, by default, fill
  at the **close of the bar they were placed on**, adjusted by your slippage
  model and charged your commission model (both default to zero — configure
  them, or results are optimistic; see
  [docs/backtesting.md](docs/backtesting.md#slippage)). A same-bar close fill
  is optimistic — you trade on a price you have already seen — so for a more
  conservative model call `ctx.set_fill_timing(FillTiming::NextBarOpen)` to
  fill at the **next bar's open** instead (see
  [Fill timing](docs/backtesting.md#fill-timing)).
- **Resting `limit_order`/`stop_order`s** fill intrabar off the bar's range
  (a limit never fills worse than its limit price, even with slippage
  configured), and `set_max_volume_participation` caps fills at a fraction of
  bar volume (partial fills) — shared across all orders hitting one bar and
  rounded down to the lot. Beyond that there is no order-book depth or queue
  modeling, so be skeptical of strategies that trade large size in illiquid
  names.
- `set_holdings` sizes at the pre-slippage reference price and rounds to the
  lot (default: whole shares).
- **Buying power is unlimited by default** — cash can go arbitrarily negative
  (free leverage unless you also set a financing rate). Set a margin model to
  constrain it: `ctx.set_margin_model(MaxLeverage::new(2.0))` caps gross
  exposure at 2× equity, trimming (or rejecting) orders that would exceed it
  while always letting an over-levered book reduce. Pluggable like slippage —
  see [Margin](docs/backtesting.md#margin--buying-power).

### Sessions

Pre-market and after-market bars **are fed to `on_data`**, and orders placed
on them fill at their prices — thin, wide-spread sessions where real fills
are much worse than the print. Check `bar.market_session` and skip
`PreMarket`/`AfterMarket` unless you mean to trade them.

### Prices & corporate actions

- Bar prices are **raw/unadjusted** — what the tape actually printed. A
  price filter like `close > 100` behaves exactly as it would have live.
- **Splits** (from `get_splits.json`) adjust your *account* on execution
  date, like a real broker: quantity × ratio, basis ÷ ratio, `ctx.history()`
  rescaled, fractional remainders cashed out in lieu. Position value is
  invariant — the equity curve shows only market moves across a split
  (validated on CELH's 2023 1→3 split). **Warning:** indicators you own are
  *not* reset for you — do it in `on_split`.
- **Delistings/buyouts**: a held symbol silent for 5 trading days
  (configurable) is force-liquidated at its last known price
  (`exit_reason: "delisted"`). For bankruptcies the last print is optimistic,
  so `set_delist_haircut(fraction)` writes the fill down.
- **Ticker renames** (from `ticker_renames.json`) transfer the position,
  ledger, and history from old → new on the effective date with no trade
  emitted; `on_rename` fires so you can retarget your strategy. Note that the
  successor symbol is subscribed from the backtest start, so its bars appear
  in `on_data` *before* the effective date.
- **Cash dividends** are credited on the ex-date from `get_dividends.json`
  (Polygon format) for symbols you hold — a debit for shorts — and attributed
  to the position's PnL, so round trips report total return. `on_dividend`
  fires for reinvestment.
- **Financing** is opt-in: `set_short_borrow_rate` charges a borrow fee on
  shorts and `set_margin_interest_rate` charges interest on a negative cash
  balance, both accruing daily and attributed to position PnL.

### Accounting & stats

- A **"trade"** is one position lifetime, flat → flat: rebalance fills are
  netted into volume-weighted entry/exit averages, not counted as trades.
- The identity `initial_cash + Σ trade PnL + Σ open (realized + unrealized)
  = final equity` holds exactly (asserted in tests; ~1e-12 over a full-year
  real-data run).
- **Drawdown and Sharpe come from the daily mark-to-market equity curve**
  (ET trading dates), not per-trade PnL — open-position pain counts. Sharpe
  uses sample (n−1) variance, √252 scaling, and a zero risk-free rate unless
  you `set_risk_free_rate(annual)`. The
  curve is **daily** by
  default; `set_track_intraday_equity(true)` also records a per-bar curve
  (`intraday_equity`) so intraday drawdown is visible.
- **Survivorship bias is yours to manage**: the engine only sees symbols you
  subscribe to. Hand-picking today's winners (the AAPLs) biases results up —
  delisted losers never make it into the universe.

### Warm-up

`set_warm_up(n)` counts **time steps**, not days — several symbols sharing a
timestamp consume one step — starting at the effective start date. History and
consolidators fill during warm-up; `on_data`, `on_end_of_day`, and the equity
curve begin after it. `on_end_of_day` also fires once for the final day at the
end of data.

## Status

Data streams one month-file at a time (a full year over the 44 GB dataset runs
in ~74 s at ~42 MB RSS). The [TODO.md](TODO.md) roadmap — corporate actions,
financing, intrabar limit/stop orders, partial fills, ticker renames, per-bar
equity, and the dashboard's result switcher/compare — is fully shipped; the
one perf idea left (a streaming k-way merge) was evaluated and deliberately
deferred, since buffering already scales with the subscribed universe.

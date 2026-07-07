# Results & Statistics

When [`run`](../backtester/src/engine.rs) finishes it writes the full result to
a JSON file, prints a summary to stdout, and returns the
[`BacktestResult`](../backtester/src/engine.rs) struct. If you want the data
without any printing or file output, call `run_backtest` instead:

```rust
use backtester::run_backtest;

let result = run_backtest(algo, "backtester/tests/fixtures");
println!("{} trades, Sharpe {:.2}", result.stats.trade_count, result.stats.sharpe_ratio);
```

## Console summary

```
=== Backtest Complete ===
Result written to: backtest_result_2026-07-04T22-13-30.json  (view it with `cargo run -p ui`)
Trades: 92  |  Win Rate: 24%  |  Total PnL: $-1402  |  Final Equity: $98726
Profit Factor: 0.88  |  Max Drawdown: 3.6%  |  Sharpe: -2.01  |  Commission: $0.00
Open: AAPL 754.00 @ 130.73 (last 130.90, unrealized $128)
```

| Metric | Meaning |
|--------|---------|
| **Trades** | Completed round trips (one per position lifetime, flat → flat) |
| **Win Rate** | Fraction of trades with `pnl > 0` |
| **Total PnL** | Sum of realized trade PnL, net of commissions |
| **Final Equity** | Cash + open positions marked at their last seen price |
| **Profit Factor** | Gross profit ÷ gross loss (`inf` if there are no losing trades) |
| **Max Drawdown** | Worst peak-to-trough drop of the *daily mark-to-market* equity curve |
| **Sharpe** | Annualized (√252) from daily equity-curve returns, in excess of the `set_risk_free_rate` rate (default 0) |
| **Commission** | Total commissions charged across every fill |

The numbers come from [`stats::compute_stats`](../backtester/src/stats.rs).
Because drawdown and Sharpe are computed from the daily mark-to-market curve
(not per-trade PnL), pain from open positions counts too.

## What counts as a "trade"

A trade is one **position lifetime**: it opens on the fill that takes the
position off flat and closes on the fill that returns it to flat (or flips its
direction). Everything in between — rebalance fills from `set_holdings`,
partial adds, partial reductions — is netted into the lifetime's
volume-weighted entry/exit prices and realized PnL rather than reported as
separate trades. A strategy that re-targets `set_holdings(0.9)` on every bar
therefore reports *zero* trades until it actually exits.

## The result file

Written to the **current working directory** as
`backtest_result_<timestamp>.json` (the path is printed at the end of the run).
Set `BACKTEST_OUTPUT_DIR=/some/dir` to write it elsewhere (the directory is
created if missing).
Top-level shape:

```json
{
  "initial_cash": 100000.0,
  "final_equity": 98726.08,
  "total_commission": 0.0,
  "stats": { "trade_count": 92, "win_rate": 0.24, "total_pnl": -1402.1,
             "profit_factor": 0.88, "max_drawdown": 0.036, "sharpe_ratio": -2.01 },
  "equity_curve": [ { "time": "2023-01-02", "equity": 100000.0 }, ... ],
  "intraday_equity": [ ],
  "open_positions": [ { "symbol": "AAPL", "quantity": 754.0, "avg_price": 130.73,
                        "last_price": 130.9, "market_value": 98698.6,
                        "unrealized_pnl": 128.2, "realized_pnl": -1402.1 }, ... ],
  "trades": [ { "symbol": "AAPL", "direction": "long",
                "entry_price": 131.24, "exit_price": 131.15,
                "entry_time": "2023-01-03T14:41:00+00:00",
                "exit_time": "2023-01-03T14:43:00+00:00",
                "quantity": 761.0, "pnl": -68.6,
                "exit_reason": "signal" }, ... ]
}
```

- `equity_curve` — one point per trading day (US Eastern dates), starting at
  the initial cash and ending at the final equity.
- `intraday_equity` — a per-bar mark-to-market curve (RFC 3339 timestamps),
  empty unless `set_track_intraday_equity(true)` was set. Exposes intraday
  drawdown the daily curve can't show; stats stay computed from the daily curve.
- `open_positions` — positions still held when the run ended, marked at the
  last known price. `realized_pnl` is PnL already realized by partial unwinds
  during the *still-open* lifetime — it isn't part of any completed trade, so
  `initial_cash + Σ trades.pnl + Σ open.(unrealized+realized) == final_equity`
  always holds.
- `trades` — completed round trips, `quantity` is the total absolute quantity
  that round-tripped, `pnl` is net of commissions. `exit_reason` is `"signal"`
  for strategy-ordered closes, `"delisted"` for forced liquidations of symbols
  that stopped trading, `"split"` for positions fully cashed out in lieu by a
  reverse split (see
  [backtesting.md](./backtesting.md#corporate-actions)).

## The dashboard

The fastest way to look at a result:

```bash
cargo run -p ui               # serves every backtest_result_*.json in CWD
# open http://localhost:3001
```

The header has a picker to switch between all `backtest_result_*.json` files in
the directory (newest selected first); pass an explicit path
(`cargo run -p ui -- result.json`) to pin the dashboard to a single file.

See [visualization.md](./visualization.md#ui--the-results-dashboard).

## Exploring with jq

```bash
# Sort trades by PnL, worst first
jq '.trades | sort_by(.pnl)' backtest_result_*.json

# Total and average PnL
jq '.trades | [.[].pnl] | {total: add, avg: (add / length), n: length}' backtest_result_*.json

# Per-symbol PnL
jq '.trades | group_by(.symbol) | map({symbol: .[0].symbol, pnl: (map(.pnl) | add)})' backtest_result_*.json

# Daily equity as CSV
jq -r '.equity_curve[] | "\(.time),\(.equity)"' backtest_result_*.json
```

## Caveats

- Market fills happen at the bar **close** (or the next open under
  `NextBarOpen`), plus slippage; resting limit/stop orders fill intrabar off the
  bar's range, and `set_max_volume_participation` models partial fills. There is
  no order-book depth or queue modeling.
- With no slippage/commission configured, results are optimistic relative to
  live trading — see [backtesting.md](./backtesting.md#slippage) and
  [backtesting.md](./backtesting.md#commission).

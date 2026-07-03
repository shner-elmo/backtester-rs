# Results & Statistics

When [`run`](../backtester/src/engine.rs) finishes it does two things: writes a
JSON file of every closed trade, and prints a summary to stdout.

## Console summary

```
=== Backtest Complete ===
Trades written to: backtest_trades_2026-07-03T18-07-59.json
Trades: 92  |  Win Rate: 24%  |  Total PnL: $-1404  |  Final Equity: $98725
Profit Factor: 0.88  |  Max Drawdown: 4.8%  |  Sharpe: -0.63
```

| Metric | Meaning |
|--------|---------|
| **Trades** | Number of closed round-trip trades |
| **Win Rate** | Fraction of trades with `pnl > 0` |
| **Total PnL** | Sum of realized PnL across all trades |
| **Final Equity** | Starting cash + realized PnL + open positions marked at their last seen price |
| **Profit Factor** | Gross profit ÷ gross loss (`inf` if there are no losing trades) |
| **Max Drawdown** | Largest peak-to-trough drop of the realized-PnL equity curve |
| **Sharpe** | Mean/σ of per-trade PnL, annualized by √252 (not a time-weighted return Sharpe) |

The numbers come from
[`stats::compute_stats`](../backtester/src/stats.rs), which returns a
`BacktestStats` struct you can also call directly if you embed the engine.

> Note: `Final Equity` (printed) includes open positions valued at the last
> known price, whereas `BacktestStats::final_equity` reflects realized PnL
> only. They differ when a position is still open at the end of the run.

## The trades file

Written to the **current working directory** as
`backtest_trades_<timestamp>.json` (the path is printed at the end of the run).
It is a JSON array of trade records:

```json
[
  {
    "symbol": "AAPL",
    "entry_price": 131.24,
    "exit_price": 131.15,
    "entry_time": "2023-01-03T09:41:00+00:00",
    "exit_time": "2023-01-03T09:43:00+00:00",
    "quantity": 761.9628162145686,
    "pnl": -68.57665345931377
  }
]
```

Each record is one closed round trip: `entry_*` / `exit_*` timestamps and
prices, signed `quantity`, and realized `pnl`.

## Exploring the results

The JSON is easy to slice with `jq`:

```bash
# Sort trades by PnL, worst first
jq 'sort_by(.pnl)' backtest_trades_*.json

# Total and average PnL
jq '[.[].pnl] | {total: add, avg: (add / length), n: length}' backtest_trades_*.json

# Per-symbol PnL
jq 'group_by(.symbol) | map({symbol: .[0].symbol, pnl: (map(.pnl) | add)})' backtest_trades_*.json

# Only losing trades
jq 'map(select(.pnl < 0))' backtest_trades_*.json
```

For a **visual** exploration of price + indicators, see
[visualization.md](./visualization.md).

## Caveats

- Trade recording is currently reliable only for simple open→close round trips.
  Partial fills and direction flips are not tracked correctly yet (see
  [`TODO.md`](../TODO.md)).
- With no commission/slippage model, results are optimistic relative to live
  trading.

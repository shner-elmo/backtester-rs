# TODO

## Continuation context (read this first)

Snapshot for picking the work back up in a fresh chat. Updated 2026-07-05.

**What the project is:** an event-driven Rust backtesting engine (QuantConnect-
style ergonomics). Cargo workspace: `backtester` (core lib), `data-viz`
(DataFusion Parquet explorer, `:3000`), `ui` (results dashboard, `:3001`).
Start with `README.md` and `docs/` (backtesting, results, visualization,
data-setup).

**Repo / git state:** branch `master`; recent work is committed locally but
not pushed (`git log origin/master..HEAD`); push is fine (solo repo).

**Data & how to run:**
- Full dataset (44 GB) at
  `/path/to/data/output`
  (`minute/year=YYYY/month=M/part-0.parquet` + `encoded_tickers.json`).
- No external data needed for tests/dev: committed fixtures at
  `backtester/tests/fixtures` (AAPL, Jan 2023). Demo flow:
  `cargo run --example ema_cross -- backtester/tests/fixtures` then
  `cargo run -p ui` → http://localhost:3001.
- `cargo test --workspace` green; `cargo clippy` clean.

**The 2026-07-04/05 session shipped the whole roadmap:** commission model,
netted trade accounting (position lifetimes), `run_backtest -> BacktestResult`
(stats + daily equity curve + open positions + trades), the `ui` dashboard,
per-file streaming data load with month skipping, volume in `Bar`, Hive dir
sorting, ET trading dates, EOD-timing fix, FnMut consolidator callbacks, lot
rounding. All prior "Bugs" and "Missing Features" sections are resolved.

## Remaining ideas (none blocking a demo)

- [x] **Splits & delistings** — done 2026-07-05: splits from `get_splits.json`
  adjust position/basis/history on execution date (bar prices stay raw);
  symbols silent ≥5 trading days are force-liquidated
  (`exit_reason: "delisted"`, `on_delisted`); `on_split` callback for
  indicator resets. Synthetic-fixture tests in
  `backtester/tests/corporate_actions.rs`; validated on real data across
  CELH's 2023-11-15 1→3 split (equity moved −0.10% on split day, not −67%).
- [x] **Cash dividends** — done 2026-07-06: `load_dividends` streams
  `get_dividends.json` (Polygon format) filtering to subscribed symbols as it
  parses; on `ex_dividend_date` the engine credits `qty * cash_amount` (debits
  shorts) in the same day-boundary hook splits use, attributes the income to
  the position's PnL (round trips report total return), and fires
  `on_dividend`. Synthetic-fixture tests in `tests/corporate_actions.rs`
  (long credit, short debit, not-held no-op) hold the accounting identity.
- [x] **Delist fill haircut** — done 2026-07-06: `set_delist_haircut(fraction)`
  writes the forced-liquidation fill down to `last_price * (1 - fraction)`
  (default `0.0`). Tested in `tests/corporate_actions.rs`.
- [ ] **Ticker rename chains** — with Polygon's ticker-events data, upgrade
  renames from forced liquidation to a seamless position transfer.
- [ ] **Margin/borrow accounting** — shorts and >100% allocations just drive
  cash negative, cost-free. Add a margin-interest / borrow-fee model next to
  slippage & commission.
- [ ] **Intrabar execution** — no limit/stop orders or partial fills. Fill
  *timing* is now configurable (`set_fill_timing`: `CurrentBarClose` default,
  or `NextBarOpen` to fill at the next bar's open and drop the same-bar
  look-ahead), but a fill is still a single all-or-nothing print at one price.
- [ ] **True k-way merge streaming** — data now streams one month-file at a
  time (memory = one month of *subscribed* bars, so wide-universe runs are
  still heavy). A k-way merge across per-symbol readers would flatten that.
- [x] **Benchmark run on the full 44 GB dataset** — done 2026-07-05:
  `ema_cross` over all of 2023 (AAPL, minute bars) ran in **74 s with a
  41.8 MB peak RSS**; equity-curve dates strictly increasing across all 12
  month files, accounting identity exact to 7e-12 over 3673 trades. A
  full-ticker OHLC invariant sweep of June 2023 (31.9M bars, 11,453
  symbols; `examples/data_invariants_check.rs`) found zero violations.
- [x] **`ui` result switching** — done 2026-07-06: the dashboard serves every
  `backtest_result_*.json` in the directory (`GET /api/results`) and a header
  picker switches between them without a restart (`GET /api/result?file=`,
  validated against the scanned names). An explicit path argument still pins it
  to one file.
- [ ] **`ui` side-by-side compare** — render two result files in adjacent
  columns to diff strategies (the switcher above already lists them).
- [ ] **Per-bar equity marks** — the equity curve is daily; intraday drawdown
  inside a single day is invisible.

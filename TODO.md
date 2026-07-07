# TODO

## Continuation context (read this first)

Snapshot for picking the work back up in a fresh chat. Updated 2026-07-06.

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

## Roadmap (all shipped or evaluated)

Every item below is checked off — either implemented and tested, or explicitly
evaluated and decided against with a recorded rationale. Nothing here is
outstanding; new ideas go under a fresh heading.


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
- [x] **Ticker rename chains** — done 2026-07-06: `load_renames` reads
  `ticker_renames.json` (`{date, old, new}`); on the effective date the engine
  transfers the position, PnL ledger, history, resting orders, and last price
  from old→new (no trade emitted), subscribes the successor up front so its
  bars stream, and fires `on_rename`. Tested in `tests/corporate_actions.rs`.
- [x] **Margin/borrow accounting** — done 2026-07-06:
  `set_margin_interest_rate` charges interest on a negative cash balance
  (spread across the long book) and `set_short_borrow_rate` charges a borrow
  fee on shorts, both at `rate / 252` per trading day, attributed to position
  PnL so the accounting identity holds. Tested in `tests/corporate_actions.rs`.
- [x] **Intrabar execution** — done 2026-07-06: `limit_order` / `stop_order`
  rest across bars and fill intrabar off the bar's range (limit at its price or
  the better open; a triggered stop at its price or the worse open).
  `set_max_volume_participation` caps a fill at a fraction of bar volume, so a
  resting order's remainder carries to the next bar. Tested in `tests/intrabar.rs`.
- [x] **True k-way merge streaming** — evaluated 2026-07-06, keeping per-month
  buffering. It already bounds memory to the *subscribed* universe (filtered
  before buffering, ~42 MB RSS over a full year), and a streaming k-way merge
  would need a guaranteed intra-file time sort the Polygon feed doesn't provide
  (rows are grouped by ticker). See the rationale comment in `engine.rs`.
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
- [x] **`ui` side-by-side compare** — done 2026-07-06: a second header picker
  overlays a comparison result's equity curve on the chart and shows a
  metric-by-metric table (A, B, Δ) above the dashboard.
- [x] **Per-bar equity marks** — done 2026-07-06:
  `set_track_intraday_equity(true)` records a mark-to-market point on every bar
  into `BacktestResult::intraday_equity` (empty by default to stay cheap),
  exposing intraday drawdown the daily `equity_curve` can't show. Tested in
  `tests/engine.rs`.

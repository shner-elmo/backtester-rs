# TODO

## Continuation context (read this first)

Snapshot for picking the work back up in a fresh chat. Written 2026-07-04.

**What the project is:** an event-driven Rust backtesting engine (QuantConnect-
style ergonomics, meant to be fast). Cargo workspace: `backtester` (core lib),
`data-viz` (DataFusion Parquet explorer, works), `ui` (results dashboard, still
a stub). Start with `README.md` and `docs/` (backtesting, results,
visualization, data-setup).

**Repo / git state:**
- Branch `master`. Recent work is committed **locally but not pushed** — run
  `git log origin/master..HEAD` to see the pending commits. Push is fine to do
  (solo repo, `git push`), just hasn't been asked for yet.
- History was rewritten earlier to scrub machine-specific data paths; the tip
  was force-pushed once. All example programs now read `STONKS_DATA_ROOT`
  instead of hardcoding paths.

**Data & how to run:**
- Full dataset (44 GB) lives at
  `/path/to/data/output`
  (`minute/year=YYYY/month=M/part-0.parquet` + `encoded_tickers.json`).
- `export STONKS_DATA_ROOT=/path/to/data/output/minute` for the examples.
- Path arg conventions differ: `backtester::run(algo, path)` wants the dir
  containing `encoded_tickers.json` (`.../data/output`); `data-viz`'s
  `DATA_PATH` wants the same root; the examples' `STONKS_DATA_ROOT` wants the
  `minute/` subdir. See `docs/data-setup.md`.
- **No external data needed for tests/dev:** committed fixtures at
  `backtester/tests/fixtures` and `data-viz/tests/fixtures` (AAPL, Jan 2023,
  5000 bars). Run a backtest with:
  `cargo run --example ema_cross -- backtester/tests/fixtures`.
  Regenerate fixtures with
  `STONKS_DATA_ROOT=... cargo run -p data-viz --example make_test_fixture`.
- `cargo test --workspace` is green (20 tests + 2 doctests). `cargo clippy` clean.

**Roadmap (impressiveness-per-effort order) and where we are:**
1. ✅ **Commission/slippage** — customizable slippage shipped (`slippage.rs`,
   `ctx.set_slippage(..)`, built-ins + closures). Commission still TODO (below).
2. 🔨 **Correct trade accounting** — flips/basis fixed & tested; remaining piece
   is rebalance-noise netting (see "Trade recording" bug below).
3. ⬜ **UI results dashboard** — the `ui` crate is a stub; biggest visual win.
4. ⬜ **Streaming data load** — earns the "fast" claim; see Performance below.

**Recommended immediate next step:** make the engine return its results instead
of only printing them. Add `run_backtest(algo, path) -> BacktestResult { stats,
trades, final_equity }` and have `run` call it then print/write. Non-breaking,
and it's the seam both real engine-level trade-accounting tests and the #3 UI
dashboard need. Then do the rebalance-noise netting, then the UI.

## Bugs

- [x] **Column index assumptions in `iter_bars` were wrong** — confirmed: Parquet schema order is `ticker, volume, open, close, high, low, window_start, transactions, market_session, day`, but `data.rs` read projected columns positionally assuming `open, high, low, close`, so `high`/`low`/`close` were scrambled (`ProjectionMask` returns columns in schema order, not `COLUMNS` order). Fixed to look up columns by name in `iter_bars`. Locked in by `backtester/tests/data.rs` against a committed fixture. `data.rs`
- [~] **Final-equity reporting is inconsistent** — the *printed* Final Equity marks open positions at last market price (`engine.rs` `total_value(&last_known_prices)`), which is correct. But `BacktestStats::final_equity` from `compute_stats` is realized-PnL only and ignores open positions, so the two disagree when a position is still open at end. The `engine.rs:~200` comment claims it "adjusts the stats equity curve" but nothing actually does. Reconcile them (probably when adding `run_backtest`). `engine.rs`, `stats.rs`
- [ ] **`on_end_of_day` fires one tick late** — called at first bar of the new day after that bar's history is already pushed. `engine.rs` (day-boundary block, ~line 92)
- [~] **Trade recording** — direction flips (long↔short) now reset cost basis and open a fresh trade entry (`broker.rs` `apply_fill`, `engine.rs`); partial reductions record correctly; covered by `broker.rs` unit tests. **Remaining:** `set_holdings` re-targets every bar, so a held position emits many tiny rebalance "trades" instead of one position lifetime (this is why a long-only run reports far more "trades" than round trips). Net fills into a single open→close position ledger. `engine.rs`

## Missing Features

- [ ] **Commission model** — orders fill with zero commission. Add alongside slippage as a customizable model (`ctx.set_commission(..)`), applied in the engine fill path next to the slippage call. (Slippage is done — `slippage.rs`.)
- [ ] **`ui` results dashboard** — `ui/src/main.rs` is a placeholder returning `"backtester UI — not yet implemented"`. Build the dashboard: read the trades JSON (or better, consume `run_backtest`'s output / `backtester::stats`), render equity curve, drawdown, PnL histogram, monthly/daily bars, per-symbol table, trade log. Reuse the axum + charts pattern already working in `data-viz`. Note: `ui` and `data-viz` both bind `:3000`. Run with `cargo run -p ui`.
- [ ] **`Bar` missing `volume`** — `high`/`low` are now loaded; `volume` still isn't. Blocks VWAP, volume filters, volume-based slippage, etc. `bar.rs`, `data.rs`
- [ ] **`consolidate` takes `Fn` not `FnMut`** — can't update strategy state (e.g. an indicator) from a consolidator callback without `RefCell`. `context.rs`
- [ ] **Consolidator doesn't aggregate high/low** — when merging sub-bars only `close` and `time` are updated; `high` should be running max and `low` running min across the period. `consolidator.rs`
- [ ] **`on_time` recomputes target minute every tick** — `fire_time_callbacks` converts `hour`/`minute` to `hour * 60 + minute` on each bar; precompute once when the entry is registered. `context.rs` (`fire_time_callbacks`)
- [ ] **Round shares to nearest lot** — `set_holdings` produces fractional shares (e.g. 231.004707691839); round to a configurable tick (default 0.1 or 1.0).

## Performance

- [ ] **All bars loaded into memory before processing** — `engine.rs` builds the full `tick_map: BTreeMap<i64, Vec<..>>` upfront, so a real (44 GB) run won't fit in memory. Stream instead via a k-way merge across the chronologically-sorted files. This is the change that earns the "fast" claim; do it once real full-dataset runs start. Depends on the `sorted_parquet_files` fix below (correct file ordering). `engine.rs`
- [x] **Parquet files opened twice per file** — fixed: `iter_bars` now opens each file once and caches the projection mask. `data.rs`

## Robustness

- [ ] **`sorted_parquet_files` doesn't understand Hive dir names** — it sorts by parsing parent dir names as bare numbers, but the layout is `year=YYYY/month=M`, which parses to `0`, so multi-month ordering is not guaranteed (fine for a single file / the fixture). Parse the `key=value` names. `data.rs`
- [~] **Tests** — added: `broker.rs` (`apply_fill` open/add/reduce/close/flip), `slippage.rs` (models + closures), `context.rs` (`on_time`), `backtester/tests/data.rs` (Parquet→Bar column mapping), `data-viz/tests/integration.rs` (HTTP API). Still worth adding: engine-level trade-accounting/`set_holdings` tests once `run_backtest` returns results.

## Done this session (for reference)

- Fixed the Parquet column-scramble bug + committed fixtures + tests.
- Fixed backtest output path (was panicking on a hardcoded `../../trash/...`; now writes `backtest_trades_<ts>.json` to CWD).
- Scrubbed machine-specific data paths from files + git history; examples read `STONKS_DATA_ROOT`.
- Added customizable slippage model (`slippage.rs`) + `slippage_demo` example.
- Fixed cost-basis/trade-entry on position flips (`broker.rs`, `engine.rs`).
- Wrote `README.md` rewrite + `docs/` (backtesting, results, visualization, data-setup).

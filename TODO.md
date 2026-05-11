# TODO

## Bugs

- [ ] **Final equity wrong** — open positions at backtest end are marked at `avg_price` (cost basis) instead of last market price. `engine.rs:169`
- [ ] **`on_end_of_day` fires one tick late** — called at first bar of the new day after that bar's history is already pushed. `engine.rs:98-102`
- [ ] **Trade recording broken for partial fills / direction flips** — only simple open→close round trips tracked correctly. `engine.rs:141-163`

## Missing Features

- [ ] **`Bar` missing `high`, `low`, `volume`** — columns exist in Parquet but aren't loaded. Blocks ATR, VWAP, volume filters, etc. `bar.rs`, `data.rs`
- [ ] **No commission / slippage model** — orders fill at close price with zero friction, producing unrealistically good results
- [ ] **`consolidate` takes `Fn` not `FnMut`** — can't update strategy state (e.g. an indicator) from a consolidator callback without `RefCell`. `context.rs:82`
- [ ] **Consolidator doesn't aggregate high/low** — when merging sub-bars only `close` and `time` are updated; `high` should be running max and `low` running min across the period. Needs fixing alongside the `Bar` high/low fields. `consolidator.rs:28-32`

## Performance

- [ ] **All bars loaded into memory before processing** — `tick_map` is fully built upfront; should stream instead. `engine.rs:38-45`
- [ ] **Parquet files opened twice per file** — once for schema, once for reading; schema available from builder directly. `data.rs:34-43`

## Robustness

- [ ] **`sorted_parquet_files` parses year/month from raw folder names** — silently wrong sort order if directory layout changes. `data.rs:96-109`
- [ ] **No tests** — `apply_fill` and `SetHoldings` quantity math have subtle edge cases worth unit testing. `broker.rs`, `engine.rs`

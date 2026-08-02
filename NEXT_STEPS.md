# Next: the read path

Handoff notes for the remaining engine-throughput work. Safe to delete once done.
State as of the `perf/read-path` branch (2026-08-01).

## Where things stand

Full-dataset no-op scan (`examples/no_op_baseline`, 1.835B bars, 29 GiB, cold I/O,
release): **496s → 383s**, or **315s** with `disable_history()`. What moved it:
opt-in history, a 131k-row Parquet batch size, `lto="thin"` + `codegen-units=1`,
and a `ticker` row filter for subscriptions under half the id space. See the
Performance section of `docs/backtesting.md` for the measurement tables.

Phase split of the 496s run (temporary timers around each phase of
`Engine::process_tick`, since reverted):

| phase | share |
|---|---|
| read (`TickReader::next_tick`) | 67% |
| slice-map build + `on_data` | 16% |
| `record_history` | 13% |
| marks + last-seen-day | 4% |
| corporate actions, orders, day boundaries | <0.1% |

So the read path is the backtest. Of it, ~92s is cold disk (29 GiB at the 323
MiB/s this machine measures) and the rest is Parquet→Arrow decode plus tick
grouping. `examples/scan_stages.rs` splits those apart on any subset of months.

## The task: decode row groups in parallel, pipelined

The tick loop is sequential in time; decoding is not. Each month file holds **30
row groups of ~1M rows / 16 MiB**, each a contiguous time range — so:

1. A pool of worker threads decodes row groups (workers stream batches inside
   their row group rather than materializing a whole one — 1M rows of `(Symbol,
   Bar)` is ~56 MB, too much to hold per worker).
2. Workers send sequence-tagged chunks of grouped ticks to the engine thread.
3. The engine thread reassembles in sequence order through a bounded reorder
   buffer and feeds `process_tick`.

Expected: decode falls behind the engine's own ~90s of bookkeeping and the disk
read overlaps too, landing a full scan near 150–200s. Past that, I/O is the floor
and the question becomes reading fewer bytes, not decoding faster.

Watch for:

- **Ticks straddle row-group boundaries** (a tick is ~1638 bars, a row group ~1M
  rows), so the merge stage must join the last group of chunk *k* with the first
  of chunk *k+1* when they share a timestamp.
- **`OutOfOrderData` must stay deterministic**: workers check order within their
  own range and send `Result`s; the consumer surfaces the first error *in
  sequence order*, not the first to arrive.
- **Bounded memory**: cap in-flight chunks, or a fast worker running ahead of a
  slow one reintroduces the month-sized buffering `TickReader` was written to
  avoid.
- Write the equivalence test first: pipelined and unpipelined reads of the same
  files must produce identical ticks in identical order.

## After that

The per-tick `Slice` is a `SymbolMap<Bar>` rebuilt every tick — 1.8B hash inserts
over the dataset, 16% of the profile. A tick-stamped dense table (`SymbolVec<(u64,
Bar)>` plus a `Vec<Symbol>` of what is present, clear by bumping the stamp) makes
it an array write, the same shape `last_known_prices` already uses for 4%. It
breaks `Slice.bars` for every strategy and doc, so batch it with any other API
break rather than spending the churn alone.

## Verification

- `cargo test && cargo clippy --all-targets && cargo +nightly fmt`
- `backtester/benches/baseline.bencher.txt` still holds pre-interning CI numbers
  and is due a refresh — per CLAUDE.md, only from a CI run.
- The real proof is `examples/no_op_baseline` against the full dataset; compare to
  the numbers at the top of this file.

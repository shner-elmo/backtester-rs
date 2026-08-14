# Next: the engine path

Handoff notes for the remaining engine-throughput work. Safe to delete once done.
State as of the `perf/read-path` branch (2026-08-01).

## Where things stand

Full-dataset no-op scan (`examples/no_op_baseline`, 1.835B bars, 29 GiB, cold
I/O, release, 16-core machine):

| | wall | peak RSS |
|---|---|---|
| before this branch | 496s | — |
| opt-in history, 131k batches, ticker pushdown, LTO | 383s | — |
| \+ parallel row-group decode | **212s** | 402 MB |
| \+ history off (now the default) | **187s** | 355 MB |

Bar counts are identical across all of them (1,835,105,812), which is the
equivalence check that matters at this scale. The 187s row is what a default
run does today: history became opt-in, so the last line is the baseline rather
than a tuning.

The read path is no longer the bottleneck for a wide universe. On one warm
month, full universe with `enable_history()`, the decode pool saturates at
**one** thread — extra threads slightly hurt, because the engine's own per-bar
bookkeeping is now the critical path. At the default (no history), or on a
narrow universe, decode takes back over (a 100-symbol month goes 0.81s → 0.26s
on 4 threads).

Phase split of the *old* 496s run, for reference (temporary timers around each
phase of `Engine::process_tick`, since reverted): read 67%, slice-map build 16%,
`record_history` 13%, marks + last-seen-day 4%, everything else <0.1%.

## The task: a dense `Slice`

`Slice.bars` is a `SymbolMap<Bar>` rebuilt every tick — 1.8B hash inserts across
the dataset, and 16% of that old profile. It should be a tick-stamped dense
table:

```rust
slots: SymbolVec<(u64 /* tick_id */, Bar)>,
present: Vec<Symbol>,   // what to iterate
```

Build becomes an array write plus a `Vec` push, `get` an array index guarded on
`tick_id == current`, and clearing is `tick_id += 1` — free. The evidence this
is worth it: `last_known_prices` and `last_seen_day` do the same number of
per-bar writes in that shape and cost 4% against the slice map's 16%.

The catch is that `Slice.bars` is public and every strategy, example, and doc
uses `data.bars.get(&sym)` / `.contains_key()` / iteration. It has to become an
opaque `Slice` with `get` / `contains` / `len` / `iter`, so batch it with any
other API break rather than spending that churn alone.

## Smaller follow-ups

- `READ_BATCH_SIZE` (131k rows) was tuned for the old sequential reader, where a
  bigger batch meant fewer tick straddles. The parallel consumer merges
  straddles anyway, so batch size now only trades decode efficiency against
  in-flight memory — worth re-sweeping now that several threads hold batches at
  once.
- `MAX_AUTO_THREADS` is 8 with no measurement behind the cap beyond "past this
  the disk is the limit". The full-dataset runs above used it; a sweep at 4 / 8 /
  16 on the real dataset would either justify it or move it.
- `benches/baseline.bencher.txt` still holds pre-interning CI numbers and is now
  far off. Per CLAUDE.md, refresh only from a CI run.

## Verification

- `cargo test && cargo clippy --all-targets && cargo +nightly fmt`
- `tests/tick_stream.rs` is the parallel reader's equivalence suite: it
  synthesizes multi-row-group Parquet (the committed fixture is a single row
  group and exercises none of the ordering machinery) and asserts the stream
  matches a sequential `TickReader` sweep at 1/2/3/8/32 threads.
- The real proof is `examples/no_op_baseline` against the full dataset; compare
  to the table above, and check the bar count is unchanged.

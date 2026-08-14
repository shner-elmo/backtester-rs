# Read-path performance sweep — task brief

> Self-contained brief to run in a **fresh chat**. Goal: sweep the read-path
> tuning knobs on the **real dataset** and promote better defaults where the
> numbers clearly justify it (we accept more CPU/RAM for less wall-clock).
> Nothing here has been changed yet — this is the plan for that separate run.

## Context

The engine streams Parquet row groups through a decode thread pool
(`tick_stream::TickStream`) and hands the tick loop a single strictly-ordered
tick at a time. Throughput is governed by a few constants plus one `Context`
knob. In-code measurements say returns are already flat at the current values on
the machine they were taken on, but they were **not** taken on the current
production dataset/hardware — that is what this task re-checks.

## The knobs

| Knob | Where | Current | Controls | Configurable? |
|------|-------|---------|----------|---------------|
| `set_read_threads(n)` | `backtester/src/context.rs:365` | `0` → `default_threads()` | decode threads ahead of the tick loop | yes, per-run via `Context` |
| `default_threads()` | `backtester/src/tick_stream.rs:60` | `min(cores, 8)` | thread count when `0` | derived |
| `MAX_AUTO_THREADS` | `backtester/src/tick_stream.rs:57` | `8` | cap on the auto thread count | hardcoded |
| `CHANNEL_DEPTH` | `backtester/src/tick_stream.rs:53` | `2` | decoded chunks a worker may queue ahead of the consumer | hardcoded |
| `READ_BATCH_SIZE` | `backtester/src/data.rs:37` | `131_072` | rows per Arrow batch decoded at once | hardcoded |
| row-group size | `scripts/ingest_arrow.rs` | Parquet writer default | rows per row group = one decode work unit (~30/month file) | ingest-time only |

Results are **identical** regardless of thread count / channel depth — the tick
stream is deterministic — so these are pure throughput/memory trades, safe to
change without affecting backtest output.

## Existing measurements (don't re-sweep the known-flat regions)

- `READ_BATCH_SIZE`: 1024 → 5.9 s, 32k → 5.3 s, 128k → 5.1 s, 256k → 4.9 s, flat
  past 128k (7 columns ≈ 6 MB resident per batch at 128k). See the comment at
  `backtester/src/data.rs:27`.
- Threads: the comment at `tick_stream.rs:55` claims the run is disk-bound past
  ~8 threads, so extra threads only add resident batches. **Verify on the real
  dataset** — if the data is in page cache / on NVMe, the disk-bound assumption
  may not hold and more threads could still help.

## How to run

Point the benches and baseline example at the real data (warm the page cache
first — read the files once so the sweep measures decode, not cold I/O):

```bash
# Full loader + backtest throughput benches:
BENCH_DATA_ROOT=/path/to/data BENCH_SYMBOLS=AAPL,MSFT \
  cargo bench -p backtester --bench engine

# Pure engine/read overhead, prints bars/s (no strategy work):
cargo run --release --example no_op_baseline -- /path/to/data
```

- Use `--release` for anything timed (`[profile.release]` = thin LTO, 1 codegen
  unit; worth ~9% but slow to build).
- `no_op_baseline` reports bars/s and is the cleanest read-throughput signal.
- Watch RSS alongside time (e.g. `/usr/bin/time -v`) — the point of raising these
  is to spend RAM for speed, so record both.

## What to sweep

1. **threads**: 4, 8, 12, 16, `= physical cores`. Requires making the auto cap
   (`MAX_AUTO_THREADS`) or the per-run value reachable — pass `set_read_threads`
   from `no_op_baseline` (add a CLI arg) rather than editing the const each time.
2. **`CHANNEL_DEPTH`**: 2, 4, 8. Higher = more decoded batches resident per
   worker; only helps if workers currently starve waiting on the consumer.
3. **`READ_BATCH_SIZE`**: 128k, 256k, 512k. Known flat past 128k on the old box;
   confirm, then stop.
4. **ingest row-group size** (`scripts/ingest_arrow.rs`): only worth touching if
   the above bottleneck traces to per-row-group scheduling granularity; requires
   re-ingesting, so do it last and only if 1–3 point at it.

Sweep one axis at a time from the current defaults; a knob only matters if it
moves bars/s outside the bench noise band.

## Deliverable

- A short table of bars/s (and RSS) per configuration on the real dataset.
- Promote a new default **only** on a clear, repeatable win. Candidates:
  `MAX_AUTO_THREADS` / `default_threads()` (`tick_stream.rs`), `CHANNEL_DEPTH`
  (`tick_stream.rs`), `READ_BATCH_SIZE` (`data.rs`). If you make
  `CHANNEL_DEPTH` / `READ_BATCH_SIZE` tunable, expose them via `Context` setters
  mirroring `set_read_threads`.
- If defaults change, refresh `backtester/benches/baseline.bencher.txt` from a
  **CI** run (not local hardware):
  `cargo bench -p backtester --bench engine -- --output-format bencher` and paste
  the `test ... bench:` lines. CI fails the bench job at >2× the baseline.
```

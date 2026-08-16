# Read-path performance sweep — results (2026-08-15)

> Originally a brief to run in a separate session; **done**. The read-path
> tuning knobs were swept on the real minute dataset and the one clear win was
> promoted. This now records the outcome and the method so the sweep is
> repeatable when the dataset or hardware changes.

## Outcome

**`CHANNEL_DEPTH` 2 → 8** (`backtester/src/tick_stream.rs`) — the only promoted
change.

| Regime | before (depth 2) | after (depth 8) | change |
|--------|------------------|-----------------|--------|
| Warm, decode-bound (1 quarter, full universe, page cache warm) | ~17.0M bars/s | ~22.9M bars/s | **+35%** |
| Full cold-disk scan (29 GB, default threads) | 197.7 s / 9.28M b/s | 142.3 s / 12.9M b/s | **−28%** |

Why: the consumer reads its workers in a fixed rotation, so at depth 2 a worker
blocks the instant the consumer is busy elsewhere in the rotation — the pool
starves on the single tick loop instead of reading ahead of it. Deeper
read-ahead overlaps disk I/O with decode, so it helps both the warm and the
cold-disk regime. Flat past 8. The old "two is enough" comment and the
"disk-bound past 8 threads" assumption were both wrong: the limiter was channel
starvation, not the SSD.

Left unchanged:
- **`READ_BATCH_SIZE`** (128k) — 512k was a wash-to-slightly-slower under the
  parallel consumer (more resident memory, and the consumer merges straddles
  anyway). Batch size is not the bottleneck.
- **`MAX_AUTO_THREADS`** (8) — the path is consumer-bound; a warm sweep peaks
  around 4 threads and degrades past ~12. 8 is within ~2% of the peak and leaves
  headroom for machines with slower per-thread decode, so it was not worth
  hardcoding lower off one box.

Output is unchanged by all of this — the tick stream is deterministic, so these
are pure throughput/memory trades.

## Storage: SATA vs NVMe (2026-08-16)

The full cold scan above is on the production dataset, which lives on a
**2.5" SATA SSD** (Kingston, 327 MB/s sequential `dd`). To test whether that
drive was the ceiling, the 29 GB dataset was copied to a spare
**NVMe** (1.2 GB/s) and the full scan re-run at `CHANNEL_DEPTH=8`:

| Full 29 GB cold scan, depth 8 | time | bars/s | effective read |
|-------------------------------|------|--------|----------------|
| SATA SSD                      | 142.3 s | 12.9M | ~204 MB/s |
| **NVMe**                      | **78.7–82.8 s** | **22.2–23.3M** | ~363 MB/s |
| warm from RAM (decode ceiling)| —    | ~22.9M | — |

**We were SSD-bound on SATA; NVMe removes it.** The NVMe scan lands on the
warm-from-RAM decode ceiling — so on NVMe the single-threaded tick-loop consumer
is the limit, not storage, and it pulls only ~363 MB/s (≈30% of NVMe bandwidth).
Faster-than-NVMe storage would not help; the next win is the consumer side (dense
`Slice` / per-bar work — see `NEXT_STEPS.md`), not more reader/IO tuning.

Note the SATA drive delivered only ~204 MB/s to the real workload vs its 327 MB/s
sequential `dd` — the parallel row-group reader issues concurrent scattered reads
across 60 month files, which SATA degrades on and NVMe does not. So the practical
SATA penalty is larger than the raw spec gap. Actionable: keep the working
dataset on NVMe — full-universe scans roughly halve (142 s → ~80 s).

## The knobs

| Knob | Where | Value | Controls | Configurable? |
|------|-------|-------|----------|---------------|
| `set_read_threads(n)` | `backtester/src/context.rs` | `0` → `default_threads()` | decode threads ahead of the tick loop | yes, per-run via `Context` |
| `default_threads()` | `backtester/src/tick_stream.rs` | `min(cores, 8)` | thread count when `0` | derived |
| `MAX_AUTO_THREADS` | `backtester/src/tick_stream.rs` | `8` | cap on the auto thread count | hardcoded |
| `CHANNEL_DEPTH` | `backtester/src/tick_stream.rs` | `8` (was `2`) | decoded chunks a worker may queue ahead of the consumer | hardcoded |
| `READ_BATCH_SIZE` | `backtester/src/data.rs` | `131_072` | rows per Arrow batch decoded at once | hardcoded |
| row-group size | `scripts/ingest_arrow.rs` | Parquet writer default | rows per row group = one decode work unit | ingest-time only |

## Method (to repeat)

1. **Turn swap off first.** This box runs zram (RAM-backed compressed) swap with
   `swappiness=60`; it evicts the warm slice and swaps process pages, adding
   large run-to-run noise. `sudo swapoff -a`, measure, then restore.
2. **Warm a page-cache-sized slice, keep full-universe width.** RAM here holds
   only a few GB of cache, so a full-dataset run is cold-I/O bound and hides
   decode changes. Warm one quarter (`year=2024/month={1,2,3}`, ~1.5 GB —
   `cat … > /dev/null`) and run `no_op_baseline` over that date range: full
   symbol width, but the bytes stay in cache so you measure decode.
3. **Sweep one axis at a time** from the current defaults; a knob only matters if
   it moves bars/s outside the ~3% noise band. `NOOP_THREADS=n` picks threads
   without a rebuild; `CHANNEL_DEPTH` / `READ_BATCH_SIZE` are consts, so edit +
   `cargo build --release --example no_op_baseline` per value.
4. **Confirm on the full cold scan** before promoting — the production path is
   disk-bound and a knob that helps the warm case must at least not regress it.

```bash
# warm decode-bound quarter, threads via env:
cat "$D"/year=2024/month={1,2,3}/*.parquet > /dev/null   # warm
NOOP_THREADS=4 target/release/examples/no_op_baseline "$D" 2024-01-01 2024-03-31

# full cold scan (production path):
target/release/examples/no_op_baseline "$D"
```

Use `--release` for anything timed (`[profile.release]` = thin LTO, 1 codegen
unit). Watch RSS alongside time — raising `CHANNEL_DEPTH` spends
`CHANNEL_DEPTH × threads` resident chunks (~7 MB each wide-universe).

## Baseline

`backtester/benches/baseline.bencher.txt` doesn't need refreshing for this
change — the CI bench gate only fails on >2× *slowdowns*, and this is faster.
Refresh it from a **CI** run (not local hardware) if you want the recorded
numbers to reflect the speedup:
`cargo bench -p backtester --bench engine -- --output-format bencher`.

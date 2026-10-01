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

## Reader channel-depth memory benchmark (2026-10-02)

A follow-up benchmark checked whether the ~950 MiB RSS in wide-universe runs is
mostly decoded chunks queued in the reader channels. The production default was
left at depth 8; this run only added per-run configuration and measured whether
lower depths are safe enough to recommend.

Method: release build (`thin` LTO, one codegen unit), 8 reader threads, warmed
2024 Q1 on the NVMe minute dataset before every quarter run, no trades, no
backtest result output, and GNU `time -v` around every repetition. All accepted
runs exited successfully with stable ticks, bars, checksums, final equity, and
callback counts.

The initial `noop-wide` check confirms channel buffering is the main RSS driver:

| depth | median wall | median peak RSS | vs depth 8 RSS |
|---:|---:|---:|---:|
| 1 | 7.335 s | 291 MiB | −65.6% |
| 4 | 6.035 s | 541 MiB | −36.2% |
| 8 | 5.820 s | 848 MiB | baseline |

The four-workload Q1 sweep used depths 1, 2, 3, 4, 6, and 8 with three
repetitions each; high-spread cells received two more repetitions before the
recommendation. Median wall time / median RSS:

| workload | depth 1 | depth 2 | depth 3 | depth 4 | depth 6 | depth 8 |
|---|---:|---:|---:|---:|---:|---:|
| `noop-wide` | 7.387s / 290 MiB | 7.048s / 361 MiB | 6.635s / 429 MiB | 6.089s / 546 MiB | 5.820s / 744 MiB | 5.903s / 849 MiB |
| `indicators-wide` | 12.023s / 394 MiB | 11.993s / 484 MiB | 12.153s / 559 MiB | 12.661s / 659 MiB | 12.003s / 832 MiB | 11.719s / 970 MiB |
| `consolidator-wide` | 17.688s / 408 MiB | 17.021s / 499 MiB | 17.931s / 580 MiB | 17.468s / 683 MiB | 18.325s / 848 MiB | 17.441s / 993 MiB |
| `mixed-narrow` | 2.592s / 79 MiB | 2.589s / 81 MiB | 2.609s / 82 MiB | 2.626s / 81 MiB | 2.581s / 81 MiB | 2.588s / 83 MiB |

Decision: stay at depth 8 for now. Depth 1 saves a lot of memory and is fine
for strategy-heavy or narrow consumers, but it is 25% slower than the fastest
depth on `noop-wide`. Depth 4 is within 5% on `noop-wide` and
`consolidator-wide`, but misses the 5% threshold on `indicators-wide`; depth 6
misses on `consolidator-wide`. Depth 8 is the only depth within 5% of the
fastest median for every workload.

Full 56-month validation at depth 8:

| workload | median wall | median peak RSS | ticks | bars | checksum |
|---|---:|---:|---:|---:|---|
| `noop-wide` | 88.678 s | 931 MiB | 1,120,001 | 1,835,105,812 | `b045452492db67ff` |
| `mixed-narrow` | 26.024 s | 111 MiB | 1,006,021 | 10,776,702 | `19278cdbc06ad833` |

Takeaway: deep buffering is unnecessary for slower consumers, but the fastest
full-universe path still benefits enough from read-ahead that the default
should remain 8 unless a caller opts into lower memory with
`Context::set_read_channel_depth`.

## The knobs

| Knob | Where | Value | Controls | Configurable? |
|------|-------|-------|----------|---------------|
| `set_read_threads(n)` | `backtester/src/context.rs` | `0` → `default_threads()` | decode threads ahead of the tick loop | yes, per-run via `Context` |
| `default_threads()` | `backtester/src/tick_stream.rs` | `min(cores, 8)` | thread count when `0` | derived |
| `MAX_AUTO_THREADS` | `backtester/src/tick_stream.rs` | `8` | cap on the auto thread count | hardcoded |
| `set_read_channel_depth(n)` | `backtester/src/context.rs` | `8` (must be non-zero) | decoded chunks a worker may queue ahead of the consumer | yes, per-run via `Context` |
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

`backtester/benches/baseline.bencher.txt` was refreshed on 2026-09-07 from
GitHub Actions run 34124240332, job 101749036935. The CI bench gate fails on
>2× *slowdowns* relative to those runner-generated numbers. Future refreshes
must likewise come from CI rather than local hardware:
`cargo bench -p backtester --bench engine -- --output-format bencher`.

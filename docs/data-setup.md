# Data Setup & Configuration

Everything reads the same minute-bar Parquet dataset. This page covers its
layout, the `BACKTEST_DATA_DIR` environment variable, the committed test
fixture, and the helper examples.

## Dataset layout

The dataset is [Hive-partitioned](https://duckdb.org/docs/data/partitioning/hive_partitioning)
Parquet plus a ticker-encoding JSON:

```
<data root>/
  encoded_tickers.json                       # {"47": "AAPL", ...}  (id -> symbol)
  year=2023/month=1/part-0.parquet
  year=2023/month=2/part-0.parquet
  ...
  metadata/                                  # optional
    get_splits.json
    get_dividends.json
    ticker_renames.json
    insider_transactions.json
```

`encoded_tickers.json` maps the encoded `ticker` id (a `u16`) to its ticker.
That id *is* the engine's [`Symbol`](../backtester/src/symbol.rs): the map is
read once, before the strategy's `initialize`, so `ctx.add_equity("AAPL")` can
hand back the id the data already carries. Streaming a bar then costs one
array index (a subscribed-or-not flag) and never touches the ticker string.

### Optional metadata files & custom paths

Under `metadata/` the engine looks for three optional files:
`get_splits.json` (stock splits), `get_dividends.json` (cash dividends), and
`ticker_renames.json` (ticker renames). When absent they simply mean "no such
events" (the committed test fixture has none of them).

These three can be pointed elsewhere from `initialize`:

```rust
ctx.set_splits_file("/srv/meta/splits.json");      // absolute paths work too
ctx.set_dividends_file("my_dividends.json");
ctx.set_renames_file("renames/2023.json");
```

A relative path resolves against the canonical data root; an absolute
path is used as-is. Note the missing-file rule flips once you set a path
explicitly: a configured splits/dividends/renames file **must exist**, so a
typo fails the run instead of silently skipping every event.

The ticker map is always `<data root>/encoded_tickers.json`. It must be read
before `initialize`, so there is no custom ticker-map entry point.

### Insider transactions (SEC Form 4)

A fourth optional file, `metadata/insider_transactions.json`, holds open-market
insider trades extracted from SEC Form 4 filings. Unlike the files above,
**the engine never reads it** — it's strategy-side data: an algorithm loads
it in `initialize()` via `backtester::insider::load_insider_transactions`
(which streams and filters, so a full-market multi-year file is fine) and
keys the result off the slice date in `on_data`.

```rust
fn initialize(&mut self, ctx: &mut Context) {
    // `None` keeps every ticker the dataset carries; pass a `&SymbolSet` to
    // narrow it. `ctx.ticker_map()` is the map the engine already read, so
    // this does not re-parse `encoded_tickers.json`.
    let txns = load_insider_transactions(ctx.data_dir(), ctx.ticker_map(), None)?;
    for (symbol, by_filing_date) in txns { /* ... */ }
}
```

The returned [`InsiderMap`] is keyed by `Symbol`, not by ticker string:
filings are resolved against the ticker map as they stream in, and records
naming a ticker the dataset has no bars for are dropped there. Two
consequences worth knowing: signals come out ready to trade with no second
ticker→id pass, and iteration order is stable across processes (a
`HashMap<String, _>` reseeds its hasher every run, so a strategy that
commits capital in signal order would produce a different backtest each
time).

Generate it with `scripts/insider_fetch.rs` (SEC's quarterly structured data
sets, 2006q1 onward; the SEC requires a contact User-Agent):

```bash
export BACKTEST_DATA_DIR=/path/to/data
rust-script scripts/insider_fetch.rs \
    --start 2022q1 --end 2023q4 \
    --user-agent "Your Name you@example.com"     # or set $SEC_USER_AGENT
```

Only original Form 4s (no amendments) with open-market codes `P` (purchase)
and `S` (sale) are kept — grants, option exercises and gifts are dropped. A
full-market run covers thousands of tickers; pass `--tickers AAPL,MSFT` to
bound the universe. Record fields:

| Field | Type | Notes |
|-------|------|-------|
| `filing_date` | date | When the Form 4 became public — **trade on this, not `trans_date`, or the backtest has lookahead bias** |
| `trans_date` | date? | When the insider actually traded (predates the filing) |
| `ticker` | string | Issuer symbol as reported to EDGAR (may occasionally differ from the dataset's symbol, e.g. `BRK.B` vs `BRK-B`; unmatched tickers are dropped at load time). Loaded as `InsiderTransaction::symbol`, the dataset's id — the string is not kept |
| `code` | `"P"` / `"S"` | Open-market purchase / sale |
| `shares`, `price`, `value` | float | `value = shares * price` |
| `owner_name`, `officer_title` | string? | First reporting owner on the filing |
| `is_officer`, `is_director`, `is_ten_pct_owner` | bool | OR-ed across all reporting owners |
| `shares_owned_after` | float? | Post-transaction holdings |

Two insider files ship with the repo, both loadable without downloading
anything:

- `test-data/metadata/insider_transactions.json` — four **synthetic**
  AAPL records (not real filings): CEO/COO/SVP purchases filed Jan 4–5 2023
  and a CEO sale filed Jan 6, lined up with the committed minute fixture so a
  copy-trading strategy produces a round trip against it.
- `backtester/tests/fixtures/insider_sample/insider_transactions.json` — 305
  **real** records, straight out of `insider_fetch.rs`: every open-market Form 4
  transaction for AAPL and INTC filed between 2020-01-06 and 2026-02-03 (52
  purchases, 253 sales). The repo has no price bars for those names, so this
  is a parser fixture and a reference for the real-world shapes — six years of
  filings, empty `officer_title`s, a realistic buy/sell ratio — rather than
  something to backtest directly. Two issuers keep it reviewable; re-run
  `insider_fetch.rs` for a wider slice and copy it into your own data root to
  trade on it.

### Parquet schema

The engine reads exactly these columns (the `COLUMNS` const in
[`data.rs`](../backtester/src/data.rs)); this is also the schema the ingest
script ([`scripts/ingest_arrow.rs`](../scripts/ingest_arrow.rs)) produces:

| Column | Type | Notes |
|--------|------|-------|
| `ticker` | `u16` | Encoded id; resolve via `encoded_tickers.json` |
| `window_start` | timestamp (ns) | Bar start, epoch nanoseconds (read as UTC); files must be sorted non-decreasing on it |
| `open`, `high`, `low`, `close` | `f64` | |
| `volume` | `u32` | |

Extra columns (older Polygon-derived files carried `transactions`,
`market_session`, `day`) are ignored — there is no session column anymore; the
session is derived from the timestamp's US Eastern time-of-day via
`bar.session()`.

> Physical column order varies between dataset generations (older files put
> `close` before `high`/`low`). Column readers must look columns up **by
> name**, not by position — otherwise high/low/close get scrambled. This bit
> the loader once; it's now guarded by
> [`backtester/tests/data.rs`](../backtester/tests/data.rs).

## `BACKTEST_DATA_DIR`

The backtester, data-viz binary, scripts, and examples all use the same
canonical root:

```bash
export BACKTEST_DATA_DIR=/path/to/data
```

`run(algo)` and `run_backtest(algo)` return `BacktestError::MissingConfiguration`
when it is absent. Library callers with an already selected path can use
`run_with_data_dir(algo, path)` or `run_backtest_with_data_dir(algo, path)`;
those explicit APIs do not inspect the environment. `Context::data_dir()`
exposes the selected root to strategy-side loaders.

## Regenerating the dataset from raw CSVs

The `--input` raw-minute directory contains one gzipped CSV per trading day at
`<input>/<YYYY>/<MM>/<YYYY-MM-DD>.csv.gz` with columns
`ticker,volume,open,close,high,low,window_start,transactions`
(`window_start` in epoch nanoseconds). Rows are grouped by ticker,
**not** globally time-sorted.

[`scripts/ingest_arrow.rs`](../scripts/ingest_arrow.rs) (a
[rust-script](https://rust-script.org)) converts that tree into the
Hive-partitioned Parquet layout above:

```bash
cargo install rust-script   # once
export BACKTEST_DATA_DIR=/path/to/data
rust-script scripts/ingest_arrow.rs \
  --input <raw-minute-dir>
```

## Generating an early-close calendar

[`scripts/generate_early_close_calendar.rs`](../scripts/generate_early_close_calendar.rs) scans
the `window_start` column of every minute-bar Parquet file under `BACKTEST_DATA_DIR`. For each US
Eastern date it counts market-wide bars per minute between 09:30 (inclusive) and 16:00
(exclusive), finds a sustained activity drop from the morning baseline, and writes dates where
the final broad-market auction minute occurs before 16:00 to a sorted JSON object. This excludes
sparse post-close prints that can otherwise look like a later closing time.

```bash
export BACKTEST_DATA_DIR=/path/to/minute-dataset
rust-script scripts/generate_early_close_calendar.rs \
  --output /path/to/early_closes.json
```

The resulting shape is:

```json
{
  "2024-07-03": "13:00",
  "2024-11-29": "13:00",
  "2024-12-24": "13:00"
}
```

The script creates `encoded_tickers.json` as part of the dataset by scanning
the input, sorting distinct tickers, and assigning sequential `u16` ids. It
publishes the map through a temporary file and atomic rename. CSV parse errors
fail the month instead of silently dropping rows.

Design: each daily file is sorted by `(window_start, ticker)` in memory and
appended to its month's writer in date order. Consecutive trading days are
disjoint in time (after-market ends 20:00 ET, the next pre-market opens
4:00 ET), so the concatenation is globally time-sorted without ever buffering
more than one day (~1.5 M rows) per worker; the day-boundary invariant is
checked at runtime and aborts the month if violated. Months convert in
parallel and are written via tmp-file + atomic rename, so an interrupted run
can't leave a truncated file. The full 2021–2025 dataset (56 months, ~1.8 B
rows) converts in about 5 minutes.

Verify before pointing the engine at the result:

```bash
export BACKTEST_DATA_DIR=/path/to/data
cargo run --release -p backtester --example check_sorted
```

## Daily dataset

Strategies that only need daily resolution can run on a resampled copy of the
minute dataset: same layout, same schema, same ticker ids, one row per ticker
per trading day. [`scripts/resample_daily.rs`](../scripts/resample_daily.rs)
builds it:

```bash
export BACKTEST_DATA_DIR=/path/to/data/minute   # the source minute dataset
rust-script scripts/resample_daily.rs --output /path/to/data/daily
```

Then either point `BACKTEST_DATA_DIR` at the daily root, or keep the minute
root as the primary dataset and select daily bars in `initialize`:

```rust
ctx.set_bar_data_dir("/path/to/data/daily");
```

The latter keeps `ctx.data_dir()`, its ticker map, and metadata on the minute
root while only the streamed bars come from the daily copy. The engine checks
that both roots have the same `encoded_tickers.json`. In either form,
`on_data` fires once per day. Existing consolidators are unchanged and consume
whichever bars the run selected.

The full 2021–2025 dataset (1.8 B
minute rows, 32 GB) resamples in about 3 minutes (one engine run per month, 8
in parallel; peak RSS ~1.5 GB) to 12.17 M rows (217 MB), and a full-universe
no-op scan drops from ~80 s to ~3.5 s.

**Keep the two roots side by side, never nested.** The engine discovers Parquet
recursively, so a daily dataset inside the minute root (or vice versa) would
interleave the two; the script refuses nested paths. It copies
`encoded_tickers.json` and symlinks `metadata/` to the minute root's (relative
when the roots are siblings), so splits, dividends, renames and insider data
are shared rather than duplicated.

Bar semantics:

- The resampling is itself a backtest: every symbol gets a
  `ConsolidatorPeriod::Daily` consolidator, so the bars are exactly what a
  strategy consolidating the minute data with extended hours disabled would
  see. A daily bar is one ticker's NYSE regular-session minutes within one US
  Eastern trading day: first open, max high, min low, last close, summed
  volume, stamped at **US Eastern midnight**. Normal sessions are
  09:30–16:00 (the 16:00 bar is excluded); official early closes, including
  13:00 half days, are honored by the exchange calendar.
- Because of the midnight stamp, `bar.session()` reports `PreMarket` for every
  daily bar and `Context::on_time` callbacks don't line up with real market
  hours. Leave `Context`'s default extended-hours setting enabled when reading
  the already-aggregated daily root, or its midnight bars will be filtered
  out. The [fill timing](backtesting.md#fill-timing) rules are unchanged: the default
  fills at the day's close, `FillTiming::NextBarOpen` at the next day's open.
- Volume stays `UInt32`; a daily sum above `u32::MAX` saturates and the script
  reports the count. The verified 2021–2025 regular-session run had no
  saturated rows.

## Committed test fixture

A tiny slice — AAPL, January 2023, 5,000 bars (~126 KB) — is committed so the
whole suite runs with **no external data**:

```text
test-data/
  encoded_tickers.json
  year=2023/month=1/part-0.parquet
  metadata/insider_transactions.json
```

Both the backtester and data-viz integration suites use this single fixture.
Parser-only real-world SEC samples remain under
`backtester/tests/fixtures/insider_sample/`.

Regenerate the fixture from the full dataset with:

```bash
export BACKTEST_DATA_DIR=/path/to/data
cargo run -p data-viz --example make_test_fixture
# writes the shared test-data fixture
```

## Helper examples

| Example | Crate | What it does |
|---------|-------|--------------|
| `ema_cross` | backtester | The reference strategy ([backtesting.md](./backtesting.md)) |
| `print_schema` | backtester | Dump a Parquet file's Arrow schema |
| `check_sorted` | backtester | Verify every file is time-sorted (the engine's hard requirement); flags unreadable files |
| `data_invariants_check` | backtester | Sweep the canonical dataset asserting OHLC/volume invariants |
| `no_op_baseline` | backtester | Time a full-universe no-op backtest — the engine's floor cost in bars/s |
| `make_test_fixture` | data-viz | Regenerate the committed fixture |
| `read_and_filter` | data-viz | DataFusion query against the partitioned dataset |
| `schema_debug` | data-viz | Inspect inferred schema via a ListingTable |
| `rename_to_hive` | data-viz | Migrate bare `year/month` dirs to `year=/month=` |

Run any of them with `cargo run -p <crate> --example <name>` (set
`BACKTEST_DATA_DIR` first for the ones that need data).

## File ordering

`sorted_parquet_files` recursively discovers Parquet files below the data root
and orders them by `(year, month, path)`. Every discovered file must end in
`year=YYYY/month=M/*.parquet`; a malformed partition path fails immediately
instead of being silently ordered ahead of the dataset.

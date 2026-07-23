# Data Setup & Configuration

Everything reads the same minute-bar Parquet dataset. This page covers its
layout, the `STONKS_DATA_ROOT` environment variable, the committed test
fixture, and the helper examples.

## Dataset layout

The dataset is [Hive-partitioned](https://duckdb.org/docs/data/partitioning/hive_partitioning)
Parquet plus a ticker-encoding JSON:

```
<data root>/
  encoded_tickers.json                       # {"47": "AAPL", ...}  (id -> symbol)
  minute/
    year=2023/month=1/part-0.parquet
    year=2023/month=2/part-0.parquet
    ...
```

`encoded_tickers.json` maps the encoded `ticker` id (a `u16`) to its symbol.

### Optional metadata files & custom paths

Next to `encoded_tickers.json` the engine also looks for three optional
files: `get_splits.json` (stock splits), `get_dividends.json` (cash
dividends), and `ticker_renames.json` (ticker renames). When absent they
simply mean "no such events" (the committed test fixture has none of them).

Every one of these locations — including the ticker map — can be overridden
in code from `initialize`:

```rust
ctx.set_ticker_map_file("my_tickers.json");        // relative to the data root
ctx.set_splits_file("/srv/meta/splits.json");      // absolute paths work too
ctx.set_dividends_file("my_dividends.json");
ctx.set_renames_file("renames/2023.json");
```

A relative path resolves against the data root passed to `run`; an absolute
path is used as-is. Note the missing-file rule flips once you set a path
explicitly: a configured splits/dividends/renames file **must exist**, so a
typo fails the run instead of silently skipping every event.

### Parquet schema

The engine reads exactly these columns (the `COLUMNS` const in
[`data.rs`](../backtester/src/data.rs)); this is also the schema the ingest
scripts ([`scripts/ingest_arrow.rs`](../scripts/ingest_arrow.rs)) produce:

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

## Who consumes what

The two entry points have slightly different expectations for their path
argument:

- **backtester** (`run(algo, data_path)`): `data_path` is a directory
  containing `encoded_tickers.json`, with Parquet files anywhere beneath it
  (discovered recursively).
- **data-viz** (`DATA_PATH`): a data root containing `encoded_tickers.json`
  **and** a `minute/` subdirectory of Parquet.

The committed fixture (below) satisfies both.

## `STONKS_DATA_ROOT`

The example programs don't hardcode any machine-specific paths. They read
`STONKS_DATA_ROOT`, which should point at the **`minute/` directory** of your
dataset:

```bash
export STONKS_DATA_ROOT=/path/to/data/output/minute
```

Each example either uses this directly or appends a sub-path (e.g.
`make_test_fixture` reads `$STONKS_DATA_ROOT/year=2023/month=1/part-0.parquet`).

## Committed test fixture

A tiny slice — AAPL, January 2023, 5,000 bars (~126 KB) — is committed so the
whole suite runs with **no external data**:

```
backtester/tests/fixtures/   # used by cargo test -p backtester
data-viz/tests/fixtures/     # used by cargo test -p data-viz
```

Both the backtester and data-viz test suites default to these; override with
`DATA_PATH` to test against the full dataset.

Regenerate the fixture from the full dataset with:

```bash
STONKS_DATA_ROOT=/path/to/data/output/minute \
  cargo run -p data-viz --example make_test_fixture
# (writes into data-viz/tests/fixtures; copy into backtester/tests/fixtures if refreshed)
```

## Helper examples

| Example | Crate | What it does |
|---------|-------|--------------|
| `ema_cross` | backtester | The reference strategy ([backtesting.md](./backtesting.md)) |
| `print_schema` | backtester | Dump a Parquet file's Arrow schema |
| `list_parquet_files` | backtester | List discovered Parquet files in sorted order |
| `check_sorted` | backtester | Verify every file is time-sorted (the engine's hard requirement); flags unreadable files |
| `resort_parquet` | backtester | Repair unsorted files: stable re-sort by `(window_start, ticker)`, verified rewrite |
| `noop_baseline` | backtester | Time a full-universe no-op backtest — the engine's floor cost in bars/s |
| `make_test_fixture` | data-viz | Regenerate the committed fixture |
| `read_and_filter` | data-viz | DataFusion query against the partitioned dataset |
| `schema_debug` | data-viz | Inspect inferred schema via a ListingTable |
| `rename_to_hive` | data-viz | Migrate bare `year/month` dirs to `year=/month=` |

Run any of them with `cargo run -p <crate> --example <name>` (set
`STONKS_DATA_ROOT` first for the ones that need data).

## File ordering

`sorted_parquet_files` orders files by `(year, month)`, parsed from the parent
directory names. It understands both bare (`year/month`, e.g. `2023/6`) and
Hive (`year=2023/month=6`) layouts — `dir_number` takes the part after any `=`
— so multi-month runs stream in chronological order. Files whose parents don't
parse sort first, then by path.

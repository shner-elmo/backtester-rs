//! Performance benchmarks, wired into CI so throughput regressions surface in
//! the run log and fail the build (compared against a committed baseline —
//! see `scripts/bench_compare.py`).
//!
//! Two benchmarks:
//!
//! * `read_all_bars` — raw loader throughput: stream every bar and do nothing
//!   but count them. Captures the per-bar decode + symbol-string allocation.
//! * `ema_cross_backtest` — a full `run_backtest` of a small EMA-cross strategy
//!   (indicator updates + `set_holdings` / `liquidate` orders) over the same
//!   data.
//!
//! CI has no access to the real dataset, so by default the benches build their
//! input by **replicating the committed AAPL fixture** (`tests/fixtures`, 5,000
//! Jan-2023 minute bars) across ~30 months: replica `k` is the fixture shifted
//! forward `k` calendar months with its OHLC scaled by `1.02^k`, giving a
//! monotonic multi-month single-symbol dataset from real-shaped data. Set
//! `BENCH_DATA_ROOT` to a real data root (the dir holding `encoded_tickers.json`
//! and `minute/year=.../month=.../*.parquet`) to benchmark against it instead;
//! `BENCH_SYMBOLS` (comma-separated, default `AAPL`) picks what the strategy
//! trades in that case.
//!
//! Single-symbol data doesn't stress multi-symbol tick grouping the way a wide
//! universe would; it's a deterministic, real-shaped regression signal. Symbol
//! fan-out could be added later by duplicating the batch under new ticker ids.

use std::sync::Arc;

use arrow::{
    array::{Float64Array, TimestampNanosecondArray, UInt16Array, UInt32Array},
    datatypes::{DataType, Field, Schema, TimeUnit},
    record_batch::RecordBatch,
};
use backtester::{
    data::{iter_bars, load_ticker_map, sorted_parquet_files},
    indicators::{Ema, Next},
    run_backtest, Algorithm, Context, Slice,
};
use chrono::{Datelike, Months, NaiveDate, TimeZone, Utc};
use criterion::{black_box, criterion_group, criterion_main, Criterion, Throughput};
use parquet::arrow::{arrow_reader::ParquetRecordBatchReaderBuilder, ArrowWriter};
use tempfile::TempDir;

/// Roughly how many bars the replicated dataset should contain; the number of
/// monthly replicas is derived from this and the fixture's row count.
const TARGET_BARS: usize = 150_000;

/// The committed fixture: one symbol (AAPL), Jan 2023, ~5,000 minute bars.
fn fixture_root() -> String {
    concat!(env!("CARGO_MANIFEST_DIR"), "/tests/fixtures").to_string()
}

fn schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("ticker", DataType::UInt16, false),
        Field::new("volume", DataType::UInt32, false),
        Field::new("open", DataType::Float64, false),
        Field::new("close", DataType::Float64, false),
        Field::new("high", DataType::Float64, false),
        Field::new("low", DataType::Float64, false),
        Field::new("window_start", DataType::Timestamp(TimeUnit::Nanosecond, None), false),
    ]))
}

/// The fixture's columns, read once into memory so each monthly replica is a
/// cheap transform of them.
struct FixtureColumns {
    ticker: Vec<u16>,
    volume: Vec<u32>,
    open: Vec<f64>,
    high: Vec<f64>,
    low: Vec<f64>,
    close: Vec<f64>,
    ts: Vec<i64>,
}

/// Read every row of the fixture Parquet into column vectors (by name, matching
/// the loader in `data.rs`).
fn read_fixture(file: &std::path::Path) -> FixtureColumns {
    let f = std::fs::File::open(file).unwrap();
    let reader = ParquetRecordBatchReaderBuilder::try_new(f).unwrap().build().unwrap();

    let mut cols = FixtureColumns {
        ticker: Vec::new(),
        volume: Vec::new(),
        open: Vec::new(),
        high: Vec::new(),
        low: Vec::new(),
        close: Vec::new(),
        ts: Vec::new(),
    };

    for batch in reader {
        let batch = batch.unwrap();
        let u16_col = |name| {
            batch.column_by_name(name).unwrap().as_any().downcast_ref::<UInt16Array>().unwrap()
        };
        let u32_col = |name| {
            batch.column_by_name(name).unwrap().as_any().downcast_ref::<UInt32Array>().unwrap()
        };
        let f64_col = |name| {
            batch.column_by_name(name).unwrap().as_any().downcast_ref::<Float64Array>().unwrap()
        };
        let ts_col = |name| {
            batch
                .column_by_name(name)
                .unwrap()
                .as_any()
                .downcast_ref::<TimestampNanosecondArray>()
                .unwrap()
        };

        let ticker = u16_col("ticker");
        let volume = u32_col("volume");
        let open = f64_col("open");
        let high = f64_col("high");
        let low = f64_col("low");
        let close = f64_col("close");
        let ts = ts_col("window_start");

        for i in 0..batch.num_rows() {
            cols.ticker.push(ticker.value(i));
            cols.volume.push(volume.value(i));
            cols.open.push(open.value(i));
            cols.high.push(high.value(i));
            cols.low.push(low.value(i));
            cols.close.push(close.value(i));
            cols.ts.push(ts.value(i));
        }
    }

    cols
}

/// Write one replica to `minute/year=Y/month=M/part-0.parquet`.
#[allow(clippy::too_many_arguments)]
fn write_replica(
    root: &std::path::Path,
    year: i32,
    month: u32,
    ticker: &[u16],
    volume: &[u32],
    open: Vec<f64>,
    close: Vec<f64>,
    high: Vec<f64>,
    low: Vec<f64>,
    ts: Vec<i64>,
) {
    let schema = schema();
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(UInt16Array::from(ticker.to_vec())),
            Arc::new(UInt32Array::from(volume.to_vec())),
            Arc::new(Float64Array::from(open)),
            Arc::new(Float64Array::from(close)),
            Arc::new(Float64Array::from(high)),
            Arc::new(Float64Array::from(low)),
            Arc::new(TimestampNanosecondArray::from(ts)),
        ],
    )
    .unwrap();

    let dir = root.join(format!("minute/year={year}/month={month}"));
    std::fs::create_dir_all(&dir).unwrap();
    let file = std::fs::File::create(dir.join("part-0.parquet")).unwrap();
    let mut writer = ArrowWriter::try_new(file, schema, None).unwrap();
    writer.write(&batch).unwrap();
    writer.close().unwrap();
}

/// Build the benchmark dataset by replicating the fixture across months. Returns
/// the tempdir (kept alive by the caller) and the total bar count.
fn generate_dataset() -> (TempDir, u64) {
    let root = fixture_root();
    let file = sorted_parquet_files(&root).into_iter().next().expect("fixture parquet missing");
    let fx = read_fixture(&file);
    let rows = fx.ts.len();
    assert!(rows > 0, "fixture produced no rows");
    let months = (TARGET_BARS.div_ceil(rows)).max(1) as u32;

    let tmp = tempfile::tempdir().unwrap();
    // Reuse the fixture's ticker map (id 47 -> AAPL).
    std::fs::copy(format!("{root}/encoded_tickers.json"), tmp.path().join("encoded_tickers.json"))
        .unwrap();

    // The fixture is entirely within Jan 2023, so shifting a whole replica by k
    // months lands it in month (Jan 2023 + k) — dir label and data stay in sync.
    let base = NaiveDate::from_ymd_opt(2023, 1, 1).unwrap();
    for k in 0..months {
        let shifted = base.checked_add_months(Months::new(k)).unwrap();
        let scale = 1.02_f64.powi(k as i32);

        let ts: Vec<i64> = fx
            .ts
            .iter()
            .map(|&ns| {
                Utc.timestamp_nanos(ns)
                    .checked_add_months(Months::new(k))
                    .unwrap()
                    .timestamp_nanos_opt()
                    .unwrap()
            })
            .collect();
        let open = fx.open.iter().map(|&v| v * scale).collect();
        let high = fx.high.iter().map(|&v| v * scale).collect();
        let low = fx.low.iter().map(|&v| v * scale).collect();
        let close = fx.close.iter().map(|&v| v * scale).collect();

        write_replica(
            tmp.path(),
            shifted.year(),
            shifted.month(),
            &fx.ticker,
            &fx.volume,
            open,
            close,
            high,
            low,
            ts,
        );
    }

    (tmp, rows as u64 * months as u64)
}

/// EMA-cross over the traded symbol(s): exercises indicator updates and the
/// order path without dominating the run with strategy work.
struct EmaCross {
    legs: Vec<(String, Ema, Ema)>,
}

impl EmaCross {
    fn new(symbols: &[String]) -> Self {
        let legs = symbols
            .iter()
            .map(|s| (s.clone(), Ema::new(10).unwrap(), Ema::new(30).unwrap()))
            .collect();
        Self { legs }
    }
}

impl Algorithm for EmaCross {
    fn initialize(&mut self, ctx: &mut Context) {
        ctx.set_cash(100_000.0);
        for (s, _, _) in &self.legs {
            ctx.add_equity(s);
        }
    }

    fn on_data(&mut self, ctx: &mut Context, data: &Slice) {
        for (symbol, fast, slow) in &mut self.legs {
            let Some(bar) = data.bars.get(symbol) else { continue };
            let f = fast.next(bar.close);
            let s = slow.next(bar.close);
            if f > s {
                ctx.set_holdings(symbol, 0.2);
            } else {
                ctx.liquidate(symbol);
            }
        }
    }
}

fn traded_symbols() -> Vec<String> {
    match std::env::var("BENCH_DATA_ROOT") {
        // Against a real dataset, trade whatever BENCH_SYMBOLS names (default AAPL).
        Ok(_) => std::env::var("BENCH_SYMBOLS")
            .unwrap_or_else(|_| "AAPL".to_string())
            .split(',')
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty())
            .collect(),
        // The replicated fixture is AAPL only.
        Err(_) => vec!["AAPL".to_string()],
    }
}

fn benches(c: &mut Criterion) {
    // Keep the tempdir alive for the whole benchmark run.
    let (_guard, path, total_bars) = match std::env::var("BENCH_DATA_ROOT") {
        Ok(root) => (None, root, 0),
        Err(_) => {
            let (tmp, total) = generate_dataset();
            let path = tmp.path().to_str().unwrap().to_string();
            (Some(tmp), path, total)
        }
    };

    let ticker_map = load_ticker_map(&path).expect("load ticker map for benchmark data root");
    let symbols = traded_symbols();

    let mut group = c.benchmark_group("engine");
    group.sample_size(20);
    if total_bars > 0 {
        group.throughput(Throughput::Elements(total_bars));
    }

    group.bench_function("read_all_bars", |b| {
        b.iter(|| {
            let mut n = 0u64;
            iter_bars(&path, &ticker_map, |_symbol, _bar| n += 1).unwrap();
            black_box(n)
        });
    });

    group.bench_function("ema_cross_backtest", |b| {
        b.iter(|| {
            let algo = EmaCross::new(&symbols);
            black_box(run_backtest(algo, &path).unwrap())
        });
    });

    group.finish();
}

criterion_group!(engine, benches);
criterion_main!(engine);

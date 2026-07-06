//! Performance benchmarks, wired into CI so throughput regressions surface in
//! the run log.
//!
//! Two benchmarks:
//!
//! * `read_all_bars` — raw loader throughput: stream every bar and do nothing
//!   but count them. Captures the per-bar decode + symbol-string allocation.
//! * `ema_cross_backtest` — a full `run_backtest` of a small EMA-cross strategy
//!   (indicator updates + `set_holdings` / `liquidate` orders) over the same
//!   data.
//!
//! CI has no access to the real dataset, so by default the benches generate a
//! synthetic month-partitioned dataset (~160k bars) into a tempdir. Set
//! `BENCH_DATA_ROOT` to a real data root (the dir holding `encoded_tickers.json`
//! and `minute/year=.../month=.../*.parquet`) to benchmark against it instead;
//! `BENCH_SYMBOLS` (comma-separated, default `AAPL`) picks what the strategy
//! trades in that case.

use std::sync::Arc;

use arrow::array::{Float64Array, TimestampNanosecondArray, UInt16Array, UInt32Array, UInt8Array};
use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use arrow::record_batch::RecordBatch;
use chrono::{Duration, NaiveDate, TimeZone, Utc};
use criterion::{black_box, criterion_group, criterion_main, Criterion, Throughput};
use parquet::arrow::ArrowWriter;
use tempfile::TempDir;

use backtester::{
    data::{iter_bars, load_ticker_map},
    indicators::{Ema, Next},
    run_backtest, Algorithm, Context, Slice,
};

const SYMBOLS: usize = 32;
const MONTHS: &[(i32, u32)] = &[(2023, 6), (2023, 7)];
const DAYS_PER_MONTH: u32 = 20;
const BARS_PER_DAY: u32 = 130;
/// How many synthetic symbols the strategy actually trades.
const TRADED: usize = 4;

fn synthetic_symbol(i: usize) -> String {
    format!("SYM{i:03}")
}

/// Deterministic oscillating close so fast/slow EMAs cross repeatedly (the
/// strategy places real orders). Phase differs per symbol.
fn close_at(global_bar: u64, symbol: usize) -> f64 {
    let phase = symbol as f64 * 0.37;
    100.0 + 15.0 * ((global_bar as f64) / 29.0 + phase).sin()
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
        Field::new("market_session", DataType::UInt8, false),
    ]))
}

/// Write one month's worth of bars for every symbol into
/// `minute/year=Y/month=M/part-0.parquet`. Returns the row count written.
fn write_month(root: &std::path::Path, year: i32, month: u32, base_bar: u64) -> u64 {
    let schema = schema();

    let mut tickers = Vec::new();
    let mut volume = Vec::new();
    let mut open = Vec::new();
    let mut close = Vec::new();
    let mut high = Vec::new();
    let mut low = Vec::new();
    let mut ts = Vec::new();
    let mut session = Vec::new();

    let mut global_bar = base_bar;
    for day in 1..=DAYS_PER_MONTH {
        for minute in 0..BARS_PER_DAY {
            // 13:30 UTC = 09:30 ET, so UTC and ET dates agree.
            let t =
                NaiveDate::from_ymd_opt(year, month, day).unwrap().and_hms_opt(13, 30, 0).unwrap()
                    + Duration::minutes(minute as i64);
            let nanos = Utc.from_utc_datetime(&t).timestamp_nanos_opt().unwrap();
            for s in 0..SYMBOLS {
                let c = close_at(global_bar, s);
                let o = close_at(global_bar.saturating_sub(1), s);
                tickers.push((s + 1) as u16);
                volume.push(100u32);
                open.push(o);
                close.push(c);
                high.push(o.max(c) + 0.05);
                low.push(o.min(c) - 0.05);
                ts.push(nanos);
                session.push(2u8);
            }
            global_bar += 1;
        }
    }

    let rows = tickers.len() as u64;
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(UInt16Array::from(tickers)),
            Arc::new(UInt32Array::from(volume)),
            Arc::new(Float64Array::from(open)),
            Arc::new(Float64Array::from(close)),
            Arc::new(Float64Array::from(high)),
            Arc::new(Float64Array::from(low)),
            Arc::new(TimestampNanosecondArray::from(ts)),
            Arc::new(UInt8Array::from(session)),
        ],
    )
    .unwrap();

    let dir = root.join(format!("minute/year={year}/month={month}"));
    std::fs::create_dir_all(&dir).unwrap();
    let file = std::fs::File::create(dir.join("part-0.parquet")).unwrap();
    let mut writer = ArrowWriter::try_new(file, schema, None).unwrap();
    writer.write(&batch).unwrap();
    writer.close().unwrap();
    rows
}

fn generate_dataset() -> (TempDir, u64) {
    let tmp = tempfile::tempdir().unwrap();

    let map = (0..SYMBOLS)
        .map(|i| format!("\"{}\": \"{}\"", i + 1, synthetic_symbol(i)))
        .collect::<Vec<_>>()
        .join(", ");
    std::fs::write(tmp.path().join("encoded_tickers.json"), format!("{{{map}}}")).unwrap();

    let mut total = 0;
    let mut base_bar = 0u64;
    for &(y, m) in MONTHS {
        total += write_month(tmp.path(), y, m, base_bar);
        base_bar += (DAYS_PER_MONTH * BARS_PER_DAY) as u64;
    }
    (tmp, total)
}

/// EMA-cross over a handful of symbols: exercises indicator updates and the
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
    if std::env::var("BENCH_DATA_ROOT").is_ok() {
        std::env::var("BENCH_SYMBOLS")
            .unwrap_or_else(|_| "AAPL".to_string())
            .split(',')
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty())
            .collect()
    } else {
        (0..TRADED).map(synthetic_symbol).collect()
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

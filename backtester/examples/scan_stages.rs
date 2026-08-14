//! Stage-by-stage cost attribution for a full-dataset scan.
//!
//! Runs the same month files through progressively more of the read path and
//! reports the wall time each stage adds, so a slow backtest can be blamed on
//! the layer that actually costs the time (I/O, Parquet decode, timestamp
//! conversion, tick grouping, engine bookkeeping) instead of guessed at.
//!
//! Usage: cargo run --release --example scan_stages -- /path/to/data/root [YYYY-MM ...]
//!
//! With no month arguments it scans every file, which is the whole dataset —
//! pass one or two months for an iteration-speed run. The first stage warms
//! the page cache, so later stages measure CPU rather than disk.

use std::{fs::File, hint::black_box, io::Read, path::PathBuf, time::Instant};

use arrow::array::{Float64Array, TimestampNanosecondArray, UInt16Array, UInt32Array};
use backtester::data::{
    file_year_month, sorted_parquet_files, SubscriptionMask, TickReader, COLUMNS,
};
use chrono::{DateTime, TimeZone, Utc};
use parquet::arrow::{
    arrow_reader::{ParquetRecordBatchReader, ParquetRecordBatchReaderBuilder},
    ProjectionMask,
};

fn open(path: &PathBuf, mask: &mut Option<ProjectionMask>) -> ParquetRecordBatchReader {
    let file = File::open(path).expect("open parquet");
    let builder = ParquetRecordBatchReaderBuilder::try_new(file).expect("parquet builder");
    let mask = mask
        .get_or_insert_with(|| ProjectionMask::columns(builder.parquet_schema(), COLUMNS))
        .clone();
    builder.with_projection(mask).build().expect("parquet reader")
}

fn ts_to_datetime(ts_ns: i64) -> Option<DateTime<Utc>> {
    let secs = ts_ns / 1_000_000_000;
    let nanos = (ts_ns % 1_000_000_000) as u32;
    Utc.timestamp_opt(secs, nanos).single()
}

/// Time `f`, print the label, rows/s, and return the elapsed seconds.
fn stage(label: &str, rows: u64, f: impl FnOnce() -> u64) -> f64 {
    let t = Instant::now();
    let counted = f();
    let secs = t.elapsed().as_secs_f64();
    let rows = if rows == 0 { counted } else { rows };
    println!("{label:<28} {secs:>8.2}s   {:>10.1}M rows/s", rows as f64 / secs / 1e6);
    secs
}

fn main() {
    let args: Vec<String> = std::env::args().collect();
    let root = args.get(1).map(String::as_str).unwrap_or("data/output/minute");
    let months: Vec<(u32, u32)> = args[2..]
        .iter()
        .map(|s| {
            let (y, m) = s.split_once('-').expect("month arg looks like YYYY-MM");
            (y.parse().expect("year"), m.trim_start_matches('0').parse().expect("month"))
        })
        .collect();

    let files: Vec<PathBuf> = sorted_parquet_files(root)
        .into_iter()
        .filter(|p| months.is_empty() || file_year_month(p).is_some_and(|ym| months.contains(&ym)))
        .collect();
    assert!(!files.is_empty(), "no parquet files matched");
    let bytes: u64 = files.iter().map(|p| p.metadata().map(|m| m.len()).unwrap_or(0)).sum();
    println!("{} file(s), {:.2} GiB on disk\n", files.len(), bytes as f64 / (1 << 30) as f64);

    // Stage 0 — raw bytes off the device. Also warms the page cache for the
    // stages below, so they measure CPU and not the disk.
    let io = stage("0 io: read bytes", 1, || {
        let mut buf = vec![0u8; 1 << 22];
        let mut total = 0u64;
        for path in &files {
            let mut f = File::open(path).expect("open");
            while let Ok(n) = f.read(&mut buf) {
                if n == 0 {
                    break;
                }
                total += n as u64;
            }
        }
        black_box(total)
    });
    println!("   ({:.0} MiB/s)\n", bytes as f64 / (1 << 20) as f64 / io);

    // Stage 1 — Parquet -> Arrow decode, columns materialized but untouched.
    let mut rows = 0u64;
    let decode = stage("1 parquet decode", 0, || {
        let mut mask = None;
        let mut n = 0u64;
        for path in &files {
            for batch in open(path, &mut mask) {
                n += batch.expect("batch").num_rows() as u64;
            }
        }
        rows = n;
        n
    });

    // Stage 2 — + reading every column value per row (what building a Bar
    // costs before any conversion).
    let cols = stage("2 + column reads", rows, || {
        let mut mask = None;
        let (mut n, mut acc) = (0u64, 0.0f64);
        for path in &files {
            for batch in open(path, &mut mask) {
                let batch = batch.expect("batch");
                let ticker = batch.column_by_name("ticker").unwrap();
                let ticker = ticker.as_any().downcast_ref::<UInt16Array>().unwrap();
                let volume = batch.column_by_name("volume").unwrap();
                let volume = volume.as_any().downcast_ref::<UInt32Array>().unwrap();
                let f = |name: &str| {
                    let c = batch.column_by_name(name).unwrap();
                    c.as_any().downcast_ref::<Float64Array>().unwrap().clone()
                };
                let (open_, high, low, close) = (f("open"), f("high"), f("low"), f("close"));
                let ts = batch.column_by_name("window_start").unwrap();
                let ts = ts.as_any().downcast_ref::<TimestampNanosecondArray>().unwrap();
                for i in 0..batch.num_rows() {
                    acc += open_.value(i) + high.value(i) + low.value(i) + close.value(i);
                    acc += volume.value(i) as f64 + ticker.value(i) as f64 + ts.value(i) as f64;
                    n += 1;
                }
            }
        }
        black_box(acc);
        n
    });

    // Stage 3 — + the nanosecond -> DateTime<Utc> conversion the reader does
    // for every row.
    let time = stage("3 + ts -> DateTime<Utc>", rows, || {
        let mut mask = None;
        let (mut n, mut acc) = (0u64, 0i64);
        for path in &files {
            for batch in open(path, &mut mask) {
                let batch = batch.expect("batch");
                let ts = batch.column_by_name("window_start").unwrap();
                let ts = ts.as_any().downcast_ref::<TimestampNanosecondArray>().unwrap();
                for i in 0..batch.num_rows() {
                    if let Some(dt) = ts_to_datetime(ts.value(i)) {
                        acc = acc.wrapping_add(dt.timestamp());
                    }
                    n += 1;
                }
            }
        }
        black_box(acc);
        n
    });

    // Stage 4 — the engine's actual reader: decode + order check + queue +
    // per-tick Vec, everything subscribed.
    let mut all = SubscriptionMask::new();
    for id in 0..=u16::MAX {
        all.insert(backtester::Symbol::from_ticker_id(id));
    }
    let mut ticks = 0u64;
    let tick = stage("4 TickReader drain", rows, || {
        let mut mask = None;
        let (mut n, mut t) = (0u64, 0u64);
        for path in &files {
            let mut reader = TickReader::new(path, &all, &mut mask).expect("tick reader");
            while let Some((_, bars)) = reader.next_tick().expect("tick") {
                n += bars.len() as u64;
                t += 1;
                black_box(&bars);
            }
        }
        ticks = t;
        n
    });

    println!("\n{ticks} ticks, {rows} rows");
    println!("\ncumulative deltas:");
    println!("  io                       {io:>8.2}s");
    println!("  parquet decode           {:>8.2}s", decode);
    println!("  column reads             {:>8.2}s", cols - decode);
    println!("  ts -> DateTime           {:>8.2}s", time - decode);
    println!("  tick grouping + queue    {:>8.2}s", tick - time);
    println!("  reader total (stage 4)   {tick:>8.2}s");
    println!("\nrun no_op_baseline over the same months for the engine total.");
}

#!/usr/bin/env rust-script
//! Resample the minute-bar Parquet dataset into a daily-bar dataset with the same layout.
//!
//! Requires: cargo install rust-script
//! Run:
//!   export BACKTEST_DATA_DIR=/path/to/minute-dataset
//!   rust-script scripts/resample_daily.rs --output /path/to/daily-dataset
//!
//! Output layout: <output>/year=<YYYY>/month=<M>/part-0.parquet, same schema as the minute
//! dataset, one row per ticker per trading day. The output is itself a canonical dataset root:
//! point `BACKTEST_DATA_DIR` at it and every strategy runs unchanged, one `on_data` per day. It
//! gets a copy of `encoded_tickers.json` (the daily rows keep the minute ids) and a `metadata`
//! symlink to the input's `metadata/`, so splits, dividends and renames still apply. The output
//! must not sit inside the input root (or vice versa): the engine discovers Parquet files
//! recursively, so the two datasets would interleave.
//!
//! The resampling *is* a backtest, one per month run in parallel: every symbol gets a
//! `ConsolidatorPeriod::Daily` consolidator, so reading, ordering checks and aggregation are the
//! engine's own, and the bars are exactly what a strategy consolidating the minute data would
//! see after applying the NYSE regular-session calendar (stamped at US Eastern midnight, with
//! official early closes). Volume is written as UInt32 (the engine's column type); a daily sum
//! above `u32::MAX` saturates and is counted in the summary.
//!
//! ```cargo
//! [dependencies]
//! backtester = { path = "../backtester" }
//! arrow = "56"
//! parquet = { version = "56", features = ["arrow", "zstd"] }
//! chrono = "0.4"
//! rayon = "1"
//! ```

use std::{
    cell::RefCell,
    env, fs,
    path::{Path, PathBuf},
    rc::Rc,
    sync::Arc,
    time::Instant,
};

use arrow::{
    array::{Float64Array, TimestampNanosecondArray, UInt16Array, UInt32Array},
    datatypes::{DataType, Field, Schema, TimeUnit},
    record_batch::RecordBatch,
};
use backtester::{
    bar::Bar,
    consolidator::ConsolidatorPeriod,
    data::{file_year_month, sorted_parquet_files, TICKER_MAP_FILE},
    run_backtest_with_data_dir, Algorithm, Context, LogConfig, Slice, Symbol,
};
use chrono::{Datelike, Days, Months, NaiveDate};
use parquet::{
    arrow::ArrowWriter,
    basic::{Compression, ZstdLevel},
    file::properties::WriterProperties,
};
use rayon::prelude::*;

const MAX_THREADS: usize = 8;

type Collected = Rc<RefCell<Vec<(Symbol, Bar)>>>;

/// Subscribes every symbol over one month and collects its daily consolidated bars; never trades.
struct DailyCollector {
    first: NaiveDate,
    last: NaiveDate,
    out: Collected,
}

impl Algorithm for DailyCollector {
    fn initialize(&mut self, ctx: &mut Context) {
        ctx.set_log_config(LogConfig::none());
        // Months already run in parallel; more decode threads per run would only oversubscribe.
        ctx.set_read_threads(1);
        // Apply the engine's exchange calendar before the otherwise
        // session-agnostic daily consolidators see each minute.
        ctx.set_extended_market_hours(false);
        ctx.set_start_date(self.first.year(), self.first.month(), self.first.day());
        ctx.set_end_date(self.last.year(), self.last.month(), self.last.day());
        for symbol in ctx.dataset_symbols() {
            ctx.add_symbol(symbol);
            let out = self.out.clone();
            ctx.consolidate(symbol, ConsolidatorPeriod::Daily, move |bar| {
                out.borrow_mut().push((symbol, bar.clone()))
            });
        }
    }

    fn on_data(&mut self, _ctx: &mut Context, _data: &Slice) {}
}

fn usage() -> &'static str {
    "Usage:\n    export BACKTEST_DATA_DIR=/path/to/minute-dataset\n    resample_daily --output \
     <daily-dataset-dir>"
}

fn parse_output(args: impl IntoIterator<Item = String>) -> Result<Option<PathBuf>, String> {
    let mut output = None;
    let mut args = args.into_iter();
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--output" => {
                output = Some(PathBuf::from(args.next().ok_or("--output needs a value")?));
            }
            "--help" | "-h" => return Ok(None),
            other => return Err(format!("unknown argument {other:?}")),
        }
    }
    output.map(Some).ok_or_else(|| "--output is required".into())
}

fn main() {
    let input = env::var_os("BACKTEST_DATA_DIR").filter(|value| !value.is_empty());
    let result = match (parse_output(env::args().skip(1)), input) {
        (Ok(None), _) => {
            println!("{}", usage());
            return;
        }
        (Ok(Some(output)), Some(input)) => resample(Path::new(&input), &output),
        (Ok(Some(_)), None) => {
            Err("BACKTEST_DATA_DIR must point to the minute dataset root".into())
        }
        (Err(error), _) => Err(error),
    };
    if let Err(error) = result {
        eprintln!("error: {error}");
        eprintln!("{}", usage());
        std::process::exit(1);
    }
}

fn resample(input: &Path, output: &Path) -> Result<(), String> {
    let input = fs::canonicalize(input).map_err(|e| format!("input {}: {e}", input.display()))?;
    fs::create_dir_all(output).map_err(|e| format!("create {}: {e}", output.display()))?;
    let output =
        fs::canonicalize(output).map_err(|e| format!("output {}: {e}", output.display()))?;
    if output.starts_with(&input) || input.starts_with(&output) {
        return Err(format!(
            "{} and {} are nested; the engine discovers Parquet recursively, so the daily and \
             minute files would mix. Pick a sibling directory.",
            output.display(),
            input.display()
        ));
    }

    let total = Instant::now();
    publish_copy(&input.join(TICKER_MAP_FILE), &output.join(TICKER_MAP_FILE))?;
    link_metadata(&input, &output)?;

    let mut months: Vec<(u32, u32)> =
        sorted_parquet_files(&input).iter().filter_map(|path| file_year_month(path)).collect();
    months.dedup();

    // A daily bucket never spans two months, so each month is an independent backtest: its
    // consolidators flush at the month's end exactly as they would at the next month's first bar.
    // Past 8 concurrent months the runs only contend (measured on an 8-core laptop: 16 threads
    // gave the same wall time at twice the memory), so the pool is capped there.
    let threads = std::thread::available_parallelism().map_or(1, |n| n.get()).min(MAX_THREADS);
    let pool = rayon::ThreadPoolBuilder::new()
        .num_threads(threads)
        .build()
        .map_err(|e| format!("thread pool: {e}"))?;
    let results: Vec<Result<(usize, usize), String>> = pool.install(|| {
        months
            .par_iter()
            .map(|&(year, month)| {
                let t = Instant::now();
                let first =
                    NaiveDate::from_ymd_opt(year as i32, month, 1).expect("valid partition");
                let last = first + Months::new(1) - Days::new(1);
                let out: Collected = Rc::default();
                run_backtest_with_data_dir(
                    DailyCollector { first, last, out: out.clone() },
                    &input,
                )
                .map_err(|e| format!("{year}-{month:02}: {e}"))?;
                let mut bars = out.take();
                // Each symbol's day closes when its own next bar arrives (or at the month's end),
                // so symbols emit out of step with each other: restore time order.
                bars.sort_unstable_by_key(|(symbol, bar)| (bar.time, *symbol));
                let dir = output.join(format!("year={year}")).join(format!("month={month}"));
                let saturated =
                    write_month(&dir, &bars).map_err(|e| format!("{year}-{month:02}: {e}"))?;
                eprintln!(
                    "{year}-{month:02}: {} daily rows  ({:.1}s)",
                    bars.len(),
                    t.elapsed().as_secs_f32()
                );
                Ok((bars.len(), saturated))
            })
            .collect()
    });

    let (mut rows, mut saturated) = (0, 0);
    let mut errors = Vec::new();
    for result in results {
        match result {
            Ok((r, s)) => (rows, saturated) = (rows + r, saturated + s),
            Err(error) => errors.push(error),
        }
    }
    if !errors.is_empty() {
        return Err(errors.join("\n"));
    }
    eprintln!(
        "\n{} month(s), {rows} daily rows, {saturated} volume(s) saturated at u32::MAX  total: \
         {:.1}s",
        months.len(),
        total.elapsed().as_secs_f32()
    );
    Ok(())
}

/// Write one month to `<dir>/part-0.parquet` through a temporary file and atomic rename, so an
/// interrupted run can't leave a truncated file. Returns how many volumes saturated.
fn write_month(dir: &Path, bars: &[(Symbol, Bar)]) -> Result<usize, String> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("ticker", DataType::UInt16, false),
        Field::new(
            "window_start",
            DataType::Timestamp(TimeUnit::Nanosecond, Some("UTC".into())),
            false,
        ),
        Field::new("open", DataType::Float64, false),
        Field::new("high", DataType::Float64, false),
        Field::new("low", DataType::Float64, false),
        Field::new("close", DataType::Float64, false),
        Field::new("volume", DataType::UInt32, false),
    ]));
    let mut saturated = 0;
    let volumes = bars.iter().map(|(_, bar)| {
        u32::try_from(bar.volume).unwrap_or_else(|_| {
            saturated += 1;
            u32::MAX
        })
    });
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(UInt16Array::from_iter_values(bars.iter().map(|(s, _)| s.ticker_id()))),
            Arc::new(
                TimestampNanosecondArray::from_iter_values(bars.iter().map(|(_, bar)| {
                    bar.time.timestamp_nanos_opt().expect("daily stamp in i64 range")
                }))
                .with_timezone("UTC"),
            ),
            Arc::new(Float64Array::from_iter_values(bars.iter().map(|(_, bar)| bar.open))),
            Arc::new(Float64Array::from_iter_values(bars.iter().map(|(_, bar)| bar.high))),
            Arc::new(Float64Array::from_iter_values(bars.iter().map(|(_, bar)| bar.low))),
            Arc::new(Float64Array::from_iter_values(bars.iter().map(|(_, bar)| bar.close))),
            Arc::new(UInt32Array::from_iter_values(volumes)),
        ],
    )
    .expect("column lengths match schema");

    fs::create_dir_all(dir).map_err(|e| format!("create {}: {e}", dir.display()))?;
    let tmp = dir.join("part-0.parquet.tmp");
    let props = WriterProperties::builder()
        .set_compression(Compression::ZSTD(ZstdLevel::default()))
        .build();
    let write = || -> Result<(), parquet::errors::ParquetError> {
        let mut writer = ArrowWriter::try_new(fs::File::create(&tmp)?, schema, Some(props))?;
        writer.write(&batch)?;
        writer.close().map(drop)
    };
    if let Err(e) = write() {
        let _ = fs::remove_file(&tmp);
        return Err(format!("write {}: {e}", tmp.display()));
    }
    let dst = dir.join("part-0.parquet");
    fs::rename(&tmp, &dst).map_err(|e| format!("publish {}: {e}", dst.display()))?;
    Ok(saturated)
}

/// Copy `src` to `dst` through a temporary file and atomic rename.
fn publish_copy(src: &Path, dst: &Path) -> Result<(), String> {
    let tmp = dst.with_extension("json.tmp");
    fs::copy(src, &tmp).map_err(|e| format!("copy {}: {e}", src.display()))?;
    fs::rename(&tmp, dst).map_err(|e| format!("publish {}: {e}", dst.display()))
}

/// Point `<output>/metadata` at the input's `metadata/` so corporate actions and insider data
/// stay shared (and current) instead of duplicated. An existing `metadata` entry is left alone.
fn link_metadata(input: &Path, output: &Path) -> Result<(), String> {
    let src = input.join("metadata");
    let dst = output.join("metadata");
    if !src.is_dir() {
        eprintln!("note: {} does not exist; the daily dataset has no metadata", src.display());
        return Ok(());
    }
    if fs::symlink_metadata(&dst).is_ok() {
        eprintln!("note: keeping existing {}", dst.display());
        return Ok(());
    }
    // Sibling roots (`data/minute` + `data/daily`) get a relative link, so the pair can move or
    // be mounted elsewhere together.
    let target = match (input.parent(), input.file_name()) {
        (Some(parent), Some(name)) if output.parent() == Some(parent) => {
            Path::new("..").join(name).join("metadata")
        }
        _ => src.clone(),
    };
    std::os::unix::fs::symlink(&target, &dst)
        .map_err(|e| format!("link {} -> {}: {e}", dst.display(), target.display()))?;
    eprintln!("linked {} -> {}", dst.display(), target.display());
    Ok(())
}

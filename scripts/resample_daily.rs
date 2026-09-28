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
//! The resampling *is* a backtest: every symbol gets a `ConsolidatorPeriod::Daily`
//! consolidator, so reading, ordering checks and aggregation are the engine's own, and the
//! bars are exactly what a strategy consolidating the minute data would see (stamped at US
//! Eastern midnight, pre- and after-market included). Volume is written as UInt32 (the
//! engine's column type); a daily sum above `u32::MAX` saturates and is counted in the summary.
//!
//! ```cargo
//! [dependencies]
//! backtester = { path = "../backtester" }
//! arrow = "56"
//! parquet = { version = "56", features = ["arrow", "zstd"] }
//! chrono = "0.4"
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
    bar::Bar, consolidator::ConsolidatorPeriod, data::TICKER_MAP_FILE, run_backtest_with_data_dir,
    Algorithm, Context, LogConfig, Slice, Symbol,
};
use chrono::Datelike;
use parquet::{
    arrow::ArrowWriter,
    basic::{Compression, ZstdLevel},
    file::properties::WriterProperties,
};

type Collected = Rc<RefCell<Vec<(Symbol, Bar)>>>;

/// Subscribes every symbol and collects its daily consolidated bars; never trades.
struct DailyCollector {
    out: Collected,
}

impl Algorithm for DailyCollector {
    fn initialize(&mut self, ctx: &mut Context) {
        ctx.set_log_config(LogConfig::none());
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
    let out: Collected = Rc::default();
    run_backtest_with_data_dir(DailyCollector { out: out.clone() }, &input)
        .map_err(|e| format!("reading {}: {e}", input.display()))?;
    let mut bars = out.take();
    // Consolidators fire per symbol as each one's next day starts, so restore time order.
    bars.sort_unstable_by_key(|(symbol, bar)| (bar.time, *symbol));
    eprintln!("resampled {} daily bars  ({:.1}s)", bars.len(), total.elapsed().as_secs_f32());

    publish_copy(&input.join(TICKER_MAP_FILE), &output.join(TICKER_MAP_FILE))?;
    link_metadata(&input, &output)?;

    // ET midnight is 04:00 or 05:00 UTC, so the UTC date of a daily bar is its trading date.
    let month_of = |bar: &Bar| (bar.time.year(), bar.time.month());
    let mut saturated = 0;
    for month in bars.chunk_by(|(_, a), (_, b)| month_of(a) == month_of(b)) {
        let (year, month_num) = month_of(&month[0].1);
        let dir = output.join(format!("year={year}")).join(format!("month={month_num}"));
        saturated += write_month(&dir, month)?;
        eprintln!("{year}-{month_num:02}: {} daily rows", month.len());
    }

    eprintln!(
        "\n{} daily rows, {saturated} volume(s) saturated at u32::MAX  total: {:.1}s",
        bars.len(),
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

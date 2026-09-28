#!/usr/bin/env rust-script
//! Resample the minute-bar Parquet dataset into a daily-bar dataset with the same layout.
//!
//! Requires: cargo install rust-script
//! Run:
//!   export BACKTEST_DATA_DIR=/path/to/minute-dataset
//!   rust-script scripts/resample_daily.rs --output /path/to/daily-dataset [--session regular]
//!
//! Input layout:  $BACKTEST_DATA_DIR/year=<YYYY>/month=<M>/*.parquet   (the canonical minute dataset)
//! Output layout: <output>/year=<YYYY>/month=<M>/part-0.parquet        (same schema, one row per
//!                                                                      ticker per trading day)
//!
//! The output is itself a canonical dataset root: point `BACKTEST_DATA_DIR` at it and every
//! strategy runs unchanged, one `on_data` per day instead of one per minute. It gets a copy of
//! `encoded_tickers.json` (the ids are the minute dataset's, so `Symbol`s agree) and a
//! `metadata` symlink to the input's `metadata/`, so splits, dividends and renames still apply.
//! The output must not sit inside the input root (or vice versa): the engine discovers Parquet
//! files recursively, so the two datasets would interleave.
//!
//! A daily bar aggregates one ticker's minute bars within one US Eastern calendar day: open of
//! the first bar, max high, min low, close of the last bar, summed volume. It is stamped at US
//! Eastern midnight of that day — the same bucket and stamp `ConsolidatorPeriod::Daily` produces,
//! so a strategy sees the bars it would have consolidated from the minute data itself.
//! `--session all` (the default) keeps pre- and after-market bars, like the consolidator;
//! `--session regular` keeps only the 9:30–16:00 ET bars (`MarketSession::Main`), so the open and
//! close are the auction prints a daily chart shows. Volume is written as UInt32 (the engine's
//! column type); a daily sum above `u32::MAX` saturates and is counted in the summary.
//!
//! Each input file must be sorted on `window_start` (the engine's own requirement); a regression
//! aborts that month. Months are processed in parallel (rayon) and written to
//! part-0.parquet.tmp then renamed, so an interrupted run can't leave a truncated file behind.
//!
//! ```cargo
//! [dependencies]
//! arrow = "56"
//! parquet = { version = "56", features = ["arrow", "zstd"] }
//! chrono = "0.4"
//! chrono-tz = "0.9"
//! rayon = "1"
//! ```

use std::{
    env, fs,
    path::{Path, PathBuf},
    sync::Arc,
    time::Instant,
};

use arrow::{
    array::{Array, AsArray, Float64Array, TimestampNanosecondArray, UInt16Array, UInt32Array},
    datatypes::{
        DataType, Field, Float64Type, Schema, TimeUnit, TimestampNanosecondType, UInt16Type,
        UInt32Type,
    },
    record_batch::RecordBatch,
};
use chrono::{DateTime, NaiveDate, NaiveTime, TimeZone};
use chrono_tz::US::Eastern;
use parquet::{
    arrow::{arrow_reader::ParquetRecordBatchReaderBuilder, ArrowWriter, ProjectionMask},
    basic::{Compression, ZstdLevel},
    file::properties::WriterProperties,
};
use rayon::prelude::*;

const COLUMNS: [&str; 7] = ["ticker", "window_start", "open", "high", "low", "close", "volume"];
const READ_BATCH_SIZE: usize = 131_072;
const TICKER_IDS: usize = u16::MAX as usize + 1;

fn usage() -> &'static str {
    "Usage:\n    export BACKTEST_DATA_DIR=/path/to/minute-dataset\n    resample_daily --output \
     <daily-dataset-dir> [--session all|regular]"
}

#[derive(Clone, Copy, PartialEq)]
enum Session {
    /// Every bar of the US Eastern day (pre-market, main, after-market).
    All,
    /// Only 9:30–16:00 ET bars.
    Regular,
}

struct Args {
    input: PathBuf,
    output: PathBuf,
    session: Session,
}

fn parse_args_from(
    args: impl IntoIterator<Item = String>,
    input: Option<PathBuf>,
) -> Result<Option<Args>, String> {
    let mut output = None;
    let mut session = Session::All;
    let mut args = args.into_iter();
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--output" => {
                output = Some(PathBuf::from(args.next().ok_or("--output needs a value")?));
            }
            "--session" => {
                session = match args.next().as_deref() {
                    Some("all") => Session::All,
                    Some("regular") => Session::Regular,
                    other => {
                        return Err(format!("--session must be all or regular, got {other:?}"))
                    }
                };
            }
            "--help" | "-h" => return Ok(None),
            other => return Err(format!("unknown argument {other:?}")),
        }
    }
    let output = output.ok_or("--output is required")?;
    let input = input.ok_or("BACKTEST_DATA_DIR must point to the minute dataset root")?;
    Ok(Some(Args { input, output, session }))
}

fn main() {
    let input = env::var_os("BACKTEST_DATA_DIR").filter(|value| !value.is_empty()).map(Into::into);
    let result = match parse_args_from(env::args().skip(1), input) {
        Ok(Some(args)) => resample(&args),
        Ok(None) => {
            println!("{}", usage());
            return;
        }
        Err(error) => Err(error),
    };
    if let Err(error) = result {
        eprintln!("error: {error}");
        eprintln!("{}", usage());
        std::process::exit(1);
    }
}

/// Per-month counts, summed for the final report.
#[derive(Default)]
struct MonthStats {
    minute_rows: usize,
    daily_rows: usize,
    saturated_volumes: usize,
}

fn resample(args: &Args) -> Result<(), String> {
    let input = fs::canonicalize(&args.input)
        .map_err(|e| format!("input {}: {e}", args.input.display()))?;
    fs::create_dir_all(&args.output)
        .map_err(|e| format!("create {}: {e}", args.output.display()))?;
    let output = fs::canonicalize(&args.output)
        .map_err(|e| format!("output {}: {e}", args.output.display()))?;
    if output.starts_with(&input) || input.starts_with(&output) {
        return Err(format!(
            "{} and {} are nested; the engine discovers Parquet recursively, so the daily and \
             minute files would mix. Pick a sibling directory.",
            output.display(),
            input.display()
        ));
    }

    let months = discover_months(&input)?;
    if months.is_empty() {
        return Err(format!("no year=YYYY/month=M/*.parquet files under {}", input.display()));
    }

    let total = Instant::now();
    publish_copy(&input.join("encoded_tickers.json"), &output.join("encoded_tickers.json"))?;
    link_metadata(&input, &output)?;

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
    let props = WriterProperties::builder()
        .set_compression(Compression::ZSTD(ZstdLevel::default()))
        .build();

    let results: Vec<Result<MonthStats, String>> = months
        .par_iter()
        .map(|(year, month, files)| {
            let t = Instant::now();
            let out_dir = output.join(format!("year={year}")).join(format!("month={month}"));
            fs::create_dir_all(&out_dir)
                .map_err(|e| format!("create {}: {e}", out_dir.display()))?;
            let tmp = out_dir.join("part-0.parquet.tmp");
            let dst = out_dir.join("part-0.parquet");

            match resample_month(files, args.session, &tmp, &schema, props.clone()) {
                Ok(stats) => {
                    fs::rename(&tmp, &dst)
                        .map_err(|e| format!("publish {}: {e}", dst.display()))?;
                    eprintln!(
                        "{year}-{month:02}: {} minute rows -> {} daily rows  ({:.1}s)",
                        stats.minute_rows,
                        stats.daily_rows,
                        t.elapsed().as_secs_f32()
                    );
                    Ok(stats)
                }
                Err(error) => {
                    let _ = fs::remove_file(&tmp);
                    Err(format!("{year}-{month:02}: {error}"))
                }
            }
        })
        .collect();

    let mut sum = MonthStats::default();
    let mut errors = Vec::new();
    for result in results {
        match result {
            Ok(stats) => {
                sum.minute_rows += stats.minute_rows;
                sum.daily_rows += stats.daily_rows;
                sum.saturated_volumes += stats.saturated_volumes;
            }
            Err(error) => errors.push(error),
        }
    }
    if !errors.is_empty() {
        return Err(errors.join("\n"));
    }
    eprintln!(
        "\n{} month(s), {} minute rows -> {} daily rows, {} volume(s) saturated at u32::MAX  \
         total: {:.1}s",
        months.len(),
        sum.minute_rows,
        sum.daily_rows,
        sum.saturated_volumes,
        total.elapsed().as_secs_f32()
    );
    Ok(())
}

/// Discover the canonical `year=YYYY/month=M/*.parquet` partitions, files sorted within a month.
fn discover_months(input: &Path) -> Result<Vec<(i32, u32, Vec<PathBuf>)>, String> {
    fn partitions<T: std::str::FromStr>(
        dir: &Path,
        prefix: &str,
    ) -> Result<Vec<(T, PathBuf)>, String> {
        let entries = fs::read_dir(dir).map_err(|e| format!("read {}: {e}", dir.display()))?;
        Ok(entries
            .filter_map(|e| e.ok())
            .filter(|e| e.path().is_dir())
            .filter_map(|e| {
                let value = e.file_name().to_str()?.strip_prefix(prefix)?.parse().ok()?;
                Some((value, e.path()))
            })
            .collect())
    }

    let mut months = Vec::new();
    for (year, year_dir) in partitions::<i32>(input, "year=")? {
        for (month, month_dir) in partitions::<u32>(&year_dir, "month=")? {
            if !(1..=12).contains(&month) {
                return Err(format!("invalid month directory {}", month_dir.display()));
            }
            let mut files: Vec<PathBuf> = fs::read_dir(&month_dir)
                .map_err(|e| format!("read {}: {e}", month_dir.display()))?
                .filter_map(|e| e.ok())
                .map(|e| e.path())
                .filter(|p| p.extension().is_some_and(|ext| ext == "parquet"))
                .collect();
            files.sort();
            if !files.is_empty() {
                months.push((year, month, files));
            }
        }
    }
    months.sort_by_key(|(year, month, _)| (*year, *month));
    Ok(months)
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

/// The UTC instant (epoch ns) of `time` US Eastern on `date`. Callers only ask for times away
/// from the 2 AM DST switch, so the local time is never skipped or repeated.
fn et_instant_ns(date: NaiveDate, time: NaiveTime) -> i64 {
    Eastern
        .from_local_datetime(&date.and_time(time))
        .single()
        .expect("time away from the DST switch is unambiguous")
        .timestamp_nanos_opt()
        .expect("timestamp in i64 range")
}

/// The US Eastern day containing the current row, as UTC ns bounds. Rows arrive time-sorted,
/// so the time-zone conversion runs once per day rather than once per row.
struct DayWindow {
    /// ET midnight: the day's start and the daily bar's `window_start`.
    start: i64,
    /// Next ET midnight (exclusive).
    end: i64,
    /// 9:30 ET (inclusive) .. 16:00 ET (exclusive).
    regular: std::ops::Range<i64>,
}

impl DayWindow {
    fn containing(ts: i64) -> Self {
        let date = DateTime::from_timestamp_nanos(ts).with_timezone(&Eastern).date_naive();
        let at = |h, m| et_instant_ns(date, NaiveTime::from_hms_opt(h, m, 0).unwrap());
        let next = date.succ_opt().expect("date in range");
        Self {
            start: at(0, 0),
            end: et_instant_ns(next, NaiveTime::MIN),
            regular: at(9, 30)..at(16, 0),
        }
    }
}

#[derive(Clone, Copy, Default)]
struct Agg {
    open: f64,
    high: f64,
    low: f64,
    close: f64,
    volume: u64,
}

/// One day's aggregates, indexed densely by ticker id; `touched` lists the ids that have a bar.
struct DayAgg {
    aggs: Vec<Agg>,
    seen: Vec<bool>,
    touched: Vec<u16>,
}

impl DayAgg {
    fn new() -> Self {
        Self {
            aggs: vec![Agg::default(); TICKER_IDS],
            seen: vec![false; TICKER_IDS],
            touched: vec![],
        }
    }

    fn add(&mut self, ticker: u16, open: f64, high: f64, low: f64, close: f64, volume: u32) {
        let i = ticker as usize;
        let agg = &mut self.aggs[i];
        if !self.seen[i] {
            self.seen[i] = true;
            self.touched.push(ticker);
            *agg = Agg { open, high, low, close, volume: volume as u64 };
        } else {
            agg.high = agg.high.max(high);
            agg.low = agg.low.min(low);
            agg.close = close;
            agg.volume += volume as u64;
        }
    }

    /// Append the day's bars, stamped `start`, to `out` in ticker order and reset for reuse.
    fn flush_into(&mut self, start: i64, out: &mut DailyColumns) {
        self.touched.sort_unstable();
        for &ticker in &self.touched {
            let i = ticker as usize;
            let agg = self.aggs[i];
            self.seen[i] = false;
            out.tickers.push(ticker);
            out.timestamps.push(start);
            out.opens.push(agg.open);
            out.highs.push(agg.high);
            out.lows.push(agg.low);
            out.closes.push(agg.close);
            out.volumes.push(u32::try_from(agg.volume).unwrap_or_else(|_| {
                out.saturated += 1;
                u32::MAX
            }));
        }
        self.touched.clear();
    }
}

/// A month of daily bars, already in (window_start, ticker) order.
#[derive(Default)]
struct DailyColumns {
    tickers: Vec<u16>,
    timestamps: Vec<i64>,
    opens: Vec<f64>,
    highs: Vec<f64>,
    lows: Vec<f64>,
    closes: Vec<f64>,
    volumes: Vec<u32>,
    saturated: usize,
}

fn resample_month(
    files: &[PathBuf],
    session: Session,
    tmp: &Path,
    schema: &Arc<Schema>,
    props: WriterProperties,
) -> Result<MonthStats, String> {
    let mut day = DayAgg::new();
    let mut out = DailyColumns::default();
    // `end = MIN` makes the first row open a real window without flushing anything.
    let mut window = DayWindow { start: i64::MIN, end: i64::MIN, regular: 0..0 };
    let mut prev_ts = i64::MIN;
    let mut minute_rows = 0usize;

    for path in files {
        let err = |e: &dyn std::fmt::Display| format!("{}: {e}", path.display());
        let file = fs::File::open(path).map_err(|e| err(&e))?;
        let builder = ParquetRecordBatchReaderBuilder::try_new(file).map_err(|e| err(&e))?;
        let mask = ProjectionMask::columns(builder.parquet_schema(), COLUMNS);
        let reader = builder
            .with_projection(mask)
            .with_batch_size(READ_BATCH_SIZE)
            .build()
            .map_err(|e| err(&e))?;

        for batch in reader {
            let batch = batch.map_err(|e| err(&e))?;
            let column = |name: &str| {
                batch.column_by_name(name).ok_or_else(|| err(&format!("missing column {name}")))
            };
            let type_err = |name: &str| err(&format!("column {name} has an unexpected type"));
            let tickers = column("ticker")?
                .as_primitive_opt::<UInt16Type>()
                .ok_or_else(|| type_err("ticker"))?;
            let times = column("window_start")?
                .as_primitive_opt::<TimestampNanosecondType>()
                .ok_or_else(|| type_err("window_start"))?;
            let price = |name: &str| {
                column(name)?.as_primitive_opt::<Float64Type>().ok_or_else(|| type_err(name))
            };
            let (opens, highs, lows, closes) =
                (price("open")?, price("high")?, price("low")?, price("close")?);
            let volumes = column("volume")?
                .as_primitive_opt::<UInt32Type>()
                .ok_or_else(|| type_err("volume"))?;
            if tickers.null_count() + times.null_count() > 0 {
                return Err(err(&"null ticker or window_start"));
            }

            minute_rows += batch.num_rows();
            for i in 0..batch.num_rows() {
                let ts = times.value(i);
                if ts < prev_ts {
                    return Err(err(&format!("window_start {ts} < previous row's {prev_ts}")));
                }
                prev_ts = ts;
                if ts >= window.end {
                    day.flush_into(window.start, &mut out);
                    window = DayWindow::containing(ts);
                }
                if session == Session::Regular && !window.regular.contains(&ts) {
                    continue;
                }
                day.add(
                    tickers.value(i),
                    opens.value(i),
                    highs.value(i),
                    lows.value(i),
                    closes.value(i),
                    volumes.value(i),
                );
            }
        }
    }
    day.flush_into(window.start, &mut out);

    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(UInt16Array::from(std::mem::take(&mut out.tickers))),
            Arc::new(
                TimestampNanosecondArray::from(std::mem::take(&mut out.timestamps))
                    .with_timezone("UTC"),
            ),
            Arc::new(Float64Array::from(std::mem::take(&mut out.opens))),
            Arc::new(Float64Array::from(std::mem::take(&mut out.highs))),
            Arc::new(Float64Array::from(std::mem::take(&mut out.lows))),
            Arc::new(Float64Array::from(std::mem::take(&mut out.closes))),
            Arc::new(UInt32Array::from(std::mem::take(&mut out.volumes))),
        ],
    )
    .expect("column lengths match schema");
    let daily_rows = batch.num_rows();

    let file = fs::File::create(tmp).map_err(|e| format!("create {}: {e}", tmp.display()))?;
    let mut writer = ArrowWriter::try_new(file, schema.clone(), Some(props))
        .map_err(|e| format!("open {}: {e}", tmp.display()))?;
    writer.write(&batch).map_err(|e| format!("write {}: {e}", tmp.display()))?;
    writer.close().map_err(|e| format!("close {}: {e}", tmp.display()))?;

    Ok(MonthStats { minute_rows, daily_rows, saturated_volumes: out.saturated })
}

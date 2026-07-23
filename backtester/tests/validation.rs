//! Date-range validation and data-order validation: an inverted range fails
//! fast, a range the data doesn't cover still runs, and time running backwards
//! — within a file or across month partitions — fails the run.

use std::fs;
use std::path::Path;
use std::sync::{Arc, Mutex};

use arrow::array::{Float64Array, TimestampNanosecondArray, UInt16Array, UInt32Array};
use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use arrow::record_batch::RecordBatch;
use chrono::{DateTime, NaiveDate, TimeZone, Utc};
use parquet::arrow::ArrowWriter;

use backtester::{run_backtest, Algorithm, BacktestError, Context, Slice};

const FIXTURE: &str = concat!(env!("CARGO_MANIFEST_DIR"), "/tests/fixtures");

/// Records the timestamp of every slice it sees; optionally configures a date
/// range.
struct RecordTimes {
    symbol: &'static str,
    start: Option<(i32, u32, u32)>,
    end: Option<(i32, u32, u32)>,
    times: Arc<Mutex<Vec<DateTime<Utc>>>>,
}

impl RecordTimes {
    fn new(
        symbol: &'static str,
        start: Option<(i32, u32, u32)>,
        end: Option<(i32, u32, u32)>,
    ) -> Self {
        Self { symbol, start, end, times: Arc::new(Mutex::new(Vec::new())) }
    }
}

impl Algorithm for RecordTimes {
    fn initialize(&mut self, ctx: &mut Context) {
        ctx.set_cash(100_000.0);
        ctx.add_equity(self.symbol);
        if let Some((y, m, d)) = self.start {
            ctx.set_start_date(y, m, d);
        }
        if let Some((y, m, d)) = self.end {
            ctx.set_end_date(y, m, d);
        }
    }

    fn on_data(&mut self, _ctx: &mut Context, data: &Slice) {
        self.times.lock().unwrap().push(data.time);
    }
}

#[test]
fn inverted_date_range_is_an_error() {
    let algo = RecordTimes::new("AAPL", Some((2023, 2, 1)), Some((2023, 1, 1)));
    let err = run_backtest(algo, FIXTURE).unwrap_err();
    assert!(
        matches!(err, BacktestError::InvalidDateRange { .. }),
        "expected InvalidDateRange, got: {err}"
    );
}

#[test]
fn date_range_beyond_the_data_warns_but_still_runs() {
    // The committed fixture covers Jan 2023 only; a range spilling out on both
    // sides must not fail — the backtest runs over the overlap.
    let algo = RecordTimes::new("AAPL", Some((2022, 6, 1)), Some((2023, 12, 31)));
    let times = algo.times.clone();
    run_backtest(algo, FIXTURE).unwrap();
    assert!(!times.lock().unwrap().is_empty(), "no bars processed over the overlap");
}

/// Write one part file of one-price minute bars for ticker id 1 ("XYZ") into
/// a `year=Y/month=M` partition. Each row is `(day, minute, price)`, with the
/// bar's date taken as given — nothing stops a row being dated outside its
/// partition's month, which is exactly what the out-of-order test needs. Bars
/// sit at 15:00 UTC + minute (11:00 ET), so UTC and ET dates agree.
fn write_part(root: &Path, year: i32, month: u32, part: &str, rows: &[(NaiveDate, u32, f64)]) {
    let schema = Arc::new(Schema::new(vec![
        Field::new("ticker", DataType::UInt16, false),
        Field::new("volume", DataType::UInt32, false),
        Field::new("open", DataType::Float64, false),
        Field::new("close", DataType::Float64, false),
        Field::new("high", DataType::Float64, false),
        Field::new("low", DataType::Float64, false),
        Field::new("window_start", DataType::Timestamp(TimeUnit::Nanosecond, None), false),
    ]));

    let ts: Vec<i64> = rows
        .iter()
        .map(|&(date, minute, _)| {
            let t = date.and_hms_opt(15, 0, 0).unwrap() + chrono::Duration::minutes(minute as i64);
            Utc.from_utc_datetime(&t).timestamp_nanos_opt().unwrap()
        })
        .collect();
    let prices: Vec<f64> = rows.iter().map(|&(_, _, p)| p).collect();
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(UInt16Array::from(vec![1u16; rows.len()])),
            Arc::new(UInt32Array::from(vec![100u32; rows.len()])),
            Arc::new(Float64Array::from(prices.clone())),
            Arc::new(Float64Array::from(prices.clone())),
            Arc::new(Float64Array::from(prices.clone())),
            Arc::new(Float64Array::from(prices)),
            Arc::new(TimestampNanosecondArray::from(ts)),
        ],
    )
    .unwrap();

    let dir = root.join(format!("minute/year={year}/month={month}"));
    fs::create_dir_all(&dir).unwrap();
    let file = fs::File::create(dir.join(part)).unwrap();
    let mut writer = ArrowWriter::try_new(file, schema, None).unwrap();
    writer.write(&batch).unwrap();
    writer.close().unwrap();
}

#[test]
fn out_of_order_bars_across_files_are_an_error() {
    let tmp = tempfile::tempdir().unwrap();
    let root = tmp.path();
    fs::write(root.join("encoded_tickers.json"), r#"{"1": "XYZ"}"#).unwrap();

    let jun1 = NaiveDate::from_ymd_opt(2023, 6, 1).unwrap();
    let jul3 = NaiveDate::from_ymd_opt(2023, 7, 3).unwrap();

    // June's file ends at 15:02. July's partition holds a legitimate July bar
    // in part-0 and — in part-1, which sorts after it — a rogue bar dated back
    // in June, before the stream's high-water mark. Processing it would run
    // time backwards mid-backtest, so the run must fail instead.
    write_part(
        root,
        2023,
        6,
        "part-0.parquet",
        &[(jun1, 0, 10.0), (jun1, 1, 11.0), (jun1, 2, 12.0)],
    );
    write_part(root, 2023, 7, "part-0.parquet", &[(jul3, 0, 13.0)]);
    write_part(root, 2023, 7, "part-1.parquet", &[(jun1, 1, 99.0)]);

    let algo = RecordTimes::new("XYZ", None, None);
    let err = run_backtest(algo, root.to_str().unwrap()).unwrap_err();
    assert!(
        matches!(err, BacktestError::OutOfOrderData { .. }),
        "expected OutOfOrderData, got: {err}"
    );
}

#[test]
fn in_order_bars_across_files_run_clean() {
    // The mirror of the out-of-order test: the same layout minus the rogue bar
    // must stream every bar through on_data in order, with no error.
    let tmp = tempfile::tempdir().unwrap();
    let root = tmp.path();
    fs::write(root.join("encoded_tickers.json"), r#"{"1": "XYZ"}"#).unwrap();

    let jun1 = NaiveDate::from_ymd_opt(2023, 6, 1).unwrap();
    let jul3 = NaiveDate::from_ymd_opt(2023, 7, 3).unwrap();

    write_part(
        root,
        2023,
        6,
        "part-0.parquet",
        &[(jun1, 0, 10.0), (jun1, 1, 11.0), (jun1, 2, 12.0)],
    );
    write_part(root, 2023, 7, "part-0.parquet", &[(jul3, 0, 13.0)]);

    let algo = RecordTimes::new("XYZ", None, None);
    let times = algo.times.clone();
    run_backtest(algo, root.to_str().unwrap()).unwrap();

    let times = times.lock().unwrap();
    assert_eq!(times.len(), 4, "expected every bar to reach on_data: {times:?}");
    assert!(times.windows(2).all(|w| w[0] < w[1]), "bars arrived out of order: {times:?}");
}

#[test]
fn unsorted_bars_within_a_file_are_an_error() {
    let tmp = tempfile::tempdir().unwrap();
    let root = tmp.path();
    fs::write(root.join("encoded_tickers.json"), r#"{"1": "XYZ"}"#).unwrap();

    // The engine streams rows in file order without re-sorting, so a file
    // whose timestamps regress (15:02 then back to 15:01) is rejected.
    let jun1 = NaiveDate::from_ymd_opt(2023, 6, 1).unwrap();
    write_part(
        root,
        2023,
        6,
        "part-0.parquet",
        &[(jun1, 0, 10.0), (jun1, 2, 12.0), (jun1, 1, 11.0)],
    );

    let algo = RecordTimes::new("XYZ", None, None);
    let err = run_backtest(algo, root.to_str().unwrap()).unwrap_err();
    assert!(
        matches!(err, BacktestError::OutOfOrderData { .. }),
        "expected OutOfOrderData, got: {err}"
    );
}

#!/usr/bin/env rust-script
//! Convert minute-bar CSV.gz files to Hive-partitioned Parquet using Arrow/Parquet directly.
//!
//! Requires: cargo install rust-script
//! Run: rust-script scripts/ingest_arrow.rs <input_dir> <output_dir> <encoded_tickers.json>
//!
//! If <encoded_tickers.json> does not exist it is bootstrapped first: all input
//! files are scanned for distinct tickers, which are sorted and assigned
//! sequential u16 ids, and the map is written to that path. Pass an existing
//! map to keep ids consistent with previously written parquet.
//!
//! Input layout:  <input>/<YYYY>/<MM>/<YYYY-MM-DD>.csv.gz  (one file per trading day,
//!                all tickers, rows grouped by ticker — NOT globally time-sorted)
//! Output layout: <output>/year=<YYYY>/month=<M>/part-0.parquet
//!
//! Columns: ticker (UInt16), window_start (TimestampNs UTC), open, high, low, close (Float64), volume (UInt32)
//!
//! Each day is sorted by (window_start, ticker) independently and appended to the
//! month's parquet writer in date order. Consecutive trading days are disjoint in
//! time (after-market ends 20:00 ET, next pre-market opens 4:00 ET), so the
//! concatenation is globally time-sorted — the engine's hard requirement — while
//! peak memory stays at ~one day per worker thread instead of a whole month.
//! The day-boundary invariant is checked, not assumed: a violation aborts that
//! month instead of silently writing an unsorted file. Months are processed in
//! parallel (rayon); each month is written to part-0.parquet.tmp and renamed on
//! success so an interrupted run can't leave a truncated file behind.
//!
//! ```cargo
//! [dependencies]
//! arrow = "56"
//! parquet = { version = "56", features = ["arrow", "zstd"] }
//! flate2 = "1"
//! csv = "1"
//! rayon = "1"
//! serde = { version = "1", features = ["derive"] }
//! serde_json = "1"
//! ```

use std::{
    collections::{HashMap, HashSet},
    env, fs,
    io::BufReader,
    path::PathBuf,
    sync::Arc,
    time::Instant,
};

use arrow::{
    array::{Float64Array, TimestampNanosecondArray, UInt16Array, UInt32Array},
    datatypes::{DataType, Field, Schema, TimeUnit},
    record_batch::RecordBatch,
};
use flate2::read::GzDecoder;
use parquet::{
    arrow::ArrowWriter,
    basic::{Compression, ZstdLevel},
    file::properties::WriterProperties,
};
use rayon::prelude::*;
use serde::Deserialize;

#[derive(Deserialize)]
struct CsvRow {
    ticker: String,
    window_start: i64,
    open: f64,
    high: f64,
    low: f64,
    close: f64,
    volume: f64, // some providers write volume as float; cast to u32 on write
}

/// Columnar buffer for one day's rows, reused across days.
#[derive(Default)]
struct DayColumns {
    tickers: Vec<u16>,
    timestamps: Vec<i64>,
    opens: Vec<f64>,
    highs: Vec<f64>,
    lows: Vec<f64>,
    closes: Vec<f64>,
    volumes: Vec<u32>,
}

impl DayColumns {
    fn clear(&mut self) {
        self.tickers.clear();
        self.timestamps.clear();
        self.opens.clear();
        self.highs.clear();
        self.lows.clear();
        self.closes.clear();
        self.volumes.clear();
    }
}

fn main() {
    let args: Vec<String> = env::args().collect();
    if args.len() != 4 {
        eprintln!("Usage: ingest_arrow <input_dir> <output_dir> <encoded_tickers.json>");
        std::process::exit(1);
    }
    let input = PathBuf::from(&args[1]);
    let output = PathBuf::from(&args[2]);

    fs::create_dir_all(&output).expect("create output dir");

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

    let total = Instant::now();

    // (year, month, day files sorted by name = by date)
    let mut months: Vec<(i32, i32, Vec<PathBuf>)> = Vec::new();

    let mut year_dirs: Vec<_> = fs::read_dir(&input)
        .expect("read input dir")
        .filter_map(|e| e.ok())
        .filter(|e| e.path().is_dir()) // follows symlinks, unlike DirEntry::file_type()
        .filter(|e| e.file_name().to_string_lossy().parse::<i32>().is_ok())
        .collect();
    year_dirs.sort_by_key(|e| e.file_name());

    for year_entry in year_dirs {
        let year: i32 = year_entry.file_name().to_string_lossy().parse().unwrap();

        let mut month_dirs: Vec<_> = fs::read_dir(year_entry.path())
            .expect("read year dir")
            .filter_map(|e| e.ok())
            .filter(|e| e.path().is_dir()) // follows symlinks, unlike DirEntry::file_type()
            .filter(|e| e.file_name().to_string_lossy().parse::<i32>().is_ok())
            .collect();
        month_dirs.sort_by_key(|e| e.file_name());

        for month_entry in month_dirs {
            let month: i32 = month_entry.file_name().to_string_lossy().parse().unwrap();

            let mut day_files: Vec<PathBuf> = fs::read_dir(month_entry.path())
                .expect("read month dir")
                .filter_map(|e| e.ok())
                .map(|e| e.path())
                .filter(|p| p.to_string_lossy().ends_with(".csv.gz"))
                .collect();
            day_files.sort();

            if !day_files.is_empty() {
                months.push((year, month, day_files));
            }
        }
    }

    let tickers_path = PathBuf::from(&args[3]);
    if !tickers_path.exists() {
        eprintln!("{} not found — scanning input for distinct tickers...", tickers_path.display());
        let t = Instant::now();
        bootstrap_ticker_map(&tickers_path, &months);
        eprintln!("  → wrote {}  ({:.1}s)", tickers_path.display(), t.elapsed().as_secs_f32());
    }

    // encoded_tickers.json is {"0": "AAPL", "1": "MSFT", ...} — reverse to sym → id
    let json = fs::read_to_string(&tickers_path).expect("read encoded_tickers.json");
    let id_to_sym: HashMap<String, String> = serde_json::from_str(&json).unwrap();
    let sym_to_id: HashMap<String, u16> =
        id_to_sym.into_iter().map(|(id, sym)| (sym, id.parse::<u16>().unwrap())).collect();

    let results: Vec<(usize, usize)> = months
        .par_iter()
        .map(|(year, month, day_files)| {
            let t = Instant::now();
            let out_dir = output.join(format!("year={year}")).join(format!("month={month}"));
            fs::create_dir_all(&out_dir).expect("create partition dir");
            let tmp = out_dir.join("part-0.parquet.tmp");
            let dst = out_dir.join("part-0.parquet");

            match write_month(&tmp, &schema, props.clone(), &sym_to_id, day_files) {
                Ok((rows, day_errors)) => {
                    fs::rename(&tmp, &dst).expect("rename tmp parquet into place");
                    eprintln!(
                        "{year}-{month:02}: {} day(s), {rows} rows, {day_errors} error(s)  ({:.1}s)",
                        day_files.len(),
                        t.elapsed().as_secs_f32()
                    );
                    (rows, day_errors)
                }
                Err(e) => {
                    let _ = fs::remove_file(&tmp);
                    eprintln!("{year}-{month:02}: FAILED — {e}");
                    (0, 1)
                }
            }
        })
        .collect();

    let rows: usize = results.iter().map(|r| r.0).sum();
    let errors: usize = results.iter().map(|r| r.1).sum();
    let ok_months = results.iter().filter(|r| r.0 > 0).count();
    eprintln!(
        "\n{ok_months}/{} month(s), {rows} rows, {errors} error(s)  total: {:.1}s",
        months.len(),
        total.elapsed().as_secs_f32()
    );
    if errors > 0 {
        std::process::exit(1);
    }
}

/// Scan every input file for distinct tickers, assign sequential ids in sorted
/// ticker order, and write the {"id": "SYM"} map (same shape and id assignment
/// as the retired DuckDB ingest, which used ROW_NUMBER() OVER (ORDER BY ticker)).
fn bootstrap_ticker_map(tickers_path: &PathBuf, months: &[(i32, i32, Vec<PathBuf>)]) {
    let all_days: Vec<&PathBuf> = months.iter().flat_map(|(_, _, days)| days).collect();
    let distinct: HashSet<String> = all_days
        .par_iter()
        .map(|src| {
            let gz = GzDecoder::new(BufReader::new(fs::File::open(src).expect("open csv")));
            let mut rdr = csv::Reader::from_reader(gz);
            let ticker_idx = rdr
                .headers()
                .expect("csv headers")
                .iter()
                .position(|h| h == "ticker")
                .expect("ticker column");
            let mut set = HashSet::new();
            let mut last = String::new();
            let mut rec = csv::StringRecord::new();
            // Rows are grouped by ticker, so skipping consecutive repeats keeps
            // this to one set insert per ticker instead of one per row.
            while rdr.read_record(&mut rec).expect("csv record") {
                let t = &rec[ticker_idx];
                if t != last {
                    last = t.to_string();
                    set.insert(last.clone());
                }
            }
            set
        })
        .reduce(HashSet::new, |mut a, b| {
            a.extend(b);
            a
        });

    let mut sorted: Vec<String> = distinct.into_iter().collect();
    sorted.sort_unstable();
    assert!(sorted.len() <= u16::MAX as usize + 1, "too many tickers for u16 ids");

    let map: HashMap<String, String> =
        sorted.into_iter().enumerate().map(|(id, sym)| (id.to_string(), sym)).collect();
    fs::write(tickers_path, serde_json::to_string(&map).unwrap())
        .expect("write encoded_tickers.json");
    eprintln!("  {} distinct ticker(s)", map.len());
}

/// Write one month: each day file is read, sorted by (window_start, ticker), and
/// appended in date order. Returns (rows written, per-day read errors).
fn write_month(
    tmp: &PathBuf,
    schema: &Arc<Schema>,
    props: WriterProperties,
    sym_to_id: &HashMap<String, u16>,
    day_files: &[PathBuf],
) -> Result<(usize, usize), Box<dyn std::error::Error>> {
    let out_file = fs::File::create(tmp)?;
    let mut writer = ArrowWriter::try_new(out_file, schema.clone(), Some(props))?;

    let mut cols = DayColumns::default();
    let mut rows = 0usize;
    let mut day_errors = 0usize;
    let mut prev_day_last_ts = i64::MIN;

    for src in day_files {
        cols.clear();
        if let Err(e) = read_csv_into(src, sym_to_id, &mut cols) {
            // Keep whatever parsed before the error — it still sorts within the day.
            eprintln!("  ERROR reading {}: {e}", src.display());
            day_errors += 1;
        }
        if cols.tickers.is_empty() {
            continue;
        }

        let batch = sorted_day_batch(schema, &cols);

        // Days must be disjoint in time or the output file is not globally sorted.
        let ts = batch.column(1).as_any().downcast_ref::<TimestampNanosecondArray>().unwrap();
        let first = ts.value(0);
        if first < prev_day_last_ts {
            return Err(format!(
                "day boundary violated at {}: first ts {first} < previous day's last ts \
                 {prev_day_last_ts}",
                src.display()
            )
            .into());
        }
        prev_day_last_ts = ts.value(ts.len() - 1);

        writer.write(&batch)?;
        rows += batch.num_rows();
    }

    writer.close()?;
    Ok((rows, day_errors))
}

fn read_csv_into(
    src: &PathBuf,
    sym_to_id: &HashMap<String, u16>,
    cols: &mut DayColumns,
) -> Result<(), Box<dyn std::error::Error>> {
    let gz = GzDecoder::new(BufReader::new(fs::File::open(src)?));
    let mut rdr = csv::Reader::from_reader(gz);
    for result in rdr.deserialize() {
        let row: CsvRow = result?;
        let Some(&id) = sym_to_id.get(&row.ticker) else { continue };
        cols.tickers.push(id);
        cols.timestamps.push(row.window_start);
        cols.opens.push(row.open);
        cols.highs.push(row.high);
        cols.lows.push(row.low);
        cols.closes.push(row.close);
        cols.volumes.push(row.volume as u32);
    }
    Ok(())
}

fn sorted_day_batch(schema: &Arc<Schema>, cols: &DayColumns) -> RecordBatch {
    let mut idx: Vec<usize> = (0..cols.tickers.len()).collect();
    idx.sort_unstable_by_key(|&i| (cols.timestamps[i], cols.tickers[i]));

    RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(UInt16Array::from_iter_values(idx.iter().map(|&i| cols.tickers[i]))),
            Arc::new(
                TimestampNanosecondArray::from_iter_values(idx.iter().map(|&i| cols.timestamps[i]))
                    .with_timezone("UTC"),
            ),
            Arc::new(Float64Array::from_iter_values(idx.iter().map(|&i| cols.opens[i]))),
            Arc::new(Float64Array::from_iter_values(idx.iter().map(|&i| cols.highs[i]))),
            Arc::new(Float64Array::from_iter_values(idx.iter().map(|&i| cols.lows[i]))),
            Arc::new(Float64Array::from_iter_values(idx.iter().map(|&i| cols.closes[i]))),
            Arc::new(UInt32Array::from_iter_values(idx.iter().map(|&i| cols.volumes[i]))),
        ],
    )
    .expect("column lengths match schema")
}

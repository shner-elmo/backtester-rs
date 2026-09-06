#!/usr/bin/env rust-script
//! Convert minute-bar CSV.gz files to Hive-partitioned Parquet using Arrow/Parquet directly.
//!
//! Requires: cargo install rust-script
//! Run:
//!   export BACKTEST_DATA_DIR=/path/to/dataset
//!   rust-script scripts/ingest_arrow.rs --input <raw-minute-dir> [--extend-tickers]
//!
//! If `encoded_tickers.json` does not exist it is bootstrapped first: all input
//! files are scanned for distinct tickers, which are sorted and assigned
//! sequential u16 ids. Existing maps keep their ids; new tickers fail before
//! any Parquet is written unless `--extend-tickers` is passed, in which case
//! they are appended in sorted order. Ticker-map updates use a temporary file
//! and atomic rename.
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
//!
//! [dev-dependencies]
//! tempfile = "3"
//! ```

use std::{
    collections::{BTreeMap, HashMap, HashSet},
    env, fs,
    io::BufReader,
    path::{Path, PathBuf},
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

fn usage() -> &'static str {
    "Usage:\n    export BACKTEST_DATA_DIR=/path/to/dataset\n    ingest_arrow --input \
     <raw-minute-dir> [--extend-tickers]"
}

struct Args {
    input: PathBuf,
    output: PathBuf,
    extend_tickers: bool,
}

fn parse_args_from(
    args: impl IntoIterator<Item = String>,
    output: Option<PathBuf>,
) -> Result<Option<Args>, String> {
    let mut input = None;
    let mut extend_tickers = false;
    let mut args = args.into_iter();
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--input" => {
                input = Some(PathBuf::from(args.next().ok_or("--input needs a value")?));
            }
            "--extend-tickers" => extend_tickers = true,
            "--help" | "-h" => return Ok(None),
            other => return Err(format!("unknown argument {other:?}")),
        }
    }
    let input = input.ok_or("--input is required")?;
    let output = output.ok_or("BACKTEST_DATA_DIR must point to the canonical dataset root")?;
    Ok(Some(Args { input, output, extend_tickers }))
}

fn main() {
    let output = env::var_os("BACKTEST_DATA_DIR").filter(|value| !value.is_empty()).map(Into::into);
    let result = match parse_args_from(env::args().skip(1), output) {
        Ok(Some(args)) => ingest(&args.input, &args.output, args.extend_tickers),
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

fn ingest(input: &Path, output: &Path, extend_tickers: bool) -> Result<(), String> {
    fs::create_dir_all(output).map_err(|e| format!("create {}: {e}", output.display()))?;

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

    let months = discover_months(input)?;
    if months.is_empty() {
        return Err(format!("no .csv.gz files found under {}", input.display()));
    }

    let tickers_path = output.join("encoded_tickers.json");
    let input_tickers = scan_tickers(&months)?;
    let (id_to_sym, changed) = prepare_ticker_map(&tickers_path, input_tickers, extend_tickers)?;
    if changed {
        publish_ticker_map(&tickers_path, &id_to_sym)?;
    }
    let sym_to_id: HashMap<String, u16> =
        id_to_sym.into_iter().map(|(id, symbol)| (symbol, id)).collect();

    let results: Vec<Result<usize, String>> = months
        .par_iter()
        .map(|(year, month, day_files)| {
            let t = Instant::now();
            let out_dir = output.join(format!("year={year}")).join(format!("month={month}"));
            fs::create_dir_all(&out_dir)
                .map_err(|e| format!("create {}: {e}", out_dir.display()))?;
            let tmp = out_dir.join("part-0.parquet.tmp");
            let dst = out_dir.join("part-0.parquet");

            match write_month(&tmp, &schema, props.clone(), &sym_to_id, day_files) {
                Ok(rows) => {
                    fs::rename(&tmp, &dst)
                        .map_err(|e| format!("publish {}: {e}", dst.display()))?;
                    eprintln!(
                        "{year}-{month:02}: {} day(s), {rows} rows  ({:.1}s)",
                        day_files.len(),
                        t.elapsed().as_secs_f32()
                    );
                    Ok(rows)
                }
                Err(error) => {
                    let _ = fs::remove_file(&tmp);
                    Err(format!("{year}-{month:02}: {error}"))
                }
            }
        })
        .collect();

    let mut rows = 0usize;
    let mut errors = Vec::new();
    for result in results {
        match result {
            Ok(count) => rows += count,
            Err(error) => errors.push(error),
        }
    }
    if !errors.is_empty() {
        return Err(errors.join("\n"));
    }
    eprintln!(
        "\n{} month(s), {rows} rows  total: {:.1}s",
        months.len(),
        total.elapsed().as_secs_f32()
    );
    Ok(())
}

/// Discover raw `<YYYY>/<MM>/*.csv.gz` input months.
fn discover_months(input: &Path) -> Result<Vec<(i32, i32, Vec<PathBuf>)>, String> {
    let mut months = Vec::new();
    let mut year_dirs: Vec<_> = fs::read_dir(input)
        .map_err(|e| format!("read {}: {e}", input.display()))?
        .filter_map(|e| e.ok())
        .filter(|e| e.path().is_dir()) // follows symlinks, unlike DirEntry::file_type()
        .filter(|e| e.file_name().to_string_lossy().parse::<i32>().is_ok())
        .collect();
    year_dirs.sort_by_key(|e| e.file_name());

    for year_entry in year_dirs {
        let year: i32 = year_entry.file_name().to_string_lossy().parse().unwrap();

        let mut month_dirs: Vec<_> = fs::read_dir(year_entry.path())
            .map_err(|e| format!("read {}: {e}", year_entry.path().display()))?
            .filter_map(|e| e.ok())
            .filter(|e| e.path().is_dir()) // follows symlinks, unlike DirEntry::file_type()
            .filter(|e| e.file_name().to_string_lossy().parse::<i32>().is_ok())
            .collect();
        month_dirs.sort_by_key(|e| e.file_name());

        for month_entry in month_dirs {
            let month: i32 = month_entry.file_name().to_string_lossy().parse().unwrap();
            if !(1..=12).contains(&month) {
                return Err(format!("invalid month directory {}", month_entry.path().display()));
            }

            let mut day_files: Vec<PathBuf> = fs::read_dir(month_entry.path())
                .map_err(|e| format!("read {}: {e}", month_entry.path().display()))?
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

    Ok(months)
}

fn scan_tickers(months: &[(i32, i32, Vec<PathBuf>)]) -> Result<HashSet<String>, String> {
    let all_days: Vec<&PathBuf> = months.iter().flat_map(|(_, _, days)| days).collect();
    all_days
        .par_iter()
        .try_fold(HashSet::new, |mut distinct, src| {
            let file = fs::File::open(src).map_err(|e| format!("open {}: {e}", src.display()))?;
            let gz = GzDecoder::new(BufReader::new(file));
            let mut rdr = csv::Reader::from_reader(gz);
            let ticker_idx = rdr
                .headers()
                .map_err(|e| format!("{}: {e}", src.display()))?
                .iter()
                .position(|h| h == "ticker")
                .ok_or_else(|| format!("{}: missing ticker column", src.display()))?;
            let mut last = String::new();
            let mut rec = csv::StringRecord::new();
            // Rows are grouped by ticker, so skipping consecutive repeats keeps
            // this to one set insert per ticker instead of one per row.
            while rdr.read_record(&mut rec).map_err(|e| format!("{}: {e}", src.display()))? {
                let t = &rec[ticker_idx];
                if t != last {
                    last = t.to_string();
                    distinct.insert(last.clone());
                }
            }
            Ok(distinct)
        })
        .try_reduce(HashSet::new, |mut distinct, set| {
            distinct.extend(set);
            Ok(distinct)
        })
}

fn prepare_ticker_map(
    path: &Path,
    input_tickers: HashSet<String>,
    extend: bool,
) -> Result<(BTreeMap<u16, String>, bool), String> {
    let mut map: BTreeMap<u16, String> = if path.exists() {
        let json = fs::read_to_string(path).map_err(|e| format!("read {}: {e}", path.display()))?;
        let raw: BTreeMap<String, String> =
            serde_json::from_str(&json).map_err(|e| format!("parse {}: {e}", path.display()))?;
        raw.into_iter()
            .map(|(id, symbol)| {
                id.parse::<u16>()
                    .map(|id| (id, symbol))
                    .map_err(|_| format!("{}: ticker id {id:?} is not a u16", path.display()))
            })
            .collect::<Result<_, _>>()?
    } else {
        BTreeMap::new()
    };
    let existing: HashSet<String> = map.values().cloned().collect();
    if existing.len() != map.len() {
        return Err(format!("{} contains duplicate ticker names", path.display()));
    }
    let mut unknown: Vec<String> =
        input_tickers.into_iter().filter(|ticker| !existing.contains(ticker)).collect();
    unknown.sort_unstable();
    if path.exists() && !unknown.is_empty() && !extend {
        return Err(format!(
            "input contains ticker(s) absent from {}: {}; rerun with --extend-tickers",
            path.display(),
            unknown.join(", ")
        ));
    }
    let changed = !path.exists() || !unknown.is_empty();
    let next = map.keys().next_back().map_or(0usize, |id| *id as usize + 1);
    if next + unknown.len() > u16::MAX as usize + 1 {
        return Err("too many tickers for u16 ids".into());
    }
    for (offset, symbol) in unknown.into_iter().enumerate() {
        map.insert((next + offset) as u16, symbol);
    }
    Ok((map, changed))
}

fn publish_ticker_map(path: &Path, map: &BTreeMap<u16, String>) -> Result<(), String> {
    let tmp = path.with_file_name("encoded_tickers.json.tmp");
    let json = serde_json::to_string_pretty(map).map_err(|e| e.to_string())? + "\n";
    fs::write(&tmp, json).map_err(|e| format!("write {}: {e}", tmp.display()))?;
    fs::rename(&tmp, path).map_err(|e| format!("publish {}: {e}", path.display()))
}

/// Write one month: each day file is read, sorted by (window_start, ticker), and
/// appended in date order. Any input error aborts the month; rows are never
/// silently dropped or published from a partially read day.
fn write_month(
    tmp: &PathBuf,
    schema: &Arc<Schema>,
    props: WriterProperties,
    sym_to_id: &HashMap<String, u16>,
    day_files: &[PathBuf],
) -> Result<usize, String> {
    let out_file = fs::File::create(tmp).map_err(|e| format!("create {}: {e}", tmp.display()))?;
    let mut writer = ArrowWriter::try_new(out_file, schema.clone(), Some(props))
        .map_err(|e| format!("open {}: {e}", tmp.display()))?;

    let mut cols = DayColumns::default();
    let mut rows = 0usize;
    let mut prev_day_last_ts = i64::MIN;

    for src in day_files {
        cols.clear();
        read_csv_into(src, sym_to_id, &mut cols)?;
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
            ));
        }
        prev_day_last_ts = ts.value(ts.len() - 1);

        writer.write(&batch).map_err(|e| format!("write {}: {e}", tmp.display()))?;
        rows += batch.num_rows();
    }

    writer.close().map_err(|e| format!("close {}: {e}", tmp.display()))?;
    Ok(rows)
}

fn read_csv_into(
    src: &PathBuf,
    sym_to_id: &HashMap<String, u16>,
    cols: &mut DayColumns,
) -> Result<(), String> {
    let file = fs::File::open(src).map_err(|e| format!("open {}: {e}", src.display()))?;
    let gz = GzDecoder::new(BufReader::new(file));
    let mut rdr = csv::Reader::from_reader(gz);
    for result in rdr.deserialize() {
        let row: CsvRow = result.map_err(|e| format!("{}: {e}", src.display()))?;
        let &id = sym_to_id.get(&row.ticker).ok_or_else(|| {
            format!(
                "{}: ticker {:?} is missing from encoded_tickers.json",
                src.display(),
                row.ticker
            )
        })?;
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

#[cfg(test)]
mod tests {
    use std::io::Write;

    use flate2::{write::GzEncoder, Compression as GzCompression};
    use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

    use super::*;

    const HEADER: &str = "ticker,window_start,open,high,low,close,volume\n";

    fn write_day(input: &Path, name: &str, rows: &str) {
        let dir = input.join("2023/01");
        fs::create_dir_all(&dir).unwrap();
        let file = fs::File::create(dir.join(name)).unwrap();
        let mut gz = GzEncoder::new(file, GzCompression::default());
        gz.write_all(HEADER.as_bytes()).unwrap();
        gz.write_all(rows.as_bytes()).unwrap();
        gz.finish().unwrap();
    }

    fn ticker_map(output: &Path) -> BTreeMap<u16, String> {
        serde_json::from_str(&fs::read_to_string(output.join("encoded_tickers.json")).unwrap())
            .unwrap()
    }

    fn output_rows(output: &Path) -> Vec<(u16, i64)> {
        let file = fs::File::open(output.join("year=2023/month=1/part-0.parquet")).unwrap();
        let reader = ParquetRecordBatchReaderBuilder::try_new(file).unwrap().build().unwrap();
        let mut rows = Vec::new();
        for batch in reader {
            let batch = batch.unwrap();
            let tickers = batch
                .column_by_name("ticker")
                .unwrap()
                .as_any()
                .downcast_ref::<UInt16Array>()
                .unwrap();
            let timestamps = batch
                .column_by_name("window_start")
                .unwrap()
                .as_any()
                .downcast_ref::<TimestampNanosecondArray>()
                .unwrap();
            rows.extend((0..batch.num_rows()).map(|i| (tickers.value(i), timestamps.value(i))));
        }
        rows
    }

    #[test]
    fn initial_ingestion_is_stable_and_generates_the_canonical_layout() {
        let raw = tempfile::tempdir().unwrap();
        let dataset = tempfile::tempdir().unwrap();
        write_day(
            raw.path(),
            "2023-01-03.csv.gz",
            "ZZZ,2,20,20,20,20,200\nAAA,1,10,10,10,10,100\n",
        );

        ingest(raw.path(), dataset.path(), false).unwrap();
        let map = ticker_map(dataset.path());
        assert_eq!(map, BTreeMap::from([(0, "AAA".into()), (1, "ZZZ".into())]));
        assert!(dataset.path().join("year=2023/month=1/part-0.parquet").is_file());
        assert!(!dataset.path().join("encoded_tickers.json.tmp").exists());
        let rows = output_rows(dataset.path());
        assert_eq!(rows, vec![(0, 1), (1, 2)]);
        assert!(rows.iter().all(|(id, _)| map.contains_key(id)));

        let original_map = fs::read(dataset.path().join("encoded_tickers.json")).unwrap();
        ingest(raw.path(), dataset.path(), false).unwrap();
        assert_eq!(fs::read(dataset.path().join("encoded_tickers.json")).unwrap(), original_map);
        assert_eq!(output_rows(dataset.path()), rows);
    }

    #[test]
    fn unknown_tickers_are_rejected_before_partitions_are_written() {
        let raw = tempfile::tempdir().unwrap();
        let dataset = tempfile::tempdir().unwrap();
        write_day(raw.path(), "2023-01-03.csv.gz", "BBB,1,10,10,10,10,100\n");
        let map_path = dataset.path().join("encoded_tickers.json");
        fs::write(&map_path, "{\"7\":\"AAA\"}\n").unwrap();

        let err = ingest(raw.path(), dataset.path(), false).unwrap_err();
        assert!(err.contains("BBB") && err.contains("--extend-tickers"), "{err}");
        assert!(!dataset.path().join("year=2023").exists());
        assert_eq!(fs::read_to_string(map_path).unwrap(), "{\"7\":\"AAA\"}\n");
    }

    #[test]
    fn extension_preserves_existing_ids_and_appends_new_tickers_in_sorted_order() {
        let raw = tempfile::tempdir().unwrap();
        let dataset = tempfile::tempdir().unwrap();
        write_day(
            raw.path(),
            "2023-01-03.csv.gz",
            "ZZZ,1,10,10,10,10,100\nBBB,1,10,10,10,10,100\nAAA,1,10,10,10,10,100\n",
        );
        fs::write(dataset.path().join("encoded_tickers.json"), "{\"7\":\"ZZZ\"}\n").unwrap();

        ingest(raw.path(), dataset.path(), true).unwrap();
        let map = ticker_map(dataset.path());
        assert_eq!(map, BTreeMap::from([(7, "ZZZ".into()), (8, "AAA".into()), (9, "BBB".into())]));
        assert!(output_rows(dataset.path()).iter().all(|(id, _)| map.contains_key(id)));
    }

    #[test]
    fn malformed_rows_abort_instead_of_publishing_partial_parquet() {
        let raw = tempfile::tempdir().unwrap();
        let dataset = tempfile::tempdir().unwrap();
        write_day(
            raw.path(),
            "2023-01-03.csv.gz",
            "AAA,1,10,10,10,10,100\nAAA,not-a-timestamp,10,10,10,10,100\n",
        );

        let err = ingest(raw.path(), dataset.path(), false).unwrap_err();
        assert!(err.contains("not-a-timestamp") || err.contains("invalid digit"), "{err}");
        assert!(!dataset.path().join("year=2023/month=1/part-0.parquet").exists());
    }

    #[test]
    fn cli_requires_input_and_uses_the_configured_output() {
        let args = parse_args_from(
            ["--input".into(), "/raw/minute".into(), "--extend-tickers".into()],
            Some(PathBuf::from("/dataset")),
        )
        .unwrap()
        .unwrap();
        assert_eq!(args.input, Path::new("/raw/minute"));
        assert_eq!(args.output, Path::new("/dataset"));
        assert!(args.extend_tickers);
        assert!(parse_args_from(Vec::<String>::new(), Some("/dataset".into())).is_err());
        assert!(parse_args_from(["--input".into(), "/raw".into()], None).is_err());
        assert!(parse_args_from(["--help".into()], None).unwrap().is_none());
    }
}

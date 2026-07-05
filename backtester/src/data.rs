use std::{
    collections::HashMap,
    fs::{read_to_string, File},
    path::PathBuf,
};

use arrow::array::{Float64Array, TimestampNanosecondArray, UInt16Array, UInt32Array, UInt8Array};
use chrono::{TimeZone, Utc};
use parquet::arrow::{arrow_reader::ParquetRecordBatchReaderBuilder, ProjectionMask};
use walkdir::WalkDir;

use crate::bar::{Bar, MarketSession};

pub const COLUMNS: [&str; 8] =
    ["ticker", "volume", "open", "high", "low", "close", "window_start", "market_session"];

pub fn load_ticker_map(data_root: &str) -> HashMap<u16, String> {
    let path = format!("{}/encoded_tickers.json", data_root);
    let content = read_to_string(&path).unwrap_or_else(|_| panic!("could not read {}", path));
    let temp: HashMap<String, String> = serde_json::from_str(&content).unwrap();
    temp.into_iter().map(|(k, v)| (k.parse::<u16>().unwrap(), v)).collect()
}

/// Stock splits per symbol: execution date → ratio (`split_to / split_from`,
/// so 3.0 for a 1→3 forward split, 0.1 for a 10→1 reverse split). Reads the
/// Polygon-format `get_splits.json` next to `encoded_tickers.json`; returns an
/// empty map when the file doesn't exist (e.g. the test fixture). Only splits
/// for `symbols` are kept.
pub fn load_splits(
    data_root: &str,
    symbols: &std::collections::HashSet<String>,
) -> HashMap<String, std::collections::BTreeMap<chrono::NaiveDate, f64>> {
    #[derive(serde::Deserialize)]
    struct SplitRecord {
        execution_date: String,
        ticker: String,
        split_from: f64,
        split_to: f64,
    }

    let path = format!("{}/get_splits.json", data_root);
    let Ok(content) = read_to_string(&path) else {
        return HashMap::new();
    };
    let records: Vec<SplitRecord> = serde_json::from_str(&content)
        .unwrap_or_else(|e| panic!("{path} is not valid splits JSON: {e}"));

    let mut splits: HashMap<String, std::collections::BTreeMap<chrono::NaiveDate, f64>> =
        HashMap::new();
    for r in records {
        if !symbols.contains(&r.ticker) || r.split_from <= 0.0 || r.split_to <= 0.0 {
            continue;
        }
        let Ok(date) = r.execution_date.parse::<chrono::NaiveDate>() else {
            continue;
        };
        // Same-day duplicate records multiply together (defensive; not seen
        // in practice).
        let day_ratio = splits.entry(r.ticker).or_default().entry(date).or_insert(1.0);
        *day_ratio *= r.split_to / r.split_from;
    }
    splits
}

/// Parse a directory name that is either a bare number (`2023`) or a Hive
/// partition (`year=2023`), returning the numeric part.
fn dir_number(component: &std::ffi::OsStr) -> Option<u32> {
    let s = component.to_string_lossy();
    let value = s.rsplit('=').next().unwrap_or(&s);
    value.parse().ok()
}

/// The (year, month) a data file belongs to, derived from its
/// `year=YYYY/month=M` (or bare `YYYY/M`) parent directories.
pub fn file_year_month(path: &std::path::Path) -> Option<(u32, u32)> {
    let month_dir = path.parent()?;
    let year_dir = month_dir.parent()?;
    Some((dir_number(year_dir.file_name()?)?, dir_number(month_dir.file_name()?)?))
}

pub fn sorted_parquet_files(data_root: &str) -> Vec<PathBuf> {
    let mut files: Vec<PathBuf> = WalkDir::new(data_root)
        .into_iter()
        .filter_map(|e| e.ok())
        .filter(|e| e.path().extension().is_some_and(|ext| ext == "parquet"))
        .map(|e| e.path().to_path_buf())
        .collect();

    files.sort_by_key(|x| file_year_month(x).unwrap_or((0, 0)));

    files
}

pub fn iter_bars(
    data_root: &str,
    ticker_map: &HashMap<u16, String>,
    mut cb: impl FnMut(String, Bar),
) {
    let files = sorted_parquet_files(data_root);
    let mut mask: Option<ProjectionMask> = None;

    for file_path in &files {
        read_bars_from_file(file_path, ticker_map, &mut mask, &mut cb);
    }
}

/// Read one Parquet file, invoking `cb` for every decodable bar. `mask` caches
/// the projection across files sharing a schema (pass the same `Option` for
/// each file of a dataset).
pub fn read_bars_from_file(
    file_path: &std::path::Path,
    ticker_map: &HashMap<u16, String>,
    mask: &mut Option<ProjectionMask>,
    mut cb: impl FnMut(String, Bar),
) {
    let file = File::open(file_path).unwrap();
    let builder = ParquetRecordBatchReaderBuilder::try_new(file).unwrap();
    let mask = mask
        .get_or_insert_with(|| ProjectionMask::columns(builder.parquet_schema(), COLUMNS))
        .clone();
    let reader = builder.with_projection(mask).build().unwrap();

    for batch in reader {
        let batch = batch.unwrap();

        // Look up columns by name: `ProjectionMask::columns` returns columns
        // in the file's schema order, which differs from `COLUMNS` order, so
        // positional indexing would silently scramble high/low/close.
        let f64_col = |name: &str| {
            batch
                .column_by_name(name)
                .and_then(|c| c.as_any().downcast_ref::<Float64Array>())
                .unwrap_or_else(|| panic!("missing f64 column {name}"))
        };
        let ticker_col = batch
            .column_by_name("ticker")
            .and_then(|c| c.as_any().downcast_ref::<UInt16Array>())
            .expect("missing column ticker");
        let volume_col = batch
            .column_by_name("volume")
            .and_then(|c| c.as_any().downcast_ref::<UInt32Array>())
            .expect("missing column volume");
        let open_col = f64_col("open");
        let high_col = f64_col("high");
        let low_col = f64_col("low");
        let close_col = f64_col("close");
        let ts_col = batch
            .column_by_name("window_start")
            .and_then(|c| c.as_any().downcast_ref::<TimestampNanosecondArray>())
            .expect("missing column window_start");
        let sess_col = batch
            .column_by_name("market_session")
            .and_then(|c| c.as_any().downcast_ref::<UInt8Array>())
            .expect("missing column market_session");

        for i in 0..batch.num_rows() {
            let ticker_id = ticker_col.value(i);
            let symbol = match ticker_map.get(&ticker_id) {
                Some(s) => s.clone(),
                None => continue,
            };

            let ts_ns = ts_col.value(i);
            let secs = ts_ns / 1_000_000_000;
            let nanos = (ts_ns % 1_000_000_000) as u32;
            let time = Utc.timestamp_opt(secs, nanos).single().unwrap();

            let session = match sess_col.value(i) {
                1 => MarketSession::PreMarket,
                2 => MarketSession::Main,
                3 => MarketSession::AfterMarket,
                _ => continue,
            };

            cb(
                symbol,
                Bar {
                    time,
                    open: open_col.value(i),
                    high: high_col.value(i),
                    low: low_col.value(i),
                    close: close_col.value(i),
                    volume: volume_col.value(i) as u64,
                    market_session: session,
                },
            );
        }
    }
}

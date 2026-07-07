use std::{
    collections::HashMap,
    fs::{read_to_string, File},
    path::{Path, PathBuf},
};

use arrow::{
    array::{Float64Array, TimestampNanosecondArray, UInt16Array, UInt32Array, UInt8Array},
    record_batch::RecordBatch,
};
use chrono::{TimeZone, Utc};
use parquet::arrow::{arrow_reader::ParquetRecordBatchReaderBuilder, ProjectionMask};
use walkdir::WalkDir;

use crate::{
    bar::{Bar, MarketSession},
    error::BacktestError,
};

pub const COLUMNS: [&str; 8] =
    ["ticker", "volume", "open", "high", "low", "close", "window_start", "market_session"];

/// Metadata files the loaders expect inside the data root, next to the
/// Parquet tree. Only `TICKER_MAP_FILE` is required; the rest are optional.
pub const TICKER_MAP_FILE: &str = "encoded_tickers.json";
pub const SPLITS_FILE: &str = "get_splits.json";
pub const DIVIDENDS_FILE: &str = "get_dividends.json";
pub const RENAMES_FILE: &str = "ticker_renames.json";

pub fn load_ticker_map(data_root: &str) -> Result<HashMap<u16, String>, BacktestError> {
    let path = PathBuf::from(format!("{data_root}/{TICKER_MAP_FILE}"));
    let content =
        read_to_string(&path).map_err(|source| BacktestError::Io { path: path.clone(), source })?;
    let temp: HashMap<String, String> = serde_json::from_str(&content)
        .map_err(|e| BacktestError::Json { path: path.clone(), message: e.to_string() })?;
    temp.into_iter()
        .map(|(k, v)| match k.parse::<u16>() {
            Ok(id) => Ok((id, v)),
            Err(_) => Err(BacktestError::InvalidTickerKey { path: path.clone(), key: k }),
        })
        .collect()
}

/// Stock splits per symbol: execution date → ratio (`split_to / split_from`,
/// so 3.0 for a 1→3 forward split, 0.1 for a 10→1 reverse split). Reads the
/// Polygon-format `get_splits.json` next to `encoded_tickers.json`; returns an
/// empty map when the file doesn't exist (e.g. the test fixture). Only splits
/// for `symbols` are kept.
pub fn load_splits(
    data_root: &str,
    symbols: &std::collections::HashSet<String>,
) -> Result<HashMap<String, std::collections::BTreeMap<chrono::NaiveDate, f64>>, BacktestError> {
    #[derive(serde::Deserialize)]
    struct SplitRecord {
        execution_date: String,
        ticker: String,
        split_from: f64,
        split_to: f64,
    }

    let path = PathBuf::from(format!("{data_root}/{SPLITS_FILE}"));
    let content = match read_to_string(&path) {
        Ok(c) => c,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(HashMap::new()),
        Err(source) => return Err(BacktestError::Io { path, source }),
    };
    let records: Vec<SplitRecord> = serde_json::from_str(&content).map_err(|e| {
        BacktestError::Json { path, message: format!("not valid splits JSON: {e}") }
    })?;

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
    Ok(splits)
}

/// Cash dividends per symbol: ex-dividend date → cash amount per share. Reads
/// the Polygon-format `get_dividends.json` next to `encoded_tickers.json`;
/// returns an empty map when the file doesn't exist (e.g. the test fixture).
/// Only dividends for `symbols` are kept.
///
/// The real dividends file is large (hundreds of MB across the whole market),
/// so this streams the JSON array one record at a time and keeps only the
/// matches, rather than deserializing the entire file into a `Vec` first.
pub fn load_dividends(
    data_root: &str,
    symbols: &std::collections::HashSet<String>,
) -> Result<HashMap<String, std::collections::BTreeMap<chrono::NaiveDate, f64>>, BacktestError> {
    use std::fmt;

    use serde::de::{Deserializer, SeqAccess, Visitor};

    #[derive(serde::Deserialize)]
    struct DividendRecord {
        ex_dividend_date: String,
        ticker: String,
        cash_amount: f64,
    }

    type DividendMap = HashMap<String, std::collections::BTreeMap<chrono::NaiveDate, f64>>;

    let path = PathBuf::from(format!("{data_root}/{DIVIDENDS_FILE}"));
    let file = match File::open(&path) {
        Ok(f) => f,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(HashMap::new()),
        Err(source) => return Err(BacktestError::Io { path, source }),
    };

    // A seq visitor that filters records as they stream in, so peak memory is
    // the matched dividends, not the whole file.
    struct FilterVisitor<'a> {
        symbols: &'a std::collections::HashSet<String>,
    }
    impl<'de> Visitor<'de> for FilterVisitor<'_> {
        type Value = DividendMap;

        fn expecting(&self, f: &mut fmt::Formatter) -> fmt::Result {
            f.write_str("an array of dividend records")
        }

        fn visit_seq<A: SeqAccess<'de>>(self, mut seq: A) -> Result<DividendMap, A::Error> {
            let mut divs: DividendMap = HashMap::new();
            while let Some(r) = seq.next_element::<DividendRecord>()? {
                if !self.symbols.contains(&r.ticker) || r.cash_amount == 0.0 {
                    continue;
                }
                let Ok(date) = r.ex_dividend_date.parse::<chrono::NaiveDate>() else {
                    continue;
                };
                // Same-day duplicate records sum together (defensive).
                *divs.entry(r.ticker).or_default().entry(date).or_insert(0.0) += r.cash_amount;
            }
            Ok(divs)
        }
    }

    let reader = std::io::BufReader::new(file);
    let mut de = serde_json::Deserializer::from_reader(reader);
    de.deserialize_seq(FilterVisitor { symbols }).map_err(|e| BacktestError::Json {
        path,
        message: format!("not valid dividends JSON: {e}"),
    })
}

/// Ticker renames keyed by effective date → list of `(old, new)` symbol pairs.
/// Reads `ticker_renames.json` next to `encoded_tickers.json` — a JSON array of
/// `{"date": "YYYY-MM-DD", "old": "FB", "new": "META"}` records; returns an
/// empty map when the file doesn't exist. Only renames whose `old` symbol is
/// subscribed are kept.
pub fn load_renames(
    data_root: &str,
    symbols: &std::collections::HashSet<String>,
) -> Result<std::collections::BTreeMap<chrono::NaiveDate, Vec<(String, String)>>, BacktestError> {
    #[derive(serde::Deserialize)]
    struct RenameRecord {
        date: String,
        old: String,
        new: String,
    }

    let path = PathBuf::from(format!("{data_root}/{RENAMES_FILE}"));
    let content = match read_to_string(&path) {
        Ok(c) => c,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
            return Ok(std::collections::BTreeMap::new())
        }
        Err(source) => return Err(BacktestError::Io { path, source }),
    };
    let records: Vec<RenameRecord> = serde_json::from_str(&content).map_err(|e| {
        BacktestError::Json { path, message: format!("not valid renames JSON: {e}") }
    })?;

    let mut renames: std::collections::BTreeMap<chrono::NaiveDate, Vec<(String, String)>> =
        std::collections::BTreeMap::new();
    for r in records {
        if !symbols.contains(&r.old) {
            continue;
        }
        let Ok(date) = r.date.parse::<chrono::NaiveDate>() else {
            continue;
        };
        renames.entry(date).or_default().push((r.old, r.new));
    }
    Ok(renames)
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

    // Path as tiebreaker so multiple part files inside one month keep a
    // deterministic order.
    files.sort_by(|a, b| {
        file_year_month(a)
            .unwrap_or((0, 0))
            .cmp(&file_year_month(b).unwrap_or((0, 0)))
            .then_with(|| a.cmp(b))
    });

    files
}

pub fn iter_bars(
    data_root: &str,
    ticker_map: &HashMap<u16, String>,
    mut cb: impl FnMut(String, Bar),
) -> Result<(), BacktestError> {
    let files = sorted_parquet_files(data_root);
    let mut mask: Option<ProjectionMask> = None;

    for file_path in &files {
        read_bars_from_file(file_path, ticker_map, &mut mask, &mut cb)?;
    }
    Ok(())
}

/// Look up a column by name and downcast it, erroring on absence or a type
/// mismatch. By-name lookup matters: `ProjectionMask::columns` returns columns
/// in the file's schema order, which differs from `COLUMNS` order, so
/// positional indexing would silently scramble high/low/close.
fn column<'a, T: 'static>(
    batch: &'a RecordBatch,
    name: &'static str,
    path: &Path,
) -> Result<&'a T, BacktestError> {
    batch
        .column_by_name(name)
        .and_then(|c| c.as_any().downcast_ref::<T>())
        .ok_or_else(|| BacktestError::MissingColumn { path: path.to_path_buf(), column: name })
}

/// Read one Parquet file, invoking `cb` for every decodable bar. `mask` caches
/// the projection across files sharing a schema (pass the same `Option` for
/// each file of a dataset).
pub fn read_bars_from_file(
    file_path: &std::path::Path,
    ticker_map: &HashMap<u16, String>,
    mask: &mut Option<ProjectionMask>,
    mut cb: impl FnMut(String, Bar),
) -> Result<(), BacktestError> {
    let file = File::open(file_path)
        .map_err(|source| BacktestError::Io { path: file_path.to_path_buf(), source })?;
    let builder = ParquetRecordBatchReaderBuilder::try_new(file).map_err(|e| {
        BacktestError::Parquet { path: file_path.to_path_buf(), message: e.to_string() }
    })?;
    let mask = mask
        .get_or_insert_with(|| ProjectionMask::columns(builder.parquet_schema(), COLUMNS))
        .clone();
    let reader = builder.with_projection(mask).build().map_err(|e| BacktestError::Parquet {
        path: file_path.to_path_buf(),
        message: e.to_string(),
    })?;

    for batch in reader {
        let batch = batch.map_err(|e| BacktestError::Parquet {
            path: file_path.to_path_buf(),
            message: e.to_string(),
        })?;

        let ticker_col: &UInt16Array = column(&batch, "ticker", file_path)?;
        let volume_col: &UInt32Array = column(&batch, "volume", file_path)?;
        let open_col: &Float64Array = column(&batch, "open", file_path)?;
        let high_col: &Float64Array = column(&batch, "high", file_path)?;
        let low_col: &Float64Array = column(&batch, "low", file_path)?;
        let close_col: &Float64Array = column(&batch, "close", file_path)?;
        let ts_col: &TimestampNanosecondArray = column(&batch, "window_start", file_path)?;
        let sess_col: &UInt8Array = column(&batch, "market_session", file_path)?;

        for i in 0..batch.num_rows() {
            let ticker_id = ticker_col.value(i);
            let symbol = match ticker_map.get(&ticker_id) {
                Some(s) => s.clone(),
                None => continue,
            };

            let ts_ns = ts_col.value(i);
            let secs = ts_ns / 1_000_000_000;
            let nanos = (ts_ns % 1_000_000_000) as u32;
            let Some(time) = Utc.timestamp_opt(secs, nanos).single() else {
                continue;
            };

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
    Ok(())
}

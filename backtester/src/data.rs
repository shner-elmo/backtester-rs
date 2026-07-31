use std::{
    collections::{HashMap, VecDeque},
    fs::{read_to_string, File},
    path::{Path, PathBuf},
};

use arrow::{
    array::{Float64Array, TimestampNanosecondArray, UInt16Array, UInt32Array},
    record_batch::RecordBatch,
};
use chrono::{DateTime, TimeZone, Utc};
use parquet::arrow::{
    arrow_reader::{ParquetRecordBatchReader, ParquetRecordBatchReaderBuilder},
    ProjectionMask,
};
use walkdir::WalkDir;

use crate::{bar::Bar, error::BacktestError, symbol::Symbol};

pub const COLUMNS: [&str; 7] = ["ticker", "volume", "open", "high", "low", "close", "window_start"];

/// Metadata files the loaders expect inside the data root, next to the
/// Parquet tree. Only `TICKER_MAP_FILE` is required; the rest are optional.
pub const TICKER_MAP_FILE: &str = "encoded_tickers.json";
pub const SPLITS_FILE: &str = "get_splits.json";
pub const DIVIDENDS_FILE: &str = "get_dividends.json";
pub const RENAMES_FILE: &str = "ticker_renames.json";

pub fn load_ticker_map(data_root: &str) -> Result<HashMap<u16, String>, BacktestError> {
    load_ticker_map_from(&PathBuf::from(format!("{data_root}/{TICKER_MAP_FILE}")))
}

/// The dataset's ticker naming, both ways: a [`Symbol`] (an encoded ticker id)
/// to its ticker string, and a ticker string to its symbol.
///
/// This is the *only* place the engine relates integers to tickers. It is read
/// when a strategy subscribes, when the corporate-action files are matched by
/// ticker, and when results and log lines are written — never per bar.
#[derive(Debug, Default)]
pub struct TickerMap {
    /// Ticker id → name, `None` for ids the map doesn't list.
    names: Vec<Option<Box<str>>>,
    ids: HashMap<Box<str>, Symbol>,
}

impl TickerMap {
    /// Read `encoded_tickers.json` from `path`.
    pub fn load(path: &Path) -> Result<Self, BacktestError> {
        Ok(Self::from_pairs(load_ticker_map_from(path)?))
    }

    pub fn from_pairs(pairs: HashMap<u16, String>) -> Self {
        let capacity = pairs.keys().copied().max().map_or(0, |max| max as usize + 1);
        let mut names: Vec<Option<Box<str>>> = vec![None; capacity];
        let mut ids = HashMap::with_capacity(pairs.len());
        for (id, name) in pairs {
            let name: Box<str> = name.into();
            ids.insert(name.clone(), Symbol::from_ticker_id(id));
            names[id as usize] = Some(name);
        }
        Self { names, ids }
    }

    /// The symbol this dataset encodes `ticker` as, or `None` if it has no
    /// data for that ticker.
    pub fn symbol(&self, ticker: &str) -> Option<Symbol> {
        self.ids.get(ticker).copied()
    }

    /// The ticker `symbol` stands for, or `None` for an id the map doesn't
    /// list.
    pub fn name(&self, symbol: Symbol) -> Option<&str> {
        self.names.get(symbol.index())?.as_deref()
    }

    /// Every symbol the dataset has data for, in ticker-id order.
    pub fn symbols(&self) -> impl Iterator<Item = Symbol> + '_ {
        self.names
            .iter()
            .enumerate()
            .filter_map(|(id, name)| name.as_ref().map(|_| Symbol::from_ticker_id(id as u16)))
    }

    /// One past the highest ticker id, i.e. how many slots a dense
    /// per-symbol table needs to cover the dataset.
    pub fn id_space(&self) -> usize {
        self.names.len()
    }

    pub fn is_empty(&self) -> bool {
        self.ids.is_empty()
    }
}

/// Like [`load_ticker_map`], but from an explicit file path instead of the
/// default name inside the data root.
pub fn load_ticker_map_from(path: &Path) -> Result<HashMap<u16, String>, BacktestError> {
    let content = read_to_string(path)
        .map_err(|source| BacktestError::Io { path: path.to_path_buf(), source })?;
    let temp: HashMap<String, String> = serde_json::from_str(&content)
        .map_err(|e| BacktestError::Json { path: path.to_path_buf(), message: e.to_string() })?;
    temp.into_iter()
        .map(|(k, v)| match k.parse::<u16>() {
            Ok(id) => Ok((id, v)),
            Err(_) => Err(BacktestError::InvalidTickerKey { path: path.to_path_buf(), key: k }),
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
    load_splits_from(&PathBuf::from(format!("{data_root}/{SPLITS_FILE}")), symbols, false)
}

/// Like [`load_splits`], but from an explicit file path. With `required`,
/// a missing file is an error instead of an empty map — used when the caller
/// configured the path on purpose and silence would hide a typo.
pub fn load_splits_from(
    path: &Path,
    symbols: &std::collections::HashSet<String>,
    required: bool,
) -> Result<HashMap<String, std::collections::BTreeMap<chrono::NaiveDate, f64>>, BacktestError> {
    #[derive(serde::Deserialize)]
    struct SplitRecord {
        execution_date: String,
        ticker: String,
        split_from: f64,
        split_to: f64,
    }

    let content = match read_to_string(path) {
        Ok(c) => c,
        Err(e) if !required && e.kind() == std::io::ErrorKind::NotFound => {
            return Ok(HashMap::new())
        }
        Err(source) => return Err(BacktestError::Io { path: path.to_path_buf(), source }),
    };
    let records: Vec<SplitRecord> =
        serde_json::from_str(&content).map_err(|e| BacktestError::Json {
            path: path.to_path_buf(),
            message: format!("not valid splits JSON: {e}"),
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
    load_dividends_from(&PathBuf::from(format!("{data_root}/{DIVIDENDS_FILE}")), symbols, false)
}

/// Like [`load_dividends`], but from an explicit file path. With `required`,
/// a missing file is an error instead of an empty map.
pub fn load_dividends_from(
    path: &Path,
    symbols: &std::collections::HashSet<String>,
    required: bool,
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

    let file = match File::open(path) {
        Ok(f) => f,
        Err(e) if !required && e.kind() == std::io::ErrorKind::NotFound => {
            return Ok(HashMap::new())
        }
        Err(source) => return Err(BacktestError::Io { path: path.to_path_buf(), source }),
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
        path: path.to_path_buf(),
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
    load_renames_from(&PathBuf::from(format!("{data_root}/{RENAMES_FILE}")), symbols, false)
}

/// Like [`load_renames`], but from an explicit file path. With `required`,
/// a missing file is an error instead of an empty map.
pub fn load_renames_from(
    path: &Path,
    symbols: &std::collections::HashSet<String>,
    required: bool,
) -> Result<std::collections::BTreeMap<chrono::NaiveDate, Vec<(String, String)>>, BacktestError> {
    #[derive(serde::Deserialize)]
    struct RenameRecord {
        date: String,
        old: String,
        new: String,
    }

    let content = match read_to_string(path) {
        Ok(c) => c,
        Err(e) if !required && e.kind() == std::io::ErrorKind::NotFound => {
            return Ok(std::collections::BTreeMap::new())
        }
        Err(source) => return Err(BacktestError::Io { path: path.to_path_buf(), source }),
    };
    let records: Vec<RenameRecord> =
        serde_json::from_str(&content).map_err(|e| BacktestError::Json {
            path: path.to_path_buf(),
            message: format!("not valid renames JSON: {e}"),
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
    let reader = open_bar_reader(file_path, mask)?;
    for batch in reader {
        let batch = batch.map_err(|e| BacktestError::Parquet {
            path: file_path.to_path_buf(),
            message: e.to_string(),
        })?;
        let cols = BarColumns::extract(&batch, file_path)?;
        for i in 0..batch.num_rows() {
            let symbol = match ticker_map.get(&cols.ticker.value(i)) {
                Some(s) => s.clone(),
                None => continue,
            };
            let Some(bar) = cols.bar(i) else { continue };
            cb(symbol, bar);
        }
    }
    Ok(())
}

fn open_bar_reader(
    file_path: &Path,
    mask: &mut Option<ProjectionMask>,
) -> Result<ParquetRecordBatchReader, BacktestError> {
    let file = File::open(file_path)
        .map_err(|source| BacktestError::Io { path: file_path.to_path_buf(), source })?;
    let builder = ParquetRecordBatchReaderBuilder::try_new(file).map_err(|e| {
        BacktestError::Parquet { path: file_path.to_path_buf(), message: e.to_string() }
    })?;
    let mask = mask
        .get_or_insert_with(|| ProjectionMask::columns(builder.parquet_schema(), COLUMNS))
        .clone();
    builder.with_projection(mask).build().map_err(|e| BacktestError::Parquet {
        path: file_path.to_path_buf(),
        message: e.to_string(),
    })
}

/// The bar columns of one record batch, downcast once per batch.
struct BarColumns<'a> {
    ticker: &'a UInt16Array,
    volume: &'a UInt32Array,
    open: &'a Float64Array,
    high: &'a Float64Array,
    low: &'a Float64Array,
    close: &'a Float64Array,
    ts: &'a TimestampNanosecondArray,
}

impl<'a> BarColumns<'a> {
    fn extract(batch: &'a RecordBatch, path: &Path) -> Result<Self, BacktestError> {
        Ok(Self {
            ticker: column(batch, "ticker", path)?,
            volume: column(batch, "volume", path)?,
            open: column(batch, "open", path)?,
            high: column(batch, "high", path)?,
            low: column(batch, "low", path)?,
            close: column(batch, "close", path)?,
            ts: column(batch, "window_start", path)?,
        })
    }

    /// Decode row `i` into a [`Bar`], or `None` for a corrupt row (a timestamp
    /// outside the nanosecond-representable range).
    fn bar(&self, i: usize) -> Option<Bar> {
        self.bar_with_time(i, ts_to_datetime(self.ts.value(i))?)
    }

    /// [`bar`](Self::bar) for callers that already decoded the row's time.
    fn bar_with_time(&self, i: usize, time: DateTime<Utc>) -> Option<Bar> {
        Some(Bar {
            time,
            open: self.open.value(i),
            high: self.high.value(i),
            low: self.low.value(i),
            close: self.close.value(i),
            volume: self.volume.value(i) as u64,
        })
    }
}

fn ts_to_datetime(ts_ns: i64) -> Option<DateTime<Utc>> {
    let secs = ts_ns / 1_000_000_000;
    let nanos = (ts_ns % 1_000_000_000) as u32;
    Utc.timestamp_opt(secs, nanos).single()
}

/// One tick: a nanosecond timestamp and every subscribed bar printed on it.
pub type Tick = (i64, Vec<(Symbol, Bar)>);

/// Which ticker ids a run subscribes, as a flag per id.
///
/// This is the whole of the reader's per-row symbol work: a row's `ticker`
/// column *is* its [`Symbol`], so all that's left to decide is whether the
/// run wants it — one array index, no map lookup, no string comparison, no
/// allocation.
#[derive(Debug, Default)]
pub struct SubscriptionMask {
    subscribed: Vec<bool>,
}

impl SubscriptionMask {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn insert(&mut self, symbol: Symbol) {
        if self.subscribed.len() <= symbol.index() {
            self.subscribed.resize(symbol.index() + 1, false);
        }
        self.subscribed[symbol.index()] = true;
    }

    #[inline]
    pub fn contains(&self, ticker_id: u16) -> bool {
        self.subscribed.get(ticker_id as usize).copied().unwrap_or(false)
    }

    /// Whether nothing is subscribed at all.
    pub fn is_empty(&self) -> bool {
        !self.subscribed.contains(&true)
    }
}

/// Streams one Parquet file's subscribed bars grouped into ticks — all
/// consecutive rows sharing one `window_start` — in row order, so only a
/// single tick is ever resident instead of a whole month.
///
/// This relies on the file being sorted by `window_start` (non-decreasing;
/// ties may be split across part files but not interleaved with later times).
/// Nothing is re-sorted: a timestamp regression between rows is an
/// [`BacktestError::OutOfOrderData`] error, so unsorted data must be sorted at
/// ingest. Rows with a timestamp outside the nanosecond-representable range
/// are corrupt and dropped before the order check, as are rows with a
/// ticker id missing from the map.
pub struct TickReader<'a> {
    path: PathBuf,
    reader: ParquetRecordBatchReader,
    subscribed: &'a SubscriptionMask,
    /// Decoded subscribed rows not yet grouped into a tick.
    queue: VecDeque<(Symbol, i64, Bar)>,
    /// Timestamp of the last decodable row seen, for the sort check. Tracks
    /// every row (not just subscribed ones): an unsorted file is bad data
    /// regardless of which symbols this run happens to subscribe.
    last_ts: Option<i64>,
    exhausted: bool,
}

impl<'a> TickReader<'a> {
    pub fn new(
        file_path: &Path,
        subscribed: &'a SubscriptionMask,
        mask: &mut Option<ProjectionMask>,
    ) -> Result<Self, BacktestError> {
        Ok(Self {
            path: file_path.to_path_buf(),
            reader: open_bar_reader(file_path, mask)?,
            subscribed,
            queue: VecDeque::new(),
            last_ts: None,
            exhausted: false,
        })
    }

    /// The next tick: every subscribed bar sharing the file's next timestamp.
    /// `Ok(None)` once the file is exhausted.
    pub fn next_tick(&mut self) -> Result<Option<Tick>, BacktestError> {
        let Some(tick_ts) = self.peek_ts()? else { return Ok(None) };
        let mut bars = Vec::new();
        while self.peek_ts()? == Some(tick_ts) {
            let (symbol, _, bar) = self.queue.pop_front().expect("peeked row vanished");
            bars.push((symbol, bar));
        }
        Ok(Some((tick_ts, bars)))
    }

    /// Timestamp of the next queued row, decoding further batches as needed.
    fn peek_ts(&mut self) -> Result<Option<i64>, BacktestError> {
        while self.queue.is_empty() && !self.exhausted {
            self.decode_next_batch()?;
        }
        Ok(self.queue.front().map(|&(_, ts, _)| ts))
    }

    fn decode_next_batch(&mut self) -> Result<(), BacktestError> {
        let Some(batch) = self.reader.next() else {
            self.exhausted = true;
            return Ok(());
        };
        let batch = batch.map_err(|e| BacktestError::Parquet {
            path: self.path.clone(),
            message: e.to_string(),
        })?;
        let cols = BarColumns::extract(&batch, &self.path)?;
        for i in 0..batch.num_rows() {
            let ts = cols.ts.value(i);
            let Some(time) = ts_to_datetime(ts) else { continue };
            if let Some(last) = self.last_ts {
                if ts < last {
                    return Err(BacktestError::OutOfOrderData {
                        path: self.path.clone(),
                        at: time,
                        stream_at: ts_to_datetime(last).expect("validated on a previous row"),
                    });
                }
            }
            self.last_ts = Some(ts);
            let ticker_id = cols.ticker.value(i);
            if !self.subscribed.contains(ticker_id) {
                continue;
            }
            let Some(bar) = cols.bar_with_time(i, time) else { continue };
            self.queue.push_back((Symbol::from_ticker_id(ticker_id), ts, bar));
        }
        Ok(())
    }
}

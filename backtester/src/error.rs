use std::{fmt, io, path::PathBuf};

use chrono::{DateTime, NaiveDate, Utc};

/// Everything that can go wrong loading data or running a backtest. The hot
/// loop still panics on internal invariant violations; this covers the
/// diagnosable failures — bad paths, malformed metadata, unreadable Parquet.
#[derive(Debug)]
pub enum BacktestError {
    /// A file could not be opened, read, or written.
    Io { path: PathBuf, source: io::Error },
    /// A JSON file (ticker map, splits, result output) failed to parse or
    /// serialize.
    Json { path: PathBuf, message: String },
    /// A Parquet file failed to open or decode.
    Parquet { path: PathBuf, message: String },
    /// A required column is missing from a Parquet file, or has the wrong
    /// type.
    MissingColumn { path: PathBuf, column: &'static str },
    /// `encoded_tickers.json` contains a key that is not a `u16` ticker id.
    InvalidTickerKey { path: PathBuf, key: String },
    /// No `.parquet` files were found under the data path — almost always a
    /// wrong `data_path` argument.
    NoData { path: PathBuf },
    /// The configured date range is inverted: the start date is after the end
    /// date.
    InvalidDateRange { start: NaiveDate, end: NaiveDate },
    /// A bar's timestamp ran backwards. The engine streams each Parquet file
    /// in row order and never re-sorts, so rows must be non-decreasing in
    /// `window_start` within a file, and a month partition must not contain
    /// rows dated before an earlier partition's bars. Sort the data at ingest.
    OutOfOrderData { path: PathBuf, at: DateTime<Utc>, stream_at: DateTime<Utc> },
    /// A decode thread died without finishing its files. Only a panic inside
    /// the reader can cause this, but it must not pass for end of data: a
    /// backtest silently truncated to the bars that made it through would
    /// report plausible, wrong results.
    ReaderThreadDied,
}

impl fmt::Display for BacktestError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Io { path, source } => write!(f, "{}: {source}", path.display()),
            Self::Json { path, message } => write!(f, "{}: {message}", path.display()),
            Self::Parquet { path, message } => write!(f, "{}: {message}", path.display()),
            Self::MissingColumn { path, column } => {
                write!(f, "{}: missing or mistyped column `{column}`", path.display())
            }
            Self::InvalidTickerKey { path, key } => {
                write!(f, "{}: ticker key `{key}` is not a u16 id", path.display())
            }
            Self::NoData { path } => {
                write!(f, "no .parquet files found under {}", path.display())
            }
            Self::InvalidDateRange { start, end } => {
                write!(f, "invalid date range: start date {start} is after end date {end}")
            }
            Self::ReaderThreadDied => {
                write!(f, "a Parquet decode thread died before reaching the end of the data")
            }
            Self::OutOfOrderData { path, at, stream_at } => {
                write!(
                    f,
                    "{}: bar at {} is out of order (stream already at {}); data files must \
                     be sorted by time",
                    path.display(),
                    at.to_rfc3339(),
                    stream_at.to_rfc3339()
                )
            }
        }
    }
}

impl std::error::Error for BacktestError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Io { source, .. } => Some(source),
            _ => None,
        }
    }
}

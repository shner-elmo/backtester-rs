//! SEC Form 4 insider-transaction data, produced by the `insider-fetch`
//! crate (`cargo run -p insider-fetch`).
//!
//! This is strategy-side data: the engine never reads it. An algorithm that
//! wants to follow insider trades calls [`load_insider_transactions`] in its
//! `initialize()` and keys the result off the slice date in `on_data`. The
//! `filing_date` is when a transaction became public knowledge — acting on it
//! any earlier (e.g. on `trans_date`) is lookahead bias.

use std::{
    collections::{BTreeMap, HashMap, HashSet},
    fmt,
    fs::File,
    path::{Path, PathBuf},
};

use chrono::NaiveDate;
use serde::de::{Deserializer, SeqAccess, Visitor};

use crate::error::BacktestError;

/// Default file name inside the data root, next to `encoded_tickers.json`.
pub const INSIDER_FILE: &str = "insider_transactions.json";

/// Open-market transaction side from Form 4's transaction code.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TransCode {
    /// Code `P`: open-market or private purchase.
    Purchase,
    /// Code `S`: open-market or private sale.
    Sale,
}

/// One non-derivative open-market transaction from a Form 4 filing.
#[derive(Debug, Clone)]
pub struct InsiderTransaction {
    /// When the filing became public — the earliest date a strategy may act.
    pub filing_date: NaiveDate,
    /// When the insider actually traded (informational; predates the filing).
    pub trans_date: Option<NaiveDate>,
    pub ticker: String,
    pub code: TransCode,
    pub shares: f64,
    pub price: f64,
    /// `shares * price`, precomputed by the pipeline.
    pub value: f64,
    pub owner_name: Option<String>,
    pub is_officer: bool,
    pub is_director: bool,
    pub is_ten_pct_owner: bool,
    pub officer_title: Option<String>,
    pub shares_owned_after: Option<f64>,
}

/// Transactions per symbol, grouped by filing date.
pub type InsiderMap = HashMap<String, BTreeMap<NaiveDate, Vec<InsiderTransaction>>>;

/// Raw JSON shape; dates stay strings and `code` stays open so one malformed
/// or unknown-code record is skipped instead of failing the whole file
/// (matching the tolerant loaders in `data.rs`).
#[derive(serde::Deserialize)]
struct RawRecord {
    filing_date: String,
    #[serde(default)]
    trans_date: Option<String>,
    ticker: String,
    code: String,
    shares: f64,
    price: f64,
    value: f64,
    #[serde(default)]
    owner_name: Option<String>,
    #[serde(default)]
    is_officer: bool,
    #[serde(default)]
    is_director: bool,
    #[serde(default)]
    is_ten_pct_owner: bool,
    #[serde(default)]
    officer_title: Option<String>,
    #[serde(default)]
    shares_owned_after: Option<f64>,
}

/// Insider transactions per symbol: filing date → transactions filed that
/// day. Reads `insider_transactions.json` next to `encoded_tickers.json`;
/// returns an empty map when the file doesn't exist (e.g. a data root without
/// insider data). Only transactions for `symbols` are kept.
///
/// A full-market history spanning years is hundreds of MB, so this streams
/// the JSON array one record at a time and keeps only the matches, like
/// [`crate::data::load_dividends`].
pub fn load_insider_transactions(
    data_root: &str,
    symbols: &HashSet<String>,
) -> Result<InsiderMap, BacktestError> {
    load_insider_transactions_from(
        &PathBuf::from(format!("{data_root}/{INSIDER_FILE}")),
        symbols,
        false,
    )
}

/// Like [`load_insider_transactions`], but from an explicit file path. With
/// `required`, a missing file is an error instead of an empty map — used when
/// the caller configured the path on purpose and silence would hide a typo.
pub fn load_insider_transactions_from(
    path: &Path,
    symbols: &HashSet<String>,
    required: bool,
) -> Result<InsiderMap, BacktestError> {
    let file = match File::open(path) {
        Ok(f) => f,
        Err(e) if !required && e.kind() == std::io::ErrorKind::NotFound => {
            return Ok(HashMap::new())
        }
        Err(source) => return Err(BacktestError::Io { path: path.to_path_buf(), source }),
    };

    // A seq visitor that filters records as they stream in, so peak memory is
    // the matched transactions, not the whole file.
    struct FilterVisitor<'a> {
        symbols: &'a HashSet<String>,
    }
    impl<'de> Visitor<'de> for FilterVisitor<'_> {
        type Value = InsiderMap;

        fn expecting(&self, f: &mut fmt::Formatter) -> fmt::Result {
            f.write_str("an array of insider transaction records")
        }

        fn visit_seq<A: SeqAccess<'de>>(self, mut seq: A) -> Result<InsiderMap, A::Error> {
            let mut map: InsiderMap = HashMap::new();
            while let Some(r) = seq.next_element::<RawRecord>()? {
                if !self.symbols.contains(&r.ticker) {
                    continue;
                }
                let code = match r.code.as_str() {
                    "P" => TransCode::Purchase,
                    "S" => TransCode::Sale,
                    _ => continue,
                };
                let Ok(filing_date) = r.filing_date.parse::<NaiveDate>() else {
                    continue;
                };
                if r.shares <= 0.0 || r.price <= 0.0 {
                    continue;
                }
                let tx = InsiderTransaction {
                    filing_date,
                    trans_date: r.trans_date.and_then(|d| d.parse().ok()),
                    ticker: r.ticker,
                    code,
                    shares: r.shares,
                    price: r.price,
                    value: r.value,
                    owner_name: r.owner_name,
                    is_officer: r.is_officer,
                    is_director: r.is_director,
                    is_ten_pct_owner: r.is_ten_pct_owner,
                    officer_title: r.officer_title,
                    shares_owned_after: r.shares_owned_after,
                };
                map.entry(tx.ticker.clone()).or_default().entry(filing_date).or_default().push(tx);
            }
            Ok(map)
        }
    }

    let reader = std::io::BufReader::new(file);
    let mut de = serde_json::Deserializer::from_reader(reader);
    de.deserialize_seq(FilterVisitor { symbols }).map_err(|e| BacktestError::Json {
        path: path.to_path_buf(),
        message: format!("not valid insider transactions JSON: {e}"),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn symbols(list: &[&str]) -> HashSet<String> {
        list.iter().map(|s| s.to_string()).collect()
    }

    fn write_temp(content: &str) -> (tempfile::TempDir, PathBuf) {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join(INSIDER_FILE);
        std::fs::write(&path, content).unwrap();
        (dir, path)
    }

    const SAMPLE: &str = r#"[
        {"filing_date": "2023-01-06", "trans_date": "2023-01-04", "ticker": "AAPL",
         "code": "P", "shares": 1000.0, "price": 125.0, "value": 125000.0,
         "owner_name": "DOE JANE", "is_officer": true, "is_director": false,
         "is_ten_pct_owner": false, "officer_title": "Chief Executive Officer",
         "shares_owned_after": 5000.0, "accession_number": "0000000000-23-000001"},
        {"filing_date": "2023-01-06", "ticker": "AAPL", "code": "S",
         "shares": 10.0, "price": 120.0, "value": 1200.0},
        {"filing_date": "2023-02-01", "ticker": "MSFT", "code": "P",
         "shares": 50.0, "price": 240.0, "value": 12000.0},
        {"filing_date": "2023-02-02", "ticker": "AAPL", "code": "X",
         "shares": 1.0, "price": 1.0, "value": 1.0},
        {"filing_date": "not-a-date", "ticker": "AAPL", "code": "P",
         "shares": 1.0, "price": 1.0, "value": 1.0},
        {"filing_date": "2023-02-03", "ticker": "AAPL", "code": "P",
         "shares": 0.0, "price": 1.0, "value": 0.0}
    ]"#;

    #[test]
    fn parses_and_filters_to_requested_symbols() {
        let (_dir, path) = write_temp(SAMPLE);
        let map = load_insider_transactions_from(&path, &symbols(&["AAPL"]), true).unwrap();

        assert_eq!(map.len(), 1, "MSFT was not requested");
        let by_date = &map["AAPL"];
        // Bad code, bad date and zero shares are skipped; the two valid
        // 2023-01-06 records group under one date.
        assert_eq!(by_date.len(), 1);
        let day = &by_date[&NaiveDate::from_ymd_opt(2023, 1, 6).unwrap()];
        assert_eq!(day.len(), 2);

        let buy = &day[0];
        assert_eq!(buy.code, TransCode::Purchase);
        assert_eq!(buy.trans_date, NaiveDate::from_ymd_opt(2023, 1, 4));
        assert_eq!(buy.value, 125000.0);
        assert!(buy.is_officer);
        assert_eq!(buy.officer_title.as_deref(), Some("Chief Executive Officer"));

        let sell = &day[1];
        assert_eq!(sell.code, TransCode::Sale);
        assert_eq!(sell.owner_name, None, "optional fields default");
        assert!(!sell.is_officer);
    }

    #[test]
    fn missing_file_is_empty_unless_required() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join(INSIDER_FILE);
        let syms = symbols(&["AAPL"]);

        let map = load_insider_transactions_from(&path, &syms, false).unwrap();
        assert!(map.is_empty());

        let err = load_insider_transactions_from(&path, &syms, true).unwrap_err();
        assert!(matches!(err, BacktestError::Io { .. }));
    }

    #[test]
    fn malformed_json_is_an_error() {
        let (_dir, path) = write_temp("{\"not\": \"an array\"}");
        let err = load_insider_transactions_from(&path, &symbols(&["AAPL"]), true).unwrap_err();
        assert!(matches!(err, BacktestError::Json { .. }));
    }

    #[test]
    fn default_root_lookup_uses_the_standard_file_name() {
        let (dir, _path) = write_temp(SAMPLE);
        let map =
            load_insider_transactions(dir.path().to_str().unwrap(), &symbols(&["MSFT"])).unwrap();
        assert_eq!(map["MSFT"].len(), 1);
    }
}

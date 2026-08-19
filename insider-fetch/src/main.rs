//! Download SEC EDGAR Form 4 insider transactions and emit backtester JSON.
//!
//! Pulls the SEC's quarterly "Insider Transactions Data Sets" (structured TSV
//! extracts of Forms 3/4/5, published since 2006q1) and flattens them into a
//! single JSON array of open-market insider trades that
//! `backtester::insider::load_insider_transactions` can read. Only original
//! Form 4 filings are kept (no Forms 3/5, no amendments) and only
//! non-derivative transactions with code P (open-market purchase) or S
//! (open-market sale) — grants, option exercises, tax withholding and gifts
//! are not copy-trade signals and are dropped, which also keeps the output
//! small.
//!
//! See `usage()` below (or run with `--help`) for the CLI. Downloads are
//! cached and skipped on re-runs. The SEC's fair-access policy requires a
//! descriptive User-Agent with a contact address: pass `--user-agent` or set
//! `SEC_USER_AGENT`.

use std::{
    collections::{HashMap, HashSet},
    fs::File,
    path::{Path, PathBuf},
};

const URL_TEMPLATE: &str =
    "https://www.sec.gov/files/structureddata/data/insider-transactions-data-sets";
const FIRST_QUARTER: (i32, u32) = (2006, 1);
const BAD_TICKERS: [&str; 3] = ["", "NONE", "N/A"];
const MONTHS: [&str; 12] =
    ["JAN", "FEB", "MAR", "APR", "MAY", "JUN", "JUL", "AUG", "SEP", "OCT", "NOV", "DEC"];

fn usage() -> String {
    let exe = "insider-fetch";
    format!(
        "Usage:
    {exe} --start 2023q1 --end 2023q4 --out /path/to/data-root/insider_transactions.json
        [--cache-dir ~/.cache/sec-form345]     downloaded zips kept here; re-runs skip download
        [--tickers AAPL,MSFT]                  optional filter; default = all issuers
        [--user-agent \"Your Name you@example.com\"]  falls back to $SEC_USER_AGENT

--start/--end accept YYYYqQ or a bare YYYY (whole year); the range is inclusive."
    )
}

/// One output record: the contract consumed by
/// `backtester::insider::load_insider_transactions`.
#[derive(Debug, serde::Serialize)]
struct OutRecord {
    /// When the filing became public — the date a strategy may trade on.
    filing_date: String,
    trans_date: Option<String>,
    ticker: String,
    code: String,
    shares: f64,
    price: f64,
    value: f64,
    owner_name: Option<String>,
    is_officer: bool,
    is_director: bool,
    is_ten_pct_owner: bool,
    officer_title: Option<String>,
    shares_owned_after: Option<f64>,
    accession_number: String,
}

#[derive(Default)]
struct Stats {
    submissions: u64,
    transactions: u64,
    dropped_no_ticker: u64,
    dropped_bad_date: u64,
    dropped_no_amount: u64,
}

struct Args {
    start: (i32, u32),
    end: (i32, u32),
    out: PathBuf,
    cache_dir: PathBuf,
    tickers: Option<HashSet<String>>,
    user_agent: String,
}

/// "2023q1" (or "2023Q1") → (2023, 1).
fn parse_quarter(text: &str) -> Result<(i32, u32), String> {
    let lower = text.to_lowercase();
    let invalid = || format!("invalid quarter {text:?}: expected YYYYqQ, e.g. 2023q1");
    let (year, quarter) = lower.split_once('q').ok_or_else(invalid)?;
    let year: i32 = year.parse().map_err(|_| invalid())?;
    let quarter: u32 = quarter.parse().map_err(|_| invalid())?;
    if !(1..=4).contains(&quarter) || year < 1000 {
        return Err(invalid());
    }
    Ok((year, quarter))
}

/// Inclusive list of (year, quarter) pairs from start to end.
fn quarter_range(start: (i32, u32), end: (i32, u32)) -> Result<Vec<(i32, u32)>, String> {
    if start > end {
        return Err(format!("start quarter {start:?} is after end quarter {end:?}"));
    }
    let mut quarters = Vec::new();
    let (mut year, mut quarter) = start;
    while (year, quarter) <= end {
        quarters.push((year, quarter));
        (year, quarter) = if quarter < 4 { (year, quarter + 1) } else { (year + 1, 1) };
    }
    Ok(quarters)
}

/// "30-JUN-2023" (data-set format) or "2023-06-30" → ISO string, else None.
fn parse_sec_date(text: &str) -> Option<String> {
    let text = text.trim();
    let parts: Vec<&str> = text.split('-').collect();
    if parts.len() == 3 {
        if let Some(month) = MONTHS.iter().position(|m| parts[1].eq_ignore_ascii_case(m)) {
            let (day, year) = (parts[0].parse().ok()?, parts[2].parse().ok()?);
            let date = chrono::NaiveDate::from_ymd_opt(year, month as u32 + 1, day)?;
            return Some(date.format("%Y-%m-%d").to_string());
        }
    }
    let date = text.parse::<chrono::NaiveDate>().ok()?;
    Some(date.format("%Y-%m-%d").to_string())
}

fn parse_float(text: &str) -> Option<f64> {
    text.parse().ok()
}

fn current_quarter() -> (i32, u32) {
    use chrono::Datelike;
    let today = chrono::Utc::now().date_naive();
    (today.year(), (today.month() - 1) / 3 + 1)
}

/// Fetch one quarterly zip into the cache (or reuse it) and return its path.
/// A cached file that fails zip validation is assumed to be a truncated
/// download and fetched again once.
fn download_quarter(
    agent: &ureq::Agent,
    year: i32,
    quarter: u32,
    cache_dir: &Path,
    user_agent: &str,
) -> Result<PathBuf, String> {
    let path = cache_dir.join(format!("{year}q{quarter}_form345.zip"));
    for _attempt in 0..2 {
        if !path.exists() {
            let url = format!("{URL_TEMPLATE}/{year}q{quarter}_form345.zip");
            eprintln!("downloading {url}");
            let mut response = agent
                .get(&url)
                .header("User-Agent", user_agent)
                .call()
                .map_err(|e| format!("GET {url}: {e}"))?;
            let mut file = File::create(&path).map_err(|e| format!("create {path:?}: {e}"))?;
            // The zips run up to ~17 MB; lift ureq's default body cap.
            let mut reader = response.body_mut().with_config().limit(1 << 30).reader();
            std::io::copy(&mut reader, &mut file).map_err(|e| format!("write {path:?}: {e}"))?;
            // SEC fair-access courtesy between downloads.
            std::thread::sleep(std::time::Duration::from_millis(500));
        }
        match File::open(&path).map(zip::ZipArchive::new) {
            Ok(Ok(_)) => return Ok(path),
            _ => {
                eprintln!("cached {} is not a valid zip — re-downloading", path.display());
                std::fs::remove_file(&path).map_err(|e| format!("remove {path:?}: {e}"))?;
            }
        }
    }
    Err(format!("downloaded {} is not a valid zip file", path.display()))
}

/// A TSV inside the quarterly zip, read row by row with columns resolved by
/// header name. Missing columns fail loudly (naming the file and column) so
/// upstream schema drift is caught, not silently mis-parsed.
struct Tsv<'a> {
    reader: csv::Reader<zip::read::ZipFile<'a>>,
    indexes: Vec<usize>,
    row: csv::StringRecord,
}

impl<'a> Tsv<'a> {
    fn open(
        archive: &'a mut zip::ZipArchive<File>,
        zip_path: &Path,
        name: &str,
        columns: &[&str],
    ) -> Result<Self, String> {
        let entry = archive.by_name(name).map_err(|e| format!("{zip_path:?}:{name}: {e}"))?;
        let mut reader =
            csv::ReaderBuilder::new().delimiter(b'\t').flexible(true).from_reader(entry);
        let headers = reader.headers().map_err(|e| format!("{zip_path:?}:{name}: {e}"))?.clone();
        let indexes = columns
            .iter()
            .map(|column| {
                headers
                    .iter()
                    .position(|h| h == *column)
                    .ok_or_else(|| format!("{zip_path:?}:{name} has no column {column:?}"))
            })
            .collect::<Result<Vec<_>, _>>()?;
        Ok(Self { reader, indexes, row: csv::StringRecord::new() })
    }

    /// Advance to the next row; `fields` receives the requested columns'
    /// trimmed values (empty string when a ragged row is short).
    fn next_row(&mut self, fields: &mut Vec<String>) -> Result<bool, String> {
        if !self.reader.read_record(&mut self.row).map_err(|e| e.to_string())? {
            return Ok(false);
        }
        fields.clear();
        fields
            .extend(self.indexes.iter().map(|&i| self.row.get(i).unwrap_or("").trim().to_string()));
        Ok(true)
    }
}

struct OwnerInfo {
    name: String,
    title: String,
    is_officer: bool,
    is_director: bool,
    is_ten_pct: bool,
}

/// Join SUBMISSION + REPORTINGOWNER + NONDERIV_TRANS into output records.
fn extract_records(
    zip_path: &Path,
    tickers: Option<&HashSet<String>>,
    stats: &mut Stats,
) -> Result<Vec<OutRecord>, String> {
    let file = File::open(zip_path).map_err(|e| format!("open {zip_path:?}: {e}"))?;
    let mut archive = zip::ZipArchive::new(file).map_err(|e| format!("{zip_path:?}: {e}"))?;
    let mut fields: Vec<String> = Vec::new();

    // Original Form 4s only: no Forms 3/5 (holdings / annual summaries) and
    // no amendments ("4/A"), whose corrections are out of scope.
    let mut filings: HashMap<String, (String, String)> = HashMap::new();
    {
        let mut tsv = Tsv::open(
            &mut archive,
            zip_path,
            "SUBMISSION.tsv",
            &["ACCESSION_NUMBER", "FILING_DATE", "DOCUMENT_TYPE", "ISSUERTRADINGSYMBOL"],
        )?;
        while tsv.next_row(&mut fields)? {
            stats.submissions += 1;
            let [accession, filing_date, doc_type, ticker] = &fields[..] else { unreachable!() };
            if doc_type != "4" {
                continue;
            }
            // A few SEC rows carry the symbol still wrapped in quote characters
            // (`""OMEX""` in the TSV, `"OMEX"` once the CSV reader unquotes it);
            // left that way they match no ticker in any dataset.
            let ticker = ticker.trim().trim_matches('"').trim().to_uppercase();
            if BAD_TICKERS.contains(&ticker.as_str()) {
                stats.dropped_no_ticker += 1;
                continue;
            }
            if let Some(wanted) = tickers {
                if !wanted.contains(&ticker) {
                    continue;
                }
            }
            let Some(filing_date) = parse_sec_date(filing_date) else {
                stats.dropped_bad_date += 1;
                continue;
            };
            filings.insert(accession.clone(), (filing_date, ticker));
        }
    }

    // A filing can list several reporting owners (e.g. a fund and its
    // manager). Keep the first owner's name/title for display but OR the
    // relationship flags across all of them.
    let mut owners: HashMap<String, OwnerInfo> = HashMap::new();
    {
        let mut tsv = Tsv::open(
            &mut archive,
            zip_path,
            "REPORTINGOWNER.tsv",
            &["ACCESSION_NUMBER", "RPTOWNERNAME", "RPTOWNER_RELATIONSHIP", "RPTOWNER_TITLE"],
        )?;
        while tsv.next_row(&mut fields)? {
            let [accession, name, relationship, title] = &fields[..] else { unreachable!() };
            if !filings.contains_key(accession) {
                continue;
            }
            let is_officer = relationship.contains("Officer");
            let is_director = relationship.contains("Director");
            let is_ten_pct = relationship.contains("TenPercentOwner");
            owners
                .entry(accession.clone())
                .and_modify(|o| {
                    o.is_officer |= is_officer;
                    o.is_director |= is_director;
                    o.is_ten_pct |= is_ten_pct;
                })
                .or_insert_with(|| OwnerInfo {
                    name: name.clone(),
                    title: title.clone(),
                    is_officer,
                    is_director,
                    is_ten_pct,
                });
        }
    }

    let mut records = Vec::new();
    {
        let mut tsv = Tsv::open(
            &mut archive,
            zip_path,
            "NONDERIV_TRANS.tsv",
            &[
                "ACCESSION_NUMBER",
                "TRANS_DATE",
                "TRANS_CODE",
                "TRANS_SHARES",
                "TRANS_PRICEPERSHARE",
                "SHRS_OWND_FOLWNG_TRANS",
            ],
        )?;
        while tsv.next_row(&mut fields)? {
            stats.transactions += 1;
            let [accession, trans_date, code, shares, price, owned_after] = &fields[..] else {
                unreachable!()
            };
            if code != "P" && code != "S" {
                continue;
            }
            let Some((filing_date, ticker)) = filings.get(accession) else {
                continue;
            };
            let (shares, price) = match (parse_float(shares), parse_float(price)) {
                (Some(s), Some(p)) if s != 0.0 && p != 0.0 => (s, p),
                // Footnote-only ("see footnote") or zero amounts carry no
                // usable signal size.
                _ => {
                    stats.dropped_no_amount += 1;
                    continue;
                }
            };
            let owner = owners.get(accession);
            records.push(OutRecord {
                filing_date: filing_date.clone(),
                trans_date: parse_sec_date(trans_date),
                ticker: ticker.clone(),
                code: code.clone(),
                shares,
                price,
                value: (shares * price * 100.0).round_ties_even() / 100.0,
                owner_name: owner.map(|o| o.name.clone()).filter(|n| !n.is_empty()),
                is_officer: owner.is_some_and(|o| o.is_officer),
                is_director: owner.is_some_and(|o| o.is_director),
                is_ten_pct_owner: owner.is_some_and(|o| o.is_ten_pct),
                officer_title: owner.map(|o| o.title.clone()).filter(|t| !t.is_empty()),
                shares_owned_after: parse_float(owned_after),
                accession_number: accession.clone(),
            });
        }
    }
    Ok(records)
}

fn parse_args() -> Result<Option<Args>, String> {
    let mut start = None;
    let mut end = None;
    let mut out = None;
    let mut cache_dir = None;
    let mut tickers = None;
    let mut user_agent = std::env::var("SEC_USER_AGENT").ok();

    let mut argv = std::env::args().skip(1);
    while let Some(arg) = argv.next() {
        let mut value = |flag: &str| argv.next().ok_or(format!("{flag} needs a value"));
        match arg.as_str() {
            "--help" | "-h" => return Ok(None),
            "--start" => start = Some(value("--start")?),
            "--end" => end = Some(value("--end")?),
            "--out" => out = Some(PathBuf::from(value("--out")?)),
            "--cache-dir" => cache_dir = Some(PathBuf::from(value("--cache-dir")?)),
            "--tickers" => tickers = Some(value("--tickers")?),
            "--user-agent" => user_agent = Some(value("--user-agent")?),
            other => return Err(format!("unknown argument {other:?}")),
        }
    }

    let (Some(start), Some(end), Some(out)) = (start, end, out) else {
        return Err("--start, --end and --out are required".into());
    };
    let Some(user_agent) = user_agent.filter(|ua| !ua.is_empty()) else {
        return Err("the SEC requires a contact User-Agent; \
             pass --user-agent \"Your Name you@example.com\" or set SEC_USER_AGENT"
            .into());
    };

    // Bare years expand to the whole year: 2023 → 2023q1 .. 2023q4.
    let expand = |text: String, quarter: &str| {
        if text.to_lowercase().contains('q') {
            text
        } else {
            format!("{text}{quarter}")
        }
    };
    let start = parse_quarter(&expand(start, "q1"))?;
    let end = parse_quarter(&expand(end, "q4"))?;
    if start < FIRST_QUARTER || end > current_quarter() {
        return Err(format!(
            "data sets cover {}q{} through the current quarter",
            FIRST_QUARTER.0, FIRST_QUARTER.1
        ));
    }

    let cache_dir = cache_dir.unwrap_or_else(|| {
        let home = std::env::var("HOME").unwrap_or_else(|_| ".".into());
        PathBuf::from(home).join(".cache/sec-form345")
    });
    let tickers = tickers.map(|t| {
        t.split(',').map(str::trim).filter(|s| !s.is_empty()).map(str::to_uppercase).collect()
    });
    Ok(Some(Args { start, end, out, cache_dir, tickers, user_agent }))
}

fn run() -> Result<(), String> {
    let Some(args) = parse_args()? else {
        println!("{}", usage());
        return Ok(());
    };
    let quarters = quarter_range(args.start, args.end)?;

    std::fs::create_dir_all(&args.cache_dir)
        .map_err(|e| format!("create {:?}: {e}", args.cache_dir))?;
    // Root certificates come from the platform verifier so corporate/proxy
    // CAs in the system store (or $SSL_CERT_FILE) are honored; the proxy
    // itself is picked up from the standard env vars by default.
    let agent: ureq::Agent = ureq::Agent::config_builder()
        .tls_config(
            ureq::tls::TlsConfig::builder()
                .root_certs(ureq::tls::RootCerts::PlatformVerifier)
                .build(),
        )
        .build()
        .into();

    let mut stats = Stats::default();
    let mut records = Vec::new();
    for &(year, quarter) in &quarters {
        let context = |e| format!("{year}q{quarter}: {e}");
        let zip_path = download_quarter(&agent, year, quarter, &args.cache_dir, &args.user_agent)
            .map_err(context)?;
        records.extend(extract_records(&zip_path, args.tickers.as_ref(), &mut stats)?);
    }

    records.sort_by(|a, b| {
        (&a.filing_date, &a.ticker, &a.accession_number).cmp(&(
            &b.filing_date,
            &b.ticker,
            &b.accession_number,
        ))
    });
    if let Some(parent) = args.out.parent() {
        std::fs::create_dir_all(parent).map_err(|e| format!("create {parent:?}: {e}"))?;
    }
    let out = File::create(&args.out).map_err(|e| format!("create {:?}: {e}", args.out))?;
    let mut writer = std::io::BufWriter::new(out);
    serde_json::to_writer_pretty(&mut writer, &records).map_err(|e| e.to_string())?;
    std::io::Write::write_all(&mut writer, b"\n").map_err(|e| e.to_string())?;

    eprintln!(
        "{} quarter(s): {} filings scanned, {} non-derivative rows, {} open-market \
         P/S records written to {}\ndropped: {} without ticker, {} bad filing date, \
         {} without shares/price",
        quarters.len(),
        stats.submissions,
        stats.transactions,
        records.len(),
        args.out.display(),
        stats.dropped_no_ticker,
        stats.dropped_bad_date,
        stats.dropped_no_amount,
    );
    Ok(())
}

fn main() {
    if let Err(message) = run() {
        eprintln!("error: {message}");
        eprintln!("{}", usage());
        std::process::exit(if message.starts_with("--") || message.contains("required") {
            2
        } else {
            1
        });
    }
}

#[cfg(test)]
mod tests {
    use std::io::Write;

    use super::*;

    #[test]
    fn parses_quarters() {
        assert_eq!(parse_quarter("2023q1").unwrap(), (2023, 1));
        assert_eq!(parse_quarter("2023Q4").unwrap(), (2023, 4));
        assert!(parse_quarter("2023").is_err());
        assert!(parse_quarter("2023q5").is_err());
        assert!(parse_quarter("q1").is_err());
    }

    #[test]
    fn quarter_ranges_cross_year_boundaries() {
        assert_eq!(
            quarter_range((2022, 3), (2023, 2)).unwrap(),
            vec![(2022, 3), (2022, 4), (2023, 1), (2023, 2)]
        );
        assert!(quarter_range((2023, 2), (2023, 1)).is_err());
    }

    #[test]
    fn parses_sec_dates() {
        assert_eq!(parse_sec_date("30-JUN-2023").as_deref(), Some("2023-06-30"));
        assert_eq!(parse_sec_date("2023-06-30").as_deref(), Some("2023-06-30"));
        assert_eq!(parse_sec_date("31-JUN-2023"), None, "June has 30 days");
        assert_eq!(parse_sec_date("garbage"), None);
    }

    /// Build a minimal quarterly zip covering the join and every drop rule.
    fn write_test_zip(path: &Path) {
        let file = File::create(path).unwrap();
        let mut zip = zip::ZipWriter::new(file);
        let options = zip::write::SimpleFileOptions::default();

        zip.start_file("SUBMISSION.tsv", options).unwrap();
        zip.write_all(
            b"ACCESSION_NUMBER\tFILING_DATE\tDOCUMENT_TYPE\tISSUERTRADINGSYMBOL\n\
              acc-1\t06-JAN-2023\t4\taapl\n\
              acc-2\t09-JAN-2023\t4/A\tAAPL\n\
              acc-3\t10-JAN-2023\t4\tNONE\n\
              acc-4\t11-JAN-2023\t4\t\"\"MSFT\"\"\n",
        )
        .unwrap();

        zip.start_file("REPORTINGOWNER.tsv", options).unwrap();
        zip.write_all(
            b"ACCESSION_NUMBER\tRPTOWNERNAME\tRPTOWNER_RELATIONSHIP\tRPTOWNER_TITLE\n\
              acc-1\tDOE JANE\tOfficer\tCEO\n\
              acc-1\tBIG FUND\tDirector,TenPercentOwner\t\n",
        )
        .unwrap();

        zip.start_file("NONDERIV_TRANS.tsv", options).unwrap();
        zip.write_all(
            b"ACCESSION_NUMBER\tTRANS_DATE\tTRANS_CODE\tTRANS_SHARES\tTRANS_PRICEPERSHARE\tSHRS_OWND_FOLWNG_TRANS\n\
              acc-1\t04-JAN-2023\tP\t1000\t125.5\t5000\n\
              acc-1\t04-JAN-2023\tA\t99\t1\t5099\n\
              acc-1\t05-JAN-2023\tS\t10\t\t4990\n\
              acc-2\t06-JAN-2023\tP\t1\t1\t1\n\
              acc-4\t10-JAN-2023\tS\t20\t250\t0\n",
        )
        .unwrap();
        zip.finish().unwrap();
    }

    #[test]
    fn extracts_joined_open_market_records() {
        let dir = tempfile::tempdir().unwrap();
        let zip_path = dir.path().join("2023q1_form345.zip");
        write_test_zip(&zip_path);

        let mut stats = Stats::default();
        let records = extract_records(&zip_path, None, &mut stats).unwrap();

        // acc-1 P survives; acc-1 A (grant) and footnote-priced S are
        // dropped; acc-2 is an amendment; acc-3 has no ticker; acc-4 S
        // survives with no owner info and a quote-wrapped symbol.
        assert_eq!(records.len(), 2);
        let buy = &records[0];
        assert_eq!(buy.ticker, "AAPL", "ticker is uppercased");
        assert_eq!(buy.filing_date, "2023-01-06");
        assert_eq!(buy.trans_date.as_deref(), Some("2023-01-04"));
        assert_eq!(buy.code, "P");
        assert_eq!(buy.value, 125_500.0);
        assert_eq!(buy.owner_name.as_deref(), Some("DOE JANE"), "first owner wins");
        assert!(buy.is_officer && buy.is_director && buy.is_ten_pct_owner, "flags are OR-ed");
        assert_eq!(buy.officer_title.as_deref(), Some("CEO"));
        assert_eq!(buy.shares_owned_after, Some(5000.0));

        let sell = &records[1];
        assert_eq!(
            (sell.ticker.as_str(), sell.code.as_str()),
            ("MSFT", "S"),
            "quote-wrapped symbols are unwrapped"
        );
        assert_eq!(sell.owner_name, None);
        assert!(!sell.is_officer);

        assert_eq!(stats.submissions, 4);
        assert_eq!(stats.transactions, 5);
        assert_eq!(stats.dropped_no_ticker, 1);
        assert_eq!(stats.dropped_no_amount, 1);
    }

    #[test]
    fn ticker_filter_drops_other_issuers() {
        let dir = tempfile::tempdir().unwrap();
        let zip_path = dir.path().join("2023q1_form345.zip");
        write_test_zip(&zip_path);

        let wanted: HashSet<String> = ["MSFT".to_string()].into();
        let mut stats = Stats::default();
        let records = extract_records(&zip_path, Some(&wanted), &mut stats).unwrap();
        assert_eq!(records.len(), 1);
        assert_eq!(records[0].ticker, "MSFT");
    }

    #[test]
    fn missing_column_fails_loudly() {
        let dir = tempfile::tempdir().unwrap();
        let zip_path = dir.path().join("bad.zip");
        let file = File::create(&zip_path).unwrap();
        let mut zip = zip::ZipWriter::new(file);
        zip.start_file("SUBMISSION.tsv", zip::write::SimpleFileOptions::default()).unwrap();
        zip.write_all(b"ACCESSION_NUMBER\tFILING_DATE\n").unwrap();
        zip.finish().unwrap();

        let err = extract_records(&zip_path, None, &mut Stats::default()).unwrap_err();
        assert!(err.contains("DOCUMENT_TYPE"), "error names the missing column: {err}");
    }
}

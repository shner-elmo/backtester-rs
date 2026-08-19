//! Insider-transaction loading and the copy-trading pattern it enables,
//! tested against synthetic Parquet + `insider_transactions.json` fixtures
//! written into a tempdir. The key property under test: signals fire on the
//! first trading day strictly *after* the Form 4 `filing_date` (no
//! lookahead), never on it.

use std::{
    collections::{BTreeMap, HashMap, HashSet},
    fs,
    path::Path,
    sync::Arc,
};

use arrow::{
    array::{Float64Array, TimestampNanosecondArray, UInt16Array, UInt32Array, UInt8Array},
    datatypes::{DataType, Field, Schema, TimeUnit},
    record_batch::RecordBatch,
};
use backtester::{
    data::load_ticker_map,
    insider::{load_insider_transactions, load_insider_transactions_from, TransCode},
    run_backtest, Algorithm, BacktestResult, Context, Slice, Symbol, SymbolMap,
};
use chrono::{NaiveDate, TimeZone, Utc};
use chrono_tz::US::Eastern;
use parquet::arrow::ArrowWriter;

/// One synthetic minute bar: (ticker id, date, minute-of-day offset, price).
/// o=h=l=c=price, volume=100, main session; bars sit at 15:00+offset UTC
/// (= 11:00 ET, so UTC and ET dates agree).
struct Row {
    ticker: u16,
    date: NaiveDate,
    minute: u32,
    price: f64,
}

fn row(ticker: u16, y: i32, m: u32, d: u32, minute: u32, price: f64) -> Row {
    Row { ticker, date: NaiveDate::from_ymd_opt(y, m, d).unwrap(), minute, price }
}

fn write_fixture(root: &Path, rows: &[Row], tickers: &[(u16, &str)], insider_json: &str) {
    let tickers_json: String = format!(
        "{{{}}}",
        tickers
            .iter()
            .map(|(id, sym)| format!("\"{id}\": \"{sym}\""))
            .collect::<Vec<_>>()
            .join(", ")
    );
    fs::write(root.join("encoded_tickers.json"), tickers_json).unwrap();
    fs::write(root.join("insider_transactions.json"), insider_json).unwrap();

    let schema = Arc::new(Schema::new(vec![
        Field::new("ticker", DataType::UInt16, false),
        Field::new("volume", DataType::UInt32, false),
        Field::new("open", DataType::Float64, false),
        Field::new("close", DataType::Float64, false),
        Field::new("high", DataType::Float64, false),
        Field::new("low", DataType::Float64, false),
        Field::new("window_start", DataType::Timestamp(TimeUnit::Nanosecond, None), false),
        Field::new("market_session", DataType::UInt8, false),
    ]));

    let ts: Vec<i64> = rows
        .iter()
        .map(|r| {
            let t =
                r.date.and_hms_opt(15, 0, 0).unwrap() + chrono::Duration::minutes(r.minute as i64);
            Utc.from_utc_datetime(&t).timestamp_nanos_opt().unwrap()
        })
        .collect();
    let prices: Vec<f64> = rows.iter().map(|r| r.price).collect();
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(UInt16Array::from(rows.iter().map(|r| r.ticker).collect::<Vec<_>>())),
            Arc::new(UInt32Array::from(vec![100u32; rows.len()])),
            Arc::new(Float64Array::from(prices.clone())),
            Arc::new(Float64Array::from(prices.clone())),
            Arc::new(Float64Array::from(prices.clone())),
            Arc::new(Float64Array::from(prices)),
            Arc::new(TimestampNanosecondArray::from(ts)),
            Arc::new(UInt8Array::from(vec![2u8; rows.len()])),
        ],
    )
    .unwrap();

    let dir = root.join("minute/year=2023/month=6");
    fs::create_dir_all(&dir).unwrap();
    let file = fs::File::create(dir.join("part-0.parquet")).unwrap();
    let mut writer = ArrowWriter::try_new(file, schema, None).unwrap();
    writer.write(&batch).unwrap();
    writer.close().unwrap();
}

fn identity_error(r: &BacktestResult) -> f64 {
    let realized: f64 = r.trades.iter().map(|t| t.pnl).sum();
    let open: f64 = r.open_positions.iter().map(|p| p.unrealized_pnl + p.realized_pnl).sum();
    (r.initial_cash + realized + open - r.final_equity).abs()
}

/// Weekdays 2023-06-05 (Mon) .. 2023-06-16 (Fri), two bars per day for every
/// ticker so the clock keeps ticking whether or not a position is open.
const TRADING_DAYS: [u32; 10] = [5, 6, 7, 8, 9, 12, 13, 14, 15, 16];

/// Minute is the outer loop: the engine streams a file without re-sorting it,
/// so rows have to leave here in timestamp order across all tickers.
fn bars_for(tickers: &[u16]) -> Vec<Row> {
    let mut rows = Vec::new();
    for &day in &TRADING_DAYS {
        for minute in 0..2 {
            for &ticker in tickers {
                rows.push(row(ticker, 2023, 6, day, minute, 100.0));
            }
        }
    }
    rows
}

/// A minimal copy-trading strategy over the loaded transactions, with the
/// thresholds as fields so each test can pin them.
struct Follower {
    data_root: String,
    min_buy: f64,
    min_sell: f64,
    max_positions: usize,
    hold_days: usize,
    max_age_days: i64,
    buy_signals: BTreeMap<NaiveDate, Vec<Symbol>>,
    sell_signals: BTreeMap<NaiveDate, Vec<Symbol>>,
    held: SymbolMap<usize>,
    last_date: Option<NaiveDate>,
}

impl Follower {
    fn new(data_root: &str) -> Self {
        Self {
            data_root: data_root.to_string(),
            min_buy: 100_000.0,
            min_sell: 50_000.0,
            max_positions: 10,
            hold_days: 100,
            max_age_days: 5,
            buy_signals: BTreeMap::new(),
            sell_signals: BTreeMap::new(),
            held: SymbolMap::default(),
            last_date: None,
        }
    }

    fn on_day_open(&mut self, ctx: &mut Context, today: NaiveDate) {
        for days in self.held.values_mut() {
            *days += 1;
        }

        let not_yet_due = self.sell_signals.split_off(&today);
        let due = std::mem::replace(&mut self.sell_signals, not_yet_due);
        for symbol in due.into_values().flatten() {
            if self.held.remove(&symbol).is_some() {
                ctx.liquidate(symbol);
            }
        }
        let hold_days = self.hold_days;
        self.held.retain(|symbol, days| {
            let expired = *days >= hold_days;
            if expired {
                ctx.liquidate(*symbol);
            }
            !expired
        });

        let not_yet_due = self.buy_signals.split_off(&today);
        let due = std::mem::replace(&mut self.buy_signals, not_yet_due);
        for (filing_date, symbols) in due {
            if (today - filing_date).num_days() > self.max_age_days {
                continue;
            }
            for symbol in symbols {
                if self.held.len() >= self.max_positions {
                    return;
                }
                if let std::collections::hash_map::Entry::Vacant(entry) = self.held.entry(symbol) {
                    ctx.set_holdings(symbol, 1.0 / self.max_positions as f64);
                    entry.insert(0);
                }
            }
        }
    }
}

impl Algorithm for Follower {
    fn initialize(&mut self, ctx: &mut Context) {
        ctx.set_cash(100_000.0);

        let universe: HashSet<String> =
            load_ticker_map(&self.data_root).unwrap().into_values().collect();
        let transactions = load_insider_transactions(&self.data_root, &universe).unwrap();

        let mut buy_tickers: BTreeMap<NaiveDate, Vec<String>> = BTreeMap::new();
        let mut sell_tickers: BTreeMap<NaiveDate, Vec<String>> = BTreeMap::new();
        for (ticker, by_date) in transactions {
            for (filing_date, txs) in by_date {
                let buys: f64 = txs
                    .iter()
                    .filter(|t| t.code == TransCode::Purchase && (t.is_officer || t.is_director))
                    .map(|t| t.value)
                    .sum();
                let sells: f64 =
                    txs.iter().filter(|t| t.code == TransCode::Sale).map(|t| t.value).sum();
                if buys >= self.min_buy {
                    buy_tickers.entry(filing_date).or_default().push(ticker.clone());
                }
                if sells >= self.min_sell {
                    sell_tickers.entry(filing_date).or_default().push(ticker.clone());
                }
            }
        }

        // Subscribe the whole fixture universe, keeping each returned
        // `Symbol` so the signal maps hold dataset ids rather than strings.
        let mut ids: HashMap<String, Symbol> = HashMap::new();
        for ticker in &universe {
            if let Some(symbol) = ctx.try_add_equity(ticker) {
                ids.insert(ticker.clone(), symbol);
            }
        }
        let resolve = |by_date: BTreeMap<NaiveDate, Vec<String>>| {
            by_date
                .into_iter()
                .map(|(date, tickers)| {
                    (date, tickers.iter().filter_map(|t| ids.get(t).copied()).collect())
                })
                .collect()
        };
        self.buy_signals = resolve(buy_tickers);
        self.sell_signals = resolve(sell_tickers);
    }

    fn on_data(&mut self, ctx: &mut Context, data: &Slice) {
        let today = data.time.with_timezone(&Eastern).date_naive();
        if self.last_date != Some(today) {
            self.last_date = Some(today);
            self.on_day_open(ctx, today);
        }
    }
}

fn buy_record(ticker: &str, filing_date: &str, value: f64) -> String {
    format!(
        r#"{{"filing_date": "{filing_date}", "ticker": "{ticker}", "code": "P",
            "shares": {}, "price": 100.0, "value": {value}, "is_officer": true}}"#,
        value / 100.0
    )
}

fn sell_record(ticker: &str, filing_date: &str, value: f64) -> String {
    format!(
        r#"{{"filing_date": "{filing_date}", "ticker": "{ticker}", "code": "S",
            "shares": {}, "price": 100.0, "value": {value}, "is_officer": true}}"#,
        value / 100.0
    )
}

#[test]
fn buys_on_the_day_after_the_filing_not_the_filing_day() {
    let tmp = tempfile::tempdir().unwrap();
    // Buy filed Tue 06-06 with bars available that same day: the entry must
    // still wait for Wed 06-07. A sale filing later forces a round trip so
    // the entry shows up in `trades`.
    let json = format!(
        "[{}, {}]",
        buy_record("INSD", "2023-06-06", 500_000.0),
        sell_record("INSD", "2023-06-13", 200_000.0)
    );
    write_fixture(tmp.path(), &bars_for(&[1]), &[(1, "INSD")], &json);

    let result =
        run_backtest(Follower::new(tmp.path().to_str().unwrap()), tmp.path().to_str().unwrap())
            .unwrap();

    assert_eq!(result.trades.len(), 1);
    let trade = &result.trades[0];
    assert_eq!(trade.symbol, "INSD");
    assert!(
        trade.entry_time.starts_with("2023-06-07"),
        "entered {} instead of the day after the filing",
        trade.entry_time
    );
    assert!(identity_error(&result) < 1e-6);
}

#[test]
fn exits_on_insider_sale_filing_the_next_trading_day() {
    let tmp = tempfile::tempdir().unwrap();
    let json = format!(
        "[{}, {}]",
        buy_record("INSD", "2023-06-06", 500_000.0),
        sell_record("INSD", "2023-06-12", 200_000.0)
    );
    write_fixture(tmp.path(), &bars_for(&[1]), &[(1, "INSD")], &json);

    let result =
        run_backtest(Follower::new(tmp.path().to_str().unwrap()), tmp.path().to_str().unwrap())
            .unwrap();

    assert_eq!(result.trades.len(), 1);
    assert!(
        result.trades[0].exit_time.starts_with("2023-06-13"),
        "exited {} instead of the day after the sale filing",
        result.trades[0].exit_time
    );
    assert!(result.open_positions.is_empty());
    assert!(identity_error(&result) < 1e-6);
}

#[test]
fn exits_after_hold_days_without_a_sale_filing() {
    let tmp = tempfile::tempdir().unwrap();
    let json = format!("[{}]", buy_record("INSD", "2023-06-06", 500_000.0));
    write_fixture(tmp.path(), &bars_for(&[1]), &[(1, "INSD")], &json);

    let mut algo = Follower::new(tmp.path().to_str().unwrap());
    algo.hold_days = 3;
    let result = run_backtest(algo, tmp.path().to_str().unwrap()).unwrap();

    // Entry Wed 06-07 (day 0); the count hits 3 at the open of Mon 06-12.
    assert_eq!(result.trades.len(), 1);
    assert!(result.trades[0].entry_time.starts_with("2023-06-07"));
    assert!(
        result.trades[0].exit_time.starts_with("2023-06-12"),
        "exited {} instead of after 3 trading days",
        result.trades[0].exit_time
    );
    assert!(identity_error(&result) < 1e-6);
}

#[test]
fn weekend_filing_enters_monday_and_stale_signals_are_dropped() {
    let tmp = tempfile::tempdir().unwrap();
    // FRI filed Fri 06-09 → first chance is Mon 06-12. OLDD filed 05-25,
    // 11 calendar days before the first trading day — stale, never traded.
    let json = format!(
        "[{}, {}]",
        buy_record("FRID", "2023-06-09", 500_000.0),
        buy_record("OLDD", "2023-05-25", 500_000.0)
    );
    write_fixture(tmp.path(), &bars_for(&[1, 2]), &[(1, "FRID"), (2, "OLDD")], &json);

    let result =
        run_backtest(Follower::new(tmp.path().to_str().unwrap()), tmp.path().to_str().unwrap())
            .unwrap();

    assert!(result.trades.is_empty(), "no round trips expected: {:?}", result.trades);
    assert_eq!(result.open_positions.len(), 1, "only FRID should be held");
    assert_eq!(result.open_positions[0].symbol, "FRID");
    assert!(identity_error(&result) < 1e-6);
}

#[test]
fn ignores_unmapped_tickers_and_below_threshold_buys() {
    let tmp = tempfile::tempdir().unwrap();
    // ZZZZ has a huge buy but no entry in the ticker map; CHEP is mapped but
    // its buy is below the threshold. Neither may trade.
    let json = format!(
        "[{}, {}]",
        buy_record("ZZZZ", "2023-06-06", 9_000_000.0),
        buy_record("CHEP", "2023-06-06", 10_000.0)
    );
    write_fixture(tmp.path(), &bars_for(&[1]), &[(1, "CHEP")], &json);

    let result =
        run_backtest(Follower::new(tmp.path().to_str().unwrap()), tmp.path().to_str().unwrap())
            .unwrap();

    assert!(result.trades.is_empty());
    assert!(result.open_positions.is_empty());
    assert_eq!(result.final_equity, result.initial_cash);
    assert!(identity_error(&result) < 1e-6);
}

/// The committed slice of genuine `insider-fetch` output: every open-market
/// Form 4 transaction for ten large-cap issuers, 2020-01-06 .. 2026-03-23.
/// Synthetic fixtures can only prove the loader handles the shapes we thought
/// to write down; this proves it handles what the SEC actually publishes.
const SAMPLE_TICKERS: [&str; 10] =
    ["AAPL", "MSFT", "JPM", "INTC", "XOM", "PFE", "BA", "F", "GE", "T"];

fn sample_path() -> std::path::PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("tests/fixtures/insider_sample/insider_transactions.json")
}

fn count(map: &backtester::insider::InsiderMap) -> usize {
    map.values().flat_map(|by_date| by_date.values()).map(Vec::len).sum()
}

#[test]
fn parses_the_committed_sample_of_real_sec_output() {
    let universe: HashSet<String> = SAMPLE_TICKERS.iter().map(|t| t.to_string()).collect();
    let map = load_insider_transactions_from(&sample_path(), &universe, true).unwrap();

    assert_eq!(map.len(), SAMPLE_TICKERS.len(), "every issuer in the slice was kept");
    assert_eq!(count(&map), 965);

    let all: Vec<_> = map.values().flat_map(|by_date| by_date.values()).flatten().collect();
    let purchases = all.iter().filter(|t| t.code == TransCode::Purchase).count();
    assert_eq!((purchases, all.len() - purchases), (158, 807), "P/S split");

    // The property every insider strategy leans on: a filing never becomes
    // public before the trade it reports.
    assert!(all.iter().all(|t| t.trans_date.is_none_or(|d| d <= t.filing_date)));
    // Real filings leave optional fields empty; they have to survive as None
    // rather than failing the record.
    assert_eq!(all.iter().filter(|t| t.officer_title.is_none()).count(), 118);
    assert!(all.iter().all(|t| t.value > 0.0 && t.shares > 0.0 && t.price > 0.0));

    // Transactions land under the date the filing became public, not the
    // trade date, and each issuer's dates come back in order.
    let intc = &map["INTC"];
    assert!(intc.keys().zip(intc.keys().skip(1)).all(|(a, b)| a < b));
    let intc_buys = intc.values().flatten().filter(|t| t.code == TransCode::Purchase).count();
    assert_eq!(intc_buys, 52);
}

#[test]
fn sample_load_filters_to_the_requested_issuers() {
    let universe: HashSet<String> = ["AAPL".to_string()].into();
    let map = load_insider_transactions_from(&sample_path(), &universe, true).unwrap();

    assert_eq!(map.keys().collect::<Vec<_>>(), vec!["AAPL"]);
    assert_eq!(count(&map), 207);
}

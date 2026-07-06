//! Split handling and delist liquidation, tested against synthetic Parquet
//! fixtures written into a tempdir (same schema as the real dataset).

use std::fs;
use std::path::Path;
use std::sync::{Arc, Mutex};

use arrow::array::{Float64Array, TimestampNanosecondArray, UInt16Array, UInt32Array, UInt8Array};
use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use arrow::record_batch::RecordBatch;
use chrono::{NaiveDate, TimeZone, Utc};
use parquet::arrow::ArrowWriter;

use backtester::{run_backtest, Algorithm, BacktestResult, Context, Slice};

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

fn write_fixture(
    root: &Path,
    rows: &[Row],
    tickers: &[(u16, &str)],
    splits_json: Option<&str>,
    dividends_json: Option<&str>,
) {
    let tickers_json: String = format!(
        "{{{}}}",
        tickers
            .iter()
            .map(|(id, sym)| format!("\"{id}\": \"{sym}\""))
            .collect::<Vec<_>>()
            .join(", ")
    );
    fs::write(root.join("encoded_tickers.json"), tickers_json).unwrap();
    if let Some(s) = splits_json {
        fs::write(root.join("get_splits.json"), s).unwrap();
    }
    if let Some(d) = dividends_json {
        fs::write(root.join("get_dividends.json"), d).unwrap();
    }

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

/// Buys a fixed quantity of one symbol on the first bar, then holds. Records
/// every on_split / on_delisted call and the history closes seen on the last
/// on_data call.
struct BuyAndHold {
    symbol: String,
    qty: f64,
    bought: bool,
    splits: Arc<Mutex<Vec<(String, f64)>>>,
    delistings: Arc<Mutex<Vec<String>>>,
    dividends: Arc<Mutex<Vec<(String, f64)>>>,
    last_history: Arc<Mutex<Vec<f64>>>,
}

impl BuyAndHold {
    fn new(symbol: &str, qty: f64) -> Self {
        Self {
            symbol: symbol.into(),
            qty,
            bought: false,
            splits: Arc::new(Mutex::new(Vec::new())),
            delistings: Arc::new(Mutex::new(Vec::new())),
            dividends: Arc::new(Mutex::new(Vec::new())),
            last_history: Arc::new(Mutex::new(Vec::new())),
        }
    }
}

impl Algorithm for BuyAndHold {
    fn initialize(&mut self, ctx: &mut Context) {
        ctx.set_cash(100_000.0);
        ctx.add_equity(&self.symbol.clone());
        ctx.add_equity("STAY"); // second symbol keeps the clock ticking
    }

    fn on_data(&mut self, ctx: &mut Context, data: &Slice) {
        if !self.bought && data.bars.contains_key(&self.symbol) {
            self.bought = true;
            ctx.market_order(&self.symbol.clone(), self.qty);
        }
        let hist: Vec<f64> = ctx.history(&self.symbol, 100).iter().map(|b| b.close).collect();
        *self.last_history.lock().unwrap() = hist;
    }

    fn on_split(&mut self, _ctx: &mut Context, symbol: &str, ratio: f64) {
        self.splits.lock().unwrap().push((symbol.to_string(), ratio));
    }

    fn on_delisted(&mut self, _ctx: &mut Context, symbol: &str) {
        self.delistings.lock().unwrap().push(symbol.to_string());
    }

    fn on_dividend(&mut self, _ctx: &mut Context, symbol: &str, amount: f64) {
        self.dividends.lock().unwrap().push((symbol.to_string(), amount));
    }
}

/// Weekdays 2023-06-05 (Mon) .. 2023-06-16 (Fri), two bars per day.
const TRADING_DAYS: [u32; 10] = [5, 6, 7, 8, 9, 12, 13, 14, 15, 16];

fn days_of(symbol_id: u16, days: &[u32], price: impl Fn(u32) -> f64) -> Vec<Row> {
    days.iter()
        .flat_map(|&d| {
            [row(symbol_id, 2023, 6, d, 0, price(d)), row(symbol_id, 2023, 6, d, 1, price(d))]
        })
        .collect()
}

#[test]
fn forward_split_adjusts_position_history_and_keeps_equity_flat() {
    let tmp = tempfile::tempdir().unwrap();
    // SPLT trades at 90 through 06-06, splits 1->3 on 06-07, trades at 30 after.
    let mut rows = days_of(1, &TRADING_DAYS, |d| if d < 7 { 90.0 } else { 30.0 });
    rows.extend(days_of(2, &TRADING_DAYS, |_| 10.0));
    write_fixture(
        tmp.path(),
        &rows,
        &[(1, "SPLT"), (2, "STAY")],
        Some(
            r#"[{"execution_date": "2023-06-07", "id": "x", "split_from": 1, "split_to": 3, "ticker": "SPLT"}]"#,
        ),
        None,
    );

    let algo = BuyAndHold::new("SPLT", 10.0);
    let splits = algo.splits.clone();
    let history = algo.last_history.clone();
    let result = run_backtest(algo, tmp.path().to_str().unwrap()).unwrap();

    // The callback fired exactly once with the ratio.
    assert_eq!(*splits.lock().unwrap(), vec![("SPLT".to_string(), 3.0)]);

    // 10 shares @ 90 became 30 shares @ 30 — same value, no trade emitted.
    assert!(result.trades.is_empty(), "split must not emit a trade: {:?}", result.trades);
    let pos = result.open_positions.iter().find(|p| p.symbol == "SPLT").unwrap();
    assert!((pos.quantity - 30.0).abs() < 1e-9);
    assert!((pos.avg_price - 30.0).abs() < 1e-9);

    // Equity never moves: prices were flat apart from the split itself.
    for p in &result.equity_curve {
        assert!(
            (p.equity - 100_000.0).abs() < 1e-6,
            "equity jumped across the split: {} = {}",
            p.time,
            p.equity
        );
    }
    assert!(identity_error(&result) < 1e-6);

    // ctx.history is fully rescaled into post-split terms.
    let hist = history.lock().unwrap();
    assert!(!hist.is_empty());
    assert!(hist.iter().all(|&c| (c - 30.0).abs() < 1e-9), "history not rescaled: {:?}", *hist);
}

#[test]
fn reverse_split_cashes_out_a_fractional_position_in_lieu() {
    let tmp = tempfile::tempdir().unwrap();
    // TINY trades at 10, reverse splits 10->1 on 06-07 (price becomes 100).
    let mut rows = days_of(1, &TRADING_DAYS, |d| if d < 7 { 10.0 } else { 100.0 });
    rows.extend(days_of(2, &TRADING_DAYS, |_| 10.0));
    write_fixture(
        tmp.path(),
        &rows,
        &[(1, "TINY"), (2, "STAY")],
        Some(
            r#"[{"execution_date": "2023-06-07", "id": "x", "split_from": 10, "split_to": 1, "ticker": "TINY"}]"#,
        ),
        None,
    );

    // 5 shares * 0.1 = 0.5 shares -> rounds to 0 against the whole-share lot;
    // the entire position is cashed out in lieu.
    let algo = BuyAndHold::new("TINY", 5.0);
    let result = run_backtest(algo, tmp.path().to_str().unwrap()).unwrap();

    assert!(result.open_positions.iter().all(|p| p.symbol != "TINY"));
    assert_eq!(result.trades.len(), 1);
    let t = &result.trades[0];
    assert_eq!(t.exit_reason, "split");
    // Bought 5 @ 10, cashed out 0.5 @ 100: zero PnL, equity intact.
    assert!(t.pnl.abs() < 1e-9);
    assert!((result.final_equity - 100_000.0).abs() < 1e-6);
    assert!(identity_error(&result) < 1e-6);
}

#[test]
fn silent_symbol_is_force_liquidated_as_delisted() {
    let tmp = tempfile::tempdir().unwrap();
    // GONE trades 06-05..06-07 at 20, then vanishes; STAY keeps the clock
    // running through 06-16.
    let mut rows = days_of(1, &[5, 6, 7], |_| 20.0);
    rows.extend(days_of(2, &TRADING_DAYS, |_| 10.0));
    write_fixture(tmp.path(), &rows, &[(1, "GONE"), (2, "STAY")], None, None);

    let algo = BuyAndHold::new("GONE", 10.0);
    let delistings = algo.delistings.clone();
    let result = run_backtest(algo, tmp.path().to_str().unwrap()).unwrap();

    assert_eq!(*delistings.lock().unwrap(), vec!["GONE".to_string()]);
    assert!(result.open_positions.iter().all(|p| p.symbol != "GONE"));
    assert_eq!(result.trades.len(), 1);
    let t = &result.trades[0];
    assert_eq!(t.exit_reason, "delisted");
    // Closed at the last known price (20), where it was also bought: flat PnL.
    assert!((t.exit_price - 20.0).abs() < 1e-9);
    assert!(t.pnl.abs() < 1e-9);
    // GONE last traded on day index of 06-07; the 5th silent trading day is
    // 06-14.
    assert!(t.exit_time.starts_with("2023-06-14"));
    assert!(identity_error(&result) < 1e-6);
}

#[test]
fn delist_scan_can_be_disabled() {
    let tmp = tempfile::tempdir().unwrap();
    let mut rows = days_of(1, &[5, 6, 7], |_| 20.0);
    rows.extend(days_of(2, &TRADING_DAYS, |_| 10.0));
    write_fixture(tmp.path(), &rows, &[(1, "GONE"), (2, "STAY")], None, None);

    struct NoDelist(BuyAndHold);
    impl Algorithm for NoDelist {
        fn initialize(&mut self, ctx: &mut Context) {
            self.0.initialize(ctx);
            ctx.set_delist_after_days(0);
        }
        fn on_data(&mut self, ctx: &mut Context, data: &Slice) {
            self.0.on_data(ctx, data);
        }
    }

    let result =
        run_backtest(NoDelist(BuyAndHold::new("GONE", 10.0)), tmp.path().to_str().unwrap())
            .unwrap();
    assert!(result.trades.is_empty());
    assert!(result.open_positions.iter().any(|p| p.symbol == "GONE"));
    assert!(identity_error(&result) < 1e-6);
}

#[test]
fn cash_dividend_credits_a_held_long_position() {
    let tmp = tempfile::tempdir().unwrap();
    // DIV trades flat at 100; STAY keeps the clock running. A $2.00/share
    // dividend goes ex on 06-08, while we hold 10 shares bought on 06-05.
    let mut rows = days_of(1, &TRADING_DAYS, |_| 100.0);
    rows.extend(days_of(2, &TRADING_DAYS, |_| 10.0));
    write_fixture(
        tmp.path(),
        &rows,
        &[(1, "DIV"), (2, "STAY")],
        None,
        Some(r#"[{"ex_dividend_date": "2023-06-08", "cash_amount": 2.0, "ticker": "DIV"}]"#),
    );

    let algo = BuyAndHold::new("DIV", 10.0);
    let dividends = algo.dividends.clone();
    let result = run_backtest(algo, tmp.path().to_str().unwrap()).unwrap();

    // The callback fired once with the per-share amount.
    assert_eq!(*dividends.lock().unwrap(), vec![("DIV".to_string(), 2.0)]);
    // 10 shares * $2 = $20 income, attributed to the still-open position and
    // credited to cash, so equity rises by exactly the dividend.
    let pos = result.open_positions.iter().find(|p| p.symbol == "DIV").unwrap();
    assert!((pos.realized_pnl - 20.0).abs() < 1e-9);
    assert!((result.final_equity - 100_020.0).abs() < 1e-6);
    assert!(identity_error(&result) < 1e-6);
}

#[test]
fn cash_dividend_debits_a_held_short_position() {
    let tmp = tempfile::tempdir().unwrap();
    let mut rows = days_of(1, &TRADING_DAYS, |_| 100.0);
    rows.extend(days_of(2, &TRADING_DAYS, |_| 10.0));
    write_fixture(
        tmp.path(),
        &rows,
        &[(1, "DIV"), (2, "STAY")],
        None,
        Some(r#"[{"ex_dividend_date": "2023-06-08", "cash_amount": 2.0, "ticker": "DIV"}]"#),
    );

    // Short 10 shares: a dividend on a short is a cash *debit* of qty*amount.
    let algo = BuyAndHold::new("DIV", -10.0);
    let result = run_backtest(algo, tmp.path().to_str().unwrap()).unwrap();

    let pos = result.open_positions.iter().find(|p| p.symbol == "DIV").unwrap();
    assert!((pos.realized_pnl - -20.0).abs() < 1e-9);
    assert!((result.final_equity - 99_980.0).abs() < 1e-6);
    assert!(identity_error(&result) < 1e-6);
}

#[test]
fn dividend_is_not_paid_when_the_symbol_is_not_held() {
    let tmp = tempfile::tempdir().unwrap();
    let mut rows = days_of(1, &TRADING_DAYS, |_| 100.0);
    rows.extend(days_of(2, &TRADING_DAYS, |_| 10.0));
    write_fixture(
        tmp.path(),
        &rows,
        &[(1, "DIV"), (2, "STAY")],
        None,
        Some(r#"[{"ex_dividend_date": "2023-06-08", "cash_amount": 2.0, "ticker": "DIV"}]"#),
    );

    // Subscribes DIV (so the dividend loads) but never opens a position in it.
    struct Idle;
    impl Algorithm for Idle {
        fn initialize(&mut self, ctx: &mut Context) {
            ctx.set_cash(100_000.0);
            ctx.add_equity("DIV");
            ctx.add_equity("STAY");
        }
        fn on_data(&mut self, _ctx: &mut Context, _data: &Slice) {}
    }

    let result = run_backtest(Idle, tmp.path().to_str().unwrap()).unwrap();
    // No position on the ex-date means no cash flow at all.
    assert!(result.trades.is_empty());
    assert!(result.open_positions.is_empty());
    assert!((result.final_equity - 100_000.0).abs() < 1e-9);
}

#[test]
fn delist_haircut_writes_down_the_forced_liquidation() {
    let tmp = tempfile::tempdir().unwrap();
    // GONE trades at 20 through 06-07 then vanishes; a 25% haircut recovers
    // 15/share on the forced liquidation instead of the last print of 20.
    let mut rows = days_of(1, &[5, 6, 7], |_| 20.0);
    rows.extend(days_of(2, &TRADING_DAYS, |_| 10.0));
    write_fixture(tmp.path(), &rows, &[(1, "GONE"), (2, "STAY")], None, None);

    struct Haircut(BuyAndHold);
    impl Algorithm for Haircut {
        fn initialize(&mut self, ctx: &mut Context) {
            self.0.initialize(ctx);
            ctx.set_delist_haircut(0.25);
        }
        fn on_data(&mut self, ctx: &mut Context, data: &Slice) {
            self.0.on_data(ctx, data);
        }
    }

    let result =
        run_backtest(Haircut(BuyAndHold::new("GONE", 10.0)), tmp.path().to_str().unwrap()).unwrap();

    assert_eq!(result.trades.len(), 1);
    let t = &result.trades[0];
    assert_eq!(t.exit_reason, "delisted");
    // Filled at 20 * (1 - 0.25) = 15, a 5/share loss on the 10-share position.
    assert!((t.exit_price - 15.0).abs() < 1e-9);
    assert!((t.pnl - -50.0).abs() < 1e-9);
    assert!(identity_error(&result) < 1e-6);
}

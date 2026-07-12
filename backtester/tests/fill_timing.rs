//! Fill-timing semantics: the default fills at the current bar's close, while
//! `FillTiming::NextBarOpen` fills at the open of the symbol's following bar.
//! Uses a synthetic fixture whose open and close differ every bar, so the two
//! models produce distinguishable fill prices.

use std::fs;
use std::path::Path;
use std::sync::Arc;

use arrow::array::{Float64Array, TimestampNanosecondArray, UInt16Array, UInt32Array};
use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use arrow::record_batch::RecordBatch;
use chrono::{NaiveDate, TimeZone, Utc};
use parquet::arrow::ArrowWriter;

use backtester::{run_backtest, Algorithm, BacktestResult, Context, FillTiming, Slice};

/// One synthetic minute bar with distinct open and close.
struct Bar {
    date: NaiveDate,
    minute: u32,
    open: f64,
    close: f64,
}

fn bar(y: i32, m: u32, d: u32, minute: u32, open: f64, close: f64) -> Bar {
    Bar { date: NaiveDate::from_ymd_opt(y, m, d).unwrap(), minute, open, close }
}

/// Write a single-symbol ("SYM", id 1) fixture. high/low bound open & close so
/// OHLC invariants hold; volume and main-session are constant.
fn write_fixture(root: &Path, bars: &[Bar]) {
    fs::write(root.join("encoded_tickers.json"), r#"{"1": "SYM"}"#).unwrap();

    let schema = Arc::new(Schema::new(vec![
        Field::new("ticker", DataType::UInt16, false),
        Field::new("volume", DataType::UInt32, false),
        Field::new("open", DataType::Float64, false),
        Field::new("close", DataType::Float64, false),
        Field::new("high", DataType::Float64, false),
        Field::new("low", DataType::Float64, false),
        Field::new("window_start", DataType::Timestamp(TimeUnit::Nanosecond, None), false),
    ]));

    let ts: Vec<i64> = bars
        .iter()
        .map(|b| {
            let t =
                b.date.and_hms_opt(15, 0, 0).unwrap() + chrono::Duration::minutes(b.minute as i64);
            Utc.from_utc_datetime(&t).timestamp_nanos_opt().unwrap()
        })
        .collect();
    let opens: Vec<f64> = bars.iter().map(|b| b.open).collect();
    let closes: Vec<f64> = bars.iter().map(|b| b.close).collect();
    let highs: Vec<f64> = bars.iter().map(|b| b.open.max(b.close)).collect();
    let lows: Vec<f64> = bars.iter().map(|b| b.open.min(b.close)).collect();

    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(UInt16Array::from(vec![1u16; bars.len()])),
            Arc::new(UInt32Array::from(vec![100u32; bars.len()])),
            Arc::new(Float64Array::from(opens)),
            Arc::new(Float64Array::from(closes)),
            Arc::new(Float64Array::from(highs)),
            Arc::new(Float64Array::from(lows)),
            Arc::new(TimestampNanosecondArray::from(ts)),
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

/// Buys 100 shares of SYM on the very first bar it sees, then holds.
struct BuyOnceThenHold {
    timing: FillTiming,
    bought: bool,
}

impl Algorithm for BuyOnceThenHold {
    fn initialize(&mut self, ctx: &mut Context) {
        ctx.set_cash(100_000.0);
        ctx.set_fill_timing(self.timing);
        ctx.add_equity("SYM");
    }

    fn on_data(&mut self, ctx: &mut Context, data: &Slice) {
        if !self.bought && data.bars.contains_key("SYM") {
            self.bought = true;
            ctx.market_order("SYM", 100.0);
        }
    }
}

/// Three bars: opens 10/20/30, closes 11/21/31.
fn three_bars(root: &Path) {
    write_fixture(
        root,
        &[
            bar(2023, 6, 5, 0, 10.0, 11.0),
            bar(2023, 6, 5, 1, 20.0, 21.0),
            bar(2023, 6, 5, 2, 30.0, 31.0),
        ],
    );
}

#[test]
fn current_bar_close_fills_at_the_same_bars_close() {
    let tmp = tempfile::tempdir().unwrap();
    three_bars(tmp.path());

    let algo = BuyOnceThenHold { timing: FillTiming::CurrentBarClose, bought: false };
    let result = run_backtest(algo, tmp.path().to_str().unwrap()).unwrap();

    let pos = result.open_positions.iter().find(|p| p.symbol == "SYM").unwrap();
    assert!((pos.quantity - 100.0).abs() < 1e-9);
    // Decided and filled on bar 1: its close is 11.
    assert!(
        (pos.avg_price - 11.0).abs() < 1e-9,
        "expected close fill at 11, got {}",
        pos.avg_price
    );
    assert!(identity_error(&result) < 1e-6);
}

#[test]
fn next_bar_open_fills_at_the_following_bars_open() {
    let tmp = tempfile::tempdir().unwrap();
    three_bars(tmp.path());

    let algo = BuyOnceThenHold { timing: FillTiming::NextBarOpen, bought: false };
    let result = run_backtest(algo, tmp.path().to_str().unwrap()).unwrap();

    let pos = result.open_positions.iter().find(|p| p.symbol == "SYM").unwrap();
    assert!((pos.quantity - 100.0).abs() < 1e-9);
    // Decided on bar 1, filled at bar 2's open: 20.
    assert!(
        (pos.avg_price - 20.0).abs() < 1e-9,
        "expected next-open fill at 20, got {}",
        pos.avg_price
    );
    assert!(identity_error(&result) < 1e-6);
}

#[test]
fn order_on_the_final_bar_never_fills_under_next_bar_open() {
    let tmp = tempfile::tempdir().unwrap();
    // Only one bar exists, so an order decided on it has no next bar to fill on.
    write_fixture(tmp.path(), &[bar(2023, 6, 5, 0, 10.0, 11.0)]);

    let algo = BuyOnceThenHold { timing: FillTiming::NextBarOpen, bought: false };
    let result = run_backtest(algo, tmp.path().to_str().unwrap()).unwrap();

    assert!(result.trades.is_empty());
    assert!(result.open_positions.is_empty(), "order with no following bar must not fill");
    assert!((result.final_equity - 100_000.0).abs() < 1e-9);
}

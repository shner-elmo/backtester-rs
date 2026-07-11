//! `set_holdings` sizing: the portfolio must be marked at every held symbol's
//! latest market price — including symbols with no bar on the tick the order
//! fills — not at their cost basis. Uses a synthetic two-symbol fixture where
//! one symbol appreciates and then goes quiet.

use std::fs;
use std::path::Path;
use std::sync::Arc;

use arrow::array::{Float64Array, TimestampNanosecondArray, UInt16Array, UInt32Array, UInt8Array};
use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use arrow::record_batch::RecordBatch;
use chrono::{NaiveDate, TimeZone, Utc};
use parquet::arrow::ArrowWriter;

use backtester::{run_backtest, Algorithm, BacktestResult, Context, Slice};

/// One synthetic minute bar: (ticker id, day-of-June-2023, minute offset,
/// price). o=h=l=c=price, volume=100, main session; bars sit at
/// 15:00+offset UTC (= 11:00 ET, so UTC and ET dates agree).
#[derive(Clone, Copy)]
struct Row {
    ticker: u16,
    day: u32,
    minute: u32,
    price: f64,
}

fn row(ticker: u16, day: u32, minute: u32, price: f64) -> Row {
    Row { ticker, day, minute, price }
}

fn write_fixture(root: &Path, rows: &[Row], tickers: &[(u16, &str)]) {
    let tickers_json: String = format!(
        "{{{}}}",
        tickers
            .iter()
            .map(|(id, sym)| format!("\"{id}\": \"{sym}\""))
            .collect::<Vec<_>>()
            .join(", ")
    );
    fs::write(root.join("encoded_tickers.json"), tickers_json).unwrap();

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

    // The engine requires files to be time-sorted (it streams rows in file
    // order and never re-sorts), so write the rows the way a real ingest
    // would, whatever order the test listed them in.
    let mut rows: Vec<Row> = rows.to_vec();
    rows.sort_by_key(|r| (r.day, r.minute));

    let ts: Vec<i64> = rows
        .iter()
        .map(|r| {
            let date = NaiveDate::from_ymd_opt(2023, 6, r.day).unwrap();
            let t =
                date.and_hms_opt(15, 0, 0).unwrap() + chrono::Duration::minutes(r.minute as i64);
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

/// Buys 100 GROW on the first bar, then rebalances OTHR to 50% on the first
/// tick where GROW produces no bar.
struct SizeAgainstStale {
    bought: bool,
    rebalanced: bool,
}

impl Algorithm for SizeAgainstStale {
    fn initialize(&mut self, ctx: &mut Context) {
        ctx.set_cash(100_000.0);
        ctx.add_equity("GROW");
        ctx.add_equity("OTHR");
    }

    fn on_data(&mut self, ctx: &mut Context, data: &Slice) {
        if !self.bought && data.bars.contains_key("GROW") {
            self.bought = true;
            ctx.market_order("GROW", 100.0);
            return;
        }
        if self.bought && !self.rebalanced && !data.bars.contains_key("GROW") {
            self.rebalanced = true;
            ctx.set_holdings("OTHR", 0.5);
        }
    }
}

#[test]
fn set_holdings_marks_held_symbols_without_a_bar_at_market_not_cost() {
    let tmp = tempfile::tempdir().unwrap();
    // GROW: bought 100 @ 100 on 06-05, doubles to 200 on 06-06, no bars on
    // 06-07. OTHR trades flat at 10 throughout. On 06-07 the portfolio is
    // 90,000 cash + 100 GROW at its last known price of 200 = 110,000, so a
    // 50% OTHR target is 5,500 shares — not the 5,000 that marking GROW at
    // its 100 cost basis would produce.
    let rows = vec![
        row(1, 5, 0, 100.0),
        row(1, 5, 1, 100.0),
        row(1, 6, 0, 200.0),
        row(1, 6, 1, 200.0),
        row(2, 5, 0, 10.0),
        row(2, 5, 1, 10.0),
        row(2, 6, 0, 10.0),
        row(2, 6, 1, 10.0),
        row(2, 7, 0, 10.0),
        row(2, 7, 1, 10.0),
    ];
    write_fixture(tmp.path(), &rows, &[(1, "GROW"), (2, "OTHR")]);

    let algo = SizeAgainstStale { bought: false, rebalanced: false };
    let result = run_backtest(algo, tmp.path().to_str().unwrap()).unwrap();

    let othr = result.open_positions.iter().find(|p| p.symbol == "OTHR").unwrap();
    assert!(
        (othr.quantity - 5_500.0).abs() < 1e-9,
        "expected 5,500 shares sized off market value, got {}",
        othr.quantity
    );
    assert!((result.final_equity - 110_000.0).abs() < 1e-6);
    assert!(identity_error(&result) < 1e-6);
}

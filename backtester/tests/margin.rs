//! Margin (buying-power) models: `MaxLeverage` trims fills to what the
//! account can carry, always lets an over-levered book reduce, and closures
//! work as custom models. Uses a synthetic single-symbol fixture.

use std::fs;
use std::path::Path;
use std::sync::Arc;

use arrow::array::{Float64Array, TimestampNanosecondArray, UInt16Array, UInt32Array};
use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use arrow::record_batch::RecordBatch;
use chrono::{NaiveDate, TimeZone, Utc};
use parquet::arrow::ArrowWriter;

use backtester::{
    margin::{MarginContext, MaxLeverage},
    run_backtest, Algorithm, BacktestResult, Context, Slice,
};

/// One synthetic minute bar: (day-of-June-2023, minute offset, price);
/// o=h=l=c=price, volume 100,000 (no participation cap in play), main session.
struct Row {
    day: u32,
    minute: u32,
    price: f64,
}

fn row(day: u32, minute: u32, price: f64) -> Row {
    Row { day, minute, price }
}

/// Write a single-symbol ("SYM", id 1) fixture.
fn write_fixture(root: &Path, rows: &[Row]) {
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
            Arc::new(UInt16Array::from(vec![1u16; rows.len()])),
            Arc::new(UInt32Array::from(vec![100_000u32; rows.len()])),
            Arc::new(Float64Array::from(prices.clone())),
            Arc::new(Float64Array::from(prices.clone())),
            Arc::new(Float64Array::from(prices.clone())),
            Arc::new(Float64Array::from(prices)),
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

#[test]
fn cash_account_trims_a_buy_to_buying_power() {
    let tmp = tempfile::tempdir().unwrap();
    write_fixture(tmp.path(), &[row(5, 0, 300.0), row(5, 1, 300.0)]);

    // $100k cash account: a 2,000-share buy @ 300 (a $600k order) is trimmed
    // to the 333 whole shares $100k affords (333.33 rounded down to the lot).
    struct BigBuy {
        placed: bool,
    }
    impl Algorithm for BigBuy {
        fn initialize(&mut self, ctx: &mut Context) {
            ctx.set_cash(100_000.0);
            ctx.add_equity("SYM");
            ctx.set_margin_model(MaxLeverage::new(1.0));
        }
        fn on_data(&mut self, ctx: &mut Context, data: &Slice) {
            if !self.placed && data.bars.contains_key("SYM") {
                self.placed = true;
                ctx.market_order("SYM", 2_000.0);
            }
        }
    }

    let result = run_backtest(BigBuy { placed: false }, tmp.path().to_str().unwrap()).unwrap();
    let pos = result.open_positions.iter().find(|p| p.symbol == "SYM").unwrap();
    assert!(
        (pos.quantity - 333.0).abs() < 1e-9,
        "expected the buy trimmed to 333 shares, got {}",
        pos.quantity
    );
    assert!((result.final_equity - 100_000.0).abs() < 1e-6);
    assert!(identity_error(&result) < 1e-6);
}

#[test]
fn set_holdings_is_clamped_to_the_leverage_cap() {
    let tmp = tempfile::tempdir().unwrap();
    write_fixture(tmp.path(), &[row(5, 0, 100.0), row(5, 1, 100.0)]);

    // A 200% allocation under a 1.5x cap: the 2,000-share target is clamped
    // to the 1,500 shares the cap affords.
    struct DoubleUp {
        placed: bool,
    }
    impl Algorithm for DoubleUp {
        fn initialize(&mut self, ctx: &mut Context) {
            ctx.set_cash(100_000.0);
            ctx.add_equity("SYM");
            ctx.set_margin_model(MaxLeverage::new(1.5));
        }
        fn on_data(&mut self, ctx: &mut Context, data: &Slice) {
            if !self.placed && data.bars.contains_key("SYM") {
                self.placed = true;
                ctx.set_holdings("SYM", 2.0);
            }
        }
    }

    let result = run_backtest(DoubleUp { placed: false }, tmp.path().to_str().unwrap()).unwrap();
    let pos = result.open_positions.iter().find(|p| p.symbol == "SYM").unwrap();
    assert!(
        (pos.quantity - 1_500.0).abs() < 1e-9,
        "expected the target clamped to 1,500 shares, got {}",
        pos.quantity
    );
    assert!(identity_error(&result) < 1e-6);
}

#[test]
fn over_levered_book_can_liquidate_but_not_buy() {
    let tmp = tempfile::tempdir().unwrap();
    // Buy 2,000 @ 100 at exactly the 2x cap; the price then drops to 60, so
    // equity shrinks to $20k and the book sits at 6x. A further buy must be
    // rejected outright, while the liquidation must fill in full.
    write_fixture(tmp.path(), &[row(5, 0, 100.0), row(5, 1, 60.0), row(6, 0, 60.0)]);

    struct LeveredLong {
        bars_seen: usize,
    }
    impl Algorithm for LeveredLong {
        fn initialize(&mut self, ctx: &mut Context) {
            ctx.set_cash(100_000.0);
            ctx.add_equity("SYM");
            ctx.set_margin_model(MaxLeverage::new(2.0));
        }
        fn on_data(&mut self, ctx: &mut Context, data: &Slice) {
            if !data.bars.contains_key("SYM") {
                return;
            }
            self.bars_seen += 1;
            match self.bars_seen {
                1 => ctx.market_order("SYM", 2_000.0),
                2 => ctx.market_order("SYM", 10.0), // over-levered: must not fill
                3 => ctx.liquidate("SYM"),          // reducing: must fill in full
                _ => {}
            }
        }
    }

    let result = run_backtest(LeveredLong { bars_seen: 0 }, tmp.path().to_str().unwrap()).unwrap();

    assert!(result.open_positions.is_empty());
    assert_eq!(result.trades.len(), 1);
    let t = &result.trades[0];
    // 2,000 exactly: the blocked 10-share top-up never became part of it.
    assert!((t.quantity - 2_000.0).abs() < 1e-9, "blocked buy leaked in: {}", t.quantity);
    assert!((t.pnl - -80_000.0).abs() < 1e-6); // 2,000 * (60 - 100)
    assert!(identity_error(&result) < 1e-6);
}

#[test]
fn a_closure_works_as_a_margin_model() {
    let tmp = tempfile::tempdir().unwrap();
    write_fixture(tmp.path(), &[row(5, 0, 100.0), row(5, 1, 100.0), row(5, 2, 100.0)]);

    // Long-only account: sells may only reduce an existing long, so the
    // opening short is rejected, the buy fills, and the closing sell passes.
    struct ShortThenLong {
        bars_seen: usize,
    }
    impl Algorithm for ShortThenLong {
        fn initialize(&mut self, ctx: &mut Context) {
            ctx.set_cash(100_000.0);
            ctx.add_equity("SYM");
            ctx.set_margin_model(|m: &MarginContext| {
                if m.quantity < 0.0 {
                    m.quantity.max(-m.current_qty.max(0.0))
                } else {
                    m.quantity
                }
            });
        }
        fn on_data(&mut self, ctx: &mut Context, data: &Slice) {
            if !data.bars.contains_key("SYM") {
                return;
            }
            self.bars_seen += 1;
            match self.bars_seen {
                1 => ctx.market_order("SYM", -100.0), // rejected: would open a short
                2 => ctx.market_order("SYM", 100.0),
                3 => ctx.liquidate("SYM"),
                _ => {}
            }
        }
    }

    let result =
        run_backtest(ShortThenLong { bars_seen: 0 }, tmp.path().to_str().unwrap()).unwrap();

    assert!(result.open_positions.is_empty());
    assert_eq!(result.trades.len(), 1, "the rejected short must not create a trade");
    assert_eq!(result.trades[0].direction, "long");
    assert!(identity_error(&result) < 1e-6);
}

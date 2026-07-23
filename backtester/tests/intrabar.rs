//! Intrabar execution: resting limit and stop orders that fill off a bar's
//! range, and volume-participation partial fills. Uses a synthetic single-symbol
//! fixture with explicit OHLCV so the range can wick through a trigger price.

use std::{fs, path::Path, sync::Arc};

use arrow::{
    array::{Float64Array, TimestampNanosecondArray, UInt16Array, UInt32Array},
    datatypes::{DataType, Field, Schema, TimeUnit},
    record_batch::RecordBatch,
};
use backtester::{run_backtest, Algorithm, BacktestResult, Context, PercentSlippage, Slice};
use chrono::{NaiveDate, TimeZone, Utc};
use parquet::arrow::ArrowWriter;

/// One synthetic minute bar with explicit OHLCV.
struct Ohlc {
    minute: u32,
    open: f64,
    high: f64,
    low: f64,
    close: f64,
    volume: u32,
}

fn bar(minute: u32, open: f64, high: f64, low: f64, close: f64, volume: u32) -> Ohlc {
    Ohlc { minute, open, high, low, close, volume }
}

/// Write a single-symbol ("SYM", id 1) fixture on 2023-06-05.
fn write_fixture(root: &Path, bars: &[Ohlc]) {
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

    let date = NaiveDate::from_ymd_opt(2023, 6, 5).unwrap();
    let ts: Vec<i64> = bars
        .iter()
        .map(|b| {
            let t =
                date.and_hms_opt(15, 0, 0).unwrap() + chrono::Duration::minutes(b.minute as i64);
            Utc.from_utc_datetime(&t).timestamp_nanos_opt().unwrap()
        })
        .collect();

    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(UInt16Array::from(vec![1u16; bars.len()])),
            Arc::new(UInt32Array::from(bars.iter().map(|b| b.volume).collect::<Vec<_>>())),
            Arc::new(Float64Array::from(bars.iter().map(|b| b.open).collect::<Vec<_>>())),
            Arc::new(Float64Array::from(bars.iter().map(|b| b.close).collect::<Vec<_>>())),
            Arc::new(Float64Array::from(bars.iter().map(|b| b.high).collect::<Vec<_>>())),
            Arc::new(Float64Array::from(bars.iter().map(|b| b.low).collect::<Vec<_>>())),
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

/// Places one resting order (limit or stop) on the first bar, then holds.
struct RestOnce {
    limit: Option<(f64, f64)>, // (qty, limit_price)
    stop: Option<(f64, f64)>,  // (qty, stop_price)
    market_first: Option<f64>, // optional market buy on the first bar
    participation: f64,
    placed: bool,
}

impl RestOnce {
    fn limit(qty: f64, price: f64) -> Self {
        Self {
            limit: Some((qty, price)),
            stop: None,
            market_first: None,
            participation: 0.0,
            placed: false,
        }
    }
}

impl Algorithm for RestOnce {
    fn initialize(&mut self, ctx: &mut Context) {
        ctx.set_cash(100_000.0);
        ctx.add_equity("SYM");
        if self.participation > 0.0 {
            ctx.set_max_volume_participation(self.participation);
        }
    }

    fn on_data(&mut self, ctx: &mut Context, data: &Slice) {
        if self.placed || !data.bars.contains_key("SYM") {
            return;
        }
        self.placed = true;
        if let Some(q) = self.market_first {
            ctx.market_order("SYM", q);
        }
        if let Some((q, p)) = self.limit {
            ctx.limit_order("SYM", q, p);
        }
        if let Some((q, p)) = self.stop {
            ctx.stop_order("SYM", q, p);
        }
    }
}

#[test]
fn limit_buy_fills_at_the_limit_when_the_range_touches_it() {
    let tmp = tempfile::tempdir().unwrap();
    write_fixture(
        tmp.path(),
        &[
            bar(0, 100.0, 101.0, 99.0, 100.0, 1_000),
            bar(1, 100.0, 101.0, 94.0, 100.0, 1_000), // dips to 94, through the 95 limit
        ],
    );

    let result = run_backtest(RestOnce::limit(100.0, 95.0), tmp.path().to_str().unwrap()).unwrap();
    let pos = result.open_positions.iter().find(|p| p.symbol == "SYM").unwrap();
    assert!((pos.quantity - 100.0).abs() < 1e-9);
    assert!((pos.avg_price - 95.0).abs() < 1e-9, "expected fill at 95, got {}", pos.avg_price);
    assert!(identity_error(&result) < 1e-6);
}

#[test]
fn limit_order_never_touched_does_not_fill() {
    let tmp = tempfile::tempdir().unwrap();
    write_fixture(
        tmp.path(),
        &[
            bar(0, 100.0, 101.0, 99.0, 100.0, 1_000),
            bar(1, 100.0, 101.0, 98.0, 100.0, 1_000), // low 98 never reaches the 95 limit
        ],
    );

    let result = run_backtest(RestOnce::limit(100.0, 95.0), tmp.path().to_str().unwrap()).unwrap();
    assert!(result.trades.is_empty());
    assert!(result.open_positions.is_empty(), "an untouched limit must not fill");
    assert!((result.final_equity - 100_000.0).abs() < 1e-9);
}

#[test]
fn stop_loss_sell_triggers_when_the_low_breaches_it() {
    let tmp = tempfile::tempdir().unwrap();
    write_fixture(
        tmp.path(),
        &[
            bar(0, 100.0, 101.0, 99.0, 100.0, 1_000), // buy 100 @ close 100, arm stop at 90
            bar(1, 100.0, 100.0, 88.0, 95.0, 1_000),  // dips to 88, through the 90 stop
        ],
    );

    let algo = RestOnce {
        limit: None,
        stop: Some((-100.0, 90.0)),
        market_first: Some(100.0),
        participation: 0.0,
        placed: false,
    };
    let result = run_backtest(algo, tmp.path().to_str().unwrap()).unwrap();

    assert_eq!(result.trades.len(), 1);
    let t = &result.trades[0];
    assert_eq!(t.exit_reason, "signal");
    // Bought at 100, stopped out at 90: a $10/share loss on 100 shares.
    assert!((t.exit_price - 90.0).abs() < 1e-9, "expected stop fill at 90, got {}", t.exit_price);
    assert!((t.pnl - -1000.0).abs() < 1e-6);
    assert!(result.open_positions.is_empty());
    assert!(identity_error(&result) < 1e-6);
}

#[test]
fn limit_fill_is_never_worse_than_the_limit_even_with_slippage() {
    let tmp = tempfile::tempdir().unwrap();
    write_fixture(
        tmp.path(),
        &[
            bar(0, 100.0, 101.0, 99.0, 100.0, 1_000),
            bar(1, 100.0, 101.0, 94.0, 100.0, 1_000), // dips through the 95 limit
        ],
    );

    /// Same as RestOnce::limit but with 10 bps of slippage configured.
    struct SlippedLimit {
        placed: bool,
    }
    impl Algorithm for SlippedLimit {
        fn initialize(&mut self, ctx: &mut Context) {
            ctx.set_cash(100_000.0);
            ctx.add_equity("SYM");
            ctx.set_slippage(PercentSlippage::bps(10.0));
        }
        fn on_data(&mut self, ctx: &mut Context, data: &Slice) {
            if !self.placed && data.bars.contains_key("SYM") {
                self.placed = true;
                ctx.limit_order("SYM", 100.0, 95.0);
            }
        }
    }

    let result =
        run_backtest(SlippedLimit { placed: false }, tmp.path().to_str().unwrap()).unwrap();
    let pos = result.open_positions.iter().find(|p| p.symbol == "SYM").unwrap();
    // Slippage would push the buy to 95.095; a limit clamps it at 95.
    assert!(
        (pos.avg_price - 95.0).abs() < 1e-9,
        "limit buy filled through its limit: {}",
        pos.avg_price
    );
    assert!(identity_error(&result) < 1e-6);
}

#[test]
fn volume_participation_cap_rounds_down_to_the_lot() {
    let tmp = tempfile::tempdir().unwrap();
    // 3.33% of a 1,000-share bar is 33.3 shares; the fill must round down to
    // the whole-share lot, not print a fractional position.
    write_fixture(tmp.path(), &[bar(0, 100.0, 101.0, 99.0, 100.0, 1_000)]);

    let algo = RestOnce {
        limit: None,
        stop: None,
        market_first: Some(250.0),
        participation: 0.0333,
        placed: false,
    };
    let result = run_backtest(algo, tmp.path().to_str().unwrap()).unwrap();
    let pos = result.open_positions.iter().find(|p| p.symbol == "SYM").unwrap();
    assert!(
        (pos.quantity - 33.0).abs() < 1e-9,
        "expected the cap rounded to 33 whole shares, got {}",
        pos.quantity
    );
    assert!(identity_error(&result) < 1e-6);
}

#[test]
fn volume_participation_is_shared_across_orders_on_the_same_bar() {
    let tmp = tempfile::tempdir().unwrap();
    // Cap is 10% of 1,000 = 100 shares per bar *in aggregate*. Three resting
    // 100-share limits against two touching bars can fill at most 200 shares,
    // not 300.
    write_fixture(
        tmp.path(),
        &[
            bar(0, 100.0, 101.0, 99.0, 100.0, 1_000),
            bar(1, 100.0, 101.0, 94.0, 100.0, 1_000),
            bar(2, 100.0, 101.0, 94.0, 100.0, 1_000),
        ],
    );

    struct ThreeLimits {
        placed: bool,
    }
    impl Algorithm for ThreeLimits {
        fn initialize(&mut self, ctx: &mut Context) {
            ctx.set_cash(100_000.0);
            ctx.add_equity("SYM");
            ctx.set_max_volume_participation(0.1);
        }
        fn on_data(&mut self, ctx: &mut Context, data: &Slice) {
            if !self.placed && data.bars.contains_key("SYM") {
                self.placed = true;
                ctx.limit_order("SYM", 100.0, 95.0);
                ctx.limit_order("SYM", 100.0, 95.0);
                ctx.limit_order("SYM", 100.0, 95.0);
            }
        }
    }

    let result = run_backtest(ThreeLimits { placed: false }, tmp.path().to_str().unwrap()).unwrap();
    let pos = result.open_positions.iter().find(|p| p.symbol == "SYM").unwrap();
    assert!(
        (pos.quantity - 200.0).abs() < 1e-9,
        "expected 100 shares per touching bar in aggregate (200), got {}",
        pos.quantity
    );
    assert!(identity_error(&result) < 1e-6);
}

#[test]
fn volume_participation_caps_a_market_fill_and_drops_the_rest() {
    let tmp = tempfile::tempdir().unwrap();
    // 10% of a 1,000-share bar caps each fill at 100 shares.
    write_fixture(
        tmp.path(),
        &[bar(0, 100.0, 101.0, 99.0, 100.0, 1_000), bar(1, 100.0, 101.0, 99.0, 100.0, 1_000)],
    );

    let algo = RestOnce {
        limit: None,
        stop: None,
        market_first: Some(250.0), // asks for 250, only 100 can fill
        participation: 0.1,
        placed: false,
    };
    let result = run_backtest(algo, tmp.path().to_str().unwrap()).unwrap();
    let pos = result.open_positions.iter().find(|p| p.symbol == "SYM").unwrap();
    assert!(
        (pos.quantity - 100.0).abs() < 1e-9,
        "market remainder should drop, got {}",
        pos.quantity
    );
    assert!(identity_error(&result) < 1e-6);
}

#[test]
fn volume_participation_fills_a_resting_limit_across_bars() {
    let tmp = tempfile::tempdir().unwrap();
    // 100-share cap per bar; three touching bars fill 100 + 100 + 50 = 250.
    write_fixture(
        tmp.path(),
        &[
            bar(0, 100.0, 101.0, 99.0, 100.0, 1_000),
            bar(1, 100.0, 101.0, 94.0, 100.0, 1_000),
            bar(2, 100.0, 101.0, 94.0, 100.0, 1_000),
            bar(3, 100.0, 101.0, 94.0, 100.0, 1_000),
        ],
    );

    let algo = RestOnce {
        limit: Some((250.0, 95.0)),
        stop: None,
        market_first: None,
        participation: 0.1,
        placed: false,
    };
    let result = run_backtest(algo, tmp.path().to_str().unwrap()).unwrap();
    let pos = result.open_positions.iter().find(|p| p.symbol == "SYM").unwrap();
    assert!(
        (pos.quantity - 250.0).abs() < 1e-9,
        "resting remainder should carry over, got {}",
        pos.quantity
    );
    assert!(
        (pos.avg_price - 95.0).abs() < 1e-9,
        "all fills at the 95 limit, got {}",
        pos.avg_price
    );
    assert!(identity_error(&result) < 1e-6);
}

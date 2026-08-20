//! The pre-market pump fade pattern from `examples/premarket_pump_fade.rs`,
//! tested against synthetic Parquet fixtures with explicit pre-market and
//! main-session bars. Key properties: the gap is measured against the
//! previous *main-session* close using the latest *pre-market* print, the
//! short opens on the first main-session bar, and it covers at 15:55 ET the
//! same day.

use std::{fs, path::Path, sync::Arc};

use arrow::{
    array::{Float64Array, TimestampNanosecondArray, UInt16Array, UInt32Array},
    datatypes::{DataType, Field, Schema, TimeUnit},
    record_batch::RecordBatch,
};
use backtester::{
    bar::{Bar, MarketSession},
    data::load_ticker_map,
    run_backtest, Algorithm, BacktestResult, Context, Slice, SymbolMap, SymbolSet,
};
use chrono::{NaiveDate, NaiveTime, TimeZone, Utc};
use chrono_tz::US::Eastern;
use parquet::arrow::ArrowWriter;

/// One synthetic minute bar at an explicit UTC hour:minute with a session
/// tag (1 = pre-market, 2 = main, 3 = after-market). o=h=l=c=price.
struct Row {
    ticker: u16,
    date: NaiveDate,
    hour: u32,
    minute: u32,
    session: u8,
    price: f64,
}

#[allow(clippy::too_many_arguments)]
fn row(
    ticker: u16,
    y: i32,
    m: u32,
    d: u32,
    hour: u32,
    minute: u32,
    session: u8,
    price: f64,
) -> Row {
    Row { ticker, date: NaiveDate::from_ymd_opt(y, m, d).unwrap(), hour, minute, session, price }
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
    ]));

    let ts: Vec<i64> = rows
        .iter()
        .map(|r| {
            let t = r.date.and_hms_opt(r.hour, r.minute, 0).unwrap();
            Utc.from_utc_datetime(&t).timestamp_nanos_opt().unwrap()
        })
        .collect();

    // There is no session column in the data — the engine derives the session
    // from the bar's US Eastern time-of-day — so the tags above document what
    // each row is meant to be. Keep them honest against that derivation.
    for (r, ns) in rows.iter().zip(&ts) {
        let bar = Bar {
            time: Utc.timestamp_nanos(*ns),
            open: r.price,
            high: r.price,
            low: r.price,
            close: r.price,
            volume: 100,
        };
        let tagged = match r.session {
            1 => MarketSession::PreMarket,
            2 => MarketSession::Main,
            _ => MarketSession::AfterMarket,
        };
        assert_eq!(
            bar.session(),
            tagged,
            "row tagged session {} sits at {:02}:{:02} UTC on {}",
            r.session,
            r.hour,
            r.minute,
            r.date
        );
    }
    // Files must be time-sorted (the engine streams them without re-sorting,
    // and an out-of-order row fails the run). Callers list rows grouped by
    // day and ticker because that reads best, so sort by timestamp here.
    let mut order: Vec<usize> = (0..rows.len()).collect();
    order.sort_by_key(|&i| ts[i]);
    let ts: Vec<i64> = order.iter().map(|&i| ts[i]).collect();
    let rows: Vec<&Row> = order.iter().map(|&i| &rows[i]).collect();

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

/// Compact replica of `examples/premarket_pump_fade.rs` (examples can't be
/// imported by tests), minus the slippage model so fill prices are exact.
struct Fader {
    data_root: String,
    min_gain: f64,
    prev_close: SymbolMap<f64>,
    today_close: SymbolMap<f64>,
    premarket_last: SymbolMap<f64>,
    seen_main: SymbolSet,
    held: SymbolSet,
    last_date: Option<NaiveDate>,
}

impl Fader {
    fn new(data_root: &str) -> Self {
        Self {
            data_root: data_root.to_string(),
            min_gain: 1.0,
            prev_close: SymbolMap::default(),
            today_close: SymbolMap::default(),
            premarket_last: SymbolMap::default(),
            seen_main: SymbolSet::default(),
            held: SymbolSet::default(),
            last_date: None,
        }
    }
}

impl Algorithm for Fader {
    fn initialize(&mut self, ctx: &mut Context) {
        ctx.set_cash(100_000.0);
        for symbol in load_ticker_map(&self.data_root).unwrap().into_values() {
            ctx.add_equity(&symbol);
        }
    }

    fn on_data(&mut self, ctx: &mut Context, data: &Slice) {
        let eastern = data.time.with_timezone(&Eastern);
        let today = eastern.date_naive();
        if self.last_date != Some(today) {
            self.last_date = Some(today);
            for symbol in self.held.drain() {
                ctx.liquidate(symbol);
            }
            for (symbol, close) in self.today_close.drain() {
                self.prev_close.insert(symbol, close);
            }
            self.premarket_last.clear();
            self.seen_main.clear();
        }
        let past_cover = eastern.time() >= NaiveTime::from_hms_opt(15, 55, 0).unwrap();

        for (&symbol, bar) in &data.bars {
            match bar.session() {
                MarketSession::PreMarket => {
                    self.premarket_last.insert(symbol, bar.close);
                }
                MarketSession::Main => {
                    if self.seen_main.insert(symbol) && !self.held.contains(&symbol) {
                        let prev = self.prev_close.get(&symbol).copied();
                        let pre = self.premarket_last.get(&symbol).copied();
                        if let (Some(prev), Some(pre)) = (prev, pre) {
                            if prev > 0.0 && pre / prev - 1.0 >= self.min_gain && pre >= 1.0 {
                                ctx.set_holdings(symbol, -0.05);
                                self.held.insert(symbol);
                            }
                        }
                    }
                    self.today_close.insert(symbol, bar.close);
                    if past_cover && self.held.remove(&symbol) {
                        ctx.liquidate(symbol);
                    }
                }
                MarketSession::AfterMarket => {}
            }
        }
    }
}

/// Two days of bars: PUMP closes day 1 at 1.00, prints 2.50 pre-market on
/// day 2 (+150%), then fades through the main session. TAME gaps only +50%.
/// Times are UTC; 13:30 = 09:30 ET (main open), 19:55 = 15:55 ET (cover).
fn pump_rows() -> Vec<Row> {
    vec![
        // Mon 2023-06-05: quiet day establishing prev_close for both names.
        row(1, 2023, 6, 5, 14, 0, 2, 1.0),
        row(1, 2023, 6, 5, 19, 55, 2, 1.0),
        row(2, 2023, 6, 5, 14, 0, 2, 10.0),
        row(2, 2023, 6, 5, 19, 55, 2, 10.0),
        // Tue 2023-06-06 pre-market (08:00 ET): PUMP +150%, TAME +50%.
        row(1, 2023, 6, 6, 12, 0, 1, 2.5),
        row(2, 2023, 6, 6, 12, 0, 1, 15.0),
        // Tue main session: PUMP fades 2.40 → 2.00, TAME drifts.
        row(1, 2023, 6, 6, 13, 30, 2, 2.4),
        row(2, 2023, 6, 6, 13, 30, 2, 14.5),
        row(1, 2023, 6, 6, 16, 0, 2, 2.2),
        row(2, 2023, 6, 6, 16, 0, 2, 14.4),
        row(1, 2023, 6, 6, 19, 55, 2, 2.0),
        row(2, 2023, 6, 6, 19, 55, 2, 14.3),
    ]
}

#[test]
fn shorts_the_pump_at_the_open_and_covers_at_the_close() {
    let tmp = tempfile::tempdir().unwrap();
    write_fixture(tmp.path(), &pump_rows(), &[(1, "PUMP"), (2, "TAME")]);
    let root = tmp.path().to_str().unwrap();

    let result = run_backtest(Fader::new(root), root).unwrap();

    // Only PUMP qualifies (+150% pre-market); TAME's +50% is below the bar.
    assert_eq!(result.trades.len(), 1, "trades: {:?}", result.trades);
    let trade = &result.trades[0];
    assert_eq!(trade.symbol, "PUMP");
    assert_eq!(trade.direction, "short");
    // Shorted on the first main bar (2.40), covered on the 15:55 ET bar
    // (2.00), same day.
    assert_eq!(trade.entry_price, 2.4);
    assert_eq!(trade.exit_price, 2.0);
    assert!(trade.entry_time.starts_with("2023-06-06T13:30"), "entry {}", trade.entry_time);
    assert!(trade.exit_time.starts_with("2023-06-06T19:55"), "exit {}", trade.exit_time);
    assert!(trade.pnl > 0.0);
    assert!(result.open_positions.is_empty(), "everything covered by the close");
    assert!(identity_error(&result) < 1e-6);
}

#[test]
fn gap_is_measured_against_the_main_session_close_not_after_hours() {
    let tmp = tempfile::tempdir().unwrap();
    // Day 1: main close 1.00, then an after-hours melt-up to 2.10. Day 2
    // pre-market holds 2.20 (+120% vs the *main* close). The reference must
    // be the 1.00 main close, so this still qualifies.
    let rows = vec![
        row(1, 2023, 6, 5, 14, 0, 2, 1.0),
        row(1, 2023, 6, 5, 19, 55, 2, 1.0),
        row(1, 2023, 6, 5, 22, 0, 3, 2.1), // after-market, must be ignored
        row(1, 2023, 6, 6, 12, 0, 1, 2.2),
        row(1, 2023, 6, 6, 13, 30, 2, 2.1),
        row(1, 2023, 6, 6, 19, 55, 2, 1.8),
    ];
    write_fixture(tmp.path(), &rows, &[(1, "PUMP")]);
    let root = tmp.path().to_str().unwrap();

    let result = run_backtest(Fader::new(root), root).unwrap();

    assert_eq!(result.trades.len(), 1, "trades: {:?}", result.trades);
    assert_eq!(result.trades[0].direction, "short");
    assert_eq!(result.trades[0].entry_price, 2.1);
    assert!(identity_error(&result) < 1e-6);
}

#[test]
fn no_premarket_or_no_previous_close_means_no_trade() {
    let tmp = tempfile::tempdir().unwrap();
    // GAPO doubles overnight but has *no pre-market prints*; FRSH doubles in
    // pre-market on its very first day (no previous close). Neither trades.
    let rows = vec![
        row(1, 2023, 6, 5, 14, 0, 2, 1.0),
        row(1, 2023, 6, 5, 19, 55, 2, 1.0),
        row(1, 2023, 6, 6, 13, 30, 2, 2.4),
        row(1, 2023, 6, 6, 19, 55, 2, 2.0),
        row(2, 2023, 6, 6, 12, 0, 1, 8.0),
        row(2, 2023, 6, 6, 13, 30, 2, 7.5),
        row(2, 2023, 6, 6, 19, 55, 2, 7.0),
    ];
    write_fixture(tmp.path(), &rows, &[(1, "GAPO"), (2, "FRSH")]);
    let root = tmp.path().to_str().unwrap();

    let result = run_backtest(Fader::new(root), root).unwrap();

    assert!(result.trades.is_empty(), "trades: {:?}", result.trades);
    assert!(result.open_positions.is_empty());
    assert_eq!(result.final_equity, result.initial_cash);
    assert!(identity_error(&result) < 1e-6);
}

#[test]
fn short_stranded_by_missing_bars_is_covered_next_morning() {
    let tmp = tempfile::tempdir().unwrap();
    // PUMP qualifies on day 2 but its bars stop at 16:00 UTC — no 15:55 ET
    // bar to cover on. The day-boundary fallback must close it on day 3
    // instead of carrying the short indefinitely. STAY keeps the clock
    // ticking through day 3.
    let rows = vec![
        row(1, 2023, 6, 5, 14, 0, 2, 1.0),
        row(2, 2023, 6, 5, 14, 0, 2, 5.0),
        row(1, 2023, 6, 6, 12, 0, 1, 2.5),
        row(1, 2023, 6, 6, 13, 30, 2, 2.4),
        row(1, 2023, 6, 6, 16, 0, 2, 2.3), // last PUMP bar of the day
        row(2, 2023, 6, 6, 19, 55, 2, 5.0),
        row(1, 2023, 6, 7, 14, 0, 2, 2.2),
        row(2, 2023, 6, 7, 14, 0, 2, 5.0),
        row(2, 2023, 6, 7, 19, 55, 2, 5.0),
    ];
    write_fixture(tmp.path(), &rows, &[(1, "PUMP"), (2, "STAY")]);
    let root = tmp.path().to_str().unwrap();

    let result = run_backtest(Fader::new(root), root).unwrap();

    assert_eq!(result.trades.len(), 1, "trades: {:?}", result.trades);
    let trade = &result.trades[0];
    assert_eq!(trade.symbol, "PUMP");
    assert!(trade.exit_time.starts_with("2023-06-07"), "exit {}", trade.exit_time);
    assert!(result.open_positions.is_empty());
    assert!(identity_error(&result) < 1e-6);
}

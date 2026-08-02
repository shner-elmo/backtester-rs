//! Engine-level tests against the committed AAPL fixture (Jan 2023): trade
//! netting, equity-curve consistency, commission accounting.

use backtester::{
    commission::PerShareCommission, run, run_backtest, Algorithm, BacktestResult, Context, Slice,
};

const FIXTURE: &str = concat!(env!("CARGO_MANIFEST_DIR"), "/tests/fixtures");

/// Rebalances to 90% AAPL on every single bar — the pathological case that
/// used to report thousands of tiny rebalance "trades".
struct RebalanceEveryBar {
    commission: bool,
}

impl Algorithm for RebalanceEveryBar {
    fn initialize(&mut self, ctx: &mut Context) {
        ctx.set_cash(100_000.0);
        ctx.add_equity("AAPL");
        if self.commission {
            ctx.set_commission(PerShareCommission { per_share: 0.005, minimum: 1.0 });
        }
    }

    fn on_data(&mut self, ctx: &mut Context, data: &Slice) {
        let sym = ctx.symbol("AAPL").expect("subscribed in initialize");
        if data.bars.contains_key(&sym) {
            ctx.set_holdings(sym, 0.9);
        }
    }
}

/// Buys once, holds five bars, liquidates, then stays flat.
struct OneRoundTrip {
    bars_seen: usize,
}

impl Algorithm for OneRoundTrip {
    fn initialize(&mut self, ctx: &mut Context) {
        ctx.set_cash(100_000.0);
        ctx.add_equity("AAPL");
    }

    fn on_data(&mut self, ctx: &mut Context, data: &Slice) {
        let sym = ctx.symbol("AAPL").expect("subscribed in initialize");
        if !data.bars.contains_key(&sym) {
            return;
        }
        self.bars_seen += 1;
        match self.bars_seen {
            1 => ctx.set_holdings(sym, 0.5),
            6 => ctx.liquidate(sym),
            _ => {}
        }
    }
}

fn cash_conservation_error(r: &BacktestResult) -> f64 {
    // initial + realized pnl (closed trades and partial unwinds of open
    // positions) + unrealized pnl must equal final equity; commissions are
    // already inside the realized figures.
    let realized: f64 = r.trades.iter().map(|t| t.pnl).sum();
    let open: f64 = r.open_positions.iter().map(|p| p.unrealized_pnl + p.realized_pnl).sum();
    (r.initial_cash + realized + open - r.final_equity).abs()
}

#[test]
fn constant_rebalancing_nets_into_a_single_position_lifetime() {
    let result = run_backtest(RebalanceEveryBar { commission: false }, FIXTURE).unwrap();

    // The position opens on the first bar and never returns to flat, so there
    // are no completed round trips — only one open position at the end.
    assert_eq!(result.trades.len(), 0, "rebalance noise leaked into trades");
    assert_eq!(result.open_positions.len(), 1);
    assert_eq!(result.open_positions[0].symbol, "AAPL");
    assert!(result.open_positions[0].quantity > 0.0);

    assert!(cash_conservation_error(&result) < 1e-6);
}

#[test]
fn single_round_trip_is_one_trade() {
    let result = run_backtest(OneRoundTrip { bars_seen: 0 }, FIXTURE).unwrap();

    assert_eq!(result.trades.len(), 1);
    let t = &result.trades[0];
    assert_eq!(t.symbol, "AAPL");
    assert_eq!(t.direction, "long");
    assert!(t.quantity > 0.0);
    assert!(t.entry_time < t.exit_time);
    // PnL must match the price move times quantity.
    let expected = (t.exit_price - t.entry_price) * t.quantity;
    assert!((t.pnl - expected).abs() < 1e-6);

    assert!(result.open_positions.is_empty());
    assert!(cash_conservation_error(&result) < 1e-6);
}

#[test]
fn commissions_are_charged_and_attributed() {
    let with = run_backtest(RebalanceEveryBar { commission: true }, FIXTURE).unwrap();
    let without = run_backtest(RebalanceEveryBar { commission: false }, FIXTURE).unwrap();

    assert!(with.total_commission > 0.0);
    assert_eq!(without.total_commission, 0.0);
    // Commission comes straight out of final equity. Sizing feedback (fees
    // shrink equity, shrinking the 90% target) makes the difference not
    // exactly equal, but it must be at least the fees paid and same order.
    let equity_gap = without.final_equity - with.final_equity;
    assert!(
        equity_gap >= with.total_commission * 0.5 && equity_gap <= with.total_commission * 2.0,
        "equity gap {} vs commissions {}",
        equity_gap,
        with.total_commission
    );
}

#[test]
fn equity_curve_starts_at_initial_cash_and_ends_at_final_equity() {
    let result = run_backtest(OneRoundTrip { bars_seen: 0 }, FIXTURE).unwrap();

    let curve = &result.equity_curve;
    assert!(curve.len() >= 3, "expected multiple daily points, got {}", curve.len());
    assert_eq!(curve[0].equity, result.initial_cash);
    let last = curve.last().unwrap();
    assert!((last.equity - result.final_equity).abs() < 1e-9);

    // Dates strictly increase and are weekdays-only trading dates.
    for w in curve.windows(2) {
        assert!(w[0].time < w[1].time, "curve not sorted: {} -> {}", w[0].time, w[1].time);
    }
}

#[test]
fn intraday_equity_is_opt_in_and_finer_than_the_daily_curve() {
    struct HoldWithIntraday {
        bought: bool,
    }
    impl Algorithm for HoldWithIntraday {
        fn initialize(&mut self, ctx: &mut Context) {
            ctx.set_cash(100_000.0);
            ctx.add_equity("AAPL");
            ctx.set_track_intraday_equity(true);
        }
        fn on_data(&mut self, ctx: &mut Context, data: &Slice) {
            let sym = ctx.symbol("AAPL").expect("subscribed in initialize");
            if !self.bought && data.bars.contains_key(&sym) {
                self.bought = true;
                ctx.set_holdings(sym, 1.0);
            }
        }
    }

    let on = run_backtest(HoldWithIntraday { bought: false }, FIXTURE).unwrap();
    // Per-bar marks are recorded and far outnumber the daily points.
    assert!(!on.intraday_equity.is_empty());
    assert!(on.intraday_equity.len() > on.equity_curve.len());
    for w in on.intraday_equity.windows(2) {
        assert!(w[0].time < w[1].time, "intraday marks not sorted");
    }
    assert!((on.intraday_equity.last().unwrap().equity - on.final_equity).abs() < 1e-6);

    // Off by default — no per-bar marks recorded.
    let off = run_backtest(OneRoundTrip { bars_seen: 0 }, FIXTURE).unwrap();
    assert!(off.intraday_equity.is_empty());
}

#[test]
fn on_end_of_day_fires_for_every_trading_day_including_the_last() {
    use std::sync::{Arc, Mutex};

    struct CountEod {
        days: Arc<Mutex<usize>>,
    }
    impl Algorithm for CountEod {
        fn initialize(&mut self, ctx: &mut Context) {
            ctx.set_cash(100_000.0);
            ctx.add_equity("AAPL");
        }
        fn on_data(&mut self, _ctx: &mut Context, _data: &Slice) {}
        fn on_end_of_day(&mut self, _ctx: &mut Context) {
            *self.days.lock().unwrap() += 1;
        }
    }

    let days = Arc::new(Mutex::new(0));
    let result = run_backtest(CountEod { days: days.clone() }, FIXTURE).unwrap();

    // One callback per trading day — the curve has one extra point, the
    // day-before anchor at initial cash.
    assert_eq!(*days.lock().unwrap(), result.equity_curve.len() - 1);
}

#[test]
fn set_holdings_rounds_to_whole_shares_by_default() {
    let result = run_backtest(RebalanceEveryBar { commission: false }, FIXTURE).unwrap();
    let qty = result.open_positions[0].quantity;
    assert!((qty - qty.round()).abs() < 1e-9, "expected whole-share position, got {qty}");
}

#[test]
fn run_writes_the_result_json_into_the_configured_output_dir() {
    struct WithOutputDir {
        dir: std::path::PathBuf,
    }
    impl Algorithm for WithOutputDir {
        fn initialize(&mut self, ctx: &mut Context) {
            ctx.set_cash(100_000.0);
            ctx.add_equity("AAPL");
            ctx.set_output_dir(&self.dir);
        }
        fn on_data(&mut self, _ctx: &mut Context, _data: &Slice) {}
    }

    // A unique, not-yet-existing directory: proves run() creates it. The
    // set_output_dir value must also win over any BACKTEST_OUTPUT_DIR in the
    // environment (run this test with the var set to verify by hand).
    let dir = std::env::temp_dir().join(format!("backtester_out_test_{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);

    run(WithOutputDir { dir: dir.clone() }, FIXTURE).unwrap();

    let results: Vec<_> = std::fs::read_dir(&dir)
        .expect("output dir was not created")
        .filter_map(|e| e.ok())
        .map(|e| e.file_name().to_string_lossy().into_owned())
        .filter(|n| n.starts_with("backtest_result_") && n.ends_with(".json"))
        .collect();
    assert_eq!(results.len(), 1, "expected exactly one result file, got {results:?}");

    std::fs::remove_dir_all(&dir).unwrap();
}

#[test]
fn missing_data_path_is_an_error_not_a_panic() {
    // A wrong data path used to panic deep in the loader; now it surfaces as a
    // returned error the caller can report.
    let err = run_backtest(OneRoundTrip { bars_seen: 0 }, "/no/such/data/root")
        .expect_err("expected an error for a nonexistent data path");
    assert!(
        err.to_string().contains("encoded_tickers.json"),
        "error should name the missing file, got: {err}"
    );
}

/// Records how deep `ctx.history()` got for AAPL, under whichever tracking
/// mode `setup` configures.
struct HistoryProbe {
    setup: fn(&mut Context, backtester::Symbol),
    max_depth: std::rc::Rc<std::cell::Cell<usize>>,
    bars_seen: std::rc::Rc<std::cell::Cell<usize>>,
}

impl Algorithm for HistoryProbe {
    fn initialize(&mut self, ctx: &mut Context) {
        ctx.set_cash(100_000.0);
        let sym = ctx.add_equity("AAPL");
        (self.setup)(ctx, sym);
    }

    fn on_data(&mut self, ctx: &mut Context, data: &Slice) {
        let sym = ctx.symbol("AAPL").expect("subscribed in initialize");
        if !data.bars.contains_key(&sym) {
            return;
        }
        self.bars_seen.set(self.bars_seen.get() + 1);
        let depth = ctx.history(sym, 50).len();
        self.max_depth.set(self.max_depth.get().max(depth));
    }
}

/// Run the fixture with `setup` applied in `initialize`; returns
/// (bars seen, deepest history observed).
fn history_depth(setup: fn(&mut Context, backtester::Symbol)) -> (usize, usize) {
    let max_depth = std::rc::Rc::new(std::cell::Cell::new(0));
    let bars_seen = std::rc::Rc::new(std::cell::Cell::new(0));
    let algo = HistoryProbe { setup, max_depth: max_depth.clone(), bars_seen: bars_seen.clone() };
    run_backtest(algo, FIXTURE).expect("fixture backtest");
    (bars_seen.get(), max_depth.get())
}

#[test]
fn history_is_recorded_for_every_symbol_by_default() {
    let (bars, depth) = history_depth(|_, _| {});
    assert!(bars > 50, "fixture should produce plenty of bars");
    assert_eq!(depth, 50, "the rolling window should fill to the requested depth");
}

#[test]
fn disable_history_skips_the_rolling_window_entirely() {
    let (bars, depth) = history_depth(|ctx, _| ctx.disable_history());
    assert!(bars > 50, "the backtest must still see every bar");
    assert_eq!(depth, 0, "history() should stay empty when nothing is tracked");
}

#[test]
fn track_history_keeps_the_window_for_named_symbols_only() {
    let (_, tracked) = history_depth(|ctx, sym| ctx.track_history(sym));
    assert_eq!(tracked, 50, "an explicitly tracked symbol keeps its window");

    // Opting in a *different* symbol leaves AAPL untracked.
    let (_, untracked) = history_depth(|ctx, _| {
        let other = backtester::Symbol::from_ticker_id(1);
        ctx.track_history(other);
    });
    assert_eq!(untracked, 0, "a symbol left out of the opt-in list keeps no history");
}

//! Every knob at once. This strategy isn't meant to be *good* — it exists to
//! exercise every configurable feature of the engine in one place, so you can
//! see how the pieces fit together:
//!
//!   - run window, starting cash, and warm-up
//!   - a slippage model and a commission model
//!   - next-bar-open fills (no same-bar look-ahead)
//!   - a custom lot size and a delisting threshold
//!   - a consolidator building a higher-timeframe trend indicator
//!   - a scheduled time-of-day callback (flatten before the close)
//!   - `ctx.history()` lookbacks
//!   - all three order types plus the `on_split` / `on_delisted` / `on_end_of_day` hooks
//!
//!   cargo run --example kitchen_sink -- backtester/tests/fixtures
//!
//! The signal itself: buy oversold dips (RSI) that are trading above a slower
//! hourly trend, exit when overbought or the trend turns down, and always
//! flatten five minutes before the close.

use std::cell::RefCell;
use std::rc::Rc;

use backtester::{
    commission::PerShareCommission,
    consolidator::ConsolidatorPeriod,
    indicators::{Next, Rsi, Sma},
    run,
    slippage::PercentSlippage,
    Algorithm, Context, FillTiming, Slice,
};

struct KitchenSink {
    symbol: String,
    rsi: Rsi,
    bars_seen: usize,
    /// Latest hourly-trend SMA, written by the consolidator callback and read
    /// back in `on_data`. `Rc<RefCell<..>>` because the consolidator closure
    /// must be `'static` and so can't borrow the algorithm struct.
    hourly_trend: Rc<RefCell<Option<f64>>>,
}

impl Algorithm for KitchenSink {
    fn initialize(&mut self, ctx: &mut Context) {
        // --- Run window & capital ---
        ctx.set_start_date(2023, 1, 1);
        ctx.set_end_date(2023, 12, 31);
        ctx.set_cash(100_000.0);
        ctx.set_warm_up(60); // consume the first hour of minute bars before trading

        // --- Universe ---
        ctx.add_equity(&self.symbol.clone());

        // --- Fill friction ---
        ctx.set_slippage(PercentSlippage::bps(5.0)); // 0.05% against the aggressor
        ctx.set_commission(PerShareCommission { per_share: 0.005, minimum: 1.0 });

        // --- Execution realism ---
        ctx.set_fill_timing(FillTiming::NextBarOpen); // fill at the next bar's open
        ctx.set_lot_size(1.0); // trade whole shares (try 0.001 for fractional)
        ctx.set_delist_after_days(5); // force-close a symbol silent for 5 trading days

        // --- Higher-timeframe trend from a consolidator ---
        // Aggregate minute bars into hourly bars, run a 6-period SMA over their
        // closes, and publish the latest value for on_data to read.
        let trend = self.hourly_trend.clone();
        let mut hourly_sma = Sma::new(6).unwrap();
        ctx.consolidate(&self.symbol.clone(), ConsolidatorPeriod::Hours(1), move |bar| {
            *trend.borrow_mut() = Some(hourly_sma.next(bar.close));
        });

        // --- Scheduled action: flatten five minutes before the close ---
        let symbol = self.symbol.clone();
        ctx.on_time(15, 55, move |ctx| ctx.liquidate(&symbol));
    }

    fn on_data(&mut self, ctx: &mut Context, data: &Slice) {
        let Some(bar) = data.bars.get(&self.symbol) else { return };
        self.bars_seen += 1;
        let rsi = self.rsi.next(bar.close);

        // Wait until both the RSI and the hourly trend have enough data.
        if self.bars_seen <= 14 {
            return;
        }
        let Some(trend) = *self.hourly_trend.borrow() else { return };

        // A history lookback: only buy dips still holding above the last 30
        // minute-bars' low, so we skip knives that are still falling.
        let recent_low =
            ctx.history(&self.symbol, 30).iter().map(|b| b.low).fold(f64::INFINITY, f64::min);

        let uptrend = bar.close > trend;
        if uptrend && rsi < 35.0 && bar.close > recent_low {
            ctx.set_holdings(&self.symbol.clone(), 1.0); // buy the dip within an uptrend
        } else if rsi > 70.0 || !uptrend {
            ctx.liquidate(&self.symbol.clone()); // overbought, or the trend rolled over
        }
    }

    fn on_end_of_day(&mut self, _ctx: &mut Context) {
        // Fires as each new trading day opens, before the day's first bar
        // touches state. A natural place to log or run a daily rebalance.
    }

    fn on_split(&mut self, _ctx: &mut Context, symbol: &str, _ratio: f64) {
        // The engine rescales positions and ctx.history() for us, but an
        // indicator we own keeps its pre-split memory — reset it by hand.
        if symbol == self.symbol {
            self.rsi = Rsi::new(14).unwrap();
        }
    }

    fn on_delisted(&mut self, _ctx: &mut Context, symbol: &str) {
        eprintln!("{symbol} went silent — the engine force-closed the position");
    }
}

fn main() {
    let args: Vec<String> = std::env::args().collect();
    let data_path = args.get(1).map(String::as_str).unwrap_or("data/output/minute");

    let algo = KitchenSink {
        symbol: "AAPL".to_string(),
        rsi: Rsi::new(14).unwrap(),
        bars_seen: 0,
        hourly_trend: Rc::new(RefCell::new(None)),
    };

    run(algo, data_path).unwrap_or_else(|e| {
        eprintln!("backtest failed: {e}");
        std::process::exit(1);
    });
}

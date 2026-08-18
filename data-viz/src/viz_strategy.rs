//! Chart bars, produced by the backtester itself.
//!
//! Rather than re-reading and re-bucketing the Parquet in SQL, a chart request
//! runs a one-symbol [`Algorithm`] over the requested range: the engine streams
//! and (for a higher timeframe) consolidates the bars exactly as a real
//! backtest would, and this strategy keeps what each `on_data` / consolidator
//! close hands it. Same reader, same consolidation, no second implementation to
//! keep in step.

use std::{cell::RefCell, rc::Rc};

use backtester::{
    bar::{Bar, MarketSession},
    run_backtest, Algorithm, Context, LogConfig, Slice, Symbol,
};
use chrono::{Datelike, NaiveDate};
use chrono_tz::US::Eastern;

use crate::{OhlcBar, Timeframe};

/// Bars a request has collected so far, shared between the strategy and the
/// consolidator callback it registers.
type Collected = Rc<RefCell<Vec<OhlcBar>>>;

struct VizStrategy {
    ticker: String,
    tf: Timeframe,
    start: Option<NaiveDate>,
    end: Option<NaiveDate>,
    symbol: Option<Symbol>,
    out: Collected,
}

impl Algorithm for VizStrategy {
    fn initialize(&mut self, ctx: &mut Context) {
        ctx.set_log_config(LogConfig::none());
        ctx.set_cash(1.0); // Unused: this strategy never places an order.
        if let Some(s) = self.start {
            ctx.set_start_date(s.year(), s.month(), s.day());
        }
        if let Some(e) = self.end {
            ctx.set_end_date(e.year(), e.month(), e.day());
        }

        // The API layer already checked this ticker against the dataset map,
        // so this unwrap only panics if that validation boundary is broken.
        let symbol = ctx.try_add_equity(&self.ticker).unwrap();
        self.symbol = Some(symbol);

        // A higher timeframe registers the same consolidator a backtest would
        // and turns each closed period into a chart bar; raw minutes have no
        // period and are collected in `on_data`.
        if let Some(period) = self.tf.to_period() {
            let (out, tf) = (self.out.clone(), self.tf);
            ctx.consolidate(symbol, period, move |bar| out.borrow_mut().push(to_ohlc(bar, tf)));
        }
    }

    fn on_data(&mut self, _ctx: &mut Context, slice: &Slice) {
        // Consolidated timeframes flow through the callback above; only raw
        // minutes are gathered here.
        if self.tf.to_period().is_some() {
            return;
        }
        let Some(symbol) = self.symbol else { return };
        if let Some(bar) = slice.bars.get(&symbol) {
            self.out.borrow_mut().push(to_ohlc(bar, self.tf));
        }
    }
}

/// A backtester [`Bar`] as a chart bar: OHLCV plus the US/Eastern wall-clock
/// time and session flag the frontend expects. `time` is the ET local time
/// reinterpreted as a Unix timestamp (see [`OhlcBar::time`]).
fn to_ohlc(bar: &Bar, tf: Timeframe) -> OhlcBar {
    let et = bar.time.with_timezone(&Eastern);
    OhlcBar {
        time: et.naive_local().and_utc().timestamp(),
        open: bar.open,
        high: bar.high,
        low: bar.low,
        close: bar.close,
        volume: bar.volume as f64,
        // Daily/weekly bars span whole days, so the flag only means anything
        // for the intraday timeframes.
        is_extended: tf.has_extended_hours() && bar.session() != MarketSession::Main,
    }
}

/// Stream `ticker` over `[start, end]` at `tf` and return the chart bars.
/// Synchronous and thread-spawning (it drives the engine), so callers on an
/// async runtime should wrap it in `spawn_blocking`.
pub(crate) fn run_viz(
    data_path: &str,
    ticker: &str,
    start: Option<NaiveDate>,
    end: Option<NaiveDate>,
    tf: Timeframe,
) -> Result<Vec<OhlcBar>, String> {
    let out: Collected = Rc::new(RefCell::new(Vec::new()));
    let strat =
        VizStrategy { ticker: ticker.to_string(), tf, start, end, symbol: None, out: out.clone() };
    run_backtest(strat, data_path).map_err(|e| e.to_string())?;
    // The engine has dropped the strategy and its consolidator by now, so this
    // is the sole owner; clone the collected bars out of the cell.
    let bars = out.borrow().clone();
    Ok(bars)
}

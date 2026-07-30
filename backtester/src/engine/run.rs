//! The engine's state and event loop: streams ticks, delegates each phase to
//! the methods in `orders.rs` / `corporate_actions.rs`, and assembles the
//! final [`BacktestResult`].

use std::{collections::BTreeMap, path::PathBuf};

use chrono::{DateTime, Datelike, NaiveDate, TimeZone, Utc};
use chrono_tz::US::Eastern;

use super::{ledger::OpenLifetime, load_pending_actions, BacktestResult};
use crate::{
    algorithm::Algorithm,
    bar::Bar,
    broker::PriceTable,
    context::{Context, Order, RestingOrder},
    data::{file_year_month, sorted_parquet_files, TickReader},
    error::BacktestError,
    slice::Slice,
    stats::{compute_stats, EquityPoint, OpenPositionSummary, Trade},
    symbol::{Symbol, SymbolMap, SymbolVec},
};

/// Corporate actions queued by their effective date, drained at day
/// boundaries as the backtest clock reaches them. Symbols are resolved when
/// the metadata files are read, so the day-boundary work never parses a
/// ticker string.
#[derive(Default)]
pub(super) struct PendingActions {
    pub(super) splits: BTreeMap<NaiveDate, Vec<(Symbol, f64)>>,
    pub(super) dividends: BTreeMap<NaiveDate, Vec<(Symbol, f64)>>,
    pub(super) renames: BTreeMap<NaiveDate, Vec<(Symbol, Symbol)>>,
}

/// All mutable state of a running backtest. Phase methods live in
/// `orders.rs`, `corporate_actions.rs`, and `ledger.rs`; the algorithm is
/// threaded as a parameter into the ones that fire callbacks, so callback
/// ordering stays visible in [`Engine::process_tick`].
pub(super) struct Engine {
    pub(super) ctx: Context,
    pub(super) initial_cash: f64,
    // PnL ledger: one open lifetime per held symbol, netted into a `Trade`
    // when the position returns to flat.
    pub(super) lifetimes: SymbolMap<OpenLifetime>,
    pub(super) trades: Vec<Trade>,
    pub(super) total_commission: f64,
    // Marks and equity curves.
    pub(super) last_known_prices: PriceTable,
    pub(super) equity_curve: Vec<EquityPoint>,
    pub(super) intraday_equity: Vec<EquityPoint>,
    /// Last trading date processed post-warm-up (never set during warm-up).
    pub(super) last_date: Option<NaiveDate>,
    // Order books: orders awaiting the next bar under FillTiming::NextBarOpen,
    // and resting limit/stop orders filled intrabar, one queue per symbol.
    pub(super) deferred: SymbolMap<Vec<Order>>,
    pub(super) resting_book: SymbolMap<Vec<RestingOrder>>,
    /// Bar volume already consumed per symbol this tick, so all fills against
    /// one bar share the volume-participation cap. Cleared every tick.
    pub(super) participation_used: SymbolMap<f64>,
    // Corporate actions and delist detection.
    pub(super) pending: PendingActions,
    /// Last calendar date seen by the stream (including warm-up), gating the
    /// once-per-date day-start work.
    pub(super) global_last_date: Option<NaiveDate>,
    pub(super) day_index: u64,
    /// Which trading day (by index) each symbol last printed a bar on;
    /// `None` until it prints its first.
    pub(super) last_seen_day: SymbolVec<Option<u64>>,
    /// The slice map handed to `on_data`, kept between ticks so a tick's
    /// bars reuse one allocation instead of building a fresh map.
    slice_bars: SymbolMap<Bar>,
}

impl Engine {
    fn new(ctx: Context, pending: PendingActions) -> Self {
        Self {
            initial_cash: ctx.portfolio.cash,
            ctx,
            lifetimes: SymbolMap::default(),
            trades: Vec::new(),
            total_commission: 0.0,
            last_known_prices: PriceTable::new(),
            equity_curve: Vec::new(),
            intraday_equity: Vec::new(),
            last_date: None,
            deferred: SymbolMap::default(),
            resting_book: SymbolMap::default(),
            participation_used: SymbolMap::default(),
            pending,
            global_last_date: None,
            day_index: 0,
            last_seen_day: SymbolVec::new(),
            slice_bars: SymbolMap::default(),
        }
    }

    /// Process one tick: every subscribed bar sharing one timestamp. The
    /// phase order here is load-bearing — see the comments on each step.
    fn process_tick<A: Algorithm>(
        &mut self,
        algo: &mut A,
        tick_time: DateTime<Utc>,
        tick_date: NaiveDate,
        bars: Vec<(Symbol, Bar)>,
    ) {
        self.participation_used.clear();

        // Day boundary: close out the previous day *before* this bar touches
        // any state, so on_end_of_day and the equity mark see the world
        // exactly as it was at yesterday's last bar. (last_date is never set
        // during warm-up, so purely-warm-up days never fire this.)
        if let Some(prev) = self.last_date {
            if tick_date != prev {
                self.close_day(algo, prev);
            }
        }

        // Corporate actions, financing, and the delist scan, once per
        // calendar date, after the previous day's equity mark (which must see
        // pre-split state) and before the new day's bars touch anything.
        self.start_new_day(algo, tick_date, tick_time);

        for &(symbol, _) in &bars {
            self.last_seen_day.set(symbol, Some(self.day_index));
        }

        self.fill_deferred(&bars, tick_time);
        self.fill_resting(&bars, tick_time);

        // Advance the marks only *after* the deferred/resting fills above:
        // their sizing values the portfolio at the prior closes the orders
        // were decided on — no look-ahead into the bar being filled.
        for (symbol, bar) in &bars {
            self.last_known_prices.set(*symbol, Some(bar.close));
        }

        self.record_history(&bars);

        if self.ctx.warm_up_remaining > 0 {
            self.ctx.warm_up_remaining -= 1;
            if self.ctx.warm_up_remaining == 0 && self.ctx.log_config.run_summary {
                eprintln!("[backtest] warm-up complete after bar at {}", tick_time.to_rfc3339());
            }
            return;
        }

        // The first processed bar anchors the equity curve at the starting
        // capital, so day-one PnL is visible in returns and drawdown.
        if self.last_date.is_none() {
            if let Some(day_before) = tick_date.pred_opt() {
                self.equity_curve
                    .push(EquityPoint { time: day_before.to_string(), equity: self.initial_cash });
            }
        }
        self.last_date = Some(tick_date);

        // Fire scheduled time callbacks (times are US Eastern).
        self.ctx.fire_time_callbacks(&tick_time, tick_date);

        // Reuse the previous tick's map allocation: a wide universe rebuilds
        // this every minute, and the capacity is the same every time.
        let mut slice = Slice { time: tick_time, bars: std::mem::take(&mut self.slice_bars) };
        slice.bars.extend(bars);
        algo.on_data(&mut self.ctx, &slice);

        self.route_new_orders(&slice, tick_time);

        self.slice_bars = slice.bars;
        self.slice_bars.clear();

        // Optional per-bar equity mark, after this bar's fills.
        if self.ctx.track_intraday_equity {
            self.intraday_equity.push(EquityPoint {
                time: tick_time.to_rfc3339(),
                equity: self.ctx.portfolio.total_value(&self.last_known_prices),
            });
        }
    }

    /// Mark `day`'s closing equity and fire `on_end_of_day`.
    fn close_day<A: Algorithm>(&mut self, algo: &mut A, day: NaiveDate) {
        let equity = self.ctx.portfolio.total_value(&self.last_known_prices);
        self.equity_curve.push(EquityPoint { time: day.to_string(), equity });
        if self.ctx.log_config.daily_recap {
            eprintln!(
                "[recap] {day}: equity {:.2}, cash {:.2}, {} open position(s)",
                equity,
                self.ctx.portfolio.cash,
                self.ctx.portfolio.positions.len()
            );
        }
        algo.on_end_of_day(&mut self.ctx);
    }

    /// Append this tick's bars to the per-symbol history and feed the
    /// consolidators. Runs during warm-up too, so history and consolidator
    /// state (e.g. a 60-min bar) build up over the warm-up period.
    fn record_history(&mut self, bars: &[(Symbol, Bar)]) {
        for (symbol, bar) in bars {
            let hist = self.ctx.history_store.slot(*symbol);
            hist.push_front(bar.clone());
            if hist.len() > self.ctx.max_history {
                hist.pop_back();
            }
        }
        if self.ctx.consolidators.is_empty() {
            return;
        }
        for (symbol, bar) in bars {
            for c in self.ctx.consolidators.iter_mut() {
                if c.symbol == *symbol {
                    c.feed(bar);
                }
            }
        }
    }

    fn log_startup(&self, files: &[PathBuf]) {
        // A date range extending beyond the months the data source has is
        // worth flagging but not fatal: the backtest simply runs over the
        // overlap. `files` is sorted by (year, month), so coverage is
        // first..=last.
        if self.ctx.log_config.warnings {
            let months: Vec<(u32, u32)> = files.iter().filter_map(|p| file_year_month(p)).collect();
            if let (Some(&first), Some(&last)) = (months.first(), months.last()) {
                if let Some(start) = self.ctx.start_date {
                    if (start.year() as u32, start.month()) < first {
                        eprintln!(
                            "[warn] start date {start} predates the data (first month \
                             {}-{:02}); the backtest will begin where the data does",
                            first.0, first.1
                        );
                    }
                }
                if let Some(end) = self.ctx.end_date {
                    if (end.year() as u32, end.month()) > last {
                        eprintln!(
                            "[warn] end date {end} is beyond the data (last month {}-{:02}); \
                             the backtest will stop where the data does",
                            last.0, last.1
                        );
                    }
                }
            }
        }

        if self.ctx.log_config.run_summary {
            let mut symbols: Vec<&str> =
                self.ctx.subscribed_symbols.iter().map(|&s| self.ctx.symbols.name(s)).collect();
            symbols.sort();
            let fmt = |d: Option<NaiveDate>| d.map_or("open".to_string(), |d| d.to_string());
            eprintln!(
                "[backtest] start: {} -> {}, cash {:.2}, warm-up {} bars, symbols {:?}",
                fmt(self.ctx.start_date),
                fmt(self.ctx.end_date),
                self.initial_cash,
                self.ctx.warm_up_remaining,
                symbols
            );
        }
    }

    /// End of data: flush consolidators, close the final day, and assemble
    /// the result.
    fn finish<A: Algorithm>(mut self, algo: &mut A) -> BacktestResult {
        // Fire any incomplete consolidation periods buffered at end of data.
        for c in self.ctx.consolidators.iter_mut() {
            c.flush();
        }

        // The final day gets its end-of-day callback here (mid-backtest it
        // fires on the next day's first bar, which the last day doesn't
        // have). Orders placed in it have no bar left to fill against and
        // are dropped.
        if let Some(prev) = self.last_date {
            self.close_day(algo, prev);
        }

        let final_equity = self.ctx.portfolio.total_value(&self.last_known_prices);
        if self.ctx.log_config.run_summary {
            eprintln!(
                "[backtest] done: {} completed trade(s), {} open position(s), final equity {:.2}",
                self.trades.len(),
                self.ctx.portfolio.positions.len(),
                final_equity
            );
        }

        let mut open_positions: Vec<OpenPositionSummary> = self
            .ctx
            .portfolio
            .positions
            .values()
            .map(|p| {
                let last_price = self.last_known_prices.copied(p.symbol).unwrap_or(p.avg_price);
                OpenPositionSummary {
                    // Back to a ticker string: the result JSON is the other
                    // end of the run, where symbols leave the engine.
                    symbol: self.ctx.symbols.name(p.symbol).to_string(),
                    quantity: p.quantity,
                    avg_price: p.avg_price,
                    last_price,
                    market_value: p.quantity * last_price,
                    unrealized_pnl: (last_price - p.avg_price) * p.quantity,
                    realized_pnl: self
                        .lifetimes
                        .get(&p.symbol)
                        .map(|lt| lt.realized_pnl)
                        .unwrap_or(0.0),
                }
            })
            .collect();
        open_positions.sort_by(|a, b| a.symbol.cmp(&b.symbol));

        let stats = compute_stats(&self.trades, &self.equity_curve, self.ctx.risk_free_rate);

        BacktestResult {
            initial_cash: self.initial_cash,
            final_equity,
            total_commission: self.total_commission,
            stats,
            equity_curve: self.equity_curve,
            intraday_equity: self.intraday_equity,
            open_positions,
            trades: self.trades,
        }
    }
}

/// The engine body shared by [`super::run_backtest`] and [`super::run`]:
/// `ctx` has already been through the algorithm's `initialize`.
pub(super) fn run_prepared<A: Algorithm>(
    mut algo: A,
    mut ctx: Context,
    data_path: &str,
) -> Result<BacktestResult, BacktestError> {
    // An inverted date range is a configuration mistake, not a data problem:
    // fail up front instead of silently producing an empty backtest.
    if let (Some(start), Some(end)) = (ctx.start_date, ctx.end_date) {
        if start > end {
            return Err(BacktestError::InvalidDateRange { start, end });
        }
    }

    // The last place ticker strings are read: the metadata files resolve to
    // symbols here, and the encoded ticker ids in the data get an array
    // mapping into them.
    let (resolver, pending) = load_pending_actions(&mut ctx, data_path)?;

    let files = sorted_parquet_files(data_path);
    if files.is_empty() {
        return Err(BacktestError::NoData { path: data_path.into() });
    }

    let mut eng = Engine::new(ctx, pending);
    eng.log_startup(&files);

    let mut mask = None;

    // The last tick processed, across files: the stream must never move
    // backwards in time (see the check inside the tick loop).
    let mut last_tick_time: Option<DateTime<Utc>> = None;

    // Stream the dataset file by file, tick by tick: files are
    // month-partitioned and chronologically sorted, and each file is consumed
    // through a TickReader (which never re-sorts and errors on an in-file
    // timestamp regression), so only a single tick of subscribed bars is ever
    // resident. It also lets the end-date break stop decoding mid-file.
    'files: for file_path in &files {
        // Skip whole months outside the configured date range.
        if let Some((y, m)) = file_year_month(file_path) {
            if let Some(start) = eng.ctx.start_date {
                if (y, m) < (start.year() as u32, start.month()) {
                    continue;
                }
            }
            if let Some(end) = eng.ctx.end_date {
                if (y, m) > (end.year() as u32, end.month()) {
                    break;
                }
            }
        }

        let mut ticks = TickReader::new(file_path, &resolver, &mut mask)?;
        while let Some((ts_ns, bars)) = ticks.next_tick()? {
            let secs = ts_ns / 1_000_000_000;
            let nanos = (ts_ns % 1_000_000_000) as u32;
            let Some(tick_time) = Utc.timestamp_opt(secs, nanos).single() else { continue };

            // Ticks must be non-decreasing in time. Within a file TickReader
            // guarantees it, so a violation means a bar was filed under the
            // wrong month partition. Time can't run backwards mid-backtest
            // (day-boundary and fill logic would corrupt), so the run fails
            // instead of processing the bar.
            if let Some(prev) = last_tick_time {
                if tick_time < prev {
                    return Err(BacktestError::OutOfOrderData {
                        path: file_path.clone(),
                        at: tick_time,
                        stream_at: prev,
                    });
                }
            }
            last_tick_time = Some(tick_time);

            // Trading date in US Eastern, so after-market bars (which cross
            // midnight UTC) stay on the day they belong to.
            let tick_date = tick_time.with_timezone(&Eastern).date_naive();
            if let Some(start) = eng.ctx.start_date {
                if tick_date < start {
                    continue;
                }
            }
            if let Some(end) = eng.ctx.end_date {
                if tick_date > end {
                    break 'files;
                }
            }

            eng.process_tick(&mut algo, tick_time, tick_date, bars);
        }
    }

    Ok(eng.finish(&mut algo))
}

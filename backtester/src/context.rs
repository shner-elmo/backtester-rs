use std::{collections::VecDeque, path::PathBuf};

use chrono::{DateTime, NaiveDate, Utc};
use chrono_tz::US::Eastern;

use crate::{
    bar::Bar,
    broker::Portfolio,
    commission::{CommissionModel, NoCommission},
    consolidator::{ConsolidatorEntry, ConsolidatorPeriod},
    data::TickerMap,
    logging::LogConfig,
    margin::{MarginModel, NoMargin},
    slippage::{NoSlippage, SlippageModel},
    symbol::{Symbol, SymbolSet, SymbolVec},
};

pub(crate) enum OrderKind {
    Market(f64),
    SetHoldings(f64),
    Liquidate,
}

/// When an order placed during `on_data` actually fills.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum FillTiming {
    /// Fill at the close of the same bar the order was placed on. Simple, but
    /// optimistic: the strategy transacts at a price it has already seen.
    /// This is the default.
    #[default]
    CurrentBarClose,
    /// Fill at the open of the symbol's *next* bar. Removes the same-bar
    /// look-ahead; orders placed on a symbol's final bar never fill. Sizing
    /// for `set_holdings` uses prices known when the order was placed.
    NextBarOpen,
}

pub(crate) struct Order {
    pub symbol: Symbol,
    pub kind: OrderKind,
}

/// A resting order that fills intrabar when the price trades through its
/// trigger, rather than at the bar close/open like a market order.
pub(crate) struct RestingOrder {
    pub symbol: Symbol,
    /// Remaining signed quantity (shrinks as partial fills chip away at it).
    pub qty: f64,
    pub kind: RestingKind,
}

#[derive(Clone, Copy)]
pub(crate) enum RestingKind {
    /// Fill at this price or better: a buy needs `bar.low <= price`, a sell
    /// needs `bar.high >= price`.
    Limit(f64),
    /// Trigger to a market fill when the price reaches this level: a buy needs
    /// `bar.high >= price`, a sell needs `bar.low <= price`.
    Stop(f64),
}

pub(crate) struct ScheduledTimeEntry {
    /// Target time as minutes since midnight ET, precomputed at registration.
    pub target_min: u32,
    pub last_fired_date: Option<NaiveDate>,
    pub callback: Box<dyn FnMut(&mut Context)>,
}

pub struct Context {
    pub portfolio: Portfolio,
    /// The dataset's ticker naming, loaded before `initialize` runs so
    /// `add_equity` can hand back the ticker id the data itself uses. Read
    /// when subscribing, when matching corporate actions, and when reporting
    /// — never per bar.
    pub(crate) tickers: TickerMap,
    pub(crate) history_store: SymbolVec<VecDeque<Bar>>,
    pub(crate) consolidators: Vec<ConsolidatorEntry>,
    pub(crate) time_callbacks: Vec<ScheduledTimeEntry>,
    pub(crate) warm_up_remaining: usize,
    pub(crate) start_date: Option<NaiveDate>,
    pub(crate) end_date: Option<NaiveDate>,
    pub(crate) subscribed_symbols: SymbolSet,
    pub(crate) pending_orders: Vec<Order>,
    pub(crate) resting_orders: Vec<RestingOrder>,
    pub(crate) max_volume_participation: f64,
    pub(crate) max_history: usize,
    pub(crate) slippage: Box<dyn SlippageModel>,
    pub(crate) commission: Box<dyn CommissionModel>,
    pub(crate) margin: Box<dyn MarginModel>,
    pub(crate) lot_size: f64,
    pub(crate) delist_after_days: usize,
    pub(crate) delist_haircut: f64,
    pub(crate) margin_interest_rate: f64,
    pub(crate) short_borrow_rate: f64,
    pub(crate) risk_free_rate: f64,
    pub(crate) track_intraday_equity: bool,
    pub(crate) fill_timing: FillTiming,
    pub(crate) log_config: LogConfig,
    pub(crate) output_dir: Option<PathBuf>,
    pub(crate) splits_file: Option<PathBuf>,
    pub(crate) dividends_file: Option<PathBuf>,
    pub(crate) renames_file: Option<PathBuf>,
}

impl Default for Context {
    fn default() -> Self {
        Self {
            portfolio: Portfolio::default(),
            tickers: TickerMap::default(),
            history_store: SymbolVec::new(),
            consolidators: Vec::new(),
            time_callbacks: Vec::new(),
            warm_up_remaining: 0,
            start_date: None,
            end_date: None,
            subscribed_symbols: SymbolSet::default(),
            pending_orders: Vec::new(),
            resting_orders: Vec::new(),
            max_volume_participation: 0.0,
            max_history: 500,
            slippage: Box::new(NoSlippage),
            commission: Box::new(NoCommission),
            margin: Box::new(NoMargin),
            lot_size: 1.0,
            delist_after_days: 5,
            delist_haircut: 0.0,
            margin_interest_rate: 0.0,
            short_borrow_rate: 0.0,
            risk_free_rate: 0.0,
            track_intraday_equity: false,
            fill_timing: FillTiming::default(),
            log_config: LogConfig::default(),
            output_dir: None,
            splits_file: None,
            dividends_file: None,
            renames_file: None,
        }
    }
}

impl Context {
    /// A context for a run over the dataset `tickers` describes. The engine
    /// builds this before calling `initialize`, so `add_equity` can resolve a
    /// ticker to the id the data encodes it as.
    pub(crate) fn with_tickers(tickers: TickerMap) -> Self {
        let history_store = SymbolVec::with_len(tickers.id_space());
        Self { tickers, history_store, ..Self::default() }
    }

    pub fn set_start_date(&mut self, y: i32, m: u32, d: u32) {
        self.start_date = NaiveDate::from_ymd_opt(y, m, d);
    }

    pub fn set_end_date(&mut self, y: i32, m: u32, d: u32) {
        self.end_date = NaiveDate::from_ymd_opt(y, m, d);
    }

    pub fn set_cash(&mut self, cash: f64) {
        self.portfolio.cash = cash;
    }

    /// Skip the first `bars` time steps before `on_data` fires. History and
    /// consolidators still fill during warm-up. Counts *ticks*, not per-symbol
    /// bars: several symbols sharing one timestamp consume a single step.
    pub fn set_warm_up(&mut self, bars: usize) {
        self.warm_up_remaining = bars;
        if bars > self.max_history {
            self.max_history = bars;
        }
    }

    /// Subscribe a ticker's bars and return the [`Symbol`] — the dataset's id
    /// for it — that stands in for it everywhere else: orders, history,
    /// slices, callbacks. This is the one place a strategy spells a ticker
    /// out; keep the returned handle.
    ///
    /// Subscribing the same ticker twice returns the same symbol.
    ///
    /// # Panics
    ///
    /// If the dataset has no such ticker. That is a configuration mistake
    /// (usually a typo) which would otherwise cost a whole run to discover,
    /// since an unknown ticker simply never prints a bar. Use
    /// [`try_add_equity`](Self::try_add_equity) when the universe is dynamic
    /// and a miss is expected.
    pub fn add_equity(&mut self, ticker: &str) -> Symbol {
        self.try_add_equity(ticker).unwrap_or_else(|| {
            panic!(
                "ticker {ticker:?} is not in this dataset's ticker map; check the spelling \
                 or the data root"
            )
        })
    }

    /// [`add_equity`](Self::add_equity) for a ticker that may legitimately be
    /// missing: returns `None` instead of panicking.
    pub fn try_add_equity(&mut self, ticker: &str) -> Option<Symbol> {
        let symbol = self.tickers.symbol(ticker)?;
        self.subscribe(symbol);
        Some(symbol)
    }

    /// Subscribe every symbol the dataset has data for — the whole universe,
    /// for strategies that screen it rather than pick names up front.
    pub fn add_all_equities(&mut self) -> Vec<Symbol> {
        let all: Vec<Symbol> = self.tickers.symbols().collect();
        self.subscribed_symbols.extend(all.iter().copied());
        all
    }

    pub(crate) fn subscribe(&mut self, symbol: Symbol) {
        self.subscribed_symbols.insert(symbol);
    }

    /// The symbol for a ticker the dataset has, or `None`. For strategies
    /// that would rather look a ticker up than carry the handle around — a
    /// hash lookup on your side of the API, not the engine's.
    pub fn symbol(&self, ticker: &str) -> Option<Symbol> {
        self.tickers.symbol(ticker)
    }

    /// The ticker string behind a symbol, for logging and reporting (`"?"`
    /// for a symbol the dataset's map doesn't list, which subscribing can't
    /// produce).
    pub fn symbol_name(&self, symbol: Symbol) -> &str {
        self.tickers.name(symbol).unwrap_or("?")
    }

    /// Set the slippage model applied to every fill (built-in, trait impl, or
    /// closure — see [`slippage`](crate::slippage)). Defaults to none.
    pub fn set_slippage(&mut self, model: impl SlippageModel + 'static) {
        self.slippage = Box::new(model);
    }

    /// Set the commission model applied to every fill (built-in, trait impl,
    /// or closure — see [`commission`](crate::commission)). Defaults to none.
    pub fn set_commission(&mut self, model: impl CommissionModel + 'static) {
        self.commission = Box::new(model);
    }

    /// Set the margin (buying-power) model consulted before every fill
    /// (built-in, trait impl, or closure — see [`margin`](crate::margin)).
    /// Defaults to [`NoMargin`](crate::margin::NoMargin): unlimited buying
    /// power, cash may go arbitrarily negative.
    pub fn set_margin_model(&mut self, model: impl MarginModel + 'static) {
        self.margin = Box::new(model);
    }

    /// A held symbol that produces no bars for this many consecutive trading
    /// days is treated as delisted: the position is force-closed at its last
    /// known price and `Algorithm::on_delisted` fires. Defaults to 5; pass
    /// `0` to disable.
    pub fn set_delist_after_days(&mut self, days: usize) {
        self.delist_after_days = days;
    }

    /// Fraction knocked off the last known price when a delisted position is
    /// force-liquidated, modeling the gap between the last print and what you
    /// actually recover (a bankruptcy rarely liquidates at its final quote).
    /// `0.0` (the default) fills at the last price; `1.0` writes the position
    /// off entirely. Must be in `0.0..=1.0`.
    pub fn set_delist_haircut(&mut self, haircut: f64) {
        assert!((0.0..=1.0).contains(&haircut), "delist haircut must be in 0.0..=1.0");
        self.delist_haircut = haircut;
    }

    /// Annual interest rate charged on a negative cash balance (buying on
    /// margin). Accrues at `rate / 252` per trading day on any positions
    /// carried into a new day, spread across the long book by market value.
    /// Defaults to `0.0` (free leverage). Must be non-negative.
    pub fn set_margin_interest_rate(&mut self, annual_rate: f64) {
        assert!(annual_rate >= 0.0, "margin interest rate must be non-negative");
        self.margin_interest_rate = annual_rate;
    }

    /// Annual borrow fee charged on the market value of short positions.
    /// Accrues at `rate / 252` per trading day on any short carried into a new
    /// day and is attributed to that short's PnL. Defaults to `0.0` (free to
    /// borrow). Must be non-negative.
    pub fn set_short_borrow_rate(&mut self, annual_rate: f64) {
        assert!(annual_rate >= 0.0, "short borrow rate must be non-negative");
        self.short_borrow_rate = annual_rate;
    }

    /// Directory `run` writes `backtest_result_<timestamp>.json` into
    /// (created if missing). Takes precedence over the `BACKTEST_OUTPUT_DIR`
    /// env var; when neither is set the file goes to the current working
    /// directory. Ignored by `run_backtest`, which never writes a file.
    pub fn set_output_dir(&mut self, dir: impl Into<PathBuf>) {
        self.output_dir = Some(dir.into());
    }

    /// Path of the Polygon-format stock-splits JSON (default
    /// `get_splits.json` in the data root). A relative path is resolved
    /// against the data root passed to `run`/`run_backtest`; an absolute path
    /// is used as-is. When left at the default a missing file simply means
    /// "no splits"; a path set here must exist, so a typo fails the run
    /// instead of silently skipping every split.
    pub fn set_splits_file(&mut self, path: impl Into<PathBuf>) {
        self.splits_file = Some(path.into());
    }

    /// Path of the Polygon-format cash-dividends JSON (default
    /// `get_dividends.json` in the data root). Same resolution and
    /// missing-file rules as [`set_splits_file`](Self::set_splits_file).
    pub fn set_dividends_file(&mut self, path: impl Into<PathBuf>) {
        self.dividends_file = Some(path.into());
    }

    /// Path of the ticker-renames JSON (default `ticker_renames.json` in the
    /// data root). Same resolution and missing-file rules as
    /// [`set_splits_file`](Self::set_splits_file).
    pub fn set_renames_file(&mut self, path: impl Into<PathBuf>) {
        self.renames_file = Some(path.into());
    }

    /// Annual risk-free rate the Sharpe ratio is computed in excess of
    /// (daily returns less `rate / 252`). Defaults to `0.0`. Only affects
    /// reported stats — no cash accrues on idle balances.
    pub fn set_risk_free_rate(&mut self, annual_rate: f64) {
        assert!(annual_rate.is_finite(), "risk-free rate must be finite");
        self.risk_free_rate = annual_rate;
    }

    /// Set how many bars per symbol `history()` retains (the rolling window
    /// depth). Defaults to 500. `set_warm_up` raises it to cover the warm-up
    /// length, so call this *after* `set_warm_up` to pick a smaller window on
    /// purpose. Must be at least 1.
    pub fn set_max_history(&mut self, bars: usize) {
        assert!(bars >= 1, "max history must be at least 1");
        self.max_history = bars;
    }

    /// Record a mark-to-market equity point on **every bar** (into
    /// `BacktestResult::intraday_equity`), not just at day boundaries, so
    /// drawdown that opens and recovers within a single day is visible. Off by
    /// default because it can add a lot of points on minute data; the headline
    /// `equity_curve` and its stats stay daily either way.
    pub fn set_track_intraday_equity(&mut self, enabled: bool) {
        self.track_intraday_equity = enabled;
    }

    /// Choose which engine events are logged to stderr as the backtest runs:
    /// the run header/footer, every fill and completed trade, a per-day recap,
    /// corporate events, and data-quality warnings — see [`LogConfig`] for the
    /// flags. Defaults to warnings only; `LogConfig::all()` /
    /// `LogConfig::none()` turn everything on / off.
    pub fn set_log_config(&mut self, config: LogConfig) {
        self.log_config = config;
    }

    /// Choose when orders fill: at the current bar's close (the default,
    /// [`FillTiming::CurrentBarClose`]) or at the open of the symbol's next bar
    /// ([`FillTiming::NextBarOpen`]). Next-bar-open removes the same-bar
    /// look-ahead that inflates results for strategies reacting to the very
    /// bar they trade on.
    pub fn set_fill_timing(&mut self, timing: FillTiming) {
        self.fill_timing = timing;
    }

    /// Set the share lot `set_holdings` rounds order quantities to. Defaults
    /// to `1.0` (whole shares); use e.g. `0.1` or `0.001` to allow fractional
    /// shares, or `100.0` for round lots. Must be positive.
    pub fn set_lot_size(&mut self, lot: f64) {
        assert!(lot > 0.0, "lot size must be positive");
        self.lot_size = lot;
    }

    pub fn history(&self, symbol: Symbol, n: usize) -> Vec<Bar> {
        self.history_store
            .get(symbol)
            .map(|deque| deque.iter().take(n).cloned().collect())
            .unwrap_or_default()
    }

    pub fn consolidate(
        &mut self,
        symbol: Symbol,
        period: ConsolidatorPeriod,
        cb: impl FnMut(&Bar) + 'static,
    ) {
        self.consolidators.push(ConsolidatorEntry::new(symbol, period, Box::new(cb)));
    }

    /// Schedule a callback to fire once per trading day when the bar stream first
    /// reaches or crosses the given US Eastern hour:minute (e.g., 9, 30 for market open).
    pub fn on_time(
        &mut self,
        hour: u32,
        minute: u32,
        callback: impl FnMut(&mut Context) + 'static,
    ) {
        self.time_callbacks.push(ScheduledTimeEntry {
            target_min: hour * 60 + minute,
            last_fired_date: None,
            callback: Box::new(callback),
        });
    }

    pub(crate) fn fire_time_callbacks(&mut self, tick_time: &DateTime<Utc>, tick_date: NaiveDate) {
        use chrono::Timelike;
        let tick_et = tick_time.with_timezone(&Eastern);
        let tick_min = tick_et.hour() * 60 + tick_et.minute();
        let n = self.time_callbacks.len();
        for i in 0..n {
            let entry_min = self.time_callbacks[i].target_min;
            if self.time_callbacks[i].last_fired_date == Some(tick_date) {
                continue;
            }
            if tick_min >= entry_min {
                self.time_callbacks[i].last_fired_date = Some(tick_date);
                // Swap the callback out per entry (rather than taking the
                // whole vec) so a callback that registers a new on_time
                // callback isn't clobbered when the vec is restored.
                let mut cb = std::mem::replace(
                    &mut self.time_callbacks[i].callback,
                    Box::new(|_: &mut Context| {}),
                );
                cb(self);
                self.time_callbacks[i].callback = cb;
            }
        }
    }

    pub fn market_order(&mut self, symbol: Symbol, qty: f64) {
        self.pending_orders.push(Order { symbol, kind: OrderKind::Market(qty) });
    }

    pub fn set_holdings(&mut self, symbol: Symbol, pct: f64) {
        self.pending_orders.push(Order { symbol, kind: OrderKind::SetHoldings(pct) });
    }

    pub fn liquidate(&mut self, symbol: Symbol) {
        self.pending_orders.push(Order { symbol, kind: OrderKind::Liquidate });
    }

    /// Place a resting **limit** order: a buy (`qty > 0`) fills only at
    /// `limit_price` or lower, a sell (`qty < 0`) at `limit_price` or higher.
    /// It rests across bars until the price trades through the limit (or the
    /// backtest ends), filling intrabar off the bar's range — independent of
    /// `set_fill_timing`.
    pub fn limit_order(&mut self, symbol: Symbol, qty: f64, limit_price: f64) {
        // A zero-quantity order can never trigger; don't let it rest forever.
        if qty == 0.0 {
            return;
        }
        self.resting_orders.push(RestingOrder {
            symbol,
            qty,
            kind: RestingKind::Limit(limit_price),
        });
    }

    /// Place a resting **stop** order: a buy (`qty > 0`) triggers to a market
    /// fill when the price rises to `stop_price`, a sell (`qty < 0`) when it
    /// falls to it. Rests across bars until triggered (or the backtest ends).
    pub fn stop_order(&mut self, symbol: Symbol, qty: f64, stop_price: f64) {
        // A zero-quantity order can never trigger; don't let it rest forever.
        if qty == 0.0 {
            return;
        }
        self.resting_orders.push(RestingOrder { symbol, qty, kind: RestingKind::Stop(stop_price) });
    }

    /// Cap every fill at this fraction of the filling bar's volume, modeling
    /// partial fills — a resting order's unfilled remainder carries to the next
    /// bar; a market order's remainder is dropped. `0.0` (the default) means
    /// unlimited: fills ignore bar volume. Must be non-negative.
    pub fn set_max_volume_participation(&mut self, fraction: f64) {
        assert!(fraction >= 0.0, "volume participation must be non-negative");
        self.max_volume_participation = fraction;
    }
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Mutex};

    use chrono::TimeZone;

    use super::*;

    fn et(y: i32, mo: u32, d: u32, h: u32, m: u32) -> DateTime<Utc> {
        Eastern.with_ymd_and_hms(y, mo, d, h, m, 0).unwrap().with_timezone(&Utc)
    }

    #[test]
    fn fires_once_at_target_time() {
        let mut ctx = Context::default();
        let count = Arc::new(Mutex::new(0usize));
        let c = count.clone();
        ctx.on_time(9, 30, move |_| *c.lock().unwrap() += 1);

        let t = et(2023, 1, 3, 9, 30);
        ctx.fire_time_callbacks(&t, t.date_naive());
        assert_eq!(*count.lock().unwrap(), 1);
    }

    #[test]
    fn does_not_fire_before_target_time() {
        let mut ctx = Context::default();
        let count = Arc::new(Mutex::new(0usize));
        let c = count.clone();
        ctx.on_time(9, 30, move |_| *c.lock().unwrap() += 1);

        let t = et(2023, 1, 3, 9, 29);
        ctx.fire_time_callbacks(&t, t.date_naive());
        assert_eq!(*count.lock().unwrap(), 0);
    }

    #[test]
    fn fires_only_once_per_day() {
        let mut ctx = Context::default();
        let count = Arc::new(Mutex::new(0usize));
        let c = count.clone();
        ctx.on_time(9, 30, move |_| *c.lock().unwrap() += 1);

        // Fire at target, then again later same day — should still be 1
        let t1 = et(2023, 1, 3, 9, 30);
        ctx.fire_time_callbacks(&t1, t1.date_naive());
        let t2 = et(2023, 1, 3, 10, 0);
        ctx.fire_time_callbacks(&t2, t2.date_naive());
        assert_eq!(*count.lock().unwrap(), 1);
    }

    #[test]
    fn fires_again_on_next_day() {
        let mut ctx = Context::default();
        let count = Arc::new(Mutex::new(0usize));
        let c = count.clone();
        ctx.on_time(9, 30, move |_| *c.lock().unwrap() += 1);

        let t1 = et(2023, 1, 3, 9, 30);
        ctx.fire_time_callbacks(&t1, t1.date_naive());
        let t2 = et(2023, 1, 4, 9, 30);
        ctx.fire_time_callbacks(&t2, t2.date_naive());
        assert_eq!(*count.lock().unwrap(), 2);
    }

    #[test]
    fn fires_on_next_bar_when_exact_minute_missing() {
        let mut ctx = Context::default();
        let count = Arc::new(Mutex::new(0usize));
        let c = count.clone();
        ctx.on_time(15, 55, move |_| *c.lock().unwrap() += 1);

        // 15:54 — not yet
        let t1 = et(2023, 1, 3, 15, 54);
        ctx.fire_time_callbacks(&t1, t1.date_naive());
        assert_eq!(*count.lock().unwrap(), 0);

        // 15:56 — 15:55 was missing, fires here
        let t2 = et(2023, 1, 3, 15, 56);
        ctx.fire_time_callbacks(&t2, t2.date_naive());
        assert_eq!(*count.lock().unwrap(), 1);
    }

    #[test]
    fn multiple_registrations_both_fire() {
        let mut ctx = Context::default();
        let fired = Arc::new(Mutex::new(Vec::<u32>::new()));

        let f1 = fired.clone();
        ctx.on_time(9, 30, move |_| f1.lock().unwrap().push(930));
        let f2 = fired.clone();
        ctx.on_time(15, 55, move |_| f2.lock().unwrap().push(1555));

        // A bar at 15:55 ET should trigger both (9:30 was also crossed)
        let t = et(2023, 1, 3, 15, 55);
        ctx.fire_time_callbacks(&t, t.date_naive());

        let result = fired.lock().unwrap().clone();
        assert!(result.contains(&930));
        assert!(result.contains(&1555));
    }
}

use std::collections::{HashMap, HashSet, VecDeque};

use chrono::{DateTime, NaiveDate, Utc};
use chrono_tz::US::Eastern;

use crate::{
    bar::Bar,
    broker::Portfolio,
    commission::{CommissionModel, NoCommission},
    consolidator::{ConsolidatorEntry, ConsolidatorPeriod},
    slippage::{NoSlippage, SlippageModel},
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
    pub symbol: String,
    pub kind: OrderKind,
}

pub(crate) struct ScheduledTimeEntry {
    /// Target time as minutes since midnight ET, precomputed at registration.
    pub target_min: u32,
    pub last_fired_date: Option<NaiveDate>,
    pub callback: Box<dyn FnMut(&mut Context)>,
}

pub struct Context {
    pub portfolio: Portfolio,
    pub(crate) history_store: HashMap<String, VecDeque<Bar>>,
    pub(crate) consolidators: Vec<ConsolidatorEntry>,
    pub(crate) time_callbacks: Vec<ScheduledTimeEntry>,
    pub(crate) warm_up_remaining: usize,
    pub(crate) start_date: Option<NaiveDate>,
    pub(crate) end_date: Option<NaiveDate>,
    pub(crate) subscribed_symbols: HashSet<String>,
    pub(crate) pending_orders: Vec<Order>,
    pub(crate) max_history: usize,
    pub(crate) slippage: Box<dyn SlippageModel>,
    pub(crate) commission: Box<dyn CommissionModel>,
    pub(crate) lot_size: f64,
    pub(crate) delist_after_days: usize,
    pub(crate) fill_timing: FillTiming,
}

impl Default for Context {
    fn default() -> Self {
        Self {
            portfolio: Portfolio::default(),
            history_store: HashMap::new(),
            consolidators: Vec::new(),
            time_callbacks: Vec::new(),
            warm_up_remaining: 0,
            start_date: None,
            end_date: None,
            subscribed_symbols: HashSet::new(),
            pending_orders: Vec::new(),
            max_history: 500,
            slippage: Box::new(NoSlippage),
            commission: Box::new(NoCommission),
            lot_size: 1.0,
            delist_after_days: 5,
            fill_timing: FillTiming::default(),
        }
    }
}

impl Context {
    pub fn set_start_date(&mut self, y: i32, m: u32, d: u32) {
        self.start_date = NaiveDate::from_ymd_opt(y, m, d);
    }

    pub fn set_end_date(&mut self, y: i32, m: u32, d: u32) {
        self.end_date = NaiveDate::from_ymd_opt(y, m, d);
    }

    pub fn set_cash(&mut self, cash: f64) {
        self.portfolio.cash = cash;
    }

    pub fn set_warm_up(&mut self, bars: usize) {
        self.warm_up_remaining = bars;
        if bars > self.max_history {
            self.max_history = bars;
        }
    }

    pub fn add_equity(&mut self, symbol: &str) {
        self.subscribed_symbols.insert(symbol.to_string());
    }

    /// Set the slippage model applied to every fill. Accepts any built-in
    /// model, a custom [`SlippageModel`](crate::slippage::SlippageModel), or a
    /// closure `Fn(&FillContext) -> f64`. Defaults to no slippage.
    pub fn set_slippage(&mut self, model: impl SlippageModel + 'static) {
        self.slippage = Box::new(model);
    }

    /// Set the commission model applied to every fill. Accepts any built-in
    /// model, a custom [`CommissionModel`](crate::commission::CommissionModel),
    /// or a closure `Fn(&FillContext) -> f64` returning the cash charge.
    /// Defaults to no commission.
    pub fn set_commission(&mut self, model: impl CommissionModel + 'static) {
        self.commission = Box::new(model);
    }

    /// A held symbol that produces no bars for this many consecutive trading
    /// days is treated as delisted: the position is force-closed at its last
    /// known price and `Algorithm::on_delisted` fires. Defaults to 5; pass
    /// `0` to disable.
    pub fn set_delist_after_days(&mut self, days: usize) {
        self.delist_after_days = days;
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

    pub fn history(&self, symbol: &str, n: usize) -> Vec<Bar> {
        self.history_store
            .get(symbol)
            .map(|deque| deque.iter().take(n).cloned().collect())
            .unwrap_or_default()
    }

    pub fn consolidate(
        &mut self,
        symbol: &str,
        period: ConsolidatorPeriod,
        cb: impl FnMut(&Bar) + 'static,
    ) {
        self.consolidators.push(ConsolidatorEntry::new(symbol.to_string(), period, Box::new(cb)));
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
                let mut cb = std::mem::replace(
                    &mut self.time_callbacks[i].callback,
                    Box::new(|_: &mut Context| {}),
                );
                cb(self);
                self.time_callbacks[i].callback = cb;
            }
        }
    }

    pub fn market_order(&mut self, symbol: &str, qty: f64) {
        self.pending_orders
            .push(Order { symbol: symbol.to_string(), kind: OrderKind::Market(qty) });
    }

    pub fn set_holdings(&mut self, symbol: &str, pct: f64) {
        self.pending_orders
            .push(Order { symbol: symbol.to_string(), kind: OrderKind::SetHoldings(pct) });
    }

    pub fn liquidate(&mut self, symbol: &str) {
        self.pending_orders.push(Order { symbol: symbol.to_string(), kind: OrderKind::Liquidate });
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeZone;
    use std::sync::{Arc, Mutex};

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

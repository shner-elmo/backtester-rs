use std::collections::{HashMap, HashSet, VecDeque};

use chrono::NaiveDate;

use crate::{
    bar::Bar,
    broker::Portfolio,
    consolidator::{ConsolidatorEntry, ConsolidatorPeriod},
};

pub(crate) enum OrderKind {
    Market(f64),
    SetHoldings(f64),
    Liquidate,
}

pub(crate) struct Order {
    pub symbol: String,
    pub kind: OrderKind,
}

pub struct Context {
    pub portfolio: Portfolio,
    pub(crate) history_store: HashMap<String, VecDeque<Bar>>,
    pub(crate) consolidators: Vec<ConsolidatorEntry>,
    pub(crate) warm_up_remaining: usize,
    pub(crate) start_date: Option<NaiveDate>,
    pub(crate) end_date: Option<NaiveDate>,
    pub(crate) subscribed_symbols: HashSet<String>,
    pub(crate) pending_orders: Vec<Order>,
    pub(crate) max_history: usize,
}

impl Default for Context {
    fn default() -> Self {
        Self {
            portfolio: Portfolio::default(),
            history_store: HashMap::new(),
            consolidators: Vec::new(),
            warm_up_remaining: 0,
            start_date: None,
            end_date: None,
            subscribed_symbols: HashSet::new(),
            pending_orders: Vec::new(),
            max_history: 500,
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
        cb: impl Fn(&Bar) + 'static,
    ) {
        self.consolidators.push(ConsolidatorEntry::new(symbol.to_string(), period, Box::new(cb)));
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

//! Copy-trade corporate insiders ("CEO Stockwatcher" style).
//!
//! Reads `insider_transactions.json` from the data root (produced by the
//! `scripts/insider_fetch.rs` script from SEC Form 4 filings) and:
//!
//! - **buys** a stock the trading day after officers/directors disclose
//!   open-market purchases totalling at least `MIN_BUY_VALUE` on one filing
//!   day, equal-weighting up to `MAX_POSITIONS` concurrent positions;
//! - **sells** a held stock the trading day after insiders disclose sales
//!   totalling at least `MIN_SELL_VALUE`, or after `HOLD_DAYS` trading days,
//!   whichever comes first.
//!
//! Signals activate strictly after their `filing_date` — the day the Form 4
//! became public — never on the (earlier) transaction date, so the backtest
//! only trades on information that was actually available.
//!
//! Run against the committed fixture (one synthetic AAPL round trip):
//!
//! ```sh
//! cargo run --example insider_following -- backtester/tests/fixtures
//! ```

use std::collections::{BTreeMap, HashMap, HashSet};

use backtester::{
    data::load_ticker_map,
    insider::{load_insider_transactions, TransCode},
    run, Algorithm, Context, Slice, Symbol, SymbolMap,
};
use chrono::NaiveDate;
use chrono_tz::US::Eastern;

/// Aggregate officer/director purchase value ($) per (ticker, filing day)
/// needed to trigger a buy.
const MIN_BUY_VALUE: f64 = 250_000.0;
/// Aggregate insider sale value ($) per (ticker, filing day) that forces an
/// exit of a held position.
const MIN_SELL_VALUE: f64 = 100_000.0;
/// Equal-weight book: each position targets `1 / MAX_POSITIONS` of equity.
const MAX_POSITIONS: usize = 10;
/// Time-based exit after this many trading days without a sale signal.
const HOLD_DAYS: usize = 20;
/// Discard buy signals older than this many calendar days by the time they
/// are seen (backtest start boundary, halted stocks); a weekend or holiday
/// gap fits comfortably inside it.
const MAX_SIGNAL_AGE_DAYS: i64 = 5;

struct InsiderFollowing {
    data_root: String,
    /// filing date → symbols whose insider buys crossed the threshold.
    buy_signals: BTreeMap<NaiveDate, Vec<Symbol>>,
    /// filing date → symbols whose insider sales crossed the threshold.
    sell_signals: BTreeMap<NaiveDate, Vec<Symbol>>,
    /// symbol → trading days held so far.
    held: SymbolMap<usize>,
    last_date: Option<NaiveDate>,
}

impl InsiderFollowing {
    fn new(data_root: &str) -> Self {
        Self {
            data_root: data_root.to_string(),
            buy_signals: BTreeMap::new(),
            sell_signals: BTreeMap::new(),
            held: SymbolMap::default(),
            last_date: None,
        }
    }

    /// Everything that happens once per trading day, on its first bar:
    /// exits for sale signals and expired holds, then entries for fresh buy
    /// signals. Orders queue during `on_data` and fill on this day's bars.
    fn on_day_open(&mut self, ctx: &mut Context, today: NaiveDate) {
        for days in self.held.values_mut() {
            *days += 1;
        }

        // Sale filings dated before today (split_off keeps >= today for
        // later; what remains in the map head is due now).
        let not_yet_due = self.sell_signals.split_off(&today);
        let due = std::mem::replace(&mut self.sell_signals, not_yet_due);
        for symbol in due.into_values().flatten() {
            if self.held.remove(&symbol).is_some() {
                ctx.liquidate(symbol);
            }
        }
        self.held.retain(|symbol, days| {
            let expired = *days >= HOLD_DAYS;
            if expired {
                ctx.liquidate(*symbol);
            }
            !expired
        });

        let not_yet_due = self.buy_signals.split_off(&today);
        let due = std::mem::replace(&mut self.buy_signals, not_yet_due);
        for (filing_date, symbols) in due {
            if (today - filing_date).num_days() > MAX_SIGNAL_AGE_DAYS {
                continue; // stale: filed long before we could act
            }
            for symbol in symbols {
                if self.held.len() >= MAX_POSITIONS {
                    return;
                }
                if let std::collections::hash_map::Entry::Vacant(entry) = self.held.entry(symbol) {
                    ctx.set_holdings(symbol, 1.0 / MAX_POSITIONS as f64);
                    entry.insert(0);
                }
            }
        }
    }
}

impl Algorithm for InsiderFollowing {
    fn initialize(&mut self, ctx: &mut Context) {
        ctx.set_start_date(2023, 1, 1);
        ctx.set_end_date(2023, 12, 31);
        ctx.set_cash(100_000.0);

        // Universe = symbols present in the dataset; insider records for
        // anything else can't be traded and are dropped at load time.
        let universe: HashSet<String> = load_ticker_map(&self.data_root)
            .expect("data root must contain encoded_tickers.json")
            .into_values()
            .collect();
        let transactions = load_insider_transactions(&self.data_root, &universe)
            .expect("insider_transactions.json failed to load");

        // Signals are collected by ticker first, then resolved to dataset
        // ids in one pass below.
        let mut buy_tickers: BTreeMap<NaiveDate, Vec<String>> = BTreeMap::new();
        let mut sell_tickers: BTreeMap<NaiveDate, Vec<String>> = BTreeMap::new();
        for (ticker, by_date) in transactions {
            for (filing_date, txs) in by_date {
                let buys: f64 = txs
                    .iter()
                    .filter(|t| {
                        // Officer/director conviction buys only: 10%-owner
                        // purchases are often funds rebalancing.
                        t.code == TransCode::Purchase && (t.is_officer || t.is_director)
                    })
                    .map(|t| t.value)
                    .sum();
                let sells: f64 =
                    txs.iter().filter(|t| t.code == TransCode::Sale).map(|t| t.value).sum();
                if buys >= MIN_BUY_VALUE {
                    buy_tickers.entry(filing_date).or_default().push(ticker.clone());
                }
                if sells >= MIN_SELL_VALUE {
                    sell_tickers.entry(filing_date).or_default().push(ticker.clone());
                }
            }
        }

        // Subscribe once per ticker that actually produced a signal — the
        // universe is far wider than the names we end up trading — and keep
        // the returned `Symbol` so the signal maps hold dataset ids, not
        // strings, for the rest of the run.
        let mut ids: HashMap<String, Symbol> = HashMap::new();
        for ticker in buy_tickers.values().chain(sell_tickers.values()).flatten() {
            if !ids.contains_key(ticker) {
                if let Some(symbol) = ctx.try_add_equity(ticker) {
                    ids.insert(ticker.clone(), symbol);
                }
            }
        }
        let resolve = |by_date: BTreeMap<NaiveDate, Vec<String>>| {
            by_date
                .into_iter()
                .map(|(date, tickers)| {
                    (date, tickers.iter().filter_map(|t| ids.get(t).copied()).collect())
                })
                .collect()
        };
        self.buy_signals = resolve(buy_tickers);
        self.sell_signals = resolve(sell_tickers);
    }

    fn on_data(&mut self, ctx: &mut Context, data: &Slice) {
        // Trading date in US Eastern, matching the engine's own day
        // boundaries (after-market bars cross midnight UTC).
        let today = data.time.with_timezone(&Eastern).date_naive();
        if self.last_date != Some(today) {
            self.last_date = Some(today);
            self.on_day_open(ctx, today);
        }
    }
}

fn main() {
    let args: Vec<String> = std::env::args().collect();
    let data_path = args.get(1).map(String::as_str).unwrap_or("data/output/minute");

    run(InsiderFollowing::new(data_path), data_path).unwrap_or_else(|e| {
        eprintln!("backtest failed: {e}");
        std::process::exit(1);
    });
}

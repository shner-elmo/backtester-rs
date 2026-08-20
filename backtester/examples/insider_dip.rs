//! Insider *dip* buying: follow insiders only when they buy into a drawdown.
//!
//! An insider purchase near all-time highs often just rides momentum. The
//! more interesting case is an officer/director buying after their stock has
//! already been crushed — they see the same scary chart as everyone else and
//! still choose to add personal money. This example combines the two data
//! sources this repo has: minute bars for the price path, Form 4 filings for
//! the insider signal.
//!
//! Rule: when officers/directors file open-market purchases totalling
//! `MIN_BUY_VALUE`+ on one day, and on the next trading day the stock trades
//! at least `DIP_THRESHOLD` below its trailing `HIGH_LOOKBACK_DAYS`-day
//! closing high, buy. Exit on a disclosed insider sale, or after `HOLD_DAYS`
//! trading days. Signals that arrive without a dip are simply dropped — no
//! dip, no knife worth catching.
//!
//! Needs at least `MIN_HISTORY_DAYS` of daily closes before it will trade, so
//! the committed one-month fixture produces no trades — point it at a real
//! multi-month dataset:
//!
//! ```sh
//! cargo run --release --example insider_dip -- /path/to/data/output
//! ```

use std::collections::{BTreeMap, HashMap, HashSet, VecDeque};

use backtester::{
    data::load_ticker_map,
    insider::{load_insider_transactions, TransCode},
    run, Algorithm, Context, Slice, Symbol, SymbolMap,
};
use chrono::NaiveDate;
use chrono_tz::US::Eastern;

/// Aggregate officer/director purchase value ($) per (ticker, filing day).
const MIN_BUY_VALUE: f64 = 50_000.0;
/// Entry requires the price to sit at least this far below the trailing high.
const DIP_THRESHOLD: f64 = 0.20;
/// Trailing window (trading days) for the reference closing high.
const HIGH_LOOKBACK_DAYS: usize = 60;
/// Minimum daily closes before the drawdown is considered meaningful.
const MIN_HISTORY_DAYS: usize = 20;
/// Aggregate insider sale value ($) that forces an exit of a held position.
const MIN_SELL_VALUE: f64 = 100_000.0;
/// Equal-weight book: each position targets `1 / MAX_POSITIONS` of equity.
const MAX_POSITIONS: usize = 10;
/// Time-based exit after this many trading days without a sale signal.
const HOLD_DAYS: usize = 45;
/// Discard signals older than this many calendar days by the time they are
/// seen.
const MAX_SIGNAL_AGE_DAYS: i64 = 5;

struct InsiderDip {
    data_root: String,
    buy_signals: BTreeMap<NaiveDate, Vec<Symbol>>,
    sell_signals: BTreeMap<NaiveDate, Vec<Symbol>>,
    /// Rolling daily closes per symbol (newest at the back), for the
    /// trailing-high computation.
    daily_closes: SymbolMap<VecDeque<f64>>,
    /// Last close seen so far *today* per symbol; rolled into
    /// `daily_closes` at each day boundary.
    today_close: SymbolMap<f64>,
    /// symbol → trading days held so far.
    held: SymbolMap<usize>,
    last_date: Option<NaiveDate>,
}

impl InsiderDip {
    fn new(data_root: &str) -> Self {
        Self {
            data_root: data_root.to_string(),
            buy_signals: BTreeMap::new(),
            sell_signals: BTreeMap::new(),
            daily_closes: SymbolMap::default(),
            today_close: SymbolMap::default(),
            held: SymbolMap::default(),
            last_date: None,
        }
    }

    /// The dip test: latest close at least DIP_THRESHOLD below the trailing
    /// high, with enough history for the high to mean something.
    fn in_a_dip(&self, symbol: Symbol) -> bool {
        let Some(closes) = self.daily_closes.get(&symbol) else { return false };
        if closes.len() < MIN_HISTORY_DAYS {
            return false;
        }
        let last = *closes.back().unwrap();
        let high = closes.iter().cloned().fold(f64::MIN, f64::max);
        last <= high * (1.0 - DIP_THRESHOLD)
    }

    fn on_day_open(&mut self, ctx: &mut Context, today: NaiveDate) {
        // Yesterday's final prices become the newest daily closes.
        for (symbol, close) in self.today_close.drain() {
            let closes = self.daily_closes.entry(symbol).or_default();
            closes.push_back(close);
            if closes.len() > HIGH_LOOKBACK_DAYS {
                closes.pop_front();
            }
        }

        for days in self.held.values_mut() {
            *days += 1;
        }
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
                continue;
            }
            for symbol in symbols {
                if self.held.len() >= MAX_POSITIONS {
                    return;
                }
                if !self.in_a_dip(symbol) {
                    continue; // insiders bought, but there's no dip to buy
                }
                if let std::collections::hash_map::Entry::Vacant(entry) = self.held.entry(symbol) {
                    ctx.set_holdings(symbol, 1.0 / MAX_POSITIONS as f64);
                    entry.insert(0);
                }
            }
        }
    }
}

impl Algorithm for InsiderDip {
    fn initialize(&mut self, ctx: &mut Context) {
        ctx.set_start_date(2023, 1, 1);
        ctx.set_end_date(2023, 12, 31);
        ctx.set_cash(100_000.0);

        let universe: HashSet<String> = load_ticker_map(&self.data_root)
            .expect("data root must contain encoded_tickers.json")
            .into_values()
            .collect();
        let transactions = load_insider_transactions(&self.data_root, &universe)
            .expect("insider_transactions.json failed to load");

        let mut buy_tickers: BTreeMap<NaiveDate, Vec<String>> = BTreeMap::new();
        let mut sell_tickers: BTreeMap<NaiveDate, Vec<String>> = BTreeMap::new();
        for (ticker, by_date) in transactions {
            for (filing_date, txs) in by_date {
                let buys: f64 = txs
                    .iter()
                    .filter(|t| t.code == TransCode::Purchase && (t.is_officer || t.is_director))
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

        // Subscribe once per ticker that produced a signal, keeping the
        // returned `Symbol` so the signal maps hold dataset ids, not strings.
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
        let today = data.time.with_timezone(&Eastern).date_naive();
        if self.last_date != Some(today) {
            self.last_date = Some(today);
            self.on_day_open(ctx, today);
        }
        for (&symbol, bar) in &data.bars {
            self.today_close.insert(symbol, bar.close);
        }
    }
}

fn main() {
    let args: Vec<String> = std::env::args().collect();
    let data_path = args.get(1).map(String::as_str).unwrap_or("data/output/minute");

    run(InsiderDip::new(data_path), data_path).unwrap_or_else(|e| {
        eprintln!("backtest failed: {e}");
        std::process::exit(1);
    });
}

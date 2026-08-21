//! C-suite *conviction* buying: weigh who bought and how much it meant to
//! them, not just the dollar figure.
//!
//! Two refinements over naive copy-trading:
//!
//! 1. **Who** — only CEO/CFO/COO/president/chairman purchases count. The top
//!    of the org chart sees the whole business; a division VP may not.
//! 2. **How much, relative to them** — a $200k buy that doubles an
//!    executive's personal stake says more than a $1M buy that adds 1% to a
//!    huge position. A purchase qualifies when it grows the filer's stake by
//!    `MIN_STAKE_INCREASE`+, or is simply enormous (`BIG_VALUE`+) in absolute
//!    terms.
//!
//! Exit when any C-suite insider discloses a sale, or after `HOLD_DAYS`
//! trading days. Signals activate strictly after `filing_date` (no
//! lookahead).
//!
//! Run against the committed fixture (synthetic CEO/COO purchases):
//!
//! ```sh
//! cargo run --example insider_conviction -- backtester/tests/fixtures
//! ```

use std::collections::{BTreeMap, HashMap, HashSet};

use backtester::{
    data::load_ticker_map,
    insider::{load_insider_transactions, InsiderTransaction, TransCode},
    run, Algorithm, Context, Slice, Symbol, SymbolMap,
};
use chrono::NaiveDate;
use chrono_tz::US::Eastern;

/// Floor on the purchase size ($); below this, noise.
const MIN_VALUE: f64 = 100_000.0;
/// A buy this large ($) qualifies regardless of stake increase.
const BIG_VALUE: f64 = 1_000_000.0;
/// Otherwise the buy must grow the filer's stake by at least this fraction.
const MIN_STAKE_INCREASE: f64 = 0.20;
/// C-suite sale value ($) that forces an exit of a held position.
const MIN_SELL_VALUE: f64 = 50_000.0;
/// Equal-weight book: each position targets `1 / MAX_POSITIONS` of equity.
const MAX_POSITIONS: usize = 8;
/// Time-based exit after this many trading days without a sale signal.
const HOLD_DAYS: usize = 60;
/// Discard signals older than this many calendar days by the time they are
/// seen.
const MAX_SIGNAL_AGE_DAYS: i64 = 5;

/// Officer titles are free text on Form 4; match the common C-suite spellings.
fn is_c_suite(tx: &InsiderTransaction) -> bool {
    if !tx.is_officer {
        return false;
    }
    let Some(title) = &tx.officer_title else { return false };
    let title = title.to_uppercase();
    [
        "CEO",
        "CHIEF EXECUTIVE",
        "CFO",
        "CHIEF FINANCIAL",
        "COO",
        "CHIEF OPERATING",
        "PRESIDENT",
        "CHAIRMAN",
    ]
    .iter()
    .any(|keyword| title.contains(keyword))
}

/// The fraction by which this purchase grew the filer's stake, judged from
/// post-transaction holdings. Unknown holdings → 0 (the `BIG_VALUE` branch
/// can still qualify the buy).
fn stake_increase(tx: &InsiderTransaction) -> f64 {
    let Some(owned_after) = tx.shares_owned_after else { return 0.0 };
    let owned_before = owned_after - tx.shares;
    if owned_before <= 0.0 {
        // A stake built from (near) nothing: maximal conviction.
        return f64::INFINITY;
    }
    tx.shares / owned_before
}

struct InsiderConviction {
    data_root: String,
    buy_signals: BTreeMap<NaiveDate, Vec<Symbol>>,
    sell_signals: BTreeMap<NaiveDate, Vec<Symbol>>,
    /// symbol → trading days held so far.
    held: SymbolMap<usize>,
    last_date: Option<NaiveDate>,
}

impl InsiderConviction {
    fn new(data_root: &str) -> Self {
        Self {
            data_root: data_root.to_string(),
            buy_signals: BTreeMap::new(),
            sell_signals: BTreeMap::new(),
            held: SymbolMap::default(),
            last_date: None,
        }
    }

    fn on_day_open(&mut self, ctx: &mut Context, today: NaiveDate) {
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
                if let std::collections::hash_map::Entry::Vacant(entry) = self.held.entry(symbol) {
                    ctx.set_holdings(symbol, 1.0 / MAX_POSITIONS as f64);
                    entry.insert(0);
                }
            }
        }
    }
}

impl Algorithm for InsiderConviction {
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
                let conviction_buy = txs.iter().any(|t| {
                    t.code == TransCode::Purchase
                        && is_c_suite(t)
                        && t.value >= MIN_VALUE
                        && (t.value >= BIG_VALUE || stake_increase(t) >= MIN_STAKE_INCREASE)
                });
                let c_suite_sales: f64 = txs
                    .iter()
                    .filter(|t| t.code == TransCode::Sale && is_c_suite(t))
                    .map(|t| t.value)
                    .sum();
                if conviction_buy {
                    buy_tickers.entry(filing_date).or_default().push(ticker.clone());
                }
                if c_suite_sales >= MIN_SELL_VALUE {
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
    }
}

fn main() {
    let args: Vec<String> = std::env::args().collect();
    let data_path = args.get(1).map(String::as_str).unwrap_or("data/output/minute");

    run(InsiderConviction::new(data_path), data_path).unwrap_or_else(|e| {
        eprintln!("backtest failed: {e}");
        std::process::exit(1);
    });
}

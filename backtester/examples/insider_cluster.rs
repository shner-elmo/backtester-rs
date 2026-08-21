//! Insider *cluster* buying: act only when several insiders buy together.
//!
//! A single officer purchase can mean anything — a contrarian habit, a PR
//! gesture, mechanical dip-buying. Several distinct insiders reaching for
//! their own wallets inside the same short window is a much rarer and, in the
//! academic literature, much stronger signal (insiders disagree about many
//! things, but rarely all buy a stock that's about to fall).
//!
//! Rule: buy when at least `MIN_INSIDERS` distinct officers/directors file
//! open-market purchases totalling `MIN_TOTAL_VALUE`+ within a trailing
//! `WINDOW_DAYS`-calendar-day window. Positions are equal-weighted and exited
//! after `HOLD_DAYS` trading days — clusters are rare enough that there is no
//! sale-based exit; the thesis simply expires.
//!
//! Signals activate strictly after the last filing date of the cluster, so
//! the backtest only trades on information that was public at the time.
//!
//! Run against the committed fixture (three synthetic insiders buying AAPL):
//!
//! ```sh
//! cargo run --example insider_cluster -- backtester/tests/fixtures
//! ```

use std::collections::{BTreeMap, HashMap, HashSet};

use backtester::{
    data::load_ticker_map,
    insider::{load_insider_transactions, TransCode},
    run, Algorithm, Context, Slice, Symbol, SymbolMap,
};
use chrono::NaiveDate;
use chrono_tz::US::Eastern;

/// Trailing calendar window in which purchases count toward one cluster.
const WINDOW_DAYS: i64 = 14;
/// Distinct officer/director buyers needed inside the window.
const MIN_INSIDERS: usize = 3;
/// Aggregate purchase value ($) the cluster must reach.
const MIN_TOTAL_VALUE: f64 = 500_000.0;
/// Equal-weight book: each position targets `1 / MAX_POSITIONS` of equity.
const MAX_POSITIONS: usize = 10;
/// Time-based exit; clusters have no natural sell signal.
const HOLD_DAYS: usize = 60;
/// Discard signals older than this many calendar days by the time they are
/// seen (backtest start boundary, halted stocks).
const MAX_SIGNAL_AGE_DAYS: i64 = 5;

struct InsiderCluster {
    data_root: String,
    /// filing date on which a cluster completed → symbols.
    buy_signals: BTreeMap<NaiveDate, Vec<Symbol>>,
    /// symbol → trading days held so far.
    held: SymbolMap<usize>,
    last_date: Option<NaiveDate>,
}

impl InsiderCluster {
    fn new(data_root: &str) -> Self {
        Self {
            data_root: data_root.to_string(),
            buy_signals: BTreeMap::new(),
            held: SymbolMap::default(),
            last_date: None,
        }
    }

    fn on_day_open(&mut self, ctx: &mut Context, today: NaiveDate) {
        for days in self.held.values_mut() {
            *days += 1;
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

impl Algorithm for InsiderCluster {
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

        // Cluster detection: slide a WINDOW_DAYS window over each symbol's
        // officer/director purchases (already date-sorted via BTreeMap) and
        // signal on any day the window holds MIN_INSIDERS distinct buyers.
        let mut buy_tickers: BTreeMap<NaiveDate, Vec<String>> = BTreeMap::new();
        for (ticker, by_date) in transactions {
            let buys: Vec<(NaiveDate, String, f64)> = by_date
                .into_iter()
                .flat_map(|(date, txs)| {
                    txs.into_iter()
                        .filter(|t| {
                            t.code == TransCode::Purchase && (t.is_officer || t.is_director)
                        })
                        .map(move |t| {
                            (date, t.owner_name.unwrap_or_else(|| "UNKNOWN".into()), t.value)
                        })
                })
                .collect();

            let mut window_start = 0;
            for i in 0..buys.len() {
                let day = buys[i].0;
                while (day - buys[window_start].0).num_days() > WINDOW_DAYS {
                    window_start += 1;
                }
                let window = &buys[window_start..=i];
                let insiders: HashSet<&str> = window.iter().map(|(_, n, _)| n.as_str()).collect();
                let total: f64 = window.iter().map(|(_, _, v)| v).sum();
                if insiders.len() >= MIN_INSIDERS && total >= MIN_TOTAL_VALUE {
                    let tickers = buy_tickers.entry(day).or_default();
                    if !tickers.contains(&ticker) {
                        tickers.push(ticker.clone());
                    }
                }
            }
        }

        // Subscribe once per ticker that produced a cluster, keeping the
        // returned `Symbol` so the signal map holds dataset ids, not strings.
        let mut ids: HashMap<String, Symbol> = HashMap::new();
        for ticker in buy_tickers.values().flatten() {
            if !ids.contains_key(ticker) {
                if let Some(symbol) = ctx.try_add_equity(ticker) {
                    ids.insert(ticker.clone(), symbol);
                }
            }
        }
        self.buy_signals = buy_tickers
            .into_iter()
            .map(|(date, tickers)| {
                (date, tickers.iter().filter_map(|t| ids.get(t).copied()).collect())
            })
            .collect();
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

    run(InsiderCluster::new(data_path), data_path).unwrap_or_else(|e| {
        eprintln!("backtest failed: {e}");
        std::process::exit(1);
    });
}

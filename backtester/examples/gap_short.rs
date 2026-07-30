//! Fade the runners: at the 9:30 market open, short every stock that gapped up
//! 100% or more overnight (its opening print is at least 2x the previous day's
//! regular-session close), traded at least $1M of pre-market dollar volume
//! this morning, and opens above $5 — splitting a fixed short budget equally
//! across that day's names, then cover everything at 15:55 — always flat
//! overnight.
//!
//!   cargo run --example gap_short -- backtester/tests/fixtures
//!
//! There is no universe-selection API: `main` loads the ticker map and
//! `initialize` subscribes every symbol in the dataset, so `on_data` slices
//! carry them all. Per-symbol state is keyed by the `Symbol` handles
//! `add_equity` returns — integers, so the maps below never hash a ticker
//! string. A symbol with no 9:30 bar that day has no open to fade and is
//! skipped. The committed fixture holds only AAPL — which never doubles
//! overnight — so a fixture run completes with zero trades; point it at a
//! real dataset for signals.

use std::{cell::RefCell, rc::Rc};

use backtester::{
    bar::MarketSession, data::load_ticker_map, run, Algorithm, Context, Slice, Symbol, SymbolMap,
};
use chrono::Timelike;
use chrono_tz::US::Eastern;

/// Total short exposure as a fraction of equity, split across the day's signals.
const SHORT_BUDGET: f64 = 1.0;
/// A stock qualifies when its open is at least this multiple of the prior close.
const GAP_MULTIPLE: f64 = 2.0;
/// Minimum dollar volume (price x shares) traded pre-market this morning.
const MIN_PREMARKET_DOLLAR_VOLUME: f64 = 1_000_000.0;
/// Minimum opening price — skip sub-$5 stocks.
const MIN_PRICE: f64 = 5.0;

struct GapShort {
    /// Every ticker in the dataset, read from the ticker map in `main` and
    /// exchanged for symbols in `initialize`.
    tickers: Vec<String>,
    /// Latest regular-session close seen per symbol, updated on every Main bar.
    last_close: SymbolMap<f64>,
    /// Snapshot of `last_close` at each day boundary — "yesterday's close".
    prev_close: SymbolMap<f64>,
    /// Dollar volume (close x shares) traded pre-market today, per symbol.
    premarket_dollar_vol: SymbolMap<f64>,
    /// Symbols shorted this morning. `Rc<RefCell<..>>` because the 15:55
    /// scheduled closure must be `'static` and so can't borrow the struct.
    shorted_today: Rc<RefCell<Vec<Symbol>>>,
}

impl Algorithm for GapShort {
    fn initialize(&mut self, ctx: &mut Context) {
        ctx.set_start_date(2023, 1, 1);
        ctx.set_end_date(2024, 12, 31);
        ctx.set_cash(100_000.0);
        for ticker in std::mem::take(&mut self.tickers) {
            ctx.add_equity(&ticker);
        }

        // Cover every short opened this morning at 15:55 ET.
        let shorted = self.shorted_today.clone();
        ctx.on_time(15, 55, move |ctx| {
            for symbol in shorted.borrow_mut().drain(..) {
                ctx.liquidate(symbol);
            }
        });
    }

    fn on_data(&mut self, ctx: &mut Context, data: &Slice) {
        let et = data.time.with_timezone(&Eastern);
        if et.hour() == 9 && et.minute() == 30 {
            let candidates: Vec<Symbol> = data
                .bars
                .iter()
                .filter(|(symbol, bar)| {
                    bar.session() == MarketSession::Main
                        && bar.open >= MIN_PRICE
                        && self
                            .premarket_dollar_vol
                            .get(*symbol)
                            .is_some_and(|&vol| vol >= MIN_PREMARKET_DOLLAR_VOLUME)
                        && self
                            .prev_close
                            .get(*symbol)
                            .is_some_and(|close| bar.open >= GAP_MULTIPLE * close)
                })
                .map(|(symbol, _)| *symbol)
                .collect();

            if !candidates.is_empty() {
                let weight = -SHORT_BUDGET / candidates.len() as f64;
                for symbol in candidates {
                    ctx.set_holdings(symbol, weight);
                    self.shorted_today.borrow_mut().push(symbol);
                }
            }
        }

        for (&symbol, bar) in &data.bars {
            match bar.session() {
                MarketSession::Main => {
                    self.last_close.insert(symbol, bar.close);
                }
                MarketSession::PreMarket => {
                    *self.premarket_dollar_vol.entry(symbol).or_insert(0.0) +=
                        bar.close * bar.volume as f64;
                }
                MarketSession::AfterMarket => {}
            }
        }
    }

    fn on_end_of_day(&mut self, _ctx: &mut Context) {
        // Fires before the next day's first bar, so this is yesterday's close.
        // A clone (not a swap) keeps the last known close of thinly-traded
        // symbols that printed no bar today.
        self.prev_close = self.last_close.clone();
        // The next day's pre-market tape starts from zero.
        self.premarket_dollar_vol.clear();
    }

    fn on_split(&mut self, _ctx: &mut Context, symbol: Symbol, ratio: f64) {
        // Bar prices are raw, so without this a reverse split would look like
        // a huge overnight gap against the pre-split stored close.
        if let Some(close) = self.last_close.get_mut(&symbol) {
            *close /= ratio;
        }
        if let Some(close) = self.prev_close.get_mut(&symbol) {
            *close /= ratio;
        }
    }
}

fn main() {
    let args: Vec<String> = std::env::args().collect();
    let data_path = args.get(1).map(String::as_str).unwrap_or("data/output/minute");

    let ticker_map = load_ticker_map(data_path).unwrap_or_else(|e| {
        eprintln!("failed to load ticker map: {e}");
        std::process::exit(1);
    });

    let algo = GapShort {
        tickers: ticker_map.into_values().collect(),
        last_close: SymbolMap::default(),
        prev_close: SymbolMap::default(),
        premarket_dollar_vol: SymbolMap::default(),
        shorted_today: Rc::new(RefCell::new(Vec::new())),
    };

    run(algo, data_path).unwrap();
}

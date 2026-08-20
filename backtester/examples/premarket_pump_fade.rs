//! Fade the pre-market pump: short at the open anything that doubled before
//! the bell, cover into the close.
//!
//! Stocks that gain 100%+ in pre-market are overwhelmingly low-float names
//! riding a promotion, a squeeze, or a news spike — and the excess tends to
//! bleed off during the regular session. The rule:
//!
//! - Track each symbol's last **main-session** close as "yesterday's close".
//! - During pre-market, track the latest pre-market trade.
//! - On the symbol's **first main-session bar** of the day, if the latest
//!   pre-market price is at least `MIN_PREMARKET_GAIN` above yesterday's
//!   close (100% = doubled), short `SHORT_WEIGHT` of equity.
//! - Cover every open short at `COVER_AT` ET; a day-boundary fallback covers
//!   anything whose bars stopped early (halts, illiquidity).
//!
//! This is a pure price-action strategy — it doesn't use the insider data,
//! just the `MarketSession` tag on each minute bar.
//!
//! Reality check before trusting the numbers: names like these are often
//! hard to borrow (borrow fees and availability are not modeled), fills at
//! the open of a violent pump are far worse than bar closes, and
//! `set_max_volume_participation` is worth enabling on real data since these
//! tickers trade thin. A small percent slippage is set below as a nod to
//! that; tighten or loosen it to taste.
//!
//! The committed fixture has no pre-market doubles, so this makes no trades
//! there — point it at a real dataset:
//!
//! ```sh
//! cargo run --release --example premarket_pump_fade -- /path/to/data/output
//! ```

use backtester::{
    bar::MarketSession, data::load_ticker_map, run, Algorithm, Context, PercentSlippage, Slice,
    SymbolMap, SymbolSet,
};
use chrono::{NaiveDate, NaiveTime};
use chrono_tz::US::Eastern;

/// Minimum pre-market gain over yesterday's main-session close (1.0 = +100%).
const MIN_PREMARKET_GAIN: f64 = 1.0;
/// Ignore sub-$1 movers: a 10¢ → 20¢ "double" is noise, not a pump.
const MIN_PREMARKET_PRICE: f64 = 1.0;
/// Fraction of equity shorted per name.
const SHORT_WEIGHT: f64 = 0.05;
/// Cap on simultaneous shorts (pumps cluster on hot days).
const MAX_POSITIONS: usize = 10;
/// Cover time, US Eastern — a few minutes before the close.
const COVER_AT: (u32, u32) = (15, 55);

struct PremarketPumpFade {
    data_root: String,
    /// Last main-session close of the previous trading day per symbol.
    prev_close: SymbolMap<f64>,
    /// Main-session closes seen so far today; rolled into `prev_close` at
    /// the day boundary.
    today_close: SymbolMap<f64>,
    /// Latest pre-market price seen today per symbol.
    premarket_last: SymbolMap<f64>,
    /// Symbols whose first main-session bar of the day has already passed.
    seen_main: SymbolSet,
    /// Open shorts.
    held: SymbolSet,
    last_date: Option<NaiveDate>,
}

impl PremarketPumpFade {
    fn new(data_root: &str) -> Self {
        Self {
            data_root: data_root.to_string(),
            prev_close: SymbolMap::default(),
            today_close: SymbolMap::default(),
            premarket_last: SymbolMap::default(),
            seen_main: SymbolSet::default(),
            held: SymbolSet::default(),
            last_date: None,
        }
    }

    fn on_day_open(&mut self, ctx: &mut Context) {
        // Fallback: anything still short lost its bars before the cover time
        // yesterday (halt, illiquidity) — close it on today's first bar
        // rather than carrying an overnight short.
        for symbol in self.held.drain() {
            ctx.liquidate(symbol);
        }
        for (symbol, close) in self.today_close.drain() {
            self.prev_close.insert(symbol, close);
        }
        self.premarket_last.clear();
        self.seen_main.clear();
    }
}

impl Algorithm for PremarketPumpFade {
    fn initialize(&mut self, ctx: &mut Context) {
        ctx.set_start_date(2023, 1, 1);
        ctx.set_end_date(2023, 12, 31);
        ctx.set_cash(100_000.0);
        // A nod to reality on thin pump names; see the module docs.
        ctx.set_slippage(PercentSlippage::bps(10.0)); // 0.1% against the aggressor

        // "Every ticker": the whole dataset universe.
        let universe =
            load_ticker_map(&self.data_root).expect("data root must contain encoded_tickers.json");
        for ticker in universe.into_values() {
            ctx.add_equity(&ticker);
        }
    }

    fn on_data(&mut self, ctx: &mut Context, data: &Slice) {
        let eastern = data.time.with_timezone(&Eastern);
        let today = eastern.date_naive();
        if self.last_date != Some(today) {
            self.last_date = Some(today);
            self.on_day_open(ctx);
        }
        let cover_time = NaiveTime::from_hms_opt(COVER_AT.0, COVER_AT.1, 0).unwrap();
        let past_cover = eastern.time() >= cover_time;

        for (&symbol, bar) in &data.bars {
            match bar.session() {
                MarketSession::PreMarket => {
                    self.premarket_last.insert(symbol, bar.close);
                }
                MarketSession::Main => {
                    // First main-session bar of the day = "the open".
                    if self.seen_main.insert(symbol)
                        && self.held.len() < MAX_POSITIONS
                        && !self.held.contains(&symbol)
                    {
                        let prev = self.prev_close.get(&symbol).copied();
                        let pre = self.premarket_last.get(&symbol).copied();
                        if let (Some(prev), Some(pre)) = (prev, pre) {
                            let pumped = prev > 0.0 && pre / prev - 1.0 >= MIN_PREMARKET_GAIN;
                            if pumped && pre >= MIN_PREMARKET_PRICE {
                                ctx.set_holdings(symbol, -SHORT_WEIGHT);
                                self.held.insert(symbol);
                            }
                        }
                    }
                    self.today_close.insert(symbol, bar.close);
                    if past_cover && self.held.remove(&symbol) {
                        ctx.liquidate(symbol);
                    }
                }
                MarketSession::AfterMarket => {}
            }
        }
    }
}

fn main() {
    let args: Vec<String> = std::env::args().collect();
    let data_path = args.get(1).map(String::as_str).unwrap_or("data/output/minute");

    run(PremarketPumpFade::new(data_path), data_path).unwrap_or_else(|e| {
        eprintln!("backtest failed: {e}");
        std::process::exit(1);
    });
}

//! MACD trend following (à la QuantConnect's MACD examples): hold long while the
//! MACD line is above its signal line (positive histogram) and flatten when it
//! crosses back below.
//!
//!   cargo run --example macd_trend -- backtester/tests/fixtures

use backtester::{
    indicators::{Macd, Next},
    run, Algorithm, Context, Slice, Symbol,
};

struct MacdTrend {
    symbol: Option<Symbol>,
    macd: Macd,
    slow: usize,
    bars_seen: usize,
}

impl Algorithm for MacdTrend {
    fn initialize(&mut self, ctx: &mut Context) {
        ctx.set_start_date(2023, 1, 1);
        ctx.set_end_date(2023, 12, 31);
        ctx.set_cash(100_000.0);
        self.symbol = Some(ctx.add_equity("AAPL"));
    }

    fn on_data(&mut self, ctx: &mut Context, data: &Slice) {
        let Some(symbol) = self.symbol else { return };
        let Some(bar) = data.bars.get(&symbol) else { return };
        let macd = self.macd.next(bar.close);

        // Give the slow EMA a full period to converge before trading.
        self.bars_seen += 1;
        if self.bars_seen <= self.slow {
            return;
        }

        if macd.histogram > 0.0 {
            ctx.set_holdings(symbol, 1.0); // MACD above signal — trend up
        } else {
            ctx.liquidate(symbol); // crossed below — exit
        }
    }
}

fn main() {
    let args: Vec<String> = std::env::args().collect();
    let data_path = args.get(1).map(String::as_str).unwrap_or("data/output/minute");

    let (fast, slow, signal) = (12, 26, 9);
    let algo = MacdTrend {
        symbol: None,
        macd: Macd::new(fast, slow, signal).unwrap(),
        slow,
        bars_seen: 0,
    };

    run(algo, data_path).unwrap_or_else(|e| {
        eprintln!("backtest failed: {e}");
        std::process::exit(1);
    });
}

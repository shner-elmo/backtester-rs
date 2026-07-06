//! Classic RSI mean reversion (à la QuantConnect's RSI examples): go long when
//! the 14-bar RSI is oversold, and flatten once it recovers toward neutral.
//!
//!   cargo run --example rsi_mean_reversion -- backtester/tests/fixtures

use backtester::{
    indicators::{Next, Rsi},
    run, Algorithm, Context, Slice,
};

struct RsiMeanReversion {
    symbol: String,
    rsi: Rsi,
    period: usize,
    bars_seen: usize,
}

impl Algorithm for RsiMeanReversion {
    fn initialize(&mut self, ctx: &mut Context) {
        ctx.set_start_date(2023, 1, 1);
        ctx.set_end_date(2023, 12, 31);
        ctx.set_cash(100_000.0);
        ctx.add_equity(&self.symbol.clone());
    }

    fn on_data(&mut self, ctx: &mut Context, data: &Slice) {
        let Some(bar) = data.bars.get(&self.symbol) else { return };
        let rsi = self.rsi.next(bar.close);

        // Let the RSI see a full period of bars before trading on it.
        self.bars_seen += 1;
        if self.bars_seen <= self.period {
            return;
        }

        if rsi < 30.0 {
            ctx.set_holdings(&self.symbol.clone(), 1.0); // oversold — buy the dip
        } else if rsi > 55.0 {
            ctx.liquidate(&self.symbol.clone()); // mean reverted — step aside
        }
    }
}

fn main() {
    let args: Vec<String> = std::env::args().collect();
    let data_path = args.get(1).map(String::as_str).unwrap_or("data/output/minute");

    let period = 14;
    let algo = RsiMeanReversion {
        symbol: "AAPL".to_string(),
        rsi: Rsi::new(period).unwrap(),
        period,
        bars_seen: 0,
    };

    run(algo, data_path).unwrap_or_else(|e| {
        eprintln!("backtest failed: {e}");
        std::process::exit(1);
    });
}

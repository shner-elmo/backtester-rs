//! Bollinger Band mean reversion (à la QuantConnect's BollingerBands examples):
//! buy when price closes below the lower band and exit once it reverts back up
//! through the middle band.
//!
//!   cargo run --example bollinger_bands -- backtester/tests/fixtures

use backtester::{
    indicators::{BollingerBands, Next},
    run, Algorithm, Context, Slice, Symbol,
};

struct BollingerReversion {
    symbol: Option<Symbol>,
    bands: BollingerBands,
    period: usize,
    bars_seen: usize,
}

impl Algorithm for BollingerReversion {
    fn initialize(&mut self, ctx: &mut Context) {
        ctx.set_start_date(2023, 1, 1);
        ctx.set_end_date(2023, 12, 31);
        ctx.set_cash(100_000.0);
        self.symbol = Some(ctx.add_equity("AAPL"));
    }

    fn on_data(&mut self, ctx: &mut Context, data: &Slice) {
        let Some(symbol) = self.symbol else { return };
        let Some(bar) = data.bars.get(&symbol) else { return };
        let bands = self.bands.next(bar.close);

        // Let the bands fill their lookback window before trading on them.
        self.bars_seen += 1;
        if self.bars_seen <= self.period {
            return;
        }

        let invested = ctx.portfolio.get(symbol).is_some();
        if !invested && bar.close < bands.lower {
            ctx.set_holdings(symbol, 1.0); // stretched below — buy
        } else if invested && bar.close > bands.average {
            ctx.liquidate(symbol); // back to the mean — take profit
        }
    }
}

fn main() {
    let args: Vec<String> = std::env::args().collect();
    let data_path = args.get(1).map(String::as_str).unwrap_or("data/output/minute");

    let period = 20;
    let algo = BollingerReversion {
        symbol: None,
        bands: BollingerBands::new(period, 2.0).unwrap(),
        period,
        bars_seen: 0,
    };

    run(algo, data_path).unwrap_or_else(|e| {
        eprintln!("backtest failed: {e}");
        std::process::exit(1);
    });
}

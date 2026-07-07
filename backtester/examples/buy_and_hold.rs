//! The simplest possible strategy: buy on the first bar and hold to the end.
//! Mirrors QuantConnect's `BasicTemplateAlgorithm` — the baseline every active
//! strategy should be measured against.
//!
//!   cargo run --example buy_and_hold -- backtester/tests/fixtures

use backtester::{run, Algorithm, Context, Slice};

struct BuyAndHold {
    symbol: String,
    invested: bool,
}

impl Algorithm for BuyAndHold {
    fn initialize(&mut self, ctx: &mut Context) {
        ctx.set_start_date(2023, 1, 1);
        ctx.set_end_date(2023, 12, 31);
        ctx.set_cash(100_000.0);
        ctx.add_equity(&self.symbol.clone());
    }

    fn on_data(&mut self, ctx: &mut Context, data: &Slice) {
        if self.invested || !data.bars.contains_key(&self.symbol) {
            return;
        }
        ctx.set_holdings(&self.symbol.clone(), 1.0); // go 100% long and never sell
        self.invested = true;
    }
}

fn main() {
    let args: Vec<String> = std::env::args().collect();
    let data_path = args.get(1).map(String::as_str).unwrap_or("data/output/minute");

    let algo = BuyAndHold { symbol: "AAPL".to_string(), invested: false };

    run(algo, data_path).unwrap_or_else(|e| {
        eprintln!("backtest failed: {e}");
        std::process::exit(1);
    });
}

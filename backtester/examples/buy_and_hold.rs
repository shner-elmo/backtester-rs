//! The simplest possible strategy: buy on the first bar and hold to the end.
//! Mirrors QuantConnect's `BasicTemplateAlgorithm` — the baseline every active
//! strategy should be measured against.
//!
//!   cargo run --example buy_and_hold -- backtester/tests/fixtures

use backtester::{run, Algorithm, Context, Slice, Symbol};

struct BuyAndHold {
    symbol: Option<Symbol>,
    invested: bool,
}

impl Algorithm for BuyAndHold {
    fn initialize(&mut self, ctx: &mut Context) {
        ctx.set_start_date(2023, 1, 1);
        ctx.set_end_date(2023, 12, 31);
        ctx.set_cash(100_000.0);
        self.symbol = Some(ctx.add_equity("AAPL"));
    }

    fn on_data(&mut self, ctx: &mut Context, data: &Slice) {
        let Some(symbol) = self.symbol else { return };
        if self.invested || !data.bars.contains_key(&symbol) {
            return;
        }
        ctx.set_holdings(symbol, 1.0); // go 100% long and never sell
        self.invested = true;
    }
}

fn main() {
    let args: Vec<String> = std::env::args().collect();
    let data_path = args.get(1).map(String::as_str).unwrap_or("data/output/minute");

    let algo = BuyAndHold { symbol: None, invested: false };

    run(algo, data_path).unwrap_or_else(|e| {
        eprintln!("backtest failed: {e}");
        std::process::exit(1);
    });
}

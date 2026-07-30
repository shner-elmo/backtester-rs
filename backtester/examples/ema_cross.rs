use backtester::{
    indicators::{Ema, Next},
    run, Algorithm, Context, Slice, Symbol,
};

struct EmaCross {
    /// Handed out by `add_equity` in `initialize` — the ticker string is
    /// spelled out once, there, and everything else trades the handle.
    symbol: Option<Symbol>,
    fast: Ema,
    slow: Ema,
}

impl Algorithm for EmaCross {
    fn initialize(&mut self, ctx: &mut Context) {
        ctx.set_start_date(2023, 1, 1);
        ctx.set_end_date(2024, 12, 31);
        ctx.set_cash(100_000.0);
        ctx.set_warm_up(30);
        self.symbol = Some(ctx.add_equity("AAPL"));
    }

    fn on_data(&mut self, ctx: &mut Context, data: &Slice) {
        let Some(symbol) = self.symbol else { return };
        let bar = match data.bars.get(&symbol) {
            Some(b) => b,
            None => return,
        };

        let fast_val = self.fast.next(bar.close);
        let slow_val = self.slow.next(bar.close);

        if fast_val > slow_val {
            ctx.set_holdings(symbol, 1.0);
        } else {
            ctx.liquidate(symbol);
        }
    }
}

fn main() {
    let args: Vec<String> = std::env::args().collect();
    let data_path = args.get(1).map(String::as_str).unwrap_or("data/output/minute");

    let algo = EmaCross { symbol: None, fast: Ema::new(10).unwrap(), slow: Ema::new(30).unwrap() };

    run(algo, data_path).unwrap_or_else(|e| {
        eprintln!("backtest failed: {e}");
        std::process::exit(1);
    });
}

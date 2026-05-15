use backtester::{
    indicators::{Ema, Next},
    run, Algorithm, Context, Slice,
};

struct EmaCross {
    symbol: String,
    fast: Ema,
    slow: Ema,
}

impl Algorithm for EmaCross {
    fn initialize(&mut self, ctx: &mut Context) {
        ctx.set_start_date(2023, 1, 1);
        ctx.set_end_date(2023, 12, 31);
        ctx.set_cash(100_000.0);
        ctx.set_warm_up(30);
        ctx.add_equity(&self.symbol.clone());
    }

    fn on_data(&mut self, ctx: &mut Context, data: &Slice) {
        let bar = match data.bars.get(&self.symbol) {
            Some(b) => b,
            None => return,
        };

        let fast_val = self.fast.next(bar.close);
        let slow_val = self.slow.next(bar.close);

        if fast_val > slow_val {
            ctx.set_holdings(&self.symbol.clone(), 1.0);
        } else {
            ctx.liquidate(&self.symbol.clone());
        }
    }
}

fn main() {
    let args: Vec<String> = std::env::args().collect();
    let data_path = args.get(1).map(String::as_str).unwrap_or("data/output/minute");

    let algo = EmaCross {
        symbol: "AAPL".to_string(),
        fast: Ema::new(10).unwrap(),
        slow: Ema::new(30).unwrap(),
    };

    run(algo, data_path);
}

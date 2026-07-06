//! Same EMA-cross strategy as `ema_cross`, but with slippage and commission
//! models applied. Run it and compare the summary against `ema_cross` (no
//! friction) to see the costs eat into PnL:
//!
//!   cargo run --example ema_cross     -- backtester/tests/fixtures
//!   cargo run --example slippage_demo -- backtester/tests/fixtures
//!
//! Swap the `set_slippage(..)` line below for any built-in model or your own
//! closure.

use backtester::{
    commission::PerShareCommission,
    indicators::{Ema, Next},
    run,
    slippage::{FillContext, PercentSlippage},
    Algorithm, Context, Slice,
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

        // A flat 10 bps against the aggressor:
        ctx.set_slippage(PercentSlippage::bps(10.0));

        // Broker-style commission: half a cent per share, $1 minimum.
        ctx.set_commission(PerShareCommission { per_share: 0.005, minimum: 1.0 });

        // ...or a custom closure using the bar — e.g. a quarter of each bar's
        // range, so slippage widens on volatile bars:
        let _volatility_aware = |fill: &FillContext| {
            let range = (fill.bar.high - fill.bar.low).max(0.0);
            fill.price + 0.25 * range * fill.direction()
        };
        // ctx.set_slippage(_volatility_aware);
    }

    fn on_data(&mut self, ctx: &mut Context, data: &Slice) {
        let Some(bar) = data.bars.get(&self.symbol) else { return };
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

    run(algo, data_path).unwrap_or_else(|e| {
        eprintln!("backtest failed: {e}");
        std::process::exit(1);
    });
}

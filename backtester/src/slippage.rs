//! Customizable slippage models.
//!
//! A slippage model maps the reference price of a fill (the bar close) to the
//! actual execution price. Set one on the [`Context`](crate::Context) in
//! `initialize`:
//!
//! ```
//! # use backtester::{Context, slippage::PercentSlippage};
//! # let mut ctx = Context::default();
//! ctx.set_slippage(PercentSlippage::bps(10.0)); // 0.1% against the aggressor
//! ```
//!
//! You can pass any built-in model, your own type implementing
//! [`SlippageModel`], or a plain closure `Fn(&FillContext) -> f64`:
//!
//! ```
//! # use backtester::{Context, slippage::FillContext};
//! # let mut ctx = Context::default();
//! // Wider slippage when the bar's range is large.
//! ctx.set_slippage(|fill: &FillContext| {
//!     let range = (fill.bar.high - fill.bar.low).max(0.0);
//!     fill.price + 0.25 * range * fill.direction()
//! });
//! ```

use crate::{bar::Bar, symbol::Symbol};

/// Everything a slippage model needs to price a single fill.
pub struct FillContext<'a> {
    /// Symbol being traded — compare it against the handles
    /// `Context::add_equity` returned, or resolve it for logging with
    /// `Context::symbol_name`.
    pub symbol: Symbol,
    /// Signed order quantity: positive = buy, negative = sell.
    pub quantity: f64,
    /// Reference price before slippage (the bar close).
    pub price: f64,
    /// The bar the fill occurs on — use its `high`/`low`/`volume` for
    /// spread-, volatility-, or volume-based models.
    pub bar: &'a Bar,
}

impl FillContext<'_> {
    /// `+1.0` for a buy, `-1.0` for a sell. Multiply by this so a model always
    /// moves the price *against* the aggressor.
    pub fn direction(&self) -> f64 {
        self.quantity.signum()
    }
}

/// Maps a reference price to an actual fill price.
///
/// Implemented for all the built-in models below and, via a blanket impl, for
/// any `Fn(&FillContext) -> f64` closure.
pub trait SlippageModel {
    fn fill_price(&self, fill: &FillContext) -> f64;
}

/// Any `Fn(&FillContext) -> f64` closure is a slippage model.
impl<F: Fn(&FillContext) -> f64> SlippageModel for F {
    fn fill_price(&self, fill: &FillContext) -> f64 {
        self(fill)
    }
}

/// No slippage — fills at the reference price. This is the default.
pub struct NoSlippage;

impl SlippageModel for NoSlippage {
    fn fill_price(&self, fill: &FillContext) -> f64 {
        fill.price
    }
}

/// Slippage as a fraction of price, moving against the aggressor: buys fill
/// higher, sells fill lower.
pub struct PercentSlippage {
    /// Fraction of price, e.g. `0.001` = 10 bps.
    pub rate: f64,
}

impl PercentSlippage {
    /// Construct from basis points (1 bp = 0.01%). `PercentSlippage::bps(10.0)`
    /// is a 0.1% adjustment.
    pub fn bps(bps: f64) -> Self {
        Self { rate: bps / 10_000.0 }
    }
}

impl SlippageModel for PercentSlippage {
    fn fill_price(&self, fill: &FillContext) -> f64 {
        fill.price * (1.0 + self.rate * fill.direction())
    }
}

/// Fixed cash slippage per share, moving against the aggressor. Clamped at 0.
pub struct FixedSlippage {
    /// Cash amount per share, e.g. `0.01` = one cent.
    pub per_share: f64,
}

impl SlippageModel for FixedSlippage {
    fn fill_price(&self, fill: &FillContext) -> f64 {
        (fill.price + self.per_share * fill.direction()).max(0.0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_util::{bar, fill};

    #[test]
    fn no_slippage_is_identity() {
        let bar = bar(100.0);
        assert_eq!(NoSlippage.fill_price(&fill(&bar, 10.0, 100.0)), 100.0);
    }

    #[test]
    fn percent_moves_against_the_aggressor() {
        let model = PercentSlippage::bps(10.0); // 0.1%
        let bar = bar(100.0);
        // Buy fills higher, sell fills lower.
        assert!((model.fill_price(&fill(&bar, 5.0, 100.0)) - 100.1).abs() < 1e-9);
        assert!((model.fill_price(&fill(&bar, -5.0, 100.0)) - 99.9).abs() < 1e-9);
    }

    #[test]
    fn fixed_per_share_moves_against_the_aggressor() {
        let model = FixedSlippage { per_share: 0.05 };
        let bar = bar(100.0);
        assert_eq!(model.fill_price(&fill(&bar, 1.0, 100.0)), 100.05);
        assert_eq!(model.fill_price(&fill(&bar, -1.0, 100.0)), 99.95);
    }

    #[test]
    fn fixed_clamps_at_zero() {
        let model = FixedSlippage { per_share: 10.0 };
        let bar = bar(5.0);
        assert_eq!(model.fill_price(&fill(&bar, -1.0, 5.0)), 0.0);
    }

    #[test]
    fn closure_is_a_slippage_model() {
        // A custom model that uses the bar's range.
        let model = |f: &FillContext| f.price + (f.bar.high - f.bar.low) * f.direction();
        let bar = bar(100.0); // range = 2.0
        assert_eq!(model.fill_price(&fill(&bar, 1.0, 100.0)), 102.0);
        assert_eq!(model.fill_price(&fill(&bar, -1.0, 100.0)), 98.0);
    }
}

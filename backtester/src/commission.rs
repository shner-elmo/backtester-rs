//! Customizable commission models.
//!
//! A commission model returns the cash cost charged for a fill. Set one on the
//! [`Context`](crate::Context) in `initialize`:
//!
//! ```
//! # use backtester::{Context, commission::PerShareCommission};
//! # let mut ctx = Context::default();
//! ctx.set_commission(PerShareCommission { per_share: 0.005, minimum: 1.0 });
//! ```
//!
//! You can pass any built-in model, your own type implementing
//! [`CommissionModel`], or a plain closure `Fn(&FillContext) -> f64`:
//!
//! ```
//! # use backtester::{Context, slippage::FillContext};
//! # let mut ctx = Context::default();
//! // Flat fee per order.
//! ctx.set_commission(|_fill: &FillContext| 1.0);
//! ```

use crate::slippage::FillContext;

/// Returns the cash commission charged for a fill. Commissions are deducted
/// from portfolio cash and attributed to the trade's PnL.
///
/// Implemented for all the built-in models below and, via a blanket impl, for
/// any `Fn(&FillContext) -> f64` closure.
pub trait CommissionModel {
    fn commission(&self, fill: &FillContext) -> f64;
}

/// Any `Fn(&FillContext) -> f64` closure is a commission model.
impl<F: Fn(&FillContext) -> f64> CommissionModel for F {
    fn commission(&self, fill: &FillContext) -> f64 {
        self(fill)
    }
}

/// No commission. This is the default.
pub struct NoCommission;

impl CommissionModel for NoCommission {
    fn commission(&self, _fill: &FillContext) -> f64 {
        0.0
    }
}

/// A fixed cash amount per share with a per-order minimum — the classic
/// interactive-brokers-style schedule.
pub struct PerShareCommission {
    /// Cash per share, e.g. `0.005`.
    pub per_share: f64,
    /// Minimum charge per order, e.g. `1.0`.
    pub minimum: f64,
}

impl CommissionModel for PerShareCommission {
    fn commission(&self, fill: &FillContext) -> f64 {
        (fill.quantity.abs() * self.per_share).max(self.minimum)
    }
}

/// Commission as a fraction of traded notional value.
pub struct PercentCommission {
    /// Fraction of notional, e.g. `0.001` = 10 bps.
    pub rate: f64,
}

impl PercentCommission {
    /// Construct from basis points (1 bp = 0.01%).
    pub fn bps(bps: f64) -> Self {
        Self { rate: bps / 10_000.0 }
    }
}

impl CommissionModel for PercentCommission {
    fn commission(&self, fill: &FillContext) -> f64 {
        fill.quantity.abs() * fill.price * self.rate
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::bar::{Bar, MarketSession};
    use chrono::Utc;

    fn bar(price: f64) -> Bar {
        Bar {
            time: Utc::now(),
            open: price,
            high: price + 1.0,
            low: price - 1.0,
            close: price,
            volume: 1_000,
            market_session: MarketSession::Main,
        }
    }

    fn fill<'a>(bar: &'a Bar, quantity: f64, price: f64) -> FillContext<'a> {
        FillContext { symbol: "AAPL", quantity, price, bar }
    }

    #[test]
    fn no_commission_is_zero() {
        let b = bar(100.0);
        assert_eq!(NoCommission.commission(&fill(&b, 100.0, 100.0)), 0.0);
    }

    #[test]
    fn per_share_charges_abs_quantity_with_minimum() {
        let model = PerShareCommission { per_share: 0.005, minimum: 1.0 };
        let b = bar(100.0);
        // 1000 shares * 0.005 = 5.0
        assert_eq!(model.commission(&fill(&b, 1000.0, 100.0)), 5.0);
        // sells charge the same as buys
        assert_eq!(model.commission(&fill(&b, -1000.0, 100.0)), 5.0);
        // 10 shares * 0.005 = 0.05 -> floored at the 1.0 minimum
        assert_eq!(model.commission(&fill(&b, 10.0, 100.0)), 1.0);
    }

    #[test]
    fn percent_charges_fraction_of_notional() {
        let model = PercentCommission::bps(10.0); // 0.1%
        let b = bar(100.0);
        // 50 shares * $100 * 0.001 = $5
        assert!((model.commission(&fill(&b, 50.0, 100.0)) - 5.0).abs() < 1e-9);
    }

    #[test]
    fn closure_is_a_commission_model() {
        let flat_fee = |_: &FillContext| 2.5;
        let b = bar(100.0);
        assert_eq!(flat_fee.commission(&fill(&b, 1.0, 100.0)), 2.5);
    }
}

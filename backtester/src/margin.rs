//! Customizable margin (buying-power) models.
//!
//! A margin model decides how much of a proposed fill the account can
//! actually carry: the full quantity, a trimmed same-sign partial (rounded
//! down to the lot size by the engine), or `0.0` to reject it. Orders that
//! *reduce* exposure should always be allowed (the built-ins do), so an
//! over-levered book can still de-risk. The default is [`NoMargin`] —
//! unlimited buying power, cash may go arbitrarily negative (pair it with
//! [`set_margin_interest_rate`](crate::Context::set_margin_interest_rate) to
//! at least pay for the leverage); `MaxLeverage::new(2.0)` gives a
//! Reg-T-style 2x gross-exposure cap.
//!
//! Same pattern as [`slippage`](crate::slippage) — a built-in, your own
//! [`MarginModel`] impl, or any `Fn(&MarginContext) -> f64` closure returning
//! the allowed signed quantity:
//!
//! ```
//! # use backtester::{Context, margin::MarginContext};
//! # let mut ctx = Context::default();
//! // A long-only account: block any fill that would open or extend a short.
//! ctx.set_margin_model(|m: &MarginContext| {
//!     if m.quantity < 0.0 {
//!         m.quantity.max(-m.current_qty.max(0.0))
//!     } else {
//!         m.quantity
//!     }
//! });
//! ```

/// Everything a margin model needs to size a single proposed fill. All values
/// are marked at the same prices order sizing uses (no look-ahead into the
/// fill bar under next-bar-open timing).
pub struct MarginContext<'a> {
    /// Symbol being traded.
    pub symbol: &'a str,
    /// Proposed signed fill quantity: positive = buy, negative = sell.
    pub quantity: f64,
    /// Reference fill price for this order (pre-slippage).
    pub price: f64,
    /// Signed quantity already held in `symbol` (0.0 if flat).
    pub current_qty: f64,
    /// Portfolio cash balance.
    pub cash: f64,
    /// Net liquidation value: cash plus all positions at their marks.
    pub equity: f64,
    /// Gross market value (Σ |quantity × mark|) of every *other* position —
    /// add `current_qty.abs() * price` for the total including this symbol.
    pub other_exposure: f64,
}

/// Returns the signed quantity of a proposed fill the account may take:
/// the full `quantity` to allow it, a smaller same-sign value for a partial
/// fill, or `0.0` to reject it outright.
///
/// Implemented for all the built-in models below and, via a blanket impl, for
/// any `Fn(&MarginContext) -> f64` closure.
pub trait MarginModel {
    fn allowed_quantity(&self, order: &MarginContext) -> f64;

    /// Fast-path hint: `true` lets the engine skip the buying-power
    /// bookkeeping entirely. Only [`NoMargin`] should return `true`.
    fn unlimited(&self) -> bool {
        false
    }
}

/// Any `Fn(&MarginContext) -> f64` closure is a margin model.
impl<F: Fn(&MarginContext) -> f64> MarginModel for F {
    fn allowed_quantity(&self, order: &MarginContext) -> f64 {
        self(order)
    }
}

/// Unlimited buying power — every fill is allowed and cash can go arbitrarily
/// negative. This is the default.
pub struct NoMargin;

impl MarginModel for NoMargin {
    fn allowed_quantity(&self, order: &MarginContext) -> f64 {
        order.quantity
    }

    fn unlimited(&self) -> bool {
        true
    }
}

/// Cap gross exposure (Σ |position market value|) at a multiple of equity.
/// `MaxLeverage::new(1.0)` is a cash account, `2.0` is Reg-T-style overnight
/// margin. Fills that would exceed the cap are trimmed to what fits (and
/// rejected once nothing fits); fills that reduce exposure always pass, so an
/// over-levered book — e.g. after a losing move shrank equity — can still
/// close out.
pub struct MaxLeverage {
    /// Maximum gross exposure as a multiple of equity, e.g. `2.0`.
    pub leverage: f64,
}

impl MaxLeverage {
    /// Cap gross exposure at `leverage` × equity. Must be positive.
    pub fn new(leverage: f64) -> Self {
        assert!(leverage > 0.0, "max leverage must be positive");
        Self { leverage }
    }
}

impl MarginModel for MaxLeverage {
    fn allowed_quantity(&self, m: &MarginContext) -> f64 {
        if m.price <= 0.0 {
            return m.quantity;
        }
        // Shares of this symbol the account may hold (in either direction)
        // with the rest of the book unchanged. Swapping cash for stock at the
        // mark leaves equity itself unchanged.
        let budget = ((self.leverage * m.equity - m.other_exposure) / m.price).max(0.0);
        if m.quantity > 0.0 {
            // Post-fill position may not exceed +budget; covering a short on
            // the way up is always fine.
            m.quantity.min((budget - m.current_qty).max(0.0))
        } else {
            // Mirror image: post-fill position may not exceed -budget.
            m.quantity.max((-budget - m.current_qty).min(0.0))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ctx(
        quantity: f64,
        current_qty: f64,
        equity: f64,
        other_exposure: f64,
    ) -> MarginContext<'static> {
        MarginContext {
            symbol: "AAPL",
            quantity,
            price: 100.0,
            current_qty,
            cash: 0.0,
            equity,
            other_exposure,
        }
    }

    #[test]
    fn no_margin_allows_everything() {
        assert_eq!(NoMargin.allowed_quantity(&ctx(1e9, 0.0, 1.0, 0.0)), 1e9);
        assert!(NoMargin.unlimited());
    }

    #[test]
    fn cash_account_caps_a_buy_at_equity() {
        let model = MaxLeverage::new(1.0);
        // $100k equity / $100 = 1,000 shares of budget.
        assert_eq!(model.allowed_quantity(&ctx(2_000.0, 0.0, 100_000.0, 0.0)), 1_000.0);
        assert_eq!(model.allowed_quantity(&ctx(500.0, 0.0, 100_000.0, 0.0)), 500.0);
        // Shorts consume the same budget.
        assert_eq!(model.allowed_quantity(&ctx(-2_000.0, 0.0, 100_000.0, 0.0)), -1_000.0);
    }

    #[test]
    fn other_positions_consume_the_budget() {
        let model = MaxLeverage::new(2.0);
        // 2 * 100k = 200k gross cap; 150k already used elsewhere leaves 500 sh.
        assert_eq!(model.allowed_quantity(&ctx(1_000.0, 0.0, 100_000.0, 150_000.0)), 500.0);
    }

    #[test]
    fn reducing_is_always_allowed_when_over_levered() {
        let model = MaxLeverage::new(1.0);
        // Long 2,000 with only 1,000 of budget: no more buying...
        assert_eq!(model.allowed_quantity(&ctx(10.0, 2_000.0, 100_000.0, 0.0)), 0.0);
        // ...but selling passes in full, and a flip stops at the short budget.
        assert_eq!(model.allowed_quantity(&ctx(-500.0, 2_000.0, 100_000.0, 0.0)), -500.0);
        assert_eq!(model.allowed_quantity(&ctx(-4_000.0, 2_000.0, 100_000.0, 0.0)), -3_000.0);
    }

    #[test]
    fn zero_equity_permits_only_reducing() {
        let model = MaxLeverage::new(2.0);
        assert_eq!(model.allowed_quantity(&ctx(100.0, 0.0, 0.0, 0.0)), 0.0);
        assert_eq!(model.allowed_quantity(&ctx(-100.0, 0.0, 0.0, 0.0)), 0.0);
        // A held position can still be closed, but not flipped.
        assert_eq!(model.allowed_quantity(&ctx(-1_500.0, 1_000.0, 0.0, 0.0)), -1_000.0);
        assert_eq!(model.allowed_quantity(&ctx(1_500.0, -1_000.0, 0.0, 0.0)), 1_000.0);
    }

    #[test]
    fn closure_is_a_margin_model() {
        let halve = |m: &MarginContext| m.quantity / 2.0;
        assert_eq!(halve.allowed_quantity(&ctx(100.0, 0.0, 1.0, 0.0)), 50.0);
        assert!(!halve.unlimited());
    }
}

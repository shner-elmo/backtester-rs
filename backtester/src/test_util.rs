//! Shared unit-test helpers.

use chrono::Utc;

use crate::{bar::Bar, slippage::FillContext};

/// A flat bar at `price`: o=c=price, high/low ±1, volume 1000.
pub(crate) fn bar(price: f64) -> Bar {
    Bar {
        time: Utc::now(),
        open: price,
        high: price + 1.0,
        low: price - 1.0,
        close: price,
        volume: 1_000,
    }
}

pub(crate) fn fill(bar: &Bar, quantity: f64, price: f64) -> FillContext<'_> {
    FillContext { symbol: "AAPL", quantity, price, bar }
}

use std::collections::HashMap;

use crate::EPSILON;

#[derive(Debug, Clone)]
pub struct Position {
    pub symbol: String,
    pub quantity: f64,
    pub avg_price: f64,
}

pub struct Portfolio {
    pub cash: f64,
    pub(crate) positions: HashMap<String, Position>,
}

impl Default for Portfolio {
    fn default() -> Self {
        Self { cash: 100_000.0, positions: HashMap::new() }
    }
}

impl Portfolio {
    pub fn get(&self, symbol: &str) -> Option<&Position> {
        self.positions.get(symbol)
    }

    pub fn total_value(&self, prices: &HashMap<String, f64>) -> f64 {
        let holdings: f64 = self
            .positions
            .values()
            .map(|p| p.quantity * prices.get(&p.symbol).copied().unwrap_or(p.avg_price))
            .sum();
        self.cash + holdings
    }

    pub(crate) fn apply_fill(&mut self, symbol: &str, qty: f64, price: f64) {
        self.cash -= qty * price;

        let prev = self.positions.get(symbol);
        let prev_qty = prev.map(|p| p.quantity).unwrap_or(0.0);
        let prev_avg = prev.map(|p| p.avg_price).unwrap_or(0.0);
        let new_qty = prev_qty + qty;

        // Fully closed (within rounding): no position remains.
        if new_qty.abs() < EPSILON {
            self.positions.remove(symbol);
            return;
        }

        let new_avg = if prev_qty == 0.0 || prev_qty.signum() != new_qty.signum() {
            // Opened from flat, or flipped through zero into the opposite
            // direction — the new position's basis is this fill's price.
            price
        } else if prev_qty.signum() == qty.signum() {
            // Added to the position in the same direction — weighted average.
            (prev_qty * prev_avg + qty * price) / new_qty
        } else {
            // Reduced the position without flipping — basis is unchanged.
            prev_avg
        };

        self.positions.insert(
            symbol.to_string(),
            Position { symbol: symbol.to_string(), quantity: new_qty, avg_price: new_avg },
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn qty_avg(p: &Portfolio, sym: &str) -> (f64, f64) {
        let pos = p.get(sym).unwrap();
        (pos.quantity, pos.avg_price)
    }

    #[test]
    fn opens_from_flat_at_fill_price() {
        let mut p = Portfolio { cash: 100_000.0, positions: HashMap::new() };
        p.apply_fill("AAPL", 100.0, 10.0);
        assert_eq!(qty_avg(&p, "AAPL"), (100.0, 10.0));
        assert_eq!(p.cash, 100_000.0 - 1_000.0);
    }

    #[test]
    fn adds_same_direction_weights_the_basis() {
        let mut p = Portfolio { cash: 100_000.0, positions: HashMap::new() };
        p.apply_fill("AAPL", 100.0, 10.0);
        p.apply_fill("AAPL", 100.0, 20.0);
        assert_eq!(qty_avg(&p, "AAPL"), (200.0, 15.0)); // (100*10 + 100*20)/200
    }

    #[test]
    fn reducing_keeps_the_basis() {
        let mut p = Portfolio { cash: 100_000.0, positions: HashMap::new() };
        p.apply_fill("AAPL", 100.0, 10.0);
        p.apply_fill("AAPL", -40.0, 25.0); // sell part at a higher price
        assert_eq!(qty_avg(&p, "AAPL"), (60.0, 10.0)); // basis unchanged
    }

    #[test]
    fn full_close_removes_the_position() {
        let mut p = Portfolio { cash: 100_000.0, positions: HashMap::new() };
        p.apply_fill("AAPL", 100.0, 10.0);
        p.apply_fill("AAPL", -100.0, 12.0);
        assert!(p.get("AAPL").is_none());
        // cash: -1000 (buy) +1200 (sell) = +200 over start
        assert_eq!(p.cash, 100_000.0 + 200.0);
    }

    #[test]
    fn flip_resets_basis_to_fill_price() {
        let mut p = Portfolio { cash: 100_000.0, positions: HashMap::new() };
        p.apply_fill("AAPL", 100.0, 10.0); // long 100 @ 10
        p.apply_fill("AAPL", -150.0, 20.0); // sell 150 @ 20 -> short 50
        let (qty, avg) = qty_avg(&p, "AAPL");
        assert_eq!(qty, -50.0);
        assert_eq!(avg, 20.0); // basis is the flip fill price, not the old long avg
    }
}

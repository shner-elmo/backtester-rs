use crate::{
    symbol::{Symbol, SymbolMap, SymbolVec},
    EPSILON,
};

/// Last known market price per symbol, `None` until the symbol prints a bar.
/// Indexed by symbol id rather than hashed: the engine rewrites it on every
/// bar and reads it for every mark-to-market.
pub type PriceTable = SymbolVec<Option<f64>>;

#[derive(Debug, Clone)]
pub struct Position {
    pub symbol: Symbol,
    pub quantity: f64,
    pub avg_price: f64,
}

pub struct Portfolio {
    pub cash: f64,
    pub(crate) positions: SymbolMap<Position>,
}

impl Default for Portfolio {
    fn default() -> Self {
        Self { cash: 100_000.0, positions: SymbolMap::default() }
    }
}

impl Portfolio {
    pub fn get(&self, symbol: Symbol) -> Option<&Position> {
        self.positions.get(&symbol)
    }

    pub fn total_value(&self, prices: &PriceTable) -> f64 {
        let holdings: f64 = self
            .positions
            .values()
            .map(|p| p.quantity * prices.copied(p.symbol).unwrap_or(p.avg_price))
            .sum();
        self.cash + holdings
    }

    pub(crate) fn apply_fill(&mut self, symbol: Symbol, qty: f64, price: f64) {
        self.cash -= qty * price;

        let prev = self.positions.get(&symbol);
        let prev_qty = prev.map(|p| p.quantity).unwrap_or(0.0);
        let prev_avg = prev.map(|p| p.avg_price).unwrap_or(0.0);
        let new_qty = prev_qty + qty;

        // Fully closed (within rounding): no position remains.
        if new_qty.abs() < EPSILON {
            self.positions.remove(&symbol);
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

        self.positions.insert(symbol, Position { symbol, quantity: new_qty, avg_price: new_avg });
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::symbol::SymbolTable;

    /// A fresh portfolio and the one symbol these tests trade.
    fn setup() -> (Portfolio, Symbol) {
        let mut table = SymbolTable::default();
        (Portfolio::default(), table.intern("AAPL"))
    }

    fn qty_avg(p: &Portfolio, sym: Symbol) -> (f64, f64) {
        let pos = p.get(sym).unwrap();
        (pos.quantity, pos.avg_price)
    }

    #[test]
    fn opens_from_flat_at_fill_price() {
        let (mut p, aapl) = setup();
        p.apply_fill(aapl, 100.0, 10.0);
        assert_eq!(qty_avg(&p, aapl), (100.0, 10.0));
        assert_eq!(p.cash, 100_000.0 - 1_000.0);
    }

    #[test]
    fn adds_same_direction_weights_the_basis() {
        let (mut p, aapl) = setup();
        p.apply_fill(aapl, 100.0, 10.0);
        p.apply_fill(aapl, 100.0, 20.0);
        assert_eq!(qty_avg(&p, aapl), (200.0, 15.0)); // (100*10 + 100*20)/200
    }

    #[test]
    fn reducing_keeps_the_basis() {
        let (mut p, aapl) = setup();
        p.apply_fill(aapl, 100.0, 10.0);
        p.apply_fill(aapl, -40.0, 25.0); // sell part at a higher price
        assert_eq!(qty_avg(&p, aapl), (60.0, 10.0)); // basis unchanged
    }

    #[test]
    fn full_close_removes_the_position() {
        let (mut p, aapl) = setup();
        p.apply_fill(aapl, 100.0, 10.0);
        p.apply_fill(aapl, -100.0, 12.0);
        assert!(p.get(aapl).is_none());
        // cash: -1000 (buy) +1200 (sell) = +200 over start
        assert_eq!(p.cash, 100_000.0 + 200.0);
    }

    #[test]
    fn flip_resets_basis_to_fill_price() {
        let (mut p, aapl) = setup();
        p.apply_fill(aapl, 100.0, 10.0); // long 100 @ 10
        p.apply_fill(aapl, -150.0, 20.0); // sell 150 @ 20 -> short 50
        let (qty, avg) = qty_avg(&p, aapl);
        assert_eq!(qty, -50.0);
        assert_eq!(avg, 20.0); // basis is the flip fill price, not the old long avg
    }

    #[test]
    fn total_value_marks_at_last_price_and_falls_back_to_basis() {
        let mut table = SymbolTable::default();
        let (aapl, msft) = (table.intern("AAPL"), table.intern("MSFT"));
        let mut p = Portfolio::default();
        p.apply_fill(aapl, 100.0, 10.0);
        p.apply_fill(msft, 10.0, 50.0);

        let mut prices = PriceTable::new();
        prices.set(aapl, Some(12.0));
        // MSFT never printed: marked at its basis.
        assert_eq!(p.total_value(&prices), p.cash + 100.0 * 12.0 + 10.0 * 50.0);
    }
}

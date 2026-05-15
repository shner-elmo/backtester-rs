use std::collections::HashMap;

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

        let current_qty = self.positions.get(symbol).map(|p| p.quantity).unwrap_or(0.0);
        let new_qty = current_qty + qty;

        if new_qty.abs() < 1e-9 {
            self.positions.remove(symbol);
            return;
        }

        let entry = self.positions.entry(symbol.to_string()).or_insert(Position {
            symbol: symbol.to_string(),
            quantity: 0.0,
            avg_price: price,
        });

        // Update avg_price only when adding to position (same direction)
        if entry.quantity * qty > 0.0 {
            let total_cost = entry.quantity * entry.avg_price + qty * price;
            entry.avg_price = total_cost / new_qty;
        }
        entry.quantity = new_qty;
    }
}

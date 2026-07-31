//! The PnL ledger: per-symbol open position lifetimes, netted into completed
//! [`Trade`]s when a position returns to flat.

use chrono::{DateTime, Utc};

use super::run::Engine;
use crate::{logging::LogConfig, stats::Trade, symbol::Symbol};

/// The full lifetime of one position, from flat to flat. Rebalance fills that
/// grow or trim the position accumulate here instead of each emitting a
/// separate "trade".
pub(super) struct OpenLifetime {
    pub(super) entry_time: DateTime<Utc>,
    /// `+1.0` long, `-1.0` short.
    pub(super) direction: f64,
    /// Cumulative absolute quantity bought into the position, and its cost.
    pub(super) entry_qty: f64,
    pub(super) entry_value: f64,
    /// Cumulative absolute quantity unwound, and its proceeds.
    pub(super) closed_qty: f64,
    pub(super) close_value: f64,
    /// Realized PnL so far, net of every commission this lifetime paid.
    pub(super) realized_pnl: f64,
}

impl OpenLifetime {
    pub(super) fn new(time: DateTime<Utc>, direction: f64) -> Self {
        Self {
            entry_time: time,
            direction,
            entry_qty: 0.0,
            entry_value: 0.0,
            closed_qty: 0.0,
            close_value: 0.0,
            realized_pnl: 0.0,
        }
    }

    fn into_trade(self, symbol: &str, exit_time: DateTime<Utc>, exit_reason: &str) -> Trade {
        Trade {
            symbol: symbol.to_string(),
            direction: if self.direction > 0.0 { "long" } else { "short" }.to_string(),
            entry_price: if self.entry_qty > 0.0 { self.entry_value / self.entry_qty } else { 0.0 },
            exit_price: if self.closed_qty > 0.0 {
                self.close_value / self.closed_qty
            } else {
                0.0
            },
            entry_time: self.entry_time.to_rfc3339(),
            exit_time: exit_time.to_rfc3339(),
            quantity: self.closed_qty,
            pnl: self.realized_pnl,
            exit_reason: exit_reason.to_string(),
        }
    }
}

impl Engine {
    /// End `symbol`'s open lifetime: net it into a [`Trade`], log it, and
    /// record it. No-op if no lifetime is open.
    pub(super) fn close_lifetime(
        &mut self,
        symbol: Symbol,
        exit_time: DateTime<Utc>,
        exit_reason: &str,
    ) {
        if let Some(lt) = self.lifetimes.remove(&symbol) {
            // A completed trade is reported, so this is where the symbol
            // becomes a ticker string again — once per round trip, not once
            // per bar.
            let trade = lt.into_trade(self.ctx.symbol_name(symbol), exit_time, exit_reason);
            log_trade(&self.ctx.log_config, &trade);
            self.trades.push(trade);
        }
    }
}

/// Log a completed round-trip trade (under the `trades` flag) at the moment
/// it is recorded, whatever closed it — a signal, a delisting, or a split
/// cash-out.
fn log_trade(cfg: &LogConfig, t: &Trade) {
    if cfg.trades {
        eprintln!(
            "[trade] {} {} {} qty {:.2}: {:.4} -> {:.4}, pnl {:+.2} ({})",
            t.exit_time,
            t.symbol,
            t.direction,
            t.quantity,
            t.entry_price,
            t.exit_price,
            t.pnl,
            t.exit_reason
        );
    }
}

//! Order execution: quantity resolution (sizing, volume-participation and
//! margin caps), slippage/commission, PnL-ledger bookkeeping, and the three
//! fill paths (current-bar-close, deferred next-bar-open, resting intrabar).

use chrono::{DateTime, Utc};

use crate::{
    bar::Bar,
    context::{FillTiming, Order, OrderKind, RestingKind},
    margin::MarginContext,
    slice::Slice,
    slippage::FillContext,
    EPSILON,
};

use super::{ledger::OpenLifetime, run::Engine};

impl Engine {
    /// Execute one order against the portfolio: resolve its target quantity,
    /// apply slippage and commission, and fold the fill into the symbol's
    /// open lifetime (emitting a completed `Trade` when it returns to flat).
    ///
    /// `exec_price` is the pre-slippage reference/fill price for this order's
    /// symbol (the current bar's close under [`FillTiming::CurrentBarClose`],
    /// the next bar's open under [`FillTiming::NextBarOpen`]); `fill_bar` is
    /// the bar the fill prints against. `SetHoldings` sizing marks the
    /// portfolio at `last_known_prices`.
    ///
    /// `limit_bound`, set for resting limit orders, caps the post-slippage
    /// fill at the limit price (a limit never fills worse than its limit —
    /// slippage can only improve it).
    ///
    /// Returns the signed quantity actually filled (0.0 if none), which is
    /// less than requested when the volume-participation cap bites — the
    /// caller uses it to shrink a partially filled resting order.
    pub(super) fn execute_order(
        &mut self,
        order: &Order,
        exec_price: f64,
        fill_bar: &Bar,
        tick_time: DateTime<Utc>,
        limit_bound: Option<f64>,
    ) -> f64 {
        let current_qty =
            self.ctx.portfolio.positions.get(&order.symbol).map(|p| p.quantity).unwrap_or(0.0);
        let qty = self.resolve_order_qty(order, exec_price, current_qty, fill_bar);
        if qty.abs() < EPSILON {
            return 0.0;
        }
        if self.ctx.max_volume_participation > 0.0 {
            *self.participation_used.entry(order.symbol.clone()).or_insert(0.0) += qty.abs();
        }

        // Actual execution price after the user's slippage model. Sizing
        // (in resolve_order_qty) uses the reference price; slippage only
        // affects the fill — except that a limit order never fills through
        // its limit.
        let fill_ctx =
            FillContext { symbol: &order.symbol, quantity: qty, price: exec_price, bar: fill_bar };
        let fill_price = self.ctx.slippage.fill_price(&fill_ctx);
        let fill_price = match limit_bound {
            Some(bound) if qty > 0.0 => fill_price.min(bound),
            Some(bound) => fill_price.max(bound),
            None => fill_price,
        };
        let commission = self.ctx.commission.commission(&fill_ctx);
        self.total_commission += commission;
        if self.ctx.log_config.trades {
            eprintln!(
                "[fill] {} {} {:+.2} @ {:.4}, commission {:.2}",
                tick_time.to_rfc3339(),
                order.symbol,
                qty,
                fill_price,
                commission
            );
        }

        self.record_fill(&order.symbol, qty, fill_price, commission, current_qty, tick_time);
        self.ctx.portfolio.apply_fill(&order.symbol, qty, fill_price);
        self.ctx.portfolio.cash -= commission;
        qty
    }

    /// Resolve an order's signed fill quantity: its own sizing (fixed,
    /// portfolio-weight target, or liquidate), then the volume-participation
    /// cap, then the margin model's buying-power cap.
    fn resolve_order_qty(
        &self,
        order: &Order,
        exec_price: f64,
        current_qty: f64,
        fill_bar: &Bar,
    ) -> f64 {
        let qty = match order.kind {
            OrderKind::Market(q) => q,
            OrderKind::SetHoldings(pct) => {
                let total = self.ctx.portfolio.total_value(&self.last_known_prices);
                let desired_qty = total * pct / exec_price;
                // Round the *target* (not the delta) to the lot so the held
                // position stays on-lot instead of drifting.
                let lot = self.ctx.lot_size;
                let target = (desired_qty / lot).round() * lot;
                target - current_qty
            }
            OrderKind::Liquidate => -current_qty,
        };

        // Volume-participation cap: fills on one symbol's bar can't take more
        // than the configured fraction of its volume *in aggregate* this
        // tick, and the remaining allowance is rounded down to the lot so a
        // capped fill stays on-lot. Unlimited when the fraction is 0.
        let qty = if self.ctx.max_volume_participation > 0.0 {
            let used = self.participation_used.get(&order.symbol).copied().unwrap_or(0.0);
            let cap = self.ctx.max_volume_participation * fill_bar.volume as f64 - used;
            let cap = ((cap / self.ctx.lot_size).trunc() * self.ctx.lot_size).max(0.0);
            qty.signum() * qty.abs().min(cap)
        } else {
            qty
        };

        // Buying-power cap from the margin model: shrink (or reject) the
        // fill. The allowed quantity is clamped to the order's own direction
        // and size — a model can only trim, never grow or reverse — and a
        // trimmed fill is rounded down to the lot.
        if self.ctx.margin.unlimited() {
            qty
        } else {
            let equity = self.ctx.portfolio.total_value(&self.last_known_prices);
            let other_exposure: f64 = self
                .ctx
                .portfolio
                .positions
                .values()
                .filter(|p| p.symbol != order.symbol)
                .map(|p| {
                    p.quantity.abs()
                        * self.last_known_prices.get(&p.symbol).copied().unwrap_or(p.avg_price)
                })
                .sum();
            let allowed = self.ctx.margin.allowed_quantity(&MarginContext {
                symbol: &order.symbol,
                quantity: qty,
                price: exec_price,
                current_qty,
                cash: self.ctx.portfolio.cash,
                equity,
                other_exposure,
            });
            let allowed = if qty > 0.0 { allowed.clamp(0.0, qty) } else { allowed.clamp(qty, 0.0) };
            if (allowed - qty).abs() < EPSILON {
                qty
            } else {
                qty.signum() * (allowed.abs() / self.ctx.lot_size).trunc() * self.ctx.lot_size
            }
        }
    }

    /// Fold a fill into the symbol's open lifetime, emitting a completed
    /// `Trade` when the position returns to flat.
    fn record_fill(
        &mut self,
        symbol: &str,
        qty: f64,
        fill_price: f64,
        commission: f64,
        current_qty: f64,
        tick_time: DateTime<Utc>,
    ) {
        let avg_price =
            self.ctx.portfolio.positions.get(symbol).map(|p| p.avg_price).unwrap_or(0.0);
        let new_qty = current_qty + qty;

        if current_qty == 0.0 {
            // Opened from flat: a new position lifetime begins.
            let mut lt = OpenLifetime::new(tick_time, qty.signum());
            lt.entry_qty = qty.abs();
            lt.entry_value = qty.abs() * fill_price;
            lt.realized_pnl = -commission;
            self.lifetimes.insert(symbol.to_string(), lt);
        } else if qty * current_qty > 0.0 {
            // Added in the same direction: grow the open lifetime.
            let lt = self
                .lifetimes
                .entry(symbol.to_string())
                .or_insert_with(|| OpenLifetime::new(tick_time, current_qty.signum()));
            lt.entry_qty += qty.abs();
            lt.entry_value += qty.abs() * fill_price;
            lt.realized_pnl -= commission;
        } else {
            // Reduced, closed, or flipped: realize PnL into the open
            // lifetime; only emit a Trade when the position is flat.
            let closed_now = qty.abs().min(current_qty.abs());
            let realized = (fill_price - avg_price) * closed_now * current_qty.signum();
            let lt = self
                .lifetimes
                .entry(symbol.to_string())
                .or_insert_with(|| OpenLifetime::new(tick_time, current_qty.signum()));
            lt.closed_qty += closed_now;
            lt.close_value += closed_now * fill_price;
            lt.realized_pnl += realized - commission;

            if closed_now >= current_qty.abs() - EPSILON {
                self.close_lifetime(symbol, tick_time, "signal");
                if new_qty.abs() > EPSILON {
                    // A flip leaves a residual position in the new direction;
                    // it starts a fresh lifetime.
                    let mut fresh = OpenLifetime::new(tick_time, new_qty.signum());
                    fresh.entry_qty = new_qty.abs();
                    fresh.entry_value = new_qty.abs() * fill_price;
                    self.lifetimes.insert(symbol.to_string(), fresh);
                }
            }
        }
    }

    /// NextBarOpen: realize orders decided on a symbol's previous bar at this
    /// bar's open, before `last_known_prices` advances and before the
    /// strategy sees the current bar — sizing marks the portfolio at the
    /// prior closes the order was decided on, no look-ahead into the bar
    /// being filled.
    pub(super) fn fill_deferred(&mut self, bars: &[(String, Bar)], tick_time: DateTime<Utc>) {
        if self.deferred.is_empty() {
            return;
        }
        for (symbol, bar) in bars {
            let Some(orders) = self.deferred.remove(symbol) else { continue };
            for order in orders {
                self.execute_order(&order, bar.open, bar, tick_time, None);
            }
        }
    }

    /// Resting limit/stop orders: fill (possibly partially) any whose trigger
    /// this bar's range trades through; keep the remainder resting. Decided
    /// on prior bars, so filling here is not look-ahead.
    pub(super) fn fill_resting(&mut self, bars: &[(String, Bar)], tick_time: DateTime<Utc>) {
        if self.resting_book.is_empty() {
            return;
        }
        for (symbol, bar) in bars {
            let Some(orders) = self.resting_book.remove(symbol) else { continue };
            let mut still_resting = Vec::new();
            for mut ro in orders {
                match resting_fill_price(&ro.kind, ro.qty, bar) {
                    Some(price) => {
                        let mkt = Order { symbol: symbol.clone(), kind: OrderKind::Market(ro.qty) };
                        let bound = match ro.kind {
                            RestingKind::Limit(p) => Some(p),
                            RestingKind::Stop(_) => None,
                        };
                        let filled = self.execute_order(&mkt, price, bar, tick_time, bound);
                        ro.qty -= filled;
                        if ro.qty.abs() > EPSILON {
                            still_resting.push(ro);
                        }
                    }
                    None => still_resting.push(ro),
                }
            }
            if !still_resting.is_empty() {
                self.resting_book.insert(symbol.clone(), still_resting);
            }
        }
    }

    /// Route the orders the strategy placed this bar according to the
    /// fill-timing model, and book any new resting orders (they start filling
    /// from the symbol's next bar).
    pub(super) fn route_new_orders(&mut self, slice: &Slice, tick_time: DateTime<Utc>) {
        let orders = std::mem::take(&mut self.ctx.pending_orders);
        match self.ctx.fill_timing {
            FillTiming::CurrentBarClose => {
                // Fill at this bar's close; a symbol with no bar this tick
                // has no price to fill against. last_known_prices has already
                // advanced to this tick's closes, so held symbols *without* a
                // bar this tick are valued at their latest market price for
                // sizing, not their cost basis.
                for order in orders {
                    let Some(bar) = slice.bars.get(&order.symbol) else { continue };
                    self.execute_order(&order, bar.close, bar, tick_time, None);
                }
            }
            FillTiming::NextBarOpen => {
                // Hold each order until its symbol's next bar, where it fills
                // at the open (see fill_deferred).
                for order in orders {
                    self.deferred.entry(order.symbol.clone()).or_default().push(order);
                }
            }
        }

        for ro in std::mem::take(&mut self.ctx.resting_orders) {
            self.resting_book.entry(ro.symbol.clone()).or_default().push(ro);
        }
    }
}

/// Given a resting order and the bar it is being tested against, return the
/// pre-slippage fill price if the bar's range triggers it, else `None`. A
/// limit fills at its price or the (better) bar open; a triggered stop fills
/// at its price or the (worse) bar open, modeling a gap through the stop.
fn resting_fill_price(kind: &RestingKind, qty: f64, bar: &Bar) -> Option<f64> {
    match *kind {
        RestingKind::Limit(price) => {
            if qty > 0.0 && bar.low <= price {
                Some(price.min(bar.open))
            } else if qty < 0.0 && bar.high >= price {
                Some(price.max(bar.open))
            } else {
                None
            }
        }
        RestingKind::Stop(price) => {
            if qty > 0.0 && bar.high >= price {
                Some(price.max(bar.open))
            } else if qty < 0.0 && bar.low <= price {
                Some(price.min(bar.open))
            } else {
                None
            }
        }
    }
}

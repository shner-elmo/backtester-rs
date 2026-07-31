//! Day-boundary work: splits, dividends, ticker renames, financing accrual,
//! and the delisting scan. All of it runs once per calendar date, after the
//! previous day's equity mark (which must see pre-split state) and before the
//! new day's bars touch anything.

use std::collections::BTreeMap;

use chrono::{DateTime, NaiveDate, Utc};

use super::{ledger::OpenLifetime, run::Engine};
use crate::{
    algorithm::Algorithm,
    broker::Position,
    context::{OrderKind, RestingKind},
    stats::TRADING_DAYS_PER_YEAR,
    symbol::Symbol,
    EPSILON,
};

/// Remove and return every pending action dated on or before `today`, oldest
/// first.
fn drain_due<T>(pending: &mut BTreeMap<NaiveDate, Vec<T>>, today: NaiveDate) -> Vec<T> {
    let mut due = Vec::new();
    while let Some(entry) = pending.first_entry() {
        if *entry.key() > today {
            break;
        }
        due.append(&mut entry.remove());
    }
    due
}

impl Engine {
    /// Once-per-date day-start work. Not warm-up gated: history built during
    /// warm-up needs split-rescaling too, and no positions can exist in
    /// warm-up so financing and the delist scan are vacuous there.
    pub(super) fn start_new_day<A: Algorithm>(
        &mut self,
        algo: &mut A,
        tick_date: NaiveDate,
        tick_time: DateTime<Utc>,
    ) {
        if self.global_last_date == Some(tick_date) {
            return;
        }
        let is_first_tick = self.global_last_date.is_none();
        self.global_last_date = Some(tick_date);
        if is_first_tick {
            // Actions dated before the first bar we ever process have
            // nothing to adjust — drain them silently.
            drain_due(&mut self.pending.splits, tick_date);
            drain_due(&mut self.pending.dividends, tick_date);
            drain_due(&mut self.pending.renames, tick_date);
            return;
        }
        self.day_index += 1;

        self.apply_due_splits(algo, tick_date, tick_time);
        self.apply_due_dividends(algo, tick_date);
        self.apply_due_renames(algo, tick_date);
        // Accrue a day of financing on positions carried into today (borrow
        // fees on shorts, margin interest on negative cash).
        self.apply_financing();
        self.close_delisted_positions(algo, tick_date, tick_time);
    }

    fn apply_due_splits<A: Algorithm>(
        &mut self,
        algo: &mut A,
        tick_date: NaiveDate,
        tick_time: DateTime<Utc>,
    ) {
        for (symbol, ratio) in drain_due(&mut self.pending.splits, tick_date) {
            if self.ctx.log_config.corporate_events {
                let name = self.ctx.symbol_name(symbol);
                eprintln!("[event] {tick_date}: split {name}, ratio {ratio}");
            }
            self.apply_split(symbol, ratio, tick_time);
            algo.on_split(&mut self.ctx, symbol, ratio);
        }
    }

    /// Cash dividends going ex on or before today, credited to any position
    /// held on the ex-date.
    fn apply_due_dividends<A: Algorithm>(&mut self, algo: &mut A, tick_date: NaiveDate) {
        for (symbol, amount) in drain_due(&mut self.pending.dividends, tick_date) {
            if self.apply_dividend(symbol, amount) {
                if self.ctx.log_config.corporate_events {
                    let name = self.ctx.symbol_name(symbol);
                    eprintln!("[event] {tick_date}: dividend {name}, {amount:.4}/share");
                }
                algo.on_dividend(&mut self.ctx, symbol, amount);
            }
        }
    }

    /// Ticker renames effective on or before today: transfer the position and
    /// ledger from the old symbol to the new one before the delist scan would
    /// otherwise force-liquidate the old.
    fn apply_due_renames<A: Algorithm>(&mut self, algo: &mut A, tick_date: NaiveDate) {
        for (old, new) in drain_due(&mut self.pending.renames, tick_date) {
            if self.ctx.log_config.corporate_events {
                let (old_name, new_name) = (self.ctx.symbol_name(old), self.ctx.symbol_name(new));
                eprintln!("[event] {tick_date}: rename {old_name} -> {new_name}");
            }
            self.apply_rename(old, new);
            algo.on_rename(&mut self.ctx, old, new);
        }
    }

    /// Apply a split that executed on `symbol`: rescale the held position
    /// (quantity × ratio, basis ÷ ratio — value invariant), the open-lifetime
    /// ledger, the stored history, the last known price, and any not-yet-filled
    /// orders into post-split terms. Bar prices in slices are never adjusted —
    /// the raw stream is what the market actually printed. A fractional
    /// remainder versus the lot size is cashed out in lieu at the (post-split)
    /// last known price, like a broker does on reverse splits.
    fn apply_split(&mut self, symbol: Symbol, ratio: f64, tick_time: DateTime<Utc>) {
        if let Some(hist) = self.ctx.history_store.get_mut(symbol) {
            for b in hist.iter_mut() {
                b.open /= ratio;
                b.high /= ratio;
                b.low /= ratio;
                b.close /= ratio;
                b.volume = (b.volume as f64 * ratio).round() as u64;
            }
        }
        if let Some(Some(p)) = self.last_known_prices.get_mut(symbol) {
            *p /= ratio;
        }
        if let Some(lt) = self.lifetimes.get_mut(&symbol) {
            // Express the lifetime in post-split shares; the dollar figures
            // (entry_value, close_value, realized_pnl) are already invariant.
            lt.entry_qty *= ratio;
            lt.closed_qty *= ratio;
        }

        // Orders decided pre-split are expressed in pre-split terms; rescale
        // them (qty × ratio, trigger ÷ ratio) so they don't spuriously fire —
        // or silently die — against the post-split tape.
        if let Some(orders) = self.resting_book.get_mut(&symbol) {
            for ro in orders {
                ro.qty *= ratio;
                ro.kind = match ro.kind {
                    RestingKind::Limit(p) => RestingKind::Limit(p / ratio),
                    RestingKind::Stop(p) => RestingKind::Stop(p / ratio),
                };
            }
        }
        if let Some(orders) = self.deferred.get_mut(&symbol) {
            for o in orders {
                if let OrderKind::Market(q) = &mut o.kind {
                    *q *= ratio;
                }
            }
        }

        let Some(pos) = self.ctx.portfolio.positions.get_mut(&symbol) else { return };
        pos.quantity *= ratio;
        pos.avg_price /= ratio;

        // Cash-in-lieu for the fractional remainder against the lot size.
        let lot = self.ctx.lot_size;
        let rounded = (pos.quantity / lot).trunc() * lot;
        let residual = pos.quantity - rounded;
        if residual.abs() < EPSILON {
            return;
        }
        let price = self.last_known_prices.copied(symbol).unwrap_or(pos.avg_price);
        let avg = pos.avg_price;
        pos.quantity = rounded;
        self.ctx.portfolio.cash += residual * price;
        if let Some(lt) = self.lifetimes.get_mut(&symbol) {
            lt.closed_qty += residual.abs();
            lt.close_value += residual.abs() * price;
            lt.realized_pnl += (price - avg) * residual.abs() * residual.signum();
        }
        if rounded.abs() < EPSILON {
            // The whole position was cashed out (deep reverse split of a tiny
            // holding): the lifetime is over.
            self.ctx.portfolio.positions.remove(&symbol);
            self.close_lifetime(symbol, tick_time, "split");
        }
    }

    /// Apply a cash dividend that went ex on `symbol`: credit
    /// `quantity * amount` to cash (a debit for shorts) and attribute that
    /// income to the open position's PnL, so the eventual round trip reports
    /// its total return. Symbols not held on the ex-date pay nothing. Returns
    /// whether a dividend was paid, so the caller only fires `on_dividend`
    /// when it actually was.
    fn apply_dividend(&mut self, symbol: Symbol, amount: f64) -> bool {
        let Some(qty) = self.ctx.portfolio.positions.get(&symbol).map(|p| p.quantity) else {
            return false;
        };
        let cash_flow = qty * amount;
        self.ctx.portfolio.cash += cash_flow;
        if let Some(lt) = self.lifetimes.get_mut(&symbol) {
            lt.realized_pnl += cash_flow;
        }
        true
    }

    /// Accrue one trading day of financing on positions carried into a new
    /// day: a borrow fee on shorts and margin interest on any negative cash
    /// balance, both at `annual_rate / 252`. Charges are deducted from cash
    /// and attributed to position PnL — shorts bear their own borrow fee;
    /// margin interest is spread across the long book by market value — so
    /// the accounting identity holds. No-op unless a financing rate is set.
    fn apply_financing(&mut self) {
        let ctx = &mut self.ctx;
        if ctx.short_borrow_rate <= 0.0 && ctx.margin_interest_rate <= 0.0 {
            return;
        }
        let prices = &self.last_known_prices;
        let price_of = |sym: Symbol, avg: f64| prices.copied(sym).unwrap_or(avg);

        // Borrow fee on each short position's market value.
        if ctx.short_borrow_rate > 0.0 {
            let daily = ctx.short_borrow_rate / TRADING_DAYS_PER_YEAR;
            let shorts: Vec<(Symbol, f64)> = ctx
                .portfolio
                .positions
                .values()
                .filter(|p| p.quantity < 0.0)
                .map(|p| (p.symbol, p.quantity.abs() * price_of(p.symbol, p.avg_price)))
                .collect();
            for (sym, market_value) in shorts {
                let fee = daily * market_value;
                ctx.portfolio.cash -= fee;
                if let Some(lt) = self.lifetimes.get_mut(&sym) {
                    lt.realized_pnl -= fee;
                }
            }
        }

        // Margin interest on a negative cash balance, spread across long
        // positions by market value (they are what the borrowing funds). With
        // no longs it falls back to the whole book by absolute market value,
        // so an all-short portfolio that went cash-negative still pays. With
        // no positions at all the charge is skipped: there is no open
        // lifetime to attribute it to, and an unattributed debit would break
        // the accounting identity.
        if ctx.margin_interest_rate > 0.0 && ctx.portfolio.cash < 0.0 {
            let interest = ctx.margin_interest_rate / TRADING_DAYS_PER_YEAR * (-ctx.portfolio.cash);
            let book = |longs_only: bool| -> Vec<(Symbol, f64)> {
                ctx.portfolio
                    .positions
                    .values()
                    .filter(|p| !longs_only || p.quantity > 0.0)
                    .map(|p| (p.symbol, p.quantity.abs() * price_of(p.symbol, p.avg_price)))
                    .collect()
            };
            let mut pool = book(true);
            if pool.is_empty() {
                pool = book(false);
            }
            let total_mv: f64 = pool.iter().map(|(_, mv)| mv).sum();
            if total_mv > 0.0 {
                ctx.portfolio.cash -= interest;
                for (sym, market_value) in pool {
                    let share = interest * (market_value / total_mv);
                    if let Some(lt) = self.lifetimes.get_mut(&sym) {
                        lt.realized_pnl -= share;
                    }
                }
            }
        }
    }

    /// Transfer all state for a renamed symbol from `old` to `new`: the held
    /// position, the PnL ledger, the stored history, the last known price,
    /// and any resting orders (retagged so they still fill). No cash moves
    /// and no trade is emitted — a rename is a relabeling, not a round trip.
    /// `new` is already subscribed (all rename targets are subscribed up
    /// front so their bars stream), so this only has to move the bookkeeping.
    /// Renames target a fresh ticker in practice; the merge branches are
    /// defensive for the rare case the destination is already held.
    fn apply_rename(&mut self, old: Symbol, new: Symbol) {
        if old == new {
            return;
        }

        let hist = self.ctx.history_store.take(old);
        if !hist.is_empty() && self.ctx.history_store.slot(new).is_empty() {
            self.ctx.history_store.set(new, hist);
        }
        if let Some(price) = self.last_known_prices.take(old) {
            if self.last_known_prices.copied(new).is_none() {
                self.last_known_prices.set(new, Some(price));
            }
        }
        if let Some(day) = self.last_seen_day.take(old) {
            let entry = self.last_seen_day.slot(new);
            *entry = Some(entry.unwrap_or(day).max(day));
        }
        if let Some(mut orders) = self.resting_book.remove(&old) {
            for o in &mut orders {
                o.symbol = new;
            }
            self.resting_book.entry(new).or_default().extend(orders);
        }

        // Position: move to `new`, weight-averaging the basis if it is
        // somehow already held in the same direction (preserves total
        // unrealized PnL).
        if let Some(mut pos) = self.ctx.portfolio.positions.remove(&old) {
            pos.symbol = new;
            let merged = match self.ctx.portfolio.positions.remove(&new) {
                None => pos,
                Some(existing) => {
                    let qty = existing.quantity + pos.quantity;
                    let avg = if qty.abs() < EPSILON {
                        0.0
                    } else if existing.quantity.signum() == pos.quantity.signum() {
                        (existing.quantity * existing.avg_price + pos.quantity * pos.avg_price)
                            / qty
                    } else if existing.quantity.abs() >= pos.quantity.abs() {
                        existing.avg_price
                    } else {
                        pos.avg_price
                    };
                    Position { symbol: new, quantity: qty, avg_price: avg }
                }
            };
            if merged.quantity.abs() > EPSILON {
                self.ctx.portfolio.positions.insert(new, merged);
            }
        }

        // PnL ledger: move (or fold) the open lifetime so realized PnL is
        // preserved.
        if let Some(lt_old) = self.lifetimes.remove(&old) {
            match self.lifetimes.get_mut(&new) {
                None => {
                    self.lifetimes.insert(new, lt_old);
                }
                Some(lt_new) => {
                    lt_new.entry_qty += lt_old.entry_qty;
                    lt_new.entry_value += lt_old.entry_value;
                    lt_new.closed_qty += lt_old.closed_qty;
                    lt_new.close_value += lt_old.close_value;
                    lt_new.realized_pnl += lt_old.realized_pnl;
                }
            }
        }
    }

    /// Force-close positions in symbols that printed no bar for the
    /// configured number of trading days (delisting).
    ///
    /// The scan runs at day start, *before* today's bars register as seen: a
    /// symbol reappearing after exactly the threshold of silent days is still
    /// force-closed that morning — the decision is made on the information
    /// available at the open.
    fn close_delisted_positions<A: Algorithm>(
        &mut self,
        algo: &mut A,
        tick_date: NaiveDate,
        tick_time: DateTime<Utc>,
    ) {
        let delist_after = self.ctx.delist_after_days as u64;
        if delist_after == 0 || self.ctx.portfolio.positions.is_empty() {
            return;
        }
        let stale: Vec<Symbol> = self
            .ctx
            .portfolio
            .positions
            .keys()
            .copied()
            .filter(|s| {
                let seen = self.last_seen_day.copied(*s).unwrap_or(self.day_index);
                self.day_index.saturating_sub(seen) >= delist_after
            })
            .collect();
        for symbol in stale {
            if self.ctx.log_config.corporate_events {
                let name = self.ctx.symbol_name(symbol);
                eprintln!(
                    "[event] {tick_date}: {name} delisted (no bars for \
                     {delist_after} trading days); position force-closed"
                );
            }
            self.force_close_delisted(symbol, tick_time);
            // Pending next-bar and resting orders can never fill against a
            // symbol that stopped trading.
            self.deferred.remove(&symbol);
            self.resting_book.remove(&symbol);
            algo.on_delisted(&mut self.ctx, symbol);
        }
    }

    /// Force-close a position in a symbol that stopped trading: fill the
    /// whole quantity at the last known price (less the configured delist
    /// haircut) with no commission, and emit the closing trade with
    /// `exit_reason: "delisted"`.
    fn force_close_delisted(&mut self, symbol: Symbol, tick_time: DateTime<Utc>) {
        let Some(pos) = self.ctx.portfolio.positions.get(&symbol) else { return };
        let qty = pos.quantity;
        let avg = pos.avg_price;
        // Recover the last print less the haircut, so a bankruptcy writes
        // down the position instead of liquidating at an optimistic final
        // quote.
        let price =
            self.last_known_prices.copied(symbol).unwrap_or(avg) * (1.0 - self.ctx.delist_haircut);

        let lt = self
            .lifetimes
            .entry(symbol)
            .or_insert_with(|| OpenLifetime::new(tick_time, qty.signum()));
        lt.closed_qty += qty.abs();
        lt.close_value += qty.abs() * price;
        lt.realized_pnl += (price - avg) * qty.abs() * qty.signum();
        self.close_lifetime(symbol, tick_time, "delisted");

        self.ctx.portfolio.apply_fill(symbol, -qty, price);
    }
}

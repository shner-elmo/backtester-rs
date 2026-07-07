use std::collections::{BTreeMap, HashMap};

use chrono::{DateTime, Datelike, NaiveDate, TimeZone, Utc};
use chrono_tz::US::Eastern;
use serde::{Deserialize, Serialize};

use crate::{
    algorithm::Algorithm,
    bar::Bar,
    broker::Position,
    context::{Context, FillTiming, Order, OrderKind, RestingKind, RestingOrder},
    data::{
        file_year_month, load_dividends, load_renames, load_splits, load_ticker_map,
        read_bars_from_file, sorted_parquet_files,
    },
    error::BacktestError,
    margin::MarginContext,
    slice::Slice,
    slippage::FillContext,
    stats::{
        compute_stats, BacktestStats, EquityPoint, OpenPositionSummary, Trade,
        TRADING_DAYS_PER_YEAR,
    },
    EPSILON,
};

/// Everything a finished backtest produced. `run` prints a summary of this and
/// writes it to disk; consume it directly (or via the JSON file) for custom
/// reporting and the results dashboard.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BacktestResult {
    pub initial_cash: f64,
    /// Cash plus open positions marked at the last known market price.
    pub final_equity: f64,
    pub total_commission: f64,
    pub stats: BacktestStats,
    /// Daily mark-to-market equity, one point per trading day.
    pub equity_curve: Vec<EquityPoint>,
    /// Per-bar mark-to-market equity (RFC 3339 timestamps), empty unless
    /// `Context::set_track_intraday_equity(true)` was set. Exposes intraday
    /// drawdown the daily `equity_curve` can't show.
    #[serde(default)]
    pub intraday_equity: Vec<EquityPoint>,
    /// Positions still open at the end, marked at the last known price.
    pub open_positions: Vec<OpenPositionSummary>,
    /// Completed round trips (rebalance fills netted per position lifetime).
    pub trades: Vec<Trade>,
}

/// The full lifetime of one position, from flat to flat. Rebalance fills that
/// grow or trim the position accumulate here instead of each emitting a
/// separate "trade".
struct OpenLifetime {
    entry_time: DateTime<Utc>,
    /// `+1.0` long, `-1.0` short.
    direction: f64,
    /// Cumulative absolute quantity bought into the position, and its cost.
    entry_qty: f64,
    entry_value: f64,
    /// Cumulative absolute quantity unwound, and its proceeds.
    closed_qty: f64,
    close_value: f64,
    /// Realized PnL so far, net of every commission this lifetime paid.
    realized_pnl: f64,
}

impl OpenLifetime {
    fn new(time: DateTime<Utc>, direction: f64) -> Self {
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

/// Apply a split that executed on `symbol`: rescale the held position
/// (quantity × ratio, basis ÷ ratio — value invariant), the open-lifetime
/// ledger, the stored history, and the last known price into post-split
/// terms. Bar prices in slices are never adjusted — the raw stream is what
/// the market actually printed. A fractional remainder versus the lot size is
/// cashed out in lieu at the (post-split) last known price, like a broker
/// does on reverse splits.
#[allow(clippy::too_many_arguments)]
fn apply_split(
    ctx: &mut Context,
    lifetimes: &mut HashMap<String, OpenLifetime>,
    trades: &mut Vec<Trade>,
    last_known_prices: &mut HashMap<String, f64>,
    symbol: &str,
    ratio: f64,
    tick_time: DateTime<Utc>,
) {
    if let Some(hist) = ctx.history_store.get_mut(symbol) {
        for b in hist.iter_mut() {
            b.open /= ratio;
            b.high /= ratio;
            b.low /= ratio;
            b.close /= ratio;
            b.volume = (b.volume as f64 * ratio).round() as u64;
        }
    }
    if let Some(p) = last_known_prices.get_mut(symbol) {
        *p /= ratio;
    }
    if let Some(lt) = lifetimes.get_mut(symbol) {
        // Express the lifetime in post-split shares; the dollar figures
        // (entry_value, close_value, realized_pnl) are already invariant.
        lt.entry_qty *= ratio;
        lt.closed_qty *= ratio;
    }

    let Some(pos) = ctx.portfolio.positions.get_mut(symbol) else { return };
    pos.quantity *= ratio;
    pos.avg_price /= ratio;

    // Cash-in-lieu for the fractional remainder against the lot size.
    let lot = ctx.lot_size;
    let rounded = (pos.quantity / lot).trunc() * lot;
    let residual = pos.quantity - rounded;
    if residual.abs() < EPSILON {
        return;
    }
    let price = last_known_prices.get(symbol).copied().unwrap_or(pos.avg_price);
    let avg = pos.avg_price;
    pos.quantity = rounded;
    ctx.portfolio.cash += residual * price;
    if let Some(lt) = lifetimes.get_mut(symbol) {
        lt.closed_qty += residual.abs();
        lt.close_value += residual.abs() * price;
        lt.realized_pnl += (price - avg) * residual.abs() * residual.signum();
    }
    if rounded.abs() < EPSILON {
        // The whole position was cashed out (deep reverse split of a tiny
        // holding): the lifetime is over.
        ctx.portfolio.positions.remove(symbol);
        if let Some(lt) = lifetimes.remove(symbol) {
            trades.push(lt.into_trade(symbol, tick_time, "split"));
        }
    }
}

/// Apply a cash dividend that went ex on `symbol`: credit `quantity * amount`
/// to cash (a debit for shorts) and attribute that income to the open
/// position's PnL, so the eventual round trip reports its total return. Symbols
/// not held on the ex-date pay nothing. Returns whether a dividend was paid, so
/// the caller only fires `on_dividend` when it actually was.
fn apply_dividend(
    ctx: &mut Context,
    lifetimes: &mut HashMap<String, OpenLifetime>,
    symbol: &str,
    amount: f64,
) -> bool {
    let Some(qty) = ctx.portfolio.positions.get(symbol).map(|p| p.quantity) else { return false };
    let cash_flow = qty * amount;
    ctx.portfolio.cash += cash_flow;
    if let Some(lt) = lifetimes.get_mut(symbol) {
        lt.realized_pnl += cash_flow;
    }
    true
}

/// Accrue one trading day of financing on positions carried into a new day: a
/// borrow fee on shorts and margin interest on any negative cash balance, both
/// at `annual_rate / 252`. Charges are deducted from cash and attributed to
/// position PnL — shorts bear their own borrow fee; margin interest is spread
/// across the long book by market value — so the accounting identity holds.
/// No-op unless a financing rate is set.
fn apply_financing(
    ctx: &mut Context,
    lifetimes: &mut HashMap<String, OpenLifetime>,
    last_known_prices: &HashMap<String, f64>,
) {
    if ctx.short_borrow_rate <= 0.0 && ctx.margin_interest_rate <= 0.0 {
        return;
    }
    let price_of = |sym: &String, avg: f64| last_known_prices.get(sym).copied().unwrap_or(avg);

    // Borrow fee on each short position's market value.
    if ctx.short_borrow_rate > 0.0 {
        let daily = ctx.short_borrow_rate / TRADING_DAYS_PER_YEAR;
        let shorts: Vec<(String, f64)> = ctx
            .portfolio
            .positions
            .values()
            .filter(|p| p.quantity < 0.0)
            .map(|p| (p.symbol.clone(), p.quantity.abs() * price_of(&p.symbol, p.avg_price)))
            .collect();
        for (sym, market_value) in shorts {
            let fee = daily * market_value;
            ctx.portfolio.cash -= fee;
            if let Some(lt) = lifetimes.get_mut(&sym) {
                lt.realized_pnl -= fee;
            }
        }
    }

    // Margin interest on a negative cash balance, spread across long positions
    // by market value (they are what the borrowing funds). With no longs it
    // falls back to the whole book by absolute market value, so an all-short
    // portfolio that went cash-negative still pays. With no positions at all
    // the charge is skipped: there is no open lifetime to attribute it to, and
    // an unattributed debit would break the accounting identity.
    if ctx.margin_interest_rate > 0.0 && ctx.portfolio.cash < 0.0 {
        let interest = ctx.margin_interest_rate / TRADING_DAYS_PER_YEAR * (-ctx.portfolio.cash);
        let book = |longs_only: bool| -> Vec<(String, f64)> {
            ctx.portfolio
                .positions
                .values()
                .filter(|p| !longs_only || p.quantity > 0.0)
                .map(|p| (p.symbol.clone(), p.quantity.abs() * price_of(&p.symbol, p.avg_price)))
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
                if let Some(lt) = lifetimes.get_mut(&sym) {
                    lt.realized_pnl -= share;
                }
            }
        }
    }
}

/// Transfer all state for a renamed symbol from `old` to `new`: the held
/// position, the PnL ledger, the stored history, and the last known price. No
/// cash moves and no trade is emitted — a rename is a relabeling, not a round
/// trip. `new` is already subscribed (all rename targets are subscribed up
/// front so their bars stream), so this only has to move the bookkeeping.
/// Renames target a fresh ticker in practice; the merge branches are defensive
/// for the rare case the destination is already held.
fn apply_rename(
    ctx: &mut Context,
    lifetimes: &mut HashMap<String, OpenLifetime>,
    last_known_prices: &mut HashMap<String, f64>,
    last_seen_day: &mut HashMap<String, u64>,
    old: &str,
    new: &str,
) {
    if old == new {
        return;
    }

    if let Some(hist) = ctx.history_store.remove(old) {
        ctx.history_store.entry(new.to_string()).or_insert(hist);
    }
    if let Some(price) = last_known_prices.remove(old) {
        last_known_prices.entry(new.to_string()).or_insert(price);
    }
    if let Some(day) = last_seen_day.remove(old) {
        let entry = last_seen_day.entry(new.to_string()).or_insert(day);
        *entry = (*entry).max(day);
    }

    // Position: move to `new`, weight-averaging the basis if it is somehow
    // already held in the same direction (preserves total unrealized PnL).
    if let Some(mut pos) = ctx.portfolio.positions.remove(old) {
        pos.symbol = new.to_string();
        let merged = match ctx.portfolio.positions.remove(new) {
            None => pos,
            Some(existing) => {
                let qty = existing.quantity + pos.quantity;
                let avg = if qty.abs() < EPSILON {
                    0.0
                } else if existing.quantity.signum() == pos.quantity.signum() {
                    (existing.quantity * existing.avg_price + pos.quantity * pos.avg_price) / qty
                } else if existing.quantity.abs() >= pos.quantity.abs() {
                    existing.avg_price
                } else {
                    pos.avg_price
                };
                Position { symbol: new.to_string(), quantity: qty, avg_price: avg }
            }
        };
        if merged.quantity.abs() > EPSILON {
            ctx.portfolio.positions.insert(new.to_string(), merged);
        }
    }

    // PnL ledger: move (or fold) the open lifetime so realized PnL is preserved.
    if let Some(lt_old) = lifetimes.remove(old) {
        match lifetimes.get_mut(new) {
            None => {
                lifetimes.insert(new.to_string(), lt_old);
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

/// Force-close a position in a symbol that stopped trading: fill the whole
/// quantity at the last known price (less the configured delist haircut) with
/// no commission, and emit the closing trade with `exit_reason: "delisted"`.
fn force_close_delisted(
    ctx: &mut Context,
    lifetimes: &mut HashMap<String, OpenLifetime>,
    trades: &mut Vec<Trade>,
    last_known_prices: &HashMap<String, f64>,
    symbol: &str,
    tick_time: DateTime<Utc>,
) {
    let Some(pos) = ctx.portfolio.positions.get(symbol) else { return };
    let qty = pos.quantity;
    let avg = pos.avg_price;
    // Recover the last print less the haircut, so a bankruptcy writes down the
    // position instead of liquidating at an optimistic final quote.
    let price = last_known_prices.get(symbol).copied().unwrap_or(avg) * (1.0 - ctx.delist_haircut);

    let lt = lifetimes
        .entry(symbol.to_string())
        .or_insert_with(|| OpenLifetime::new(tick_time, qty.signum()));
    lt.closed_qty += qty.abs();
    lt.close_value += qty.abs() * price;
    lt.realized_pnl += (price - avg) * qty.abs() * qty.signum();
    let lt = lifetimes.remove(symbol).unwrap();
    trades.push(lt.into_trade(symbol, tick_time, "delisted"));

    ctx.portfolio.apply_fill(symbol, -qty, price);
}

/// Execute one order against the portfolio: resolve its target quantity, apply
/// slippage and commission, and fold the fill into the symbol's open lifetime
/// (emitting a completed `Trade` when it returns to flat).
///
/// `mark_prices` values the portfolio for `SetHoldings` sizing; `exec_price` is
/// the pre-slippage reference/fill price for this order's symbol (the current
/// bar's close under [`FillTiming::CurrentBarClose`], the next bar's open under
/// [`FillTiming::NextBarOpen`]). `fill_bar` is the bar the fill prints against.
///
/// `limit_bound`, set for resting limit orders, caps the post-slippage fill at
/// the limit price (a limit never fills worse than its limit — slippage can
/// only improve it). `participation_used` tracks volume already taken from
/// each symbol's bar this tick, so several orders on one symbol share the
/// volume-participation cap instead of each taking the full fraction.
///
/// Returns the signed quantity actually filled (0.0 if none), which is less
/// than requested when the volume-participation cap bites — the caller uses it
/// to shrink a partially filled resting order.
#[allow(clippy::too_many_arguments)]
fn execute_order(
    ctx: &mut Context,
    lifetimes: &mut HashMap<String, OpenLifetime>,
    trades: &mut Vec<Trade>,
    total_commission: &mut f64,
    order: &Order,
    mark_prices: &HashMap<String, f64>,
    exec_price: f64,
    fill_bar: &Bar,
    tick_time: DateTime<Utc>,
    limit_bound: Option<f64>,
    participation_used: &mut HashMap<String, f64>,
) -> f64 {
    let current_qty = ctx.portfolio.positions.get(&order.symbol).map(|p| p.quantity).unwrap_or(0.0);

    let qty = match order.kind {
        OrderKind::Market(q) => q,
        OrderKind::SetHoldings(pct) => {
            let total = ctx.portfolio.total_value(mark_prices);
            let desired_qty = total * pct / exec_price;
            // Round the *target* (not the delta) to the lot so the held
            // position stays on-lot instead of drifting.
            let lot = ctx.lot_size;
            let target = (desired_qty / lot).round() * lot;
            target - current_qty
        }
        OrderKind::Liquidate => -current_qty,
    };

    // Volume-participation cap: fills on one symbol's bar can't take more than
    // the configured fraction of its volume *in aggregate* this tick, and the
    // remaining allowance is rounded down to the lot so a capped fill stays
    // on-lot. Unlimited when the fraction is 0.
    let qty = if ctx.max_volume_participation > 0.0 {
        let used = participation_used.get(&order.symbol).copied().unwrap_or(0.0);
        let cap = ctx.max_volume_participation * fill_bar.volume as f64 - used;
        let cap = ((cap / ctx.lot_size).trunc() * ctx.lot_size).max(0.0);
        qty.signum() * qty.abs().min(cap)
    } else {
        qty
    };

    // Buying-power cap from the margin model: shrink (or reject) the fill.
    // The allowed quantity is clamped to the order's own direction and size —
    // a model can only trim, never grow or reverse — and a trimmed fill is
    // rounded down to the lot.
    let qty = if ctx.margin.unlimited() {
        qty
    } else {
        let equity = ctx.portfolio.total_value(mark_prices);
        let other_exposure: f64 = ctx
            .portfolio
            .positions
            .values()
            .filter(|p| p.symbol != order.symbol)
            .map(|p| p.quantity.abs() * mark_prices.get(&p.symbol).copied().unwrap_or(p.avg_price))
            .sum();
        let allowed = ctx.margin.allowed_quantity(&MarginContext {
            symbol: &order.symbol,
            quantity: qty,
            price: exec_price,
            current_qty,
            cash: ctx.portfolio.cash,
            equity,
            other_exposure,
        });
        let allowed = if qty > 0.0 { allowed.clamp(0.0, qty) } else { allowed.clamp(qty, 0.0) };
        if (allowed - qty).abs() < EPSILON {
            qty
        } else {
            qty.signum() * (allowed.abs() / ctx.lot_size).trunc() * ctx.lot_size
        }
    };

    if qty.abs() < EPSILON {
        return 0.0;
    }
    if ctx.max_volume_participation > 0.0 {
        *participation_used.entry(order.symbol.clone()).or_insert(0.0) += qty.abs();
    }

    // Actual execution price after the user's slippage model. Sizing (above)
    // uses the reference price; slippage only affects the fill — except that a
    // limit order never fills through its limit.
    let fill_ctx =
        FillContext { symbol: &order.symbol, quantity: qty, price: exec_price, bar: fill_bar };
    let fill_price = ctx.slippage.fill_price(&fill_ctx);
    let fill_price = match limit_bound {
        Some(bound) if qty > 0.0 => fill_price.min(bound),
        Some(bound) => fill_price.max(bound),
        None => fill_price,
    };
    let commission = ctx.commission.commission(&fill_ctx);
    *total_commission += commission;

    let avg_price = ctx.portfolio.positions.get(&order.symbol).map(|p| p.avg_price).unwrap_or(0.0);
    let new_qty = current_qty + qty;

    if current_qty == 0.0 {
        // Opened from flat: a new position lifetime begins.
        let mut lt = OpenLifetime::new(tick_time, qty.signum());
        lt.entry_qty = qty.abs();
        lt.entry_value = qty.abs() * fill_price;
        lt.realized_pnl = -commission;
        lifetimes.insert(order.symbol.clone(), lt);
    } else if qty * current_qty > 0.0 {
        // Added in the same direction: grow the open lifetime.
        let lt = lifetimes
            .entry(order.symbol.clone())
            .or_insert_with(|| OpenLifetime::new(tick_time, current_qty.signum()));
        lt.entry_qty += qty.abs();
        lt.entry_value += qty.abs() * fill_price;
        lt.realized_pnl -= commission;
    } else {
        // Reduced, closed, or flipped: realize PnL into the open lifetime;
        // only emit a Trade when the position is flat.
        let closed_now = qty.abs().min(current_qty.abs());
        let realized = (fill_price - avg_price) * closed_now * current_qty.signum();
        let lt = lifetimes
            .entry(order.symbol.clone())
            .or_insert_with(|| OpenLifetime::new(tick_time, current_qty.signum()));
        lt.closed_qty += closed_now;
        lt.close_value += closed_now * fill_price;
        lt.realized_pnl += realized - commission;

        let is_full_close = closed_now >= current_qty.abs() - EPSILON;
        if is_full_close {
            let lt = lifetimes.remove(&order.symbol).unwrap();
            trades.push(lt.into_trade(&order.symbol, tick_time, "signal"));
            if new_qty.abs() > EPSILON {
                // A flip leaves a residual position in the new direction; it
                // starts a fresh lifetime.
                let mut fresh = OpenLifetime::new(tick_time, new_qty.signum());
                fresh.entry_qty = new_qty.abs();
                fresh.entry_value = new_qty.abs() * fill_price;
                lifetimes.insert(order.symbol.clone(), fresh);
            }
        }
    }

    ctx.portfolio.apply_fill(&order.symbol, qty, fill_price);
    ctx.portfolio.cash -= commission;
    qty
}

/// Given a resting order and the bar it is being tested against, return the
/// pre-slippage fill price if the bar's range triggers it, else `None`. A limit
/// fills at its price or the (better) bar open; a triggered stop fills at its
/// price or the (worse) bar open, modeling a gap through the stop.
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

/// Run a backtest and return its full results without printing anything.
pub fn run_backtest<A: Algorithm>(
    mut algo: A,
    data_path: &str,
) -> Result<BacktestResult, BacktestError> {
    let mut ctx = Context::default();
    algo.initialize(&mut ctx);

    let initial_cash = ctx.portfolio.cash;
    let ticker_map = load_ticker_map(data_path)?;
    let mut subscribed = ctx.subscribed_symbols.clone();

    // Ticker renames, queued by effective date. Subscribe every rename target
    // up front so its bars stream from the start (the position is only
    // transferred on the effective date); this also covers a successor that
    // begins trading in the same month-file as the rename.
    let mut pending_renames: BTreeMap<NaiveDate, Vec<(String, String)>> = BTreeMap::new();
    for (date, pairs) in load_renames(data_path, &subscribed)? {
        for (old, new) in pairs {
            subscribed.insert(new.clone());
            pending_renames.entry(date).or_default().push((old, new));
        }
    }

    let mut trades: Vec<Trade> = Vec::new();
    let mut lifetimes: HashMap<String, OpenLifetime> = HashMap::new();
    let mut equity_curve: Vec<EquityPoint> = Vec::new();
    let mut intraday_equity: Vec<EquityPoint> = Vec::new();
    let mut last_date: Option<NaiveDate> = None;
    let mut last_known_prices: HashMap<String, f64> = HashMap::new();
    let mut total_commission = 0.0;

    // Orders awaiting the next bar under FillTiming::NextBarOpen, one queue per
    // symbol (empty and unused under the default CurrentBarClose).
    let mut deferred: HashMap<String, Vec<Order>> = HashMap::new();

    // Resting limit/stop orders, one queue per symbol, filled intrabar off the
    // bar's range until triggered or the backtest ends.
    let mut resting_book: HashMap<String, Vec<RestingOrder>> = HashMap::new();

    // Bar volume already consumed per symbol this tick, so all fills against
    // one bar share the volume-participation cap. Cleared every tick.
    let mut participation_used: HashMap<String, f64> = HashMap::new();

    // Splits for subscribed symbols, queued by execution date.
    let mut pending_splits: BTreeMap<NaiveDate, Vec<(String, f64)>> = BTreeMap::new();
    for (symbol, by_date) in load_splits(data_path, &subscribed)? {
        for (date, ratio) in by_date {
            pending_splits.entry(date).or_default().push((symbol.clone(), ratio));
        }
    }

    // Cash dividends for subscribed symbols, queued by ex-dividend date.
    let mut pending_dividends: BTreeMap<NaiveDate, Vec<(String, f64)>> = BTreeMap::new();
    for (symbol, by_date) in load_dividends(data_path, &subscribed)? {
        for (date, amount) in by_date {
            pending_dividends.entry(date).or_default().push((symbol.clone(), amount));
        }
    }

    // Delist detection: which trading day (by index) each symbol last had a
    // bar on.
    let mut global_last_date: Option<NaiveDate> = None;
    let mut day_index: u64 = 0;
    let mut last_seen_day: HashMap<String, u64> = HashMap::new();

    // Stream the dataset one file at a time: files are month-partitioned and
    // chronologically sorted, so only a single month of subscribed bars is
    // ever resident, instead of the whole dataset.
    let files = sorted_parquet_files(data_path);
    if files.is_empty() {
        return Err(BacktestError::NoData { path: data_path.into() });
    }
    let mut mask = None;

    'files: for file_path in &files {
        // Skip whole months outside the configured date range.
        if let Some((y, m)) = file_year_month(file_path) {
            if let Some(start) = ctx.start_date {
                if (y, m) < (start.year() as u32, start.month()) {
                    continue;
                }
            }
            if let Some(end) = ctx.end_date {
                if (y, m) > (end.year() as u32, end.month()) {
                    break;
                }
            }
        }

        // Buffer one month's *subscribed* bars into a timestamp-ordered map.
        // This is intentionally order-agnostic: the map re-sorts whatever order
        // the file yields rows in (Polygon flat files are grouped by ticker,
        // not globally by time), so it is correct without any sort guarantee.
        // A k-way merge that streamed instead of buffering would only pay off
        // for a very wide subscribed universe *and* would need a guaranteed
        // intra-file time sort; measured peak RSS is ~42 MB over a full year of
        // one symbol, so the buffer stays. Memory scales with the subscribed
        // set, not the whole dataset, because the filter runs before the push.
        let mut tick_map: BTreeMap<i64, Vec<(String, Bar)>> = BTreeMap::new();
        read_bars_from_file(file_path, &ticker_map, &mut mask, |symbol, bar| {
            // A timestamp outside the nanosecond-representable range (pre-1677
            // or post-2262) is corrupt data; drop the bar rather than pinning
            // it to the epoch.
            if subscribed.contains(&symbol) {
                if let Some(ts) = bar.time.timestamp_nanos_opt() {
                    tick_map.entry(ts).or_default().push((symbol, bar));
                }
            }
        })?;

        for (ts_ns, bars) in tick_map {
            let secs = ts_ns / 1_000_000_000;
            let nanos = (ts_ns % 1_000_000_000) as u32;
            let Some(tick_time) = Utc.timestamp_opt(secs, nanos).single() else { continue };
            participation_used.clear();
            // Trading date in US Eastern, so after-market bars (which cross
            // midnight UTC) stay on the day they belong to.
            let tick_date = tick_time.with_timezone(&Eastern).date_naive();

            // Date range filter
            if let Some(start) = ctx.start_date {
                if tick_date < start {
                    continue;
                }
            }
            if let Some(end) = ctx.end_date {
                if tick_date > end {
                    break 'files;
                }
            }

            // Day boundary: close out the previous day *before* this bar
            // touches any state, so on_end_of_day and the equity mark see the
            // world exactly as it was at yesterday's last bar.
            // (last_date is never set during warm-up, so purely-warm-up days
            // never fire this.)
            if ctx.warm_up_remaining == 0 {
                if let Some(prev) = last_date {
                    if tick_date != prev {
                        equity_curve.push(EquityPoint {
                            time: prev.to_string(),
                            equity: ctx.portfolio.total_value(&last_known_prices),
                        });
                        algo.on_end_of_day(&mut ctx);
                    }
                }
            }

            // Corporate actions & delist scan, once per calendar date, after
            // the previous day's equity mark (which must see pre-split state)
            // and before the new day's bars touch anything. Not warm-up
            // gated: history built during warm-up needs rescaling too, and
            // no positions can exist in warm-up so the delist scan is
            // vacuous there.
            if global_last_date != Some(tick_date) {
                let is_first_tick = global_last_date.is_none();
                global_last_date = Some(tick_date);
                if !is_first_tick {
                    day_index += 1;
                }

                while let Some((&date, _)) = pending_splits.first_key_value() {
                    if date > tick_date {
                        break;
                    }
                    let (_, actions) = pending_splits.pop_first().unwrap();
                    // Splits dated before the first bar we ever process have
                    // nothing to adjust — drain them silently.
                    if is_first_tick {
                        continue;
                    }
                    for (symbol, ratio) in actions {
                        apply_split(
                            &mut ctx,
                            &mut lifetimes,
                            &mut trades,
                            &mut last_known_prices,
                            &symbol,
                            ratio,
                            tick_time,
                        );
                        // Orders decided pre-split are expressed in pre-split
                        // terms; rescale them (qty × ratio, trigger ÷ ratio)
                        // so they don't spuriously fire — or silently die —
                        // against the post-split tape.
                        if let Some(orders) = resting_book.get_mut(&symbol) {
                            for ro in orders {
                                ro.qty *= ratio;
                                ro.kind = match ro.kind {
                                    RestingKind::Limit(p) => RestingKind::Limit(p / ratio),
                                    RestingKind::Stop(p) => RestingKind::Stop(p / ratio),
                                };
                            }
                        }
                        if let Some(orders) = deferred.get_mut(&symbol) {
                            for o in orders {
                                if let OrderKind::Market(q) = &mut o.kind {
                                    *q *= ratio;
                                }
                            }
                        }
                        algo.on_split(&mut ctx, &symbol, ratio);
                    }
                }

                // Cash dividends going ex on or before today, credited to any
                // position held on the ex-date. Same day-boundary timing as
                // splits: after the previous day's equity mark, before today's
                // bars move prices.
                while let Some((&date, _)) = pending_dividends.first_key_value() {
                    if date > tick_date {
                        break;
                    }
                    let (_, actions) = pending_dividends.pop_first().unwrap();
                    if is_first_tick {
                        continue;
                    }
                    for (symbol, amount) in actions {
                        if apply_dividend(&mut ctx, &mut lifetimes, &symbol, amount) {
                            algo.on_dividend(&mut ctx, &symbol, amount);
                        }
                    }
                }

                // Ticker renames effective on or before today: transfer the
                // position and ledger from the old symbol to the new one before
                // the delist scan would otherwise force-liquidate the old.
                while let Some((&date, _)) = pending_renames.first_key_value() {
                    if date > tick_date {
                        break;
                    }
                    let (_, pairs) = pending_renames.pop_first().unwrap();
                    if is_first_tick {
                        continue;
                    }
                    for (old, new) in pairs {
                        apply_rename(
                            &mut ctx,
                            &mut lifetimes,
                            &mut last_known_prices,
                            &mut last_seen_day,
                            &old,
                            &new,
                        );
                        // Carry resting orders over to the new ticker, retagging
                        // their symbol so they still fill.
                        if let Some(mut orders) = resting_book.remove(&old) {
                            for o in &mut orders {
                                o.symbol = new.clone();
                            }
                            resting_book.entry(new.clone()).or_default().extend(orders);
                        }
                        algo.on_rename(&mut ctx, &old, &new);
                    }
                }

                // Accrue a day of financing on positions carried into today
                // (borrow fees on shorts, margin interest on negative cash).
                if !is_first_tick {
                    apply_financing(&mut ctx, &mut lifetimes, &last_known_prices);
                }

                let delist_after = ctx.delist_after_days as u64;
                if delist_after > 0 && !ctx.portfolio.positions.is_empty() {
                    let stale: Vec<String> = ctx
                        .portfolio
                        .positions
                        .keys()
                        .filter(|s| {
                            let seen = last_seen_day.get(*s).copied().unwrap_or(day_index);
                            day_index.saturating_sub(seen) >= delist_after
                        })
                        .cloned()
                        .collect();
                    for symbol in stale {
                        force_close_delisted(
                            &mut ctx,
                            &mut lifetimes,
                            &mut trades,
                            &last_known_prices,
                            &symbol,
                            tick_time,
                        );
                        // Pending next-bar and resting orders can never fill
                        // against a symbol that stopped trading.
                        deferred.remove(&symbol);
                        resting_book.remove(&symbol);
                        algo.on_delisted(&mut ctx, &symbol);
                    }
                }
            }
            for (symbol, _) in &bars {
                last_seen_day.insert(symbol.clone(), day_index);
            }

            let current_prices: HashMap<String, f64> =
                bars.iter().map(|(s, b)| (s.clone(), b.close)).collect();

            // NextBarOpen: realize orders decided on a symbol's previous bar at
            // this bar's open, before last_known_prices advances and before the
            // strategy sees the current bar. Sizing marks the portfolio with
            // last_known_prices, which still holds the prior closes the order
            // was decided on — no look-ahead into the bar being filled.
            if !deferred.is_empty() {
                for (symbol, bar) in &bars {
                    let Some(orders) = deferred.remove(symbol) else { continue };
                    for order in orders {
                        execute_order(
                            &mut ctx,
                            &mut lifetimes,
                            &mut trades,
                            &mut total_commission,
                            &order,
                            &last_known_prices,
                            bar.open,
                            bar,
                            tick_time,
                            None,
                            &mut participation_used,
                        );
                    }
                }
            }

            // Resting limit/stop orders: fill (possibly partially) any whose
            // trigger this bar's range trades through; keep the remainder
            // resting. Decided on prior bars, so filling here is not look-ahead.
            if !resting_book.is_empty() {
                for (symbol, bar) in &bars {
                    let Some(orders) = resting_book.remove(symbol) else { continue };
                    let mut still_resting = Vec::new();
                    for mut ro in orders {
                        match resting_fill_price(&ro.kind, ro.qty, bar) {
                            Some(price) => {
                                let mkt = Order {
                                    symbol: symbol.clone(),
                                    kind: OrderKind::Market(ro.qty),
                                };
                                let bound = match ro.kind {
                                    RestingKind::Limit(p) => Some(p),
                                    RestingKind::Stop(_) => None,
                                };
                                let filled = execute_order(
                                    &mut ctx,
                                    &mut lifetimes,
                                    &mut trades,
                                    &mut total_commission,
                                    &mkt,
                                    &last_known_prices,
                                    price,
                                    bar,
                                    tick_time,
                                    bound,
                                    &mut participation_used,
                                );
                                ro.qty -= filled;
                                if ro.qty.abs() > EPSILON {
                                    still_resting.push(ro);
                                }
                            }
                            None => still_resting.push(ro),
                        }
                    }
                    if !still_resting.is_empty() {
                        resting_book.insert(symbol.clone(), still_resting);
                    }
                }
            }

            for (s, &p) in &current_prices {
                last_known_prices.insert(s.clone(), p);
            }

            // Update history
            for (symbol, bar) in &bars {
                let hist = ctx.history_store.entry(symbol.clone()).or_default();
                hist.push_front(bar.clone());
                if hist.len() > ctx.max_history {
                    hist.pop_back();
                }
            }

            // Feed consolidators before the warm-up gate so their internal
            // state (e.g. a 60-min bar) builds up over the warm-up period.
            for (symbol, bar) in &bars {
                for c in ctx.consolidators.iter_mut() {
                    if c.symbol == *symbol {
                        c.feed(bar);
                    }
                }
            }

            if ctx.warm_up_remaining > 0 {
                ctx.warm_up_remaining -= 1;
                continue;
            }

            // The first processed bar anchors the equity curve at the
            // starting capital, so day-one PnL is visible in returns and
            // drawdown.
            if last_date.is_none() {
                if let Some(day_before) = tick_date.pred_opt() {
                    equity_curve
                        .push(EquityPoint { time: day_before.to_string(), equity: initial_cash });
                }
            }
            last_date = Some(tick_date);

            // Fire scheduled time callbacks (times are US Eastern)
            ctx.fire_time_callbacks(&tick_time, tick_date);

            // Build slice
            let slice_bars: HashMap<String, Bar> = bars.into_iter().collect();
            let slice = Slice { time: tick_time, bars: slice_bars };

            algo.on_data(&mut ctx, &slice);

            // Route orders placed this bar according to the fill-timing model.
            let orders = std::mem::take(&mut ctx.pending_orders);
            match ctx.fill_timing {
                FillTiming::CurrentBarClose => {
                    for order in orders {
                        // Fill at this bar's close; a symbol with no bar this
                        // tick has no price to fill against. Sizing marks the
                        // portfolio with last_known_prices — already advanced
                        // to this tick's closes — so held symbols *without* a
                        // bar this tick are valued at their latest market
                        // price, not their cost basis.
                        let Some(bar) = slice.bars.get(&order.symbol) else { continue };
                        let Some(&price) = current_prices.get(&order.symbol) else { continue };
                        execute_order(
                            &mut ctx,
                            &mut lifetimes,
                            &mut trades,
                            &mut total_commission,
                            &order,
                            &last_known_prices,
                            price,
                            bar,
                            tick_time,
                            None,
                            &mut participation_used,
                        );
                    }
                }
                FillTiming::NextBarOpen => {
                    // Hold each order until its symbol's next bar, where it
                    // fills at the open (see the deferred-fill block above).
                    for order in orders {
                        deferred.entry(order.symbol.clone()).or_default().push(order);
                    }
                }
            }

            // Book any resting limit/stop orders placed this bar; they start
            // filling from the symbol's next bar (checked at the top of the loop).
            for ro in std::mem::take(&mut ctx.resting_orders) {
                resting_book.entry(ro.symbol.clone()).or_default().push(ro);
            }

            // Optional per-bar equity mark, after this bar's fills.
            if ctx.track_intraday_equity {
                intraday_equity.push(EquityPoint {
                    time: tick_time.to_rfc3339(),
                    equity: ctx.portfolio.total_value(&last_known_prices),
                });
            }
        }
    }

    // Fire any incomplete consolidation periods buffered at end of data
    for c in ctx.consolidators.iter_mut() {
        c.flush();
    }

    // Close the equity curve with the final day's mark and give the final day
    // its end-of-day callback (mid-backtest it fires on the next day's first
    // bar, which the last day doesn't have). Orders placed here have no bar
    // left to fill against and are dropped.
    if let Some(prev) = last_date {
        equity_curve.push(EquityPoint {
            time: prev.to_string(),
            equity: ctx.portfolio.total_value(&last_known_prices),
        });
        algo.on_end_of_day(&mut ctx);
    }

    let final_equity = ctx.portfolio.total_value(&last_known_prices);

    let mut open_positions: Vec<OpenPositionSummary> = ctx
        .portfolio
        .positions
        .values()
        .map(|p| {
            let last_price = last_known_prices.get(&p.symbol).copied().unwrap_or(p.avg_price);
            OpenPositionSummary {
                symbol: p.symbol.clone(),
                quantity: p.quantity,
                avg_price: p.avg_price,
                last_price,
                market_value: p.quantity * last_price,
                unrealized_pnl: (last_price - p.avg_price) * p.quantity,
                realized_pnl: lifetimes.get(&p.symbol).map(|lt| lt.realized_pnl).unwrap_or(0.0),
            }
        })
        .collect();
    open_positions.sort_by(|a, b| a.symbol.cmp(&b.symbol));

    let stats = compute_stats(&trades, &equity_curve, ctx.risk_free_rate);

    Ok(BacktestResult {
        initial_cash,
        final_equity,
        total_commission,
        stats,
        equity_curve,
        intraday_equity,
        open_positions,
        trades,
    })
}

/// Run a backtest, print a summary, and write the full result JSON
/// (`backtest_result_<timestamp>.json`) for the `ui` dashboard. The file goes
/// to the current directory, or to `$BACKTEST_OUTPUT_DIR` when that is set
/// (created if missing).
pub fn run<A: Algorithm>(algo: A, data_path: &str) -> Result<BacktestResult, BacktestError> {
    let result = run_backtest(algo, data_path)?;
    let stats = &result.stats;

    let ts = chrono::Local::now().format("%Y-%m-%dT%H-%M-%S");
    let file_name = format!("backtest_result_{ts}.json");
    let out_path = match std::env::var("BACKTEST_OUTPUT_DIR") {
        Ok(dir) if !dir.is_empty() => {
            std::fs::create_dir_all(&dir)
                .map_err(|source| BacktestError::Io { path: dir.clone().into(), source })?;
            std::path::Path::new(&dir).join(file_name)
        }
        _ => std::path::PathBuf::from(file_name),
    };
    let file = std::fs::File::create(&out_path)
        .map_err(|source| BacktestError::Io { path: out_path.clone(), source })?;
    serde_json::to_writer_pretty(file, &result)
        .map_err(|e| BacktestError::Json { path: out_path.clone(), message: e.to_string() })?;

    println!("=== Backtest Complete ===");
    println!("Result written to: {}  (view it with `cargo run -p ui`)", out_path.display());
    println!(
        "Trades: {}  |  Win Rate: {:.0}%  |  Total PnL: ${:.0}  |  Final Equity: ${:.0}",
        stats.trade_count,
        stats.win_rate * 100.0,
        stats.total_pnl,
        result.final_equity,
    );
    println!(
        "Profit Factor: {:.2}  |  Max Drawdown: {:.1}%  |  Sharpe: {:.2}  |  Commission: ${:.2}",
        stats.profit_factor,
        stats.max_drawdown * 100.0,
        stats.sharpe_ratio,
        result.total_commission,
    );
    if !result.open_positions.is_empty() {
        for p in &result.open_positions {
            println!(
                "Open: {} {:.2} @ {:.2} (last {:.2}, unrealized ${:.0})",
                p.symbol, p.quantity, p.avg_price, p.last_price, p.unrealized_pnl
            );
        }
    }

    Ok(result)
}

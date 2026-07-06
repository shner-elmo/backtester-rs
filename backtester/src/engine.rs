use std::collections::{BTreeMap, HashMap};

use chrono::{DateTime, Datelike, NaiveDate, TimeZone, Utc};
use chrono_tz::US::Eastern;
use serde::{Deserialize, Serialize};

use crate::{
    algorithm::Algorithm,
    bar::Bar,
    context::{Context, FillTiming, Order, OrderKind},
    data::{
        file_year_month, load_splits, load_ticker_map, read_bars_from_file, sorted_parquet_files,
    },
    error::BacktestError,
    slice::Slice,
    slippage::FillContext,
    stats::{compute_stats, BacktestStats, EquityPoint, OpenPositionSummary, Trade},
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
    if residual.abs() < 1e-9 {
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
    if rounded.abs() < 1e-9 {
        // The whole position was cashed out (deep reverse split of a tiny
        // holding): the lifetime is over.
        ctx.portfolio.positions.remove(symbol);
        if let Some(lt) = lifetimes.remove(symbol) {
            trades.push(lt.into_trade(symbol, tick_time, "split"));
        }
    }
}

/// Force-close a position in a symbol that stopped trading: fill the whole
/// quantity at the last known price with no commission, and emit the closing
/// trade with `exit_reason: "delisted"`.
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
    let price = last_known_prices.get(symbol).copied().unwrap_or(avg);

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
) {
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

    if qty.abs() < 1e-9 {
        return;
    }

    // Actual execution price after the user's slippage model. Sizing (above)
    // uses the reference price; slippage only affects the fill.
    let fill_ctx =
        FillContext { symbol: &order.symbol, quantity: qty, price: exec_price, bar: fill_bar };
    let fill_price = ctx.slippage.fill_price(&fill_ctx);
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

        let is_full_close = closed_now >= current_qty.abs() - 1e-9;
        if is_full_close {
            let lt = lifetimes.remove(&order.symbol).unwrap();
            trades.push(lt.into_trade(&order.symbol, tick_time, "signal"));
            if new_qty.abs() > 1e-9 {
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
    let subscribed = ctx.subscribed_symbols.clone();

    let mut trades: Vec<Trade> = Vec::new();
    let mut lifetimes: HashMap<String, OpenLifetime> = HashMap::new();
    let mut equity_curve: Vec<EquityPoint> = Vec::new();
    let mut last_date: Option<NaiveDate> = None;
    let mut last_known_prices: HashMap<String, f64> = HashMap::new();
    let mut total_commission = 0.0;

    // Orders awaiting the next bar under FillTiming::NextBarOpen, one queue per
    // symbol (empty and unused under the default CurrentBarClose).
    let mut deferred: HashMap<String, Vec<Order>> = HashMap::new();

    // Splits for subscribed symbols, queued by execution date.
    let mut pending_splits: BTreeMap<NaiveDate, Vec<(String, f64)>> = BTreeMap::new();
    for (symbol, by_date) in load_splits(data_path, &subscribed)? {
        for (date, ratio) in by_date {
            pending_splits.entry(date).or_default().push((symbol.clone(), ratio));
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

        let mut tick_map: BTreeMap<i64, Vec<(String, Bar)>> = BTreeMap::new();
        read_bars_from_file(file_path, &ticker_map, &mut mask, |symbol, bar| {
            if subscribed.contains(&symbol) {
                let ts = bar.time.timestamp_nanos_opt().unwrap_or(0);
                tick_map.entry(ts).or_default().push((symbol, bar));
            }
        })?;

        for (ts_ns, bars) in tick_map {
            let secs = ts_ns / 1_000_000_000;
            let nanos = (ts_ns % 1_000_000_000) as u32;
            let tick_time = Utc.timestamp_opt(secs, nanos).single().unwrap();
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
                        algo.on_split(&mut ctx, &symbol, ratio);
                    }
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
                        // A pending next-bar order can never fill against a
                        // symbol that stopped trading.
                        deferred.remove(&symbol);
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
                        );
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
                        // tick has no price to fill against.
                        let Some(bar) = slice.bars.get(&order.symbol) else { continue };
                        let Some(&price) = current_prices.get(&order.symbol) else { continue };
                        execute_order(
                            &mut ctx,
                            &mut lifetimes,
                            &mut trades,
                            &mut total_commission,
                            &order,
                            &current_prices,
                            price,
                            bar,
                            tick_time,
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
        }
    }

    // Fire any incomplete consolidation periods buffered at end of data
    for c in ctx.consolidators.iter_mut() {
        c.flush();
    }

    // Close the equity curve with the final day's mark.
    if let Some(prev) = last_date {
        equity_curve.push(EquityPoint {
            time: prev.to_string(),
            equity: ctx.portfolio.total_value(&last_known_prices),
        });
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

    let stats = compute_stats(&trades, &equity_curve);

    Ok(BacktestResult {
        initial_cash,
        final_equity,
        total_commission,
        stats,
        equity_curve,
        open_positions,
        trades,
    })
}

/// Run a backtest, print a summary, and write the full result JSON
/// (`backtest_result_<timestamp>.json`) for the `ui` dashboard.
pub fn run<A: Algorithm>(algo: A, data_path: &str) -> Result<BacktestResult, BacktestError> {
    let result = run_backtest(algo, data_path)?;
    let stats = &result.stats;

    let ts = chrono::Local::now().format("%Y-%m-%dT%H-%M-%S");
    let out_path = format!("backtest_result_{ts}.json");
    let file = std::fs::File::create(&out_path)
        .map_err(|source| BacktestError::Io { path: out_path.clone().into(), source })?;
    serde_json::to_writer_pretty(file, &result).map_err(|e| BacktestError::Json {
        path: out_path.clone().into(),
        message: e.to_string(),
    })?;

    println!("=== Backtest Complete ===");
    println!("Result written to: {out_path}  (view it with `cargo run -p ui`)");
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

use std::collections::{BTreeMap, HashMap};

use chrono::{DateTime, Datelike, NaiveDate, TimeZone, Utc};
use chrono_tz::US::Eastern;
use serde::{Deserialize, Serialize};

use crate::{
    algorithm::Algorithm,
    bar::Bar,
    context::{Context, OrderKind},
    data::{file_year_month, load_ticker_map, read_bars_from_file, sorted_parquet_files},
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

    fn into_trade(self, symbol: &str, exit_time: DateTime<Utc>) -> Trade {
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
        }
    }
}

/// Run a backtest and return its full results without printing anything.
pub fn run_backtest<A: Algorithm>(mut algo: A, data_path: &str) -> BacktestResult {
    let mut ctx = Context::default();
    algo.initialize(&mut ctx);

    let initial_cash = ctx.portfolio.cash;
    let ticker_map = load_ticker_map(data_path);
    let subscribed = ctx.subscribed_symbols.clone();

    let mut trades: Vec<Trade> = Vec::new();
    let mut lifetimes: HashMap<String, OpenLifetime> = HashMap::new();
    let mut equity_curve: Vec<EquityPoint> = Vec::new();
    let mut last_date: Option<NaiveDate> = None;
    let mut last_known_prices: HashMap<String, f64> = HashMap::new();
    let mut total_commission = 0.0;

    // Stream the dataset one file at a time: files are month-partitioned and
    // chronologically sorted, so only a single month of subscribed bars is
    // ever resident, instead of the whole dataset.
    let files = sorted_parquet_files(data_path);
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
        });

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

            let current_prices: HashMap<String, f64> =
                bars.iter().map(|(s, b)| (s.clone(), b.close)).collect();
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

            // Process orders
            let orders = std::mem::take(&mut ctx.pending_orders);
            for order in orders {
                let price = match current_prices.get(&order.symbol) {
                    Some(&p) => p,
                    None => continue,
                };

                let current_qty =
                    ctx.portfolio.positions.get(&order.symbol).map(|p| p.quantity).unwrap_or(0.0);

                let qty = match order.kind {
                    OrderKind::Market(q) => q,
                    OrderKind::SetHoldings(pct) => {
                        let total = ctx.portfolio.total_value(&current_prices);
                        let desired_qty = total * pct / price;
                        // Round the *target* (not the delta) to the lot so the
                        // held position stays on-lot instead of drifting.
                        let lot = ctx.lot_size;
                        let target = (desired_qty / lot).round() * lot;
                        target - current_qty
                    }
                    OrderKind::Liquidate => -current_qty,
                };

                if qty.abs() < 1e-9 {
                    continue;
                }

                // Actual execution price after the user's slippage model.
                // Sizing (above) uses the reference price; slippage only
                // affects the fill.
                let fill_ctx = FillContext {
                    symbol: &order.symbol,
                    quantity: qty,
                    price,
                    bar: &slice.bars[&order.symbol],
                };
                let fill_price = ctx.slippage.fill_price(&fill_ctx);
                let commission = ctx.commission.commission(&fill_ctx);
                total_commission += commission;

                let avg_price =
                    ctx.portfolio.positions.get(&order.symbol).map(|p| p.avg_price).unwrap_or(0.0);
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
                    // Reduced, closed, or flipped: realize PnL into the open
                    // lifetime; only emit a Trade when the position is flat.
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
                        trades.push(lt.into_trade(&order.symbol, tick_time));
                        if new_qty.abs() > 1e-9 {
                            // A flip leaves a residual position in the new
                            // direction; it starts a fresh lifetime.
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

    BacktestResult {
        initial_cash,
        final_equity,
        total_commission,
        stats,
        equity_curve,
        open_positions,
        trades,
    }
}

/// Run a backtest, print a summary, and write the full result JSON
/// (`backtest_result_<timestamp>.json`) for the `ui` dashboard.
pub fn run<A: Algorithm>(algo: A, data_path: &str) -> BacktestResult {
    let result = run_backtest(algo, data_path);
    let stats = &result.stats;

    let ts = chrono::Local::now().format("%Y-%m-%dT%H-%M-%S");
    let out_path = format!("backtest_result_{ts}.json");
    let file = std::fs::File::create(&out_path).unwrap();
    serde_json::to_writer_pretty(file, &result).unwrap();

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

    result
}

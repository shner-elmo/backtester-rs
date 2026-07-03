use std::collections::{BTreeMap, HashMap};

use chrono::{DateTime, NaiveDate, TimeZone, Utc};

use crate::{
    algorithm::Algorithm,
    bar::Bar,
    context::{Context, OrderKind},
    data::{iter_bars, load_ticker_map},
    slice::Slice,
    slippage::FillContext,
    stats::{compute_stats, Trade},
};

struct OpenEntry {
    time: DateTime<Utc>,
}

pub fn run<A: Algorithm>(mut algo: A, data_path: &str) {
    let mut ctx = Context::default();
    algo.initialize(&mut ctx);

    let initial_cash = ctx.portfolio.cash;
    let ticker_map = load_ticker_map(data_path);
    let subscribed = ctx.subscribed_symbols.clone();

    // Collect bars for subscribed symbols into a sorted BTreeMap keyed by timestamp_ns
    let mut tick_map: BTreeMap<i64, Vec<(String, Bar)>> = BTreeMap::new();

    iter_bars(data_path, &ticker_map, |symbol, bar| {
        if subscribed.contains(&symbol) {
            let ts = bar.time.timestamp_nanos_opt().unwrap_or(0);
            tick_map.entry(ts).or_default().push((symbol, bar));
        }
    });

    let mut trades: Vec<Trade> = Vec::new();
    let mut open_entries: HashMap<String, OpenEntry> = HashMap::new();
    let mut last_date: Option<NaiveDate> = None;
    let mut last_known_prices: HashMap<String, f64> = HashMap::new();

    for (ts_ns, bars) in tick_map {
        let secs = ts_ns / 1_000_000_000;
        let nanos = (ts_ns % 1_000_000_000) as u32;
        let tick_time = Utc.timestamp_opt(secs, nanos).single().unwrap();
        let tick_date = tick_time.date_naive();

        // Date range filter
        if let Some(start) = ctx.start_date {
            if tick_date < start {
                continue;
            }
        }
        if let Some(end) = ctx.end_date {
            if tick_date > end {
                break;
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

        // Feed consolidators before the warm-up gate so their internal state
        // (e.g. a 60-min bar) builds up over the warm-up period.
        for (symbol, bar) in &bars {
            for c in ctx.consolidators.iter_mut() {
                if c.symbol == *symbol {
                    c.feed(bar);
                }
            }
        }

        // Warm-up: last_date is intentionally not set here so that
        // on_end_of_day never fires for days that were purely in warm-up.
        if ctx.warm_up_remaining > 0 {
            ctx.warm_up_remaining -= 1;
            continue;
        }

        // Day boundary
        if let Some(prev) = last_date {
            if tick_date != prev {
                algo.on_end_of_day(&mut ctx);
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

            let qty = match order.kind {
                OrderKind::Market(q) => q,
                OrderKind::SetHoldings(pct) => {
                    let total = ctx.portfolio.total_value(&current_prices);
                    let desired_qty = total * pct / price;
                    let current_qty = ctx
                        .portfolio
                        .positions
                        .get(&order.symbol)
                        .map(|p| p.quantity)
                        .unwrap_or(0.0);
                    desired_qty - current_qty
                }
                OrderKind::Liquidate => {
                    -ctx.portfolio.positions.get(&order.symbol).map(|p| p.quantity).unwrap_or(0.0)
                }
            };

            if qty.abs() < 1e-9 {
                continue;
            }

            // Actual execution price after the user's slippage model. Sizing
            // (above) uses the reference price; slippage only affects the fill.
            let fill_price = match slice.bars.get(&order.symbol) {
                Some(bar) => ctx.slippage.fill_price(&FillContext {
                    symbol: &order.symbol,
                    quantity: qty,
                    price,
                    bar,
                }),
                None => price,
            };

            // Record trade if closing/reducing a position
            let current_qty =
                ctx.portfolio.positions.get(&order.symbol).map(|p| p.quantity).unwrap_or(0.0);
            let avg_price =
                ctx.portfolio.positions.get(&order.symbol).map(|p| p.avg_price).unwrap_or(0.0);

            let new_qty = current_qty + qty;
            if current_qty != 0.0 && qty * current_qty < 0.0 {
                // Reducing, closing, or flipping the position: record a realized
                // trade for the portion of the old position that was closed.
                let closed_qty = qty.abs().min(current_qty.abs());
                let is_full_close = closed_qty >= current_qty.abs() - 1e-9;
                let entry_time =
                    open_entries.get(&order.symbol).map(|e| e.time).unwrap_or(tick_time);
                let pnl = (fill_price - avg_price) * closed_qty * current_qty.signum();
                trades.push(Trade {
                    symbol: order.symbol.clone(),
                    entry_price: avg_price,
                    exit_price: fill_price,
                    entry_time: entry_time.to_rfc3339(),
                    exit_time: tick_time.to_rfc3339(),
                    quantity: closed_qty,
                    pnl,
                });
                if is_full_close {
                    // A flip leaves a residual position in the new direction;
                    // start a fresh entry for it, otherwise it's just closed.
                    if new_qty.abs() > 1e-9 {
                        open_entries.insert(order.symbol.clone(), OpenEntry { time: tick_time });
                    } else {
                        open_entries.remove(&order.symbol);
                    }
                }
            } else if current_qty == 0.0 {
                open_entries.insert(order.symbol.clone(), OpenEntry { time: tick_time });
            }

            ctx.portfolio.apply_fill(&order.symbol, qty, fill_price);
        }
    }

    // Fire any incomplete consolidation periods buffered at end of data
    for c in ctx.consolidators.iter_mut() {
        c.flush();
    }

    // Final equity (mark open positions at last known market price)
    let final_equity = ctx.portfolio.total_value(&last_known_prices);

    // Adjust the stats equity curve to reflect the actual final equity including open positions
    let stats = compute_stats(&trades, initial_cash);

    let ts = chrono::Local::now().format("%Y-%m-%dT%H-%M-%S");
    let out_path = format!("backtest_trades_{ts}.json");
    let file = std::fs::File::create(&out_path).unwrap();

    serde_json::to_writer_pretty(file, &trades).unwrap();

    println!("=== Backtest Complete ===");
    println!("Trades written to: {out_path}");
    println!(
        "Trades: {}  |  Win Rate: {:.0}%  |  Total PnL: ${:.0}  |  Final Equity: ${:.0}",
        stats.trade_count,
        stats.win_rate * 100.0,
        stats.total_pnl,
        final_equity,
    );
    println!(
        "Profit Factor: {:.2}  |  Max Drawdown: {:.1}%  |  Sharpe: {:.2}",
        stats.profit_factor,
        stats.max_drawdown * 100.0,
        stats.sharpe_ratio,
    );
}

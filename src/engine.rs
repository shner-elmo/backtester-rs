use std::collections::{BTreeMap, HashMap};

use chrono::{DateTime, NaiveDate, TimeZone, Utc};
use serde::Serialize;

use crate::{
    algorithm::Algorithm,
    bar::Bar,
    context::{Context, OrderKind},
    data::{iter_bars, load_ticker_map},
    slice::Slice,
};

#[derive(Serialize)]
struct TradeRecord {
    symbol: String,
    entry_price: f64,
    exit_price: f64,
    entry_time: String,
    exit_time: String,
    quantity: f64,
    pnl: f64,
}

struct OpenEntry {
    price: f64,
    time: DateTime<Utc>,
}

pub fn run<A: Algorithm>(mut algo: A, data_path: &str) {
    let mut ctx = Context::default();
    algo.initialize(&mut ctx);

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

    let mut trades: Vec<TradeRecord> = Vec::new();
    let mut open_entries: HashMap<String, OpenEntry> = HashMap::new();
    let mut last_date: Option<NaiveDate> = None;

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

        // Update history
        for (symbol, bar) in &bars {
            let hist = ctx.history_store.entry(symbol.clone()).or_default();
            hist.push_front(bar.clone());
            if hist.len() > ctx.max_history {
                hist.pop_back();
            }
        }

        // Feed consolidators
        for (symbol, bar) in &bars {
            for c in ctx.consolidators.iter_mut() {
                if c.symbol == *symbol {
                    c.feed(bar);
                }
            }
        }

        // Warm-up
        if ctx.warm_up_remaining > 0 {
            ctx.warm_up_remaining -= 1;
            last_date = Some(tick_date);
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

            // Record trade if closing/reducing a position
            let current_qty =
                ctx.portfolio.positions.get(&order.symbol).map(|p| p.quantity).unwrap_or(0.0);

            if current_qty != 0.0 && qty * current_qty < 0.0 {
                if let Some(entry) = open_entries.remove(&order.symbol) {
                    let closed_qty = qty.abs().min(current_qty.abs());
                    let pnl = (price - entry.price) * closed_qty * current_qty.signum();
                    trades.push(TradeRecord {
                        symbol: order.symbol.clone(),
                        entry_price: entry.price,
                        exit_price: price,
                        entry_time: entry.time.to_rfc3339(),
                        exit_time: tick_time.to_rfc3339(),
                        quantity: closed_qty,
                        pnl,
                    });
                }
            } else if current_qty == 0.0 {
                open_entries.insert(order.symbol.clone(), OpenEntry { price, time: tick_time });
            }

            ctx.portfolio.apply_fill(&order.symbol, qty, price);
        }
    }

    // Final equity (mark open positions at last known price)
    let last_prices: HashMap<String, f64> =
        ctx.portfolio.positions.values().map(|p| (p.symbol.clone(), p.avg_price)).collect();
    let final_equity = ctx.portfolio.total_value(&last_prices);

    let total_pnl: f64 = trades.iter().map(|t| t.pnl).sum();
    let wins = trades.iter().filter(|t| t.pnl > 0.0).count();
    let win_rate = if trades.is_empty() { 0.0 } else { wins as f64 / trades.len() as f64 * 100.0 };

    let file = std::fs::File::create("backtest_trades.json").unwrap();
    serde_json::to_writer_pretty(file, &trades).unwrap();

    println!("=== Backtest Complete ===");
    println!(
        "Trades: {}  |  Win Rate: {:.0}%  |  Total PnL: ${:.0}  |  Final Equity: ${:.0}",
        trades.len(),
        win_rate,
        total_pnl,
        final_equity
    );
}

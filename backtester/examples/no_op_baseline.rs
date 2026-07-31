//! No-op full-universe baseline: subscribe every symbol in the ticker map,
//! keep history at the minimum, place no orders, and report wall-clock time
//! and bar throughput. Measures the pure engine overhead (decode, tick
//! grouping, slice assembly, day boundaries) with no strategy work on top —
//! the floor any real backtest pays.
//!
//! Usage: cargo run --release --example no_op_baseline -- /path/to/data/root [start] [end]
//!
//! `start` / `end` are optional YYYY-MM-DD bounds; omit both to run the whole
//! dataset.

use std::{cell::Cell, rc::Rc, time::Instant};

use backtester::{run_backtest, Algorithm, Context, Slice};
use chrono::{Datelike, NaiveDate};

struct Noop {
    start: Option<NaiveDate>,
    end: Option<NaiveDate>,
    ticks: Rc<Cell<u64>>,
    bars: Rc<Cell<u64>>,
}

impl Algorithm for Noop {
    fn initialize(&mut self, ctx: &mut Context) {
        ctx.set_cash(100_000.0);
        // 1 is the smallest allowed window; keeps the per-symbol history
        // deques from doing any real work.
        ctx.set_max_history(1);
        if let Some(d) = self.start {
            ctx.set_start_date(d.year(), d.month(), d.day());
        }
        if let Some(d) = self.end {
            ctx.set_end_date(d.year(), d.month(), d.day());
        }
        ctx.add_all_equities(); // the whole dataset, the widest the engine gets
    }

    fn on_data(&mut self, _ctx: &mut Context, data: &Slice) {
        self.ticks.set(self.ticks.get() + 1);
        self.bars.set(self.bars.get() + data.bars.len() as u64);
    }
}

fn main() {
    let args: Vec<String> = std::env::args().collect();
    let data_path = args.get(1).map(String::as_str).unwrap_or("data/output/minute");

    let date = |i: usize| {
        args.get(i).map(|s| s.parse::<NaiveDate>().unwrap_or_else(|e| panic!("bad date {s}: {e}")))
    };

    let ticks = Rc::new(Cell::new(0u64));
    let bars = Rc::new(Cell::new(0u64));
    let algo = Noop { start: date(2), end: date(3), ticks: ticks.clone(), bars: bars.clone() };

    let start = Instant::now();
    let result = run_backtest(algo, data_path).unwrap_or_else(|e| {
        eprintln!("backtest failed: {e}");
        std::process::exit(1);
    });
    let elapsed = start.elapsed();

    let (ticks, bars) = (ticks.get(), bars.get());
    println!("elapsed:    {elapsed:.1?}");
    println!("ticks:      {ticks}");
    println!("bars:       {bars}");
    println!("throughput: {:.0} bars/s", bars as f64 / elapsed.as_secs_f64());
    println!("final equity: {:.2} (should equal starting cash)", result.final_equity);
}

// elapsed:    35.4s
// ticks:      19206
// bars:       33960794
// throughput: 958657 bars/s
// final equity: 100000.00 (should equal starting cash)

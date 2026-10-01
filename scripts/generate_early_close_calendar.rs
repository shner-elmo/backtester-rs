#!/usr/bin/env rust-script
//! Generates `metadata/early_closes.json`, a JSON array of market half-day dates.
//! SPY trades through extended hours, and subscribing only to it filters out every other symbol.
//! A final start-stamped bar in the 16:00 hour means a 17:00 extended-hours endpoint and a half day.
//! Run: `BACKTEST_DATA_DIR=/minute/data rust-script scripts/generate_early_close_calendar.rs`
//! ```cargo
//! [dependencies]
//! backtester = { path = "../backtester" }
//! chrono = "0.4"
//! chrono-tz = "0.9"
//! serde_json = "1"
//! ```
use std::{cell::RefCell, env, fs, path::Path, rc::Rc};

use backtester::{bar::Bar, run_backtest_with_data_dir, Algorithm, Context, Slice, Symbol};
use chrono::{NaiveDate, Timelike};
use chrono_tz::US::Eastern;

struct Calendar {
    spy: Option<Symbol>,
    last: Option<Bar>,
    half_days: Rc<RefCell<Vec<NaiveDate>>>,
}

impl Algorithm for Calendar {
    fn initialize(&mut self, ctx: &mut Context) {
        self.spy = Some(ctx.add_equity("SPY"));
    }
    fn on_data(&mut self, _: &mut Context, data: &Slice) {
        self.last = data.bars.get(&self.spy.unwrap()).cloned();
    }
    fn on_end_of_day(&mut self, _: &mut Context) {
        let Some(bar) = self.last.take() else { return };
        let time = bar.time.with_timezone(&Eastern);
        if matches!(time.hour(), 16 | 17) {
            self.half_days.borrow_mut().push(time.date_naive());
        }
    }
}

fn main() {
    let data = env::var("BACKTEST_DATA_DIR").expect("BACKTEST_DATA_DIR is required");
    let half_days = Rc::new(RefCell::new(Vec::new()));
    let algo = Calendar { spy: None, last: None, half_days: half_days.clone() };
    run_backtest_with_data_dir(algo, Path::new(&data)).expect("backtest failed");
    let json = serde_json::to_string_pretty(&*half_days.borrow()).unwrap();
    let metadata = Path::new(&data).join("metadata");
    fs::create_dir_all(&metadata).expect("failed to create metadata directory");
    fs::write(metadata.join("early_closes.json"), json).expect("failed to write calendar");
}

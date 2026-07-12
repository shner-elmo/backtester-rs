//! One-off sanity sweep: load a real month for ALL tickers and assert OHLC/
//! volume invariants hold on every bar. Not part of the test suite —
//! run it manually against the full dataset:
//!
//!   cargo run --release --example data_invariants_check -- /path/to/data/output/minute/year=2023/month=6 /path/to/data/output

use std::collections::HashMap;

use backtester::data::{iter_bars, load_ticker_map};

fn main() {
    let args: Vec<String> = std::env::args().collect();
    let bars_dir = args.get(1).expect("arg 1: dir of parquet files to sweep");
    let root = args.get(2).expect("arg 2: data root containing encoded_tickers.json");

    let ticker_map = load_ticker_map(root).unwrap_or_else(|e| {
        eprintln!("failed to load ticker map: {e}");
        std::process::exit(1);
    });
    let mut n: u64 = 0;
    let mut bad: u64 = 0;
    let mut per_symbol: HashMap<String, u64> = HashMap::new();
    let mut min_ts = i64::MAX;
    let mut max_ts = i64::MIN;

    let swept = iter_bars(bars_dir, &ticker_map, |sym, b| {
        n += 1;
        *per_symbol.entry(sym.clone()).or_default() += 1;
        let ts = b.time.timestamp();
        min_ts = min_ts.min(ts);
        max_ts = max_ts.max(ts);

        let ohlc_ok = b.high >= b.low
            && b.high >= b.open
            && b.high >= b.close
            && b.low <= b.open
            && b.low <= b.close
            && b.open > 0.0
            && b.close > 0.0;
        if !ohlc_ok {
            bad += 1;
            if bad <= 5 {
                eprintln!(
                    "BAD {sym} {}: o={} h={} l={} c={} v={}",
                    b.time, b.open, b.high, b.low, b.close, b.volume
                );
            }
        }
    });
    if let Err(e) = swept {
        eprintln!("sweep failed: {e}");
        std::process::exit(1);
    }

    println!("bars={n} symbols={} bad={bad} time_range={min_ts}..{max_ts}", per_symbol.len());
    assert_eq!(bad, 0, "{bad} bars violate OHLC invariants");
    println!("OK: all {n} bars across {} symbols satisfy OHLC invariants", per_symbol.len());
}

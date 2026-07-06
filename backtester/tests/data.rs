//! Locks in the Parquet → `Bar` column mapping against a small committed
//! fixture (AAPL, Jan 2023). Regenerate the fixture with:
//!   cargo run -p data-viz --example make_test_fixture
//! then copy tests/fixtures from data-viz into backtester.

use backtester::bar::{Bar, MarketSession};
use backtester::data::{iter_bars, load_ticker_map};

const FIXTURE: &str = concat!(env!("CARGO_MANIFEST_DIR"), "/tests/fixtures");

fn load() -> Vec<(String, Bar)> {
    let ticker_map = load_ticker_map(FIXTURE).unwrap();
    let mut bars = Vec::new();
    iter_bars(FIXTURE, &ticker_map, |sym, bar| bars.push((sym, bar))).unwrap();
    bars
}

#[test]
fn maps_columns_to_the_right_fields() {
    let bars = load();
    assert!(!bars.is_empty(), "fixture produced no bars");
    assert!(bars.iter().all(|(s, _)| s == "AAPL"));

    // First bar (earliest window_start) has known values pulled straight from
    // the Parquet by column name: open=130.28 close=131.0 high=131.0 low=130.28,
    // 2023-01-03T04:00:00-05:00 (= 09:00 UTC), market_session=1 (PreMarket).
    let (_, first) = bars.iter().min_by_key(|(_, b)| b.time).unwrap();
    assert_eq!(first.open, 130.28);
    assert_eq!(first.close, 131.0);
    assert_eq!(first.high, 131.0);
    assert_eq!(first.low, 130.28);
    assert_eq!(first.market_session, MarketSession::PreMarket);
    assert_eq!(first.time.to_rfc3339(), "2023-01-03T09:00:00+00:00");
}

#[test]
fn volume_is_loaded() {
    let bars = load();
    // Real minute data always has traded volume on at least some bars; a
    // wiring mistake (wrong column / always-zero default) would fail this.
    assert!(bars.iter().any(|(_, b)| b.volume > 0), "all volumes are zero");
}

#[test]
fn file_year_month_parses_hive_partition_dirs() {
    use backtester::data::file_year_month;
    use std::path::Path;

    let hive = Path::new("/data/minute/year=2023/month=7/part-0.parquet");
    assert_eq!(file_year_month(hive), Some((2023, 7)));

    let bare = Path::new("/data/minute/2023/7/part-0.parquet");
    assert_eq!(file_year_month(bare), Some((2023, 7)));
}

#[test]
fn every_bar_satisfies_ohlc_invariants() {
    // A scrambled high/low/close mapping breaks these on real data almost
    // immediately, so this guards against a regression to positional indexing.
    for (sym, b) in load() {
        assert!(b.high >= b.low, "{sym} {}: high {} < low {}", b.time, b.high, b.low);
        assert!(b.high >= b.open && b.high >= b.close, "{sym} {}: high below o/c", b.time);
        assert!(b.low <= b.open && b.low <= b.close, "{sym} {}: low above o/c", b.time);
    }
}

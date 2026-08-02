//! Locks in the Parquet → `Bar` column mapping against a small committed
//! fixture (AAPL, Jan 2023). Regenerate the fixture with:
//!   cargo run -p data-viz --example make_test_fixture
//! then copy tests/fixtures from data-viz into backtester.

use backtester::{
    bar::{Bar, MarketSession},
    data::{iter_bars, load_ticker_map},
};

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
    // 2023-01-03T04:00:00-05:00 (= 09:00 UTC), which derives as PreMarket.
    let (_, first) = bars.iter().min_by_key(|(_, b)| b.time).unwrap();
    assert_eq!(first.open, 130.28);
    assert_eq!(first.close, 131.0);
    assert_eq!(first.high, 131.0);
    assert_eq!(first.low, 130.28);
    assert_eq!(first.session(), MarketSession::PreMarket);
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
    use std::path::Path;

    use backtester::data::file_year_month;

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

/// Drain every tick of the fixture through a `TickReader` built on `mask`.
fn drain(mask: &backtester::data::SubscriptionMask) -> Vec<(backtester::Symbol, Bar)> {
    use backtester::data::{sorted_parquet_files, TickReader};

    let mut projection = None;
    let mut out = Vec::new();
    for path in sorted_parquet_files(FIXTURE) {
        let mut reader = TickReader::new(&path, mask, &mut projection).unwrap();
        while let Some((_, bars)) = reader.next_tick().unwrap() {
            out.extend(bars);
        }
    }
    out
}

#[test]
fn a_pushed_down_subscription_reads_the_same_bars() {
    use backtester::{data::SubscriptionMask, Symbol};

    // AAPL is id 47 and the only ticker in the fixture. Subscribing it alone
    // out of a wide id space is selective enough to become a Parquet row
    // filter; subscribing every id up to it is not, so that reader decodes
    // unfiltered. Both must see exactly the same AAPL bars.
    let mut selective = SubscriptionMask::with_id_space(20_000);
    selective.insert(Symbol::from_ticker_id(47));
    assert!(selective.worth_pushing_down(), "expected the narrow mask to push down");

    let mut wide = SubscriptionMask::new();
    for id in 0..=47 {
        wide.insert(Symbol::from_ticker_id(id));
    }
    assert!(!wide.worth_pushing_down(), "expected the wide mask to read unfiltered");

    let (filtered, unfiltered) = (drain(&selective), drain(&wide));
    assert!(!filtered.is_empty(), "fixture produced no bars");
    assert_eq!(filtered.len(), unfiltered.len());
    for ((sa, ba), (sb, bb)) in filtered.iter().zip(&unfiltered) {
        assert_eq!(sa, sb);
        assert_eq!(ba.time, bb.time);
        assert_eq!(
            (ba.open, ba.high, ba.low, ba.close, ba.volume),
            (bb.open, bb.high, bb.low, bb.close, bb.volume)
        );
    }
}

#[test]
fn pushdown_pays_off_only_for_a_selective_subscription() {
    use backtester::{data::SubscriptionMask, Symbol};

    let mut mask = SubscriptionMask::with_id_space(1_000);
    assert!(mask.is_empty());
    for id in 0..100 {
        mask.insert(Symbol::from_ticker_id(id));
    }
    assert_eq!(mask.len(), 100);
    assert!(mask.worth_pushing_down(), "10% of the id space is worth filtering");

    for id in 100..600 {
        mask.insert(Symbol::from_ticker_id(id));
    }
    assert!(!mask.worth_pushing_down(), "60% of the id space is not");

    // Inserting the same id twice must not inflate the count.
    mask.insert(Symbol::from_ticker_id(0));
    assert_eq!(mask.len(), 600);
}

//! The parallel reader must be indistinguishable from the sequential one.
//!
//! The committed fixture is a single row group, which exercises none of the
//! ordering machinery, so these tests synthesize Parquet files with several
//! row groups and ticks that straddle their boundaries — the cases the
//! round-robin hand-off and the straddle merge exist for.

use std::{fs::File, path::PathBuf, sync::Arc};

use arrow::{
    array::{Float64Array, TimestampNanosecondArray, UInt16Array, UInt32Array},
    datatypes::{DataType, Field, Schema, TimeUnit},
    record_batch::RecordBatch,
};
use backtester::{
    bar::Bar,
    data::{sorted_parquet_files, SubscriptionMask, TickReader},
    tick_stream::TickStream,
    Symbol,
};
use parquet::{arrow::ArrowWriter, basic::Compression, file::properties::WriterProperties};

/// Rows per row group in the synthetic files — small, so a handful of ticks
/// spans several groups and every worker gets work.
const ROW_GROUP_ROWS: usize = 64;

fn schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("ticker", DataType::UInt16, true),
        Field::new("volume", DataType::UInt32, true),
        Field::new("open", DataType::Float64, true),
        Field::new("high", DataType::Float64, true),
        Field::new("low", DataType::Float64, true),
        Field::new("close", DataType::Float64, true),
        Field::new(
            "window_start",
            DataType::Timestamp(TimeUnit::Nanosecond, Some("US/Eastern".into())),
            true,
        ),
    ]))
}

/// Write `rows` (ticker id, nanosecond timestamp) into a Hive-partitioned
/// month file under `root`, in the order given.
fn write_month(root: &std::path::Path, year: u32, month: u32, rows: &[(u16, i64)]) {
    let dir = root.join(format!("year={year}")).join(format!("month={month}"));
    std::fs::create_dir_all(&dir).unwrap();

    let tickers: UInt16Array = rows.iter().map(|&(t, _)| Some(t)).collect();
    let times: TimestampNanosecondArray =
        rows.iter().map(|&(_, ts)| Some(ts)).collect::<TimestampNanosecondArray>();
    let times = times.with_timezone("US/Eastern");
    // Prices encode the row's identity so a scrambled column mapping or a
    // dropped row is visible in the comparison, not just the count.
    let price = |scale: f64| -> Float64Array {
        rows.iter().map(|&(t, ts)| Some(t as f64 * scale + (ts % 1_000) as f64)).collect()
    };
    let volumes: UInt32Array = rows.iter().map(|&(t, _)| Some(t as u32 * 10)).collect();

    let batch = RecordBatch::try_new(
        schema(),
        vec![
            Arc::new(tickers),
            Arc::new(volumes),
            Arc::new(price(1.0)),
            Arc::new(price(2.0)),
            Arc::new(price(0.5)),
            Arc::new(price(1.5)),
            Arc::new(times),
        ],
    )
    .unwrap();

    let props = WriterProperties::builder()
        .set_max_row_group_size(ROW_GROUP_ROWS)
        .set_compression(Compression::UNCOMPRESSED)
        .build();
    let file = File::create(dir.join("part-0.parquet")).unwrap();
    let mut writer = ArrowWriter::try_new(file, schema(), Some(props)).unwrap();
    writer.write(&batch).unwrap();
    writer.close().unwrap();
}

/// A dataset of `months` month-files, each holding `ticks_per_month`
/// timestamps with `symbols` bars apiece — so most ticks straddle a row-group
/// boundary at 64 rows per group.
fn dataset(dir: &tempfile::TempDir, months: u32, ticks_per_month: i64, symbols: u16) {
    for month in 1..=months {
        let base = 1_700_000_000_000_000_000i64 + month as i64 * 86_400_000_000_000;
        let mut rows = Vec::new();
        for tick in 0..ticks_per_month {
            let ts = base + tick * 60_000_000_000;
            for id in 0..symbols {
                rows.push((id, ts));
            }
        }
        write_month(dir.path(), 2023, month, &rows);
    }
}

fn all_symbols(n: u16) -> SubscriptionMask {
    let mut mask = SubscriptionMask::with_id_space(n as usize);
    for id in 0..n {
        mask.insert(Symbol::from_ticker_id(id));
    }
    mask
}

/// Everything a sequential `TickReader` sweep over `files` produces.
fn sequential(files: &[PathBuf], mask: &SubscriptionMask) -> Vec<(i64, Vec<(Symbol, Bar)>)> {
    let mut projection = None;
    let mut out: Vec<(i64, Vec<(Symbol, Bar)>)> = Vec::new();
    for path in files {
        let mut reader = TickReader::new(path, mask, &mut projection).unwrap();
        while let Some((ts, bars)) = reader.next_tick().unwrap() {
            // A tick split across two files would arrive as two groups here;
            // the stream merges those, so merge them for the comparison too.
            match out.last_mut() {
                Some((last_ts, last_bars)) if *last_ts == ts => last_bars.extend(bars),
                _ => out.push((ts, bars)),
            }
        }
    }
    out
}

fn parallel(
    files: &[PathBuf],
    mask: &SubscriptionMask,
    threads: usize,
) -> Vec<(i64, Vec<(Symbol, Bar)>)> {
    let mut stream = TickStream::new(files, mask, threads).unwrap();
    let mut out = Vec::new();
    while let Some(tick) = stream.next_tick().unwrap() {
        out.push(tick);
    }
    out
}

#[test]
fn parallel_decode_matches_a_sequential_sweep() {
    let dir = tempfile::tempdir().unwrap();
    dataset(&dir, 4, 25, 7); // 175 rows/month over 64-row groups: plenty of straddles
    let files = sorted_parquet_files(dir.path().to_str().unwrap());
    assert_eq!(files.len(), 4);

    let mask = all_symbols(7);
    let expected = sequential(&files, &mask);
    assert_eq!(expected.len(), 100, "4 months x 25 ticks");
    assert!(expected.iter().all(|(_, bars)| bars.len() == 7), "every tick holds every symbol");

    // Thread counts on both sides of the unit count, so the rotation is
    // exercised with workers that get several units, one unit, and none.
    for threads in [1, 2, 3, 8, 32] {
        let got = parallel(&files, &mask, threads);
        assert_eq!(got.len(), expected.len(), "tick count with {threads} thread(s)");
        for (i, ((want_ts, want_bars), (got_ts, got_bars))) in expected.iter().zip(&got).enumerate()
        {
            assert_eq!(want_ts, got_ts, "tick {i} timestamp with {threads} thread(s)");
            assert_eq!(want_bars, got_bars, "tick {i} bars with {threads} thread(s)");
        }
    }
}

#[test]
fn a_selective_subscription_still_matches() {
    let dir = tempfile::tempdir().unwrap();
    dataset(&dir, 2, 40, 9);
    let files = sorted_parquet_files(dir.path().to_str().unwrap());

    // Two of nine ids: selective enough that the reader pushes the mask into
    // Parquet as a row filter, so this covers the filtered path too.
    let mut mask = SubscriptionMask::with_id_space(9);
    mask.insert(Symbol::from_ticker_id(1));
    mask.insert(Symbol::from_ticker_id(6));
    assert!(mask.worth_pushing_down());

    let expected = sequential(&files, &mask);
    assert!(expected.iter().all(|(_, bars)| bars.len() == 2));
    for threads in [1, 4] {
        assert_eq!(parallel(&files, &mask, threads), expected, "{threads} thread(s)");
    }
}

#[test]
fn ticks_that_straddle_a_file_boundary_arrive_whole() {
    let dir = tempfile::tempdir().unwrap();
    // Two months whose last and first timestamps coincide — the awkward case
    // for a stream that hands out a tick only once it is complete.
    let shared = 1_700_000_000_000_000_000i64;
    write_month(dir.path(), 2023, 1, &[(0, shared - 60_000_000_000), (0, shared), (1, shared)]);
    write_month(dir.path(), 2023, 2, &[(2, shared), (3, shared + 60_000_000_000)]);

    let files = sorted_parquet_files(dir.path().to_str().unwrap());
    let mask = all_symbols(4);
    let ticks = parallel(&files, &mask, 4);

    assert_eq!(ticks.len(), 3, "one tick per distinct timestamp");
    assert_eq!(ticks[1].0, shared);
    let mut ids: Vec<u16> = ticks[1].1.iter().map(|(s, _)| s.ticker_id()).collect();
    ids.sort_unstable();
    assert_eq!(ids, vec![0, 1, 2], "bars from both files land in one tick");
}

#[test]
fn unsorted_data_fails_the_stream_at_the_first_bad_row() {
    let dir = tempfile::tempdir().unwrap();
    let base = 1_700_000_000_000_000_000i64;
    // Row-group-sized runs, with one backwards step early in the file and
    // another later: whichever thread notices first, the error reported must
    // be the earlier one.
    let mut rows: Vec<(u16, i64)> = (0..200).map(|i| (0, base + i * 60_000_000_000)).collect();
    rows[80] = (0, base - 60_000_000_000);
    rows[150] = (0, base - 120_000_000_000);
    write_month(dir.path(), 2023, 1, &rows);

    let files = sorted_parquet_files(dir.path().to_str().unwrap());
    let mask = all_symbols(1);
    for threads in [1, 2, 8] {
        let mut stream = TickStream::new(&files, &mask, threads).unwrap();
        let err = loop {
            match stream.next_tick() {
                Ok(Some(_)) => continue,
                Ok(None) => panic!("unsorted data was accepted with {threads} thread(s)"),
                Err(e) => break e,
            }
        };
        // The *earlier* regression must surface, whichever worker decoded
        // which row group and whichever noticed first.
        match err {
            backtester::BacktestError::OutOfOrderData { at, .. } => assert_eq!(
                at.timestamp_nanos_opt().unwrap(),
                base - 60_000_000_000,
                "with {threads} thread(s), the first regression in file order must win"
            ),
            other => panic!("expected OutOfOrderData with {threads} thread(s), got {other}"),
        }
    }
}

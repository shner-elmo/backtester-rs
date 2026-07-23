//! Data-quality sweep: verify every Parquet file under a root is sorted by
//! `window_start` (the engine's hard requirement — an unsorted file fails a
//! backtest with `OutOfOrderData`). Reads only the timestamp column, so a
//! full-dataset sweep is quick. Also checks the boundary between consecutive
//! files (the stream must be globally non-decreasing).
//!
//! Usage: cargo run --release --example check_sorted -- /path/to/data/root

use arrow::array::TimestampNanosecondArray;
use backtester::data::sorted_parquet_files;
use parquet::arrow::{arrow_reader::ParquetRecordBatchReaderBuilder, ProjectionMask};

fn main() {
    let root = std::env::args().nth(1).expect("arg 1: data root to sweep");

    let files = sorted_parquet_files(&root);
    assert!(!files.is_empty(), "no parquet files under {root}");

    let mut bad_files = 0u32;
    let mut global_prev = i64::MIN;

    for file in &files {
        // A zero-length or unparsable .parquet (e.g. a leftover tmp file from
        // an interrupted job) would fail a backtest just like unsorted data.
        let f = std::fs::File::open(file).unwrap();
        let builder = match ParquetRecordBatchReaderBuilder::try_new(f) {
            Ok(b) => b,
            Err(e) => {
                bad_files += 1;
                println!("BROKEN   {} — unreadable: {e}", file.display());
                continue;
            }
        };
        let schema = builder.parquet_schema();
        let ts_idx = schema
            .columns()
            .iter()
            .position(|c| c.name() == "window_start")
            .expect("window_start column");
        let mask = ProjectionMask::leaves(schema, [ts_idx]);
        let reader = builder.with_projection(mask).build().unwrap();

        let mut prev = i64::MIN;
        let mut regressions = 0u64;
        let mut first_at = None;
        let mut rows = 0u64;
        for batch in reader {
            let batch = batch.unwrap();
            let col = batch.column(0).as_any().downcast_ref::<TimestampNanosecondArray>().unwrap();
            for i in 0..col.len() {
                let v = col.value(i);
                if v < prev && first_at.is_none() {
                    first_at = Some(rows + i as u64);
                }
                if v < prev {
                    regressions += 1;
                }
                prev = v;
            }
            rows += batch.num_rows() as u64;
        }

        let boundary_ok = rows == 0 || {
            // First value of this file vs last value of the previous one.
            let ok = first_value(file) >= global_prev;
            global_prev = prev;
            ok
        };

        if regressions > 0 || !boundary_ok {
            bad_files += 1;
            println!(
                "UNSORTED {} — {rows} rows, {regressions} regression(s){}{}",
                file.display(),
                first_at.map(|r| format!(", first at row {r}")).unwrap_or_default(),
                if boundary_ok { "" } else { ", starts before the previous file ends" },
            );
        } else {
            println!("ok       {} — {rows} rows", file.display());
        }
    }

    if bad_files > 0 {
        println!("\n{bad_files} of {} file(s) unsorted", files.len());
        std::process::exit(1);
    }
    println!("\nall {} file(s) sorted", files.len());
}

/// The first `window_start` value in a file (files are checked in stream order,
/// so this is what the cross-file boundary check compares).
fn first_value(file: &std::path::Path) -> i64 {
    let f = std::fs::File::open(file).unwrap();
    let builder = ParquetRecordBatchReaderBuilder::try_new(f).unwrap();
    let schema = builder.parquet_schema();
    let ts_idx =
        schema.columns().iter().position(|c| c.name() == "window_start").expect("window_start");
    let mask = ProjectionMask::leaves(schema, [ts_idx]);
    let mut reader = builder.with_projection(mask).with_batch_size(1).build().unwrap();
    let batch = reader.next().expect("empty file").unwrap();
    batch.column(0).as_any().downcast_ref::<TimestampNanosecondArray>().unwrap().value(0)
}

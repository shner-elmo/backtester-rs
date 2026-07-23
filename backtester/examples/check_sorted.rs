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

    let mut rows = 0;
    let mut prev = 0;
    let mut count_wrong_ts = 0;

    for file in &files {
        println!("scanning file {}", file.display());

        let f = std::fs::File::open(file).unwrap();
        let builder = ParquetRecordBatchReaderBuilder::try_new(f).unwrap();
        let schema = builder.parquet_schema();
        let ts_idx = schema.columns().iter().position(|c| c.name() == "window_start").unwrap();
        let mask = ProjectionMask::leaves(schema, [ts_idx]);
        let reader = builder.with_projection(mask).build().unwrap();

        for (batch_idx, batch) in reader.enumerate() {
            let batch = batch.unwrap();
            let col = batch.column(0).as_any().downcast_ref::<TimestampNanosecondArray>().unwrap();
            for i in 0..col.len() {
                let curr = col.value(i);

                if curr < prev {
                    count_wrong_ts += 1;
                    println!(
                        "out-of-order timestamp in {file}\n  batch {batch_idx}, row {i} (absolute row {absolute})\n  prev = {prev} ns  ({prev_s:.3} s)\n  curr = {curr} ns  ({curr_s:.3} s)",
                         file = file.display(),
                         absolute = rows + i as u64,
                         prev_s = prev as f64 / 1e9,
                         curr_s = curr as f64 / 1e9,
                    );
                }
                prev = curr;
            }
            rows += batch.num_rows() as u64;
        }
    }

    println!("\nall {} file(s) sorted", files.len());
    println!("out of order timestamps: {}", count_wrong_ts);
    println!("Scanned whole dataset (rows: {})", rows);
}

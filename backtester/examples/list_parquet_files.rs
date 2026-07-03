use backtester::data::sorted_parquet_files;
use std::env;

fn main() {
    let args: Vec<String> = env::args().collect();
    let data_path = args
        .get(1)
        .cloned()
        .or_else(|| env::var("STONKS_DATA_ROOT").ok())
        .unwrap_or_else(|| "data/output/minute".to_string());

    let files = sorted_parquet_files(&data_path);
    println!("Found {} parquet files:", files.len());
    for f in &files {
        println!("  {}", f.display());
    }
}

// Example output for sorted_parquet_files() over $STONKS_DATA_ROOT:
//   $STONKS_DATA_ROOT/2025/3/part-0.parquet
//   $STONKS_DATA_ROOT/2025/4/part-0.parquet
//   ...
// (ticker map lives at $STONKS_DATA_ROOT/../encoded_tickers.json)

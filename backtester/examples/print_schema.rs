use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use std::env;
use std::fs::File;

fn main() {
    let args: Vec<String> = env::args().collect();
    // Parquet file to inspect: first CLI arg, else $STONKS_DATA_ROOT/2025/3/part-0.parquet
    let path = args.get(1).cloned().unwrap_or_else(|| {
        let root = env::var("STONKS_DATA_ROOT")
            .expect("pass a Parquet path as arg 1, or set STONKS_DATA_ROOT to the minute/ dir");
        format!("{root}/2025/3/part-0.parquet")
    });

    let file = File::open(&path).unwrap_or_else(|e| panic!("failed to open {}: {}", path, e));
    let builder =
        ParquetRecordBatchReaderBuilder::try_new(file).expect("failed to read parquet metadata");
    println!("{:#?}", builder.schema());
}

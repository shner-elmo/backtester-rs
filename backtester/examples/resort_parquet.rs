//! Data repair: rewrite every Parquet month file under a root in global
//! `(window_start, ticker)` order — the engine's hard requirement. Exists
//! because an ingest bug once wrote files as concatenations of independently
//! sorted ~500k-row chunks, which fails any backtest with `OutOfOrderData`
//! (see `check_sorted.rs` to diagnose).
//!
//! Already-sorted files are left untouched. Each unsorted file is re-sorted
//! in memory (stable, so equal-key rows keep their relative order), written
//! to a `.resort_tmp` sibling, re-read and verified — row count, sortedness,
//! and an order-independent content checksum against the original — and only
//! then renamed over the original. A crash mid-run leaves at worst a stale
//! tmp file, never a damaged original.
//!
//! Usage: cargo run --release --example resort_parquet -- /path/to/data/root

use std::{fs, path::Path, sync::Arc, time::Instant};

use arrow::{
    array::{Float64Array, TimestampNanosecondArray, UInt16Array, UInt32Array},
    datatypes::DataType,
    record_batch::RecordBatch,
};
use backtester::data::sorted_parquet_files;
use parquet::{
    arrow::{arrow_reader::ParquetRecordBatchReaderBuilder, ArrowWriter},
    basic::{Compression, ZstdLevel},
    file::properties::WriterProperties,
};

struct Columns {
    ticker: Vec<u16>,
    ts: Vec<i64>,
    open: Vec<f64>,
    high: Vec<f64>,
    low: Vec<f64>,
    close: Vec<f64>,
    volume: Vec<u32>,
}

impl Columns {
    fn len(&self) -> usize {
        self.ts.len()
    }

    fn is_sorted(&self) -> bool {
        self.ts.windows(2).all(|w| w[0] <= w[1])
    }

    /// Order-independent content hash: wrapping sum of per-row hashes, so it
    /// is invariant under any permutation but changes if any row is lost,
    /// duplicated, or altered (a sum, not XOR, so identical duplicate rows
    /// don't cancel out).
    fn checksum(&self) -> u64 {
        let mut acc = 0u64;
        for i in 0..self.len() {
            let mut h = 0xcbf29ce484222325u64; // FNV offset basis
            for word in [
                self.ticker[i] as u64,
                self.ts[i] as u64,
                self.open[i].to_bits(),
                self.high[i].to_bits(),
                self.low[i].to_bits(),
                self.close[i].to_bits(),
                self.volume[i] as u64,
            ] {
                h = (h ^ word).wrapping_mul(0x100000001b3);
            }
            acc = acc.wrapping_add(h);
        }
        acc
    }
}

fn read_columns(path: &Path) -> Columns {
    let f = fs::File::open(path).unwrap();
    let reader = ParquetRecordBatchReaderBuilder::try_new(f).unwrap().build().unwrap();

    let mut c = Columns {
        ticker: Vec::new(),
        ts: Vec::new(),
        open: Vec::new(),
        high: Vec::new(),
        low: Vec::new(),
        close: Vec::new(),
        volume: Vec::new(),
    };
    for batch in reader {
        let batch = batch.unwrap();
        let col = |name: &str| batch.column_by_name(name).unwrap_or_else(|| panic!("{name}"));
        let ticker = col("ticker").as_any().downcast_ref::<UInt16Array>().unwrap();
        let ts = col("window_start").as_any().downcast_ref::<TimestampNanosecondArray>().unwrap();
        let open = col("open").as_any().downcast_ref::<Float64Array>().unwrap();
        let high = col("high").as_any().downcast_ref::<Float64Array>().unwrap();
        let low = col("low").as_any().downcast_ref::<Float64Array>().unwrap();
        let close = col("close").as_any().downcast_ref::<Float64Array>().unwrap();
        let volume = col("volume").as_any().downcast_ref::<UInt32Array>().unwrap();
        for i in 0..batch.num_rows() {
            c.ticker.push(ticker.value(i));
            c.ts.push(ts.value(i));
            c.open.push(open.value(i));
            c.high.push(high.value(i));
            c.low.push(low.value(i));
            c.close.push(close.value(i));
            c.volume.push(volume.value(i));
        }
    }
    c
}

/// Write `cols` in `idx` order using the original file's schema (field order
/// and timestamp timezone preserved), ZSTD-compressed like the ingest script.
fn write_sorted(src: &Path, dst: &Path, cols: &Columns, idx: &[usize]) {
    let f = fs::File::open(src).unwrap();
    let schema = ParquetRecordBatchReaderBuilder::try_new(f).unwrap().schema().clone();

    let arrays: Vec<Arc<dyn arrow::array::Array>> = schema
        .fields()
        .iter()
        .map(|field| -> Arc<dyn arrow::array::Array> {
            match field.name().as_str() {
                "ticker" => {
                    Arc::new(UInt16Array::from_iter_values(idx.iter().map(|&i| cols.ticker[i])))
                }
                "window_start" => {
                    let tz = match field.data_type() {
                        DataType::Timestamp(_, tz) => tz.clone(),
                        other => panic!("window_start has type {other}"),
                    };
                    Arc::new(
                        TimestampNanosecondArray::from_iter_values(idx.iter().map(|&i| cols.ts[i]))
                            .with_timezone_opt(tz),
                    )
                }
                "open" => {
                    Arc::new(Float64Array::from_iter_values(idx.iter().map(|&i| cols.open[i])))
                }
                "high" => {
                    Arc::new(Float64Array::from_iter_values(idx.iter().map(|&i| cols.high[i])))
                }
                "low" => Arc::new(Float64Array::from_iter_values(idx.iter().map(|&i| cols.low[i]))),
                "close" => {
                    Arc::new(Float64Array::from_iter_values(idx.iter().map(|&i| cols.close[i])))
                }
                "volume" => {
                    Arc::new(UInt32Array::from_iter_values(idx.iter().map(|&i| cols.volume[i])))
                }
                other => panic!("unexpected column {other}"),
            }
        })
        .collect();

    let batch = RecordBatch::try_new(schema.clone(), arrays).unwrap();
    let props = WriterProperties::builder()
        .set_compression(Compression::ZSTD(ZstdLevel::default()))
        .build();
    let out = fs::File::create(dst).unwrap();
    let mut writer = ArrowWriter::try_new(out, schema, Some(props)).unwrap();
    writer.write(&batch).unwrap();
    writer.close().unwrap();
}

fn main() {
    let root = std::env::args().nth(1).expect("arg 1: data root to repair");
    let files = sorted_parquet_files(&root);
    assert!(!files.is_empty(), "no parquet files under {root}");

    let total = Instant::now();
    let (mut fixed, mut skipped) = (0u32, 0u32);

    for file in &files {
        if fs::metadata(file).unwrap().len() == 0 {
            println!("EMPTY   {} — zero bytes, remove it by hand", file.display());
            continue;
        }

        let t = Instant::now();
        let cols = read_columns(file);
        if cols.is_sorted() {
            println!("ok      {} — {} rows, already sorted", file.display(), cols.len());
            skipped += 1;
            continue;
        }

        let before = cols.checksum();
        let mut idx: Vec<usize> = (0..cols.len()).collect();
        // Stable, matching the ingest script's (window_start, ticker) key.
        idx.sort_by_key(|&i| (cols.ts[i], cols.ticker[i]));

        let tmp = file.with_extension("resort_tmp");
        write_sorted(file, &tmp, &cols, &idx);
        drop(cols);

        // Trust nothing until the rewrite proves itself: same row count and
        // content checksum as the original, and actually sorted.
        let check = read_columns(&tmp);
        assert!(check.is_sorted(), "{}: rewrite is not sorted", tmp.display());
        assert_eq!(check.len(), idx.len(), "{}: row count changed", tmp.display());
        assert_eq!(check.checksum(), before, "{}: content changed", tmp.display());
        drop(check);

        fs::rename(&tmp, file).unwrap();
        println!(
            "SORTED  {} — {} rows re-sorted in {:.1}s",
            file.display(),
            idx.len(),
            t.elapsed().as_secs_f32()
        );
        fixed += 1;
    }

    println!(
        "\n{fixed} file(s) re-sorted, {skipped} already sorted, in {:.0}s",
        total.elapsed().as_secs_f32()
    );
}

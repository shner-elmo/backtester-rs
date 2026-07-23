#!/usr/bin/env rust-script
//! Convert minute-bar CSV.gz files to Hive-partitioned Parquet using Arrow/Parquet directly.
//!
//! Requires: cargo install rust-script
//! Run: rust-script scripts/ingest_arrow.rs <input_dir> <output_dir> <encoded_tickers.json>
//!
//! Input layout:  <input>/<YYYY>/<MM>/<ticker>.csv.gz
//! Output layout: <output>/year=<YYYY>/month=<M>/<stem>.parquet
//!
//! Columns: ticker (UInt16), window_start (TimestampNs UTC), open, high, low, close (Float64), volume (UInt32)
//!
//! Each CSV.gz is read, sorted, and written to disk independently — no global
//! accumulation. Peak memory is proportional to one file.
//!
//! ```cargo
//! [dependencies]
//! arrow = "56"
//! parquet = { version = "56", features = ["arrow", "zstd"] }
//! flate2 = "1"
//! csv = "1"
//! serde = { version = "1", features = ["derive"] }
//! serde_json = "1"
//! ```

use arrow::{
    array::{Float64Array, TimestampNanosecondArray, UInt16Array, UInt32Array},
    datatypes::{DataType, Field, Schema, TimeUnit},
    record_batch::RecordBatch,
};
use flate2::read::GzDecoder;
use parquet::{
    arrow::ArrowWriter,
    basic::{Compression, ZstdLevel},
    file::properties::WriterProperties,
};
use serde::Deserialize;
use std::{collections::HashMap, env, fs, io::BufReader, path::PathBuf, sync::Arc, time::Instant};

#[derive(Deserialize)]
struct CsvRow {
    ticker: String,
    window_start: i64,
    open: f64,
    high: f64,
    low: f64,
    close: f64,
    volume: f64, // some providers write volume as float; cast to u32 on write
}

fn main() {
    let args: Vec<String> = env::args().collect();
    if args.len() != 4 {
        eprintln!("Usage: ingest_arrow <input_dir> <output_dir> <encoded_tickers.json>");
        std::process::exit(1);
    }
    let input = PathBuf::from(&args[1]);
    let output = PathBuf::from(&args[2]);

    fs::create_dir_all(&output).expect("create output dir");

    // encoded_tickers.json is {"0": "AAPL", "1": "MSFT", ...} — reverse to sym → id
    let json = fs::read_to_string(&args[3]).expect("read encoded_tickers.json");
    let id_to_sym: HashMap<String, String> = serde_json::from_str(&json).unwrap();
    let sym_to_id: HashMap<String, u16> = id_to_sym
        .into_iter()
        .map(|(id, sym)| (sym, id.parse::<u16>().unwrap()))
        .collect();

    let schema = Arc::new(Schema::new(vec![
        Field::new("ticker", DataType::UInt16, false),
        Field::new(
            "window_start",
            DataType::Timestamp(TimeUnit::Nanosecond, Some("UTC".into())),
            false,
        ),
        Field::new("open", DataType::Float64, false),
        Field::new("high", DataType::Float64, false),
        Field::new("low", DataType::Float64, false),
        Field::new("close", DataType::Float64, false),
        Field::new("volume", DataType::UInt32, false),
    ]));

    let props = WriterProperties::builder()
        .set_compression(Compression::ZSTD(ZstdLevel::default()))
        .build();

    let total = Instant::now();
    let mut file_count = 0usize;
    let mut error_count = 0usize;

    let mut year_dirs: Vec<_> = fs::read_dir(&input)
        .expect("read input dir")
        .filter_map(|e| e.ok())
        .filter(|e| e.file_type().map(|t| t.is_dir()).unwrap_or(false))
        .filter(|e| e.file_name().to_string_lossy().parse::<i32>().is_ok())
        .collect();
    year_dirs.sort_by_key(|e| e.file_name());

    for year_entry in year_dirs {
        let year: i32 = year_entry.file_name().to_string_lossy().parse().unwrap();

        let mut month_dirs: Vec<_> = fs::read_dir(year_entry.path())
            .expect("read year dir")
            .filter_map(|e| e.ok())
            .filter(|e| e.file_type().map(|t| t.is_dir()).unwrap_or(false))
            .filter(|e| e.file_name().to_string_lossy().parse::<i32>().is_ok())
            .collect();
        month_dirs.sort_by_key(|e| e.file_name());

        for month_entry in month_dirs {
            let month: i32 = month_entry.file_name().to_string_lossy().parse().unwrap();
            let out_dir = output.join(format!("year={year}")).join(format!("month={month}"));
            fs::create_dir_all(&out_dir).expect("create partition dir");

            let mut csv_files: Vec<_> = fs::read_dir(month_entry.path())
                .expect("read month dir")
                .filter_map(|e| e.ok())
                .filter(|e| e.path().to_string_lossy().ends_with(".csv.gz"))
                .collect();
            csv_files.sort_by_key(|e| e.file_name());

            for csv_entry in csv_files {
                let src = csv_entry.path();
                let stem = PathBuf::from(src.file_stem().unwrap_or_default())
                    .file_stem()
                    .unwrap_or_default()
                    .to_string_lossy()
                    .into_owned();
                let dst = out_dir.join(format!("{stem}.parquet"));

                eprint!("  {year}-{month:02}/{stem} ... ");
                let t = Instant::now();
                match convert_file(&src, &dst, &sym_to_id, &schema, props.clone()) {
                    Ok(rows) => {
                        eprintln!("{rows} rows ({:.1}s)", t.elapsed().as_secs_f32());
                        file_count += 1;
                    }
                    Err(e) => {
                        eprintln!("ERROR: {e}");
                        error_count += 1;
                    }
                }
            }
        }
    }

    eprintln!(
        "\n{file_count} files, {error_count} errors  total: {:.1}s",
        total.elapsed().as_secs_f32()
    );
}

fn convert_file(
    src: &PathBuf,
    dst: &PathBuf,
    sym_to_id: &HashMap<String, u16>,
    schema: &Arc<Schema>,
    props: WriterProperties,
) -> Result<usize, Box<dyn std::error::Error>> {
    let gz = GzDecoder::new(BufReader::new(fs::File::open(src)?));
    let mut rdr = csv::Reader::from_reader(gz);

    let mut tickers: Vec<u16> = Vec::new();
    let mut timestamps: Vec<i64> = Vec::new();
    let mut opens: Vec<f64> = Vec::new();
    let mut highs: Vec<f64> = Vec::new();
    let mut lows: Vec<f64> = Vec::new();
    let mut closes: Vec<f64> = Vec::new();
    let mut volumes: Vec<u32> = Vec::new();

    for result in rdr.deserialize() {
        let row: CsvRow = result?;
        let Some(&id) = sym_to_id.get(&row.ticker) else { continue };
        tickers.push(id);
        timestamps.push(row.window_start);
        opens.push(row.open);
        highs.push(row.high);
        lows.push(row.low);
        closes.push(row.close);
        volumes.push(row.volume as u32);
    }

    let n = tickers.len();
    let mut idx: Vec<usize> = (0..n).collect();
    idx.sort_unstable_by_key(|&i| (timestamps[i], tickers[i]));

    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(UInt16Array::from_iter_values(idx.iter().map(|&i| tickers[i]))),
            Arc::new(
                TimestampNanosecondArray::from_iter_values(idx.iter().map(|&i| timestamps[i]))
                    .with_timezone("UTC"),
            ),
            Arc::new(Float64Array::from_iter_values(idx.iter().map(|&i| opens[i]))),
            Arc::new(Float64Array::from_iter_values(idx.iter().map(|&i| highs[i]))),
            Arc::new(Float64Array::from_iter_values(idx.iter().map(|&i| lows[i]))),
            Arc::new(Float64Array::from_iter_values(idx.iter().map(|&i| closes[i]))),
            Arc::new(UInt32Array::from_iter_values(idx.iter().map(|&i| volumes[i]))),
        ],
    )?;

    let out_file = fs::File::create(dst)?;
    let mut writer = ArrowWriter::new(out_file, schema.clone(), Some(props))?;
    writer.write(&batch)?;
    writer.close()?;

    Ok(n)
}

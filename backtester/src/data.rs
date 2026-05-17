use std::{
    collections::HashMap,
    fs::{read_to_string, File},
    path::PathBuf,
};

use arrow::array::{Float64Array, TimestampNanosecondArray, UInt16Array, UInt8Array};
use chrono::{TimeZone, Utc};
use parquet::{
    arrow::{arrow_reader::ParquetRecordBatchReaderBuilder, ProjectionMask},
    file::reader::{FileReader, SerializedFileReader},
};
use walkdir::WalkDir;

use crate::bar::{Bar, MarketSession};

pub const COLUMNS: [&str; 7] =
    ["ticker", "open", "high", "low", "close", "window_start", "market_session"];

pub fn load_ticker_map(data_root: &str) -> HashMap<u16, String> {
    let path = format!("{}/encoded_tickers.json", data_root);
    let content = read_to_string(&path).unwrap_or_else(|_| panic!("could not read {}", path));
    let temp: HashMap<String, String> = serde_json::from_str(&content).unwrap();
    temp.into_iter().map(|(k, v)| (k.parse::<u16>().unwrap(), v)).collect()
}

pub fn sorted_parquet_files(data_root: &str) -> Vec<PathBuf> {
    let mut files: Vec<PathBuf> = WalkDir::new(data_root)
        .into_iter()
        .filter_map(|e| e.ok())
        .filter(|e| e.path().extension().map_or(false, |ext| ext == "parquet"))
        .map(|e| e.path().to_path_buf())
        .collect();

    files.sort_by_key(|x| {
        let month: u32 =
            x.parent().unwrap().file_name().unwrap().to_string_lossy().parse().unwrap_or(0);
        let year: u32 = x
            .parent()
            .unwrap()
            .parent()
            .unwrap()
            .file_name()
            .unwrap()
            .to_string_lossy()
            .parse()
            .unwrap_or(0);
        (year, month)
    });

    files
}

pub fn iter_bars(
    data_root: &str,
    ticker_map: &HashMap<u16, String>,
    mut cb: impl FnMut(String, Bar),
) {
    let files = sorted_parquet_files(data_root);

    for file_path in &files {
        let file = File::open(file_path).unwrap();
        let file_reader = SerializedFileReader::new(file).unwrap();
        let schema = file_reader.metadata().file_metadata().schema_descr();

        let file = File::open(file_path).unwrap();
        let reader = ParquetRecordBatchReaderBuilder::try_new(file)
            .unwrap()
            .with_projection(ProjectionMask::columns(schema, COLUMNS))
            .build()
            .unwrap();

        for batch in reader {
            let batch = batch.unwrap();

            let ticker_col = batch.column(0).as_any().downcast_ref::<UInt16Array>().unwrap();
            let open_col = batch.column(1).as_any().downcast_ref::<Float64Array>().unwrap();
            let high_col = batch.column(2).as_any().downcast_ref::<Float64Array>().unwrap();
            let low_col = batch.column(3).as_any().downcast_ref::<Float64Array>().unwrap();
            let close_col = batch.column(4).as_any().downcast_ref::<Float64Array>().unwrap();
            let ts_col =
                batch.column(5).as_any().downcast_ref::<TimestampNanosecondArray>().unwrap();
            let sess_col = batch.column(6).as_any().downcast_ref::<UInt8Array>().unwrap();

            for i in 0..batch.num_rows() {
                let ticker_id = ticker_col.value(i);
                let symbol = match ticker_map.get(&ticker_id) {
                    Some(s) => s.clone(),
                    None => continue,
                };

                let ts_ns = ts_col.value(i);
                let secs = ts_ns / 1_000_000_000;
                let nanos = (ts_ns % 1_000_000_000) as u32;
                let time = Utc.timestamp_opt(secs, nanos).single().unwrap();

                let session = match sess_col.value(i) {
                    1 => MarketSession::PreMarket,
                    2 => MarketSession::Main,
                    3 => MarketSession::AfterMarket,
                    _ => continue,
                };

                cb(
                    symbol,
                    Bar {
                        time,
                        open: open_col.value(i),
                        high: high_col.value(i),
                        low: low_col.value(i),
                        close: close_col.value(i),
                        market_session: session,
                    },
                );
            }
        }
    }
}

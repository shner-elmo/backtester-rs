#!/usr/bin/env rust-script
//! Convert minute-bar CSV.gz files to Hive-partitioned Parquet.
//!
//! Requires: cargo install rust-script
//! Run:      rust-script scripts/ingest.rs <input_minute_dir> <output_dir>
//!
//! Input layout expected:  <input>/<YYYY>/<MM>/*.csv.gz
//!
//! If <output_dir>/encoded_tickers.json already exists, steps 1 & 2 are
//! skipped and the existing file is used to populate the ticker-id mapping.
//!
//! Step 3 processes one (year, month) partition at a time — reading only that
//! month's files — so peak memory is proportional to one month of data.
//!
//! ```cargo
//! [dependencies]
//! duckdb = { version = "1", features = ["bundled"] }
//! serde_json = "1"
//! ```

use duckdb::{Connection, Result, params};
use std::{collections::HashMap, env, fs, path::PathBuf, time::Instant};

fn main() -> Result<()> {
    let args: Vec<String> = env::args().collect();
    if args.len() != 3 {
        eprintln!("Usage: ingest <input_minute_dir> <output_dir>");
        std::process::exit(1);
    }
    let input = PathBuf::from(&args[1]);
    let output = &args[2];
    let tickers_path = PathBuf::from(output).join("encoded_tickers.json");

    fs::create_dir_all(output).expect("create output dir");

    let total = Instant::now();
    let conn = Connection::open_in_memory()?;

    if tickers_path.exists() {
        // Steps 1 & 2 skipped — load existing ticker map.
        eprintln!("[1/3] loading tickers from {} ...", tickers_path.display());
        let t = Instant::now();
        let json = fs::read_to_string(&tickers_path).expect("read encoded_tickers.json");
        let map: HashMap<String, String> = serde_json::from_str(&json).unwrap();
        conn.execute_batch("CREATE TABLE ticker_ids (ticker VARCHAR, id USMALLINT);")?;
        let mut appender = conn.appender("ticker_ids")?;
        for (id_str, ticker) in &map {
            let id: u16 = id_str.parse().expect("id must be u16");
            appender.append_row(params![ticker, id])?;
        }
        appender.flush()?;
        eprintln!("      {} symbols ({:.1}s)", map.len(), t.elapsed().as_secs_f32());
        eprintln!("[2/3] skipped (encoded_tickers.json already exists)");
    } else {
        // 1. Build sorted ticker → sequential-id table
        eprintln!("[1/3] scanning tickers ...");
        let t = Instant::now();
        let input_glob = input.join("*/*/*.csv.gz");
        conn.execute_batch(&format!(
            "CREATE TABLE ticker_ids AS
             SELECT ticker,
                    (ROW_NUMBER() OVER (ORDER BY ticker) - 1)::USMALLINT AS id
             FROM (SELECT DISTINCT ticker FROM read_csv('{}'));",
            input_glob.display()
        ))?;
        eprintln!("      done ({:.1}s)", t.elapsed().as_secs_f32());

        // 2. Write encoded_tickers.json  {"0": "AAPL", "1": "MSFT", ...}
        eprintln!("[2/3] writing encoded_tickers.json ...");
        let t = Instant::now();
        let mut stmt = conn.prepare("SELECT id, ticker FROM ticker_ids ORDER BY id")?;
        let map: HashMap<String, String> = stmt
            .query_map([], |r| {
                let id: i64 = r.get(0)?;
                let symbol: String = r.get(1)?;
                Ok((id.to_string(), symbol))
            })?
            .collect::<Result<_>>()?;
        fs::write(&tickers_path, serde_json::to_string(&map).unwrap())
            .expect("write encoded_tickers.json");
        eprintln!("      {} symbols ({:.1}s)", map.len(), t.elapsed().as_secs_f32());
    }

    // 3. Write Parquet one month at a time.
    //    - ticker: USMALLINT (integer id)
    //    - window_start: TIMESTAMP_NS (ns precision, UTC)
    //
    //    Months are discovered from the directory structure (no data scan).
    //    Each COPY reads only that month's files, so no WHERE filtering is needed.
    eprintln!("[3/3] discovering partitions ...");
    let mut months: Vec<(i32, i32)> = Vec::new();
    for year_entry in fs::read_dir(&input).expect("read input dir") {
        let year_entry = year_entry.expect("read dir entry");
        if !year_entry.file_type().expect("file type").is_dir() {
            continue;
        }
        let Ok(year) = year_entry.file_name().to_string_lossy().parse::<i32>() else {
            continue;
        };
        for month_entry in fs::read_dir(year_entry.path()).expect("read year dir") {
            let month_entry = month_entry.expect("read dir entry");
            if !month_entry.file_type().expect("file type").is_dir() {
                continue;
            }
            let Ok(month) = month_entry.file_name().to_string_lossy().parse::<i32>() else {
                continue;
            };
            months.push((year, month));
        }
    }
    months.sort();
    eprintln!("      {} months", months.len());

    eprintln!("[3/3] converting to parquet ({} months) ...", months.len());
    let t = Instant::now();
    for (year, month) in &months {
        let tm = Instant::now();
        eprint!("      {year}-{month:02} ... ");
        let month_glob = input.join(format!("{year}/{month:02}/*.csv.gz"));
        conn.execute_batch(&format!(
            r#"COPY (
                WITH base AS (
                    SELECT
                        t.id::USMALLINT AS ticker,
                        to_timestamp(d.window_start::DOUBLE / 1e9)::TIMESTAMP_NS AS window_start,
                        d.open, d.high, d.low, d.close,
                        d.volume::UINTEGER AS volume
                    FROM read_csv('{}') d
                    JOIN ticker_ids t USING (ticker)
                )
                SELECT ticker, window_start, open, high, low, close, volume,
                       {year}::INT AS year, {month}::INT AS month
                FROM base
                ORDER BY window_start, ticker
            ) TO '{output}' (
                FORMAT PARQUET,
                PARTITION_BY (year, month),
                COMPRESSION ZSTD,
                ROW_GROUP_SIZE 500000,
                OVERWRITE_OR_IGNORE true
            )"#,
            month_glob.display()
        ))?;
        eprintln!("done ({:.1}s)", tm.elapsed().as_secs_f32());
    }
    eprintln!("      done ({:.1}s)", t.elapsed().as_secs_f32());
    eprintln!("total: {:.1}s  →  {output}", total.elapsed().as_secs_f32());
    Ok(())
}

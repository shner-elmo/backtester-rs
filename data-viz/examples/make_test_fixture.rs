//! Generates the small committed test fixture from the full minute dataset.
//!
//! Reads AAPL (ticker id 47) minute bars from a single real Parquet file and
//! writes a trimmed copy — plus a matching `encoded_tickers.json` — into
//! `data-viz/tests/fixtures/`, laid out as the app expects:
//!
//! ```text
//! tests/fixtures/
//!   encoded_tickers.json          {"47": "AAPL"}
//!   minute/year=2023/month=1/part-0.parquet
//! ```
//!
//! Usage (source path may be overridden as the first arg):
//!   cargo run -p data-viz --example make_test_fixture -- [SOURCE_MONTH_PARQUET]

use std::{fs, path::PathBuf, sync::Arc};

use datafusion::{dataframe::DataFrameWriteOptions, prelude::*, scalar::ScalarValue};

const AAPL_ID: u16 = 47;
const MAX_ROWS: usize = 5_000;

#[tokio::main]
async fn main() -> datafusion::error::Result<()> {
    // Source month file: first CLI arg, else $STONKS_DATA_ROOT/year=2023/month=1/part-0.parquet
    let source = std::env::args().nth(1).unwrap_or_else(|| {
        let root = std::env::var("STONKS_DATA_ROOT").expect(
            "pass a source Parquet path as arg 1, or set STONKS_DATA_ROOT to the minute/ dir",
        );
        format!("{root}/year=2023/month=1/part-0.parquet")
    });

    let fixtures = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures");
    let month_dir = fixtures.join("minute/year=2023/month=1");
    fs::create_dir_all(&month_dir).unwrap();

    // encoded_tickers.json — only the tickers present in the fixture.
    fs::write(fixtures.join("encoded_tickers.json"), "{\n  \"47\": \"AAPL\"\n}\n").unwrap();

    let ctx = SessionContext::new();
    let df = ctx
        .read_parquet(&source, ParquetReadOptions::default())
        .await?
        .filter(col("ticker").eq(lit(ScalarValue::UInt16(Some(AAPL_ID)))))?
        .sort(vec![col("window_start").sort(true, true)])?
        .limit(0, Some(MAX_ROWS))?;

    // write_parquet emits into a directory; write to a temp dir then move the
    // single produced file to the canonical `part-0.parquet` name.
    let tmp = fixtures.join("_tmp_out");
    let _ = fs::remove_dir_all(&tmp);
    df.write_parquet(tmp.to_str().unwrap(), DataFrameWriteOptions::new(), None).await?;

    let produced = fs::read_dir(&tmp)
        .unwrap()
        .filter_map(|e| e.ok())
        .map(|e| e.path())
        .find(|p| p.extension().map(|x| x == "parquet").unwrap_or(false))
        .expect("write_parquet produced no .parquet file");
    let dest = month_dir.join("part-0.parquet");
    fs::rename(&produced, &dest).or_else(|_| fs::copy(&produced, &dest).map(|_| ())).unwrap();
    fs::remove_dir_all(&tmp).unwrap();

    let rows = Arc::new(ctx)
        .read_parquet(dest.to_str().unwrap(), ParquetReadOptions::default())
        .await?
        .count()
        .await?;
    println!("wrote {} AAPL rows -> {}", rows, dest.display());
    Ok(())
}

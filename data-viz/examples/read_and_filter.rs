use datafusion::arrow::datatypes::DataType;
use datafusion::datasource::listing::{
    ListingOptions, ListingTable, ListingTableConfig, ListingTableUrl,
};
use datafusion::prelude::*;
use std::sync::Arc;
use std::time::Instant;

#[tokio::main]
async fn main() -> datafusion::error::Result<()> {
    let ctx = SessionContext::new();

    // 1. Only map what is physically present in the directory paths
    let file_format =
        Arc::new(datafusion::datasource::file_format::parquet::ParquetFormat::default());
    let listing_options = ListingOptions::new(file_format).with_table_partition_cols(vec![
        ("year".to_string(), DataType::Int32),
        ("month".to_string(), DataType::Int32),
    ]);

    let root_path =
        std::env::var("STONKS_DATA_ROOT").expect("set STONKS_DATA_ROOT to the minute/ dir");
    let table_path = ListingTableUrl::parse(&root_path)?;

    // 2. infer_schema reads a file footer, finding the "day" field inside the Parquet file
    // and merging it seamlessly with the "year" and "month" directory fields.
    let config = ListingTableConfig::new(table_path)
        .with_listing_options(listing_options)
        .infer_schema(&ctx.state())
        .await?;

    ctx.register_table("stonks", Arc::new(ListingTable::try_new(config)?))?;

    // 3. Query normally. DataFusion knows exactly where to look for each field!
    // let df = ctx.sql("SELECT * FROM stonks WHERE year = 2022 AND month = 1 AND day = 15").await?;
    // let df = ctx.sql("SELECT * FROM stonks WHERE year = 2022 AND month = 1 AND day = 15").await?;
    // let df = ctx.sql("SELECT * FROM stonks LIMIT 5").await?;
    let query = "SELECT min(open) FROM stonks WHERE year = 2024 AND month = 4 AND day = 15";
    println!("Query: {}", query);

    let start_time = Instant::now();
    let df = ctx.sql(query).await?;
    df.show().await?; // this will actually collect the results

    let duration = start_time.elapsed();
    println!("Query completed in: {:?}", duration);

    Ok(())
}

// Query: SELECT min(open) FROM stonks
// +------------------+
// | min(stonks.open) |
// +------------------+
// | 0.0001           |
// +------------------+
// Query completed in: 85.290796226s

// Query: SELECT min(open) FROM stonks WHERE year = 2024 AND month = 4 AND day = 15
// +------------------+
// | min(stonks.open) |
// +------------------+
// | 0.0034           |
// +------------------+
// Query completed in: 732.662873ms

use std::sync::Arc;

use backtester::data::sorted_parquet_files;
use datafusion::{
    datasource::{
        file_format::parquet::ParquetFormat,
        listing::{ListingOptions, ListingTable, ListingTableConfig, ListingTableUrl},
    },
    prelude::*,
};

#[tokio::main]
async fn main() {
    let data_root =
        std::env::var("STONKS_DATA_ROOT").expect("set STONKS_DATA_ROOT to the minute/ dir");

    let files = sorted_parquet_files(&format!("{}/minute", data_root));
    println!("Found {} parquet files via WalkDir", files.len());

    let ctx = SessionContext::new();
    let table_urls: Vec<ListingTableUrl> =
        files.iter().map(|p| ListingTableUrl::parse(p.to_str().unwrap()).unwrap()).collect();

    println!("Parsing {} URLs...", table_urls.len());

    let format = Arc::new(ParquetFormat::default());
    let listing_opts = ListingOptions::new(format).with_file_extension(".parquet");

    let schema = listing_opts.infer_schema(&ctx.state(), &table_urls[0]).await.unwrap();
    println!(
        "inferred fields: {:?}",
        schema.fields().iter().map(|f| f.name().clone()).collect::<Vec<_>>()
    );

    let config = ListingTableConfig::new_with_multi_paths(table_urls)
        .with_listing_options(listing_opts)
        .with_schema(schema);
    let table = ListingTable::try_new(config).unwrap();
    ctx.register_table("bars", Arc::new(table)).unwrap();

    let df = ctx.table("bars").await.unwrap();
    println!(
        "DataFrame schema fields: {:?}",
        df.schema().fields().iter().map(|f| f.name().clone()).collect::<Vec<_>>()
    );

    let batches = ctx
        .sql("SELECT ticker, open, close, window_start FROM bars WHERE ticker = 1 LIMIT 3")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    println!("SQL returned {} batches", batches.len());
    for b in &batches {
        println!("  {} rows", b.num_rows());
    }
}

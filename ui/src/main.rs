//! Backtest results dashboard.
//!
//! Serves the most recent `backtest_result_*.json` in the current directory
//! (or an explicit path passed as the first argument):
//!
//! ```text
//! cargo run -p ui                       # latest result in CWD
//! cargo run -p ui -- path/to/result.json
//! PORT=8080 cargo run -p ui
//! ```

use std::sync::Arc;

use axum::{
    response::{Html, Json},
    routing::get,
    Router,
};
use serde_json::Value;

/// Latest `backtest_result_*.json` in the current directory. The timestamp in
/// the name sorts lexicographically, so max-by-name is newest.
fn find_latest_result() -> Option<String> {
    std::fs::read_dir(".")
        .ok()?
        .filter_map(|e| e.ok())
        .map(|e| e.file_name().to_string_lossy().into_owned())
        .filter(|n| n.starts_with("backtest_result_") && n.ends_with(".json"))
        .max()
}

#[tokio::main]
async fn main() {
    let path = std::env::args().nth(1).or_else(find_latest_result).unwrap_or_else(|| {
        eprintln!(
            "No backtest result found. Run a backtest first (e.g. \
             `cargo run --example ema_cross -- backtester/tests/fixtures`) \
             or pass a path: `cargo run -p ui -- result.json`"
        );
        std::process::exit(1);
    });

    let content =
        std::fs::read_to_string(&path).unwrap_or_else(|e| panic!("could not read {path}: {e}"));
    let mut result: Value =
        serde_json::from_str(&content).unwrap_or_else(|e| panic!("{path} is not valid JSON: {e}"));
    result["source_file"] = Value::String(path.clone());

    let state = Arc::new(result);
    let app = Router::new().route("/", get(|| async { Html(include_str!("index.html")) })).route(
        "/api/result",
        get({
            let state = state.clone();
            move || async move { Json((*state).clone()) }
        }),
    );

    let port: u16 = std::env::var("PORT").ok().and_then(|p| p.parse().ok()).unwrap_or(3001);
    let listener = tokio::net::TcpListener::bind(("0.0.0.0", port)).await.unwrap();
    println!("Serving {path} on http://localhost:{port}");
    axum::serve(listener, app).await.unwrap();
}

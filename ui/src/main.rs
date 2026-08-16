//! Backtest results dashboard.
//!
//! Serves the `backtest_result_*.json` files in the current directory, newest
//! first, with a picker to switch between them. An explicit path may be passed
//! as the first argument to pin the dashboard to a single file:
//!
//! ```text
//! cargo run -p ui                       # all results in CWD, newest selected
//! cargo run -p ui -- path/to/result.json
//! PORT=8080 cargo run -p ui
//! ```

use std::{path::PathBuf, sync::Arc};

use axum::{
    extract::{Query, State},
    http::StatusCode,
    response::{Html, IntoResponse, Json, Response},
    routing::get,
    Router,
};
use serde::Deserialize;
use serde_json::{json, Value};

struct AppState {
    /// When set (an explicit path argument), the dashboard is pinned to this
    /// one file and the CWD scan is bypassed.
    explicit: Option<PathBuf>,
}

/// `backtest_result_*.json` files in the current directory, newest first. The
/// timestamp in the name sorts lexicographically, so reverse order is newest.
fn result_files() -> Vec<String> {
    let mut files: Vec<String> = std::fs::read_dir(".")
        .into_iter()
        .flatten()
        .flatten()
        .map(|e| e.file_name().to_string_lossy().into_owned())
        .filter(|n| n.starts_with("backtest_result_") && n.ends_with(".json"))
        .collect();
    files.sort();
    files.reverse();
    files
}

/// Resolve the CWD result file to serve. A `file` request is honored only if it
/// is one of the scanned names (which are plain basenames), so the query can't
/// escape the directory; otherwise the newest result is used.
fn resolve_cwd_result(file: Option<&str>) -> Option<PathBuf> {
    let available = result_files();
    if let Some(name) = file {
        if available.iter().any(|f| f == name) {
            return Some(PathBuf::from(name));
        }
    }
    available.into_iter().next().map(PathBuf::from)
}

/// Read and parse one result file, tagging it with the name it was loaded from.
fn load_result(path: &PathBuf) -> Result<Value, String> {
    let content =
        std::fs::read_to_string(path).map_err(|e| format!("could not read {path:?}: {e}"))?;
    let mut result: Value =
        serde_json::from_str(&content).map_err(|e| format!("{path:?} is not valid JSON: {e}"))?;
    // Indexing a `Value` that is not an object panics, and the router has no
    // CatchPanicLayer — a truncated or hand-edited file holding a bare array
    // would unwind the handler and reset the connection instead of producing
    // the 500 the caller handles.
    let Some(obj) = result.as_object_mut() else {
        return Err(format!("{path:?} is not a backtest result object"));
    };
    obj.insert(
        "source_file".to_string(),
        json!(path.file_name().and_then(|n| n.to_str()).unwrap_or_default()),
    );
    Ok(result)
}

/// The list of selectable results and which one is served by default.
async fn get_results(State(state): State<Arc<AppState>>) -> Json<Value> {
    let files: Vec<String> = match &state.explicit {
        Some(p) => vec![p.file_name().and_then(|n| n.to_str()).unwrap_or_default().to_string()],
        None => result_files(),
    };
    let selected = files.first().cloned();
    Json(json!({ "files": files, "selected": selected }))
}

#[derive(Deserialize)]
struct ResultQuery {
    file: Option<String>,
}

async fn get_result(
    Query(params): Query<ResultQuery>,
    State(state): State<Arc<AppState>>,
) -> Response {
    let path = match &state.explicit {
        Some(p) => p.clone(),
        None => match resolve_cwd_result(params.file.as_deref()) {
            Some(p) => p,
            None => return (StatusCode::NOT_FOUND, "no backtest result found").into_response(),
        },
    };
    match load_result(&path) {
        Ok(v) => Json(v).into_response(),
        Err(msg) => (StatusCode::INTERNAL_SERVER_ERROR, msg).into_response(),
    }
}

#[tokio::main]
async fn main() {
    let explicit = std::env::args().nth(1).map(PathBuf::from);

    // In directory mode, fail fast if there is nothing to show yet.
    if explicit.is_none() && result_files().is_empty() {
        eprintln!(
            "No backtest result found. Run a backtest first (e.g. \
             `cargo run --example ema_cross -- backtester/tests/fixtures`) \
             or pass a path: `cargo run -p ui -- result.json`"
        );
        std::process::exit(1);
    }

    let state = Arc::new(AppState { explicit });
    let app = Router::new()
        .route("/", get(|| async { Html(include_str!("index.html")) }))
        .route("/api/results", get(get_results))
        .route("/api/result", get(get_result))
        .with_state(state);

    let port: u16 = std::env::var("PORT").ok().and_then(|p| p.parse().ok()).unwrap_or(3001);
    let listener = tokio::net::TcpListener::bind(("0.0.0.0", port)).await.unwrap();
    println!("Serving backtest results on http://localhost:{port}");
    axum::serve(listener, app).await.unwrap();
}

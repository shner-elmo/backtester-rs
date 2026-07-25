use axum::body::Body;
use data_viz::Timeframe;
use http::{Request, StatusCode};
use serde_json::Value;
use tower::ServiceExt;

// Defaults to the small committed fixture (AAPL, Jan 2023). Regenerate with:
//   cargo run -p data-viz --example make_test_fixture
// Override with DATA_DIR to run against the full dataset.
fn data_path() -> String {
    std::env::var("DATA_DIR").unwrap_or_else(|_| "tests/fixtures".to_string())
}

async fn app() -> axum::Router {
    data_viz::create_app(data_path()).await
}

async fn get(app: axum::Router, uri: &str) -> (StatusCode, Vec<u8>) {
    let response =
        app.oneshot(Request::builder().uri(uri).body(Body::empty()).unwrap()).await.unwrap();
    let status = response.status();
    let bytes = axum::body::to_bytes(response.into_body(), usize::MAX).await.unwrap().to_vec();
    (status, bytes)
}

async fn get_json(app: axum::Router, uri: &str) -> (StatusCode, Value) {
    let (status, bytes) = get(app, uri).await;
    let json = serde_json::from_slice(&bytes).expect("response body should be valid JSON");
    (status, json)
}

// ── / ─────────────────────────────────────────────────────────────────────────

#[tokio::test]
async fn index_returns_200_html() {
    let (status, body) = get(app().await, "/").await;
    assert_eq!(status, StatusCode::OK);
    let html = String::from_utf8(body).unwrap();
    assert!(html.contains("<title>data-viz</title>"));
}

// ── /api/bars ─────────────────────────────────────────────────────────────────

#[tokio::test]
async fn bars_fields_present() {
    // Fixture has minute bars — check field shape without asserting exact count.
    let (status, json) = get_json(app().await, "/api/bars?symbol=AAPL&timeframe=minute").await;
    assert_eq!(status, StatusCode::OK);
    let arr = json.as_array().expect("expected JSON array");
    assert!(!arr.is_empty(), "no data — check tests/fixtures or set DATA_DIR");
    let first = &arr[0];
    assert!(first["time"].is_number(), "time should be Unix seconds (number)");
    assert!(first["open"].is_number());
    assert!(first["high"].is_number());
    assert!(first["low"].is_number());
    assert!(first["close"].is_number());
    assert!(first["volume"].is_number());
    assert!(first["is_extended"].is_boolean());
}

#[tokio::test]
async fn bars_matches_load_bars() {
    let expected = data_viz::load_bars(&data_path(), "AAPL", None, None, Timeframe::Minute).await;
    assert!(!expected.is_empty(), "no data — check tests/fixtures or set DATA_DIR");

    let (status, json) = get_json(app().await, "/api/bars?symbol=AAPL&timeframe=minute").await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(json.as_array().unwrap().len(), expected.len());
}

#[tokio::test]
async fn bars_date_range_filters_correctly() {
    let path = data_path();
    let start = chrono::NaiveDate::from_ymd_opt(2023, 1, 1).unwrap();
    let end = chrono::NaiveDate::from_ymd_opt(2023, 1, 31).unwrap();
    let expected =
        data_viz::load_bars(&path, "AAPL", Some(start), Some(end), Timeframe::Minute).await;
    assert!(!expected.is_empty(), "no data — check tests/fixtures or set DATA_DIR");

    let (status, json) = get_json(
        app().await,
        "/api/bars?symbol=AAPL&start=2023-01-01&end=2023-01-31&timeframe=minute",
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(json.as_array().unwrap().len(), expected.len());
}

#[tokio::test]
async fn bars_unknown_symbol_returns_empty_array() {
    let (status, json) = get_json(app().await, "/api/bars?symbol=DOESNOTEXIST").await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(json, Value::Array(vec![]));
}

#[tokio::test]
async fn bars_missing_symbol_returns_400() {
    let (status, _) = get(app().await, "/api/bars").await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
}

// ── /api/indicators ───────────────────────────────────────────────────────────

async fn bar_count() -> usize {
    data_viz::load_bars(&data_path(), "AAPL", None, None, Timeframe::Minute).await.len()
}

#[tokio::test]
async fn indicators_ema_returns_one_point_per_bar() {
    let n = bar_count().await;
    assert!(n > 0, "no data — check tests/fixtures or set DATA_DIR");

    let (status, json) =
        get_json(app().await, "/api/indicators?symbol=AAPL&type=ema&period=20&timeframe=minute")
            .await;
    assert_eq!(status, StatusCode::OK);
    let arr = json["ema"].as_array().expect("expected \"ema\" key");
    assert_eq!(arr.len(), n);
    assert!(arr[0]["time"].is_number());
    assert!(arr[0]["value"].is_number());
}

#[tokio::test]
async fn indicators_sma_returns_one_point_per_bar() {
    let n = bar_count().await;
    assert!(n > 0, "no data — check tests/fixtures or set DATA_DIR");

    let (status, json) =
        get_json(app().await, "/api/indicators?symbol=AAPL&type=sma&period=20&timeframe=minute")
            .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(json["sma"].as_array().expect("expected \"sma\" key").len(), n);
}

#[tokio::test]
async fn indicators_rsi_returns_one_point_per_bar() {
    let n = bar_count().await;
    assert!(n > 0, "no data — check tests/fixtures or set DATA_DIR");

    let (status, json) =
        get_json(app().await, "/api/indicators?symbol=AAPL&type=rsi&period=14&timeframe=minute")
            .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(json["rsi"].as_array().expect("expected \"rsi\" key").len(), n);
}

#[tokio::test]
async fn indicators_macd_returns_one_point_per_bar() {
    let n = bar_count().await;
    assert!(n > 0, "no data — check tests/fixtures or set DATA_DIR");

    let (status, json) =
        get_json(app().await, "/api/indicators?symbol=AAPL&type=macd&timeframe=minute").await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(json["macd"].as_array().unwrap().len(), n);
    assert_eq!(json["signal"].as_array().unwrap().len(), n);
    assert_eq!(json["histogram"].as_array().unwrap().len(), n);
}

#[tokio::test]
async fn indicators_bbands_returns_one_point_per_bar() {
    let n = bar_count().await;
    assert!(n > 0, "no data — check tests/fixtures or set DATA_DIR");

    let (status, json) =
        get_json(app().await, "/api/indicators?symbol=AAPL&type=bbands&period=20&timeframe=minute")
            .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(json["upper"].as_array().unwrap().len(), n);
    assert_eq!(json["middle"].as_array().unwrap().len(), n);
    assert_eq!(json["lower"].as_array().unwrap().len(), n);
}

#[tokio::test]
async fn indicators_unknown_type_returns_empty_object() {
    let (status, json) = get_json(app().await, "/api/indicators?symbol=AAPL&type=bogus").await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(json, Value::Object(Default::default()));
}

#[tokio::test]
async fn indicators_missing_type_returns_400() {
    let (status, _) = get(app().await, "/api/indicators?symbol=AAPL").await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn bars_daily_timeframe_returns_data() {
    let (status, json) = get_json(
        app().await,
        "/api/bars?symbol=AAPL&start=2023-01-01&end=2023-01-31&timeframe=daily",
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let arr = json.as_array().expect("expected JSON array");
    assert!(!arr.is_empty(), "daily bars returned empty — SQL or type issue");
    let first = &arr[0];
    eprintln!("daily first bar: {first}");
    assert!(first["time"].is_number());
    assert!(first["is_extended"].as_bool() == Some(false));
}


#[tokio::test]
async fn daily_stream_via_sse_endpoint() {
    // This tests the SSE path indirectly: we read the full SSE response body and check
    // that the `done` event carries a non-zero count. If it's 0, there's an error in
    // the streaming aggregation path (execute_stream vs collect).
    let (_, bytes) = get(
        app().await,
        "/api/stream/bars?symbol=AAPL&start=2023-01-01&end=2023-01-31&timeframe=daily",
    )
    .await;
    let body = String::from_utf8(bytes).unwrap();
    eprintln!("SSE body:\n{body}");
    assert!(body.contains("event:done") || body.contains("event: done"), "no done event");
    // Extract count from done event
    let count_line = body
        .lines()
        .skip_while(|l| !l.starts_with("event:done") && !l.starts_with("event: done"))
        .nth(1)
        .unwrap_or("");
    eprintln!("done data line: {count_line}");
    assert!(!count_line.contains(r#""count":0"#), "count=0 means streaming aggregation failed — check for error events above");
}

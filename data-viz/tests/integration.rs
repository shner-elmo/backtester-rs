use axum::body::Body;
use chrono::{DateTime, Datelike, NaiveDate, Timelike, Utc};
use data_viz::{OhlcBar, Timeframe};
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

async fn get(uri: &str) -> (StatusCode, Vec<u8>) {
    let response = app()
        .await
        .oneshot(Request::builder().uri(uri).body(Body::empty()).unwrap())
        .await
        .unwrap();
    let status = response.status();
    let bytes = axum::body::to_bytes(response.into_body(), usize::MAX).await.unwrap().to_vec();
    (status, bytes)
}

async fn get_json(uri: &str) -> (StatusCode, Value) {
    let (status, bytes) = get(uri).await;
    let json = serde_json::from_slice(&bytes).expect("response body should be valid JSON");
    (status, json)
}

async fn load(tf: Timeframe) -> Vec<OhlcBar> {
    let bars = data_viz::load_bars(&data_path(), "AAPL", None, None, tf)
        .await
        .expect("query should succeed");
    assert!(!bars.is_empty(), "no data — check tests/fixtures or set DATA_DIR");
    bars
}

/// Bar times are US/Eastern wall-clock seconds, so reading them back as UTC yields the
/// market-local calendar date and clock time.
fn et(bar: &OhlcBar) -> DateTime<Utc> {
    DateTime::from_timestamp(bar.time, 0).expect("time should be a valid timestamp")
}

// ── / ─────────────────────────────────────────────────────────────────────────

#[tokio::test]
async fn index_returns_200_html() {
    let (status, body) = get("/").await;
    assert_eq!(status, StatusCode::OK);
    assert!(String::from_utf8(body).unwrap().contains("<title>data-viz</title>"));
}

// ── /api/bars ─────────────────────────────────────────────────────────────────

#[tokio::test]
async fn bars_response_shape() {
    let (status, json) = get_json("/api/bars?symbol=AAPL&tf=min1").await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(json["symbol"], "AAPL");
    assert_eq!(json["timeframe"], "min1");
    assert_eq!(json["indicators"], Value::Object(Default::default()));

    let bars = json["bars"].as_array().expect("expected \"bars\" array");
    assert!(!bars.is_empty(), "no data — check tests/fixtures or set DATA_DIR");
    for field in ["time", "open", "high", "low", "close", "volume"] {
        assert!(bars[0][field].is_number(), "{field} should be a number");
    }
    assert!(bars[0]["is_extended"].is_boolean());
}

#[tokio::test]
async fn bars_matches_load_bars() {
    let expected = load(Timeframe::Min1).await;
    let (status, json) = get_json("/api/bars?symbol=AAPL&tf=min1").await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(json["bars"].as_array().unwrap().len(), expected.len());
}

#[tokio::test]
async fn bars_date_range_filters_to_eastern_days() {
    let day = NaiveDate::from_ymd_opt(2023, 1, 4).unwrap();
    let bars = data_viz::load_bars(&data_path(), "AAPL", Some(day), Some(day), Timeframe::Min1)
        .await
        .unwrap();
    assert!(!bars.is_empty(), "no data for 2023-01-04 — check tests/fixtures");
    // An inclusive single-day range must not bleed into the neighbouring ET days, which
    // a UTC-midnight filter would do (16:00–20:00 ET is 21:00–01:00 UTC).
    for bar in &bars {
        assert_eq!(et(bar).date_naive(), day, "bar outside the requested ET day: {bar:?}");
    }
}

#[tokio::test]
async fn bars_unknown_symbol_returns_empty() {
    let (status, json) = get_json("/api/bars?symbol=DOESNOTEXIST").await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(json["bars"], Value::Array(vec![]));
}

#[tokio::test]
async fn bars_missing_symbol_returns_400() {
    let (status, _) = get("/api/bars").await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
}

// ── Timezone ──────────────────────────────────────────────────────────────────

#[tokio::test]
async fn minute_times_are_eastern_wall_clock() {
    let bars = load(Timeframe::Min1).await;

    // The regular session opens at 09:30 ET and the last regular bar starts at 15:59.
    let regular: Vec<_> = bars.iter().filter(|b| !b.is_extended).map(et).collect();
    assert!(!regular.is_empty(), "fixture has no regular-session bars");
    let min = regular.iter().map(|t| t.hour() * 60 + t.minute()).min().unwrap();
    let max = regular.iter().map(|t| t.hour() * 60 + t.minute()).max().unwrap();
    assert_eq!(min, 9 * 60 + 30, "regular session should start at 09:30 ET");
    assert_eq!(max, 15 * 60 + 59, "regular session should end at 15:59 ET");

    // Extended bars must fall strictly outside it.
    for bar in bars.iter().filter(|b| b.is_extended) {
        let mins = et(bar).hour() * 60 + et(bar).minute();
        let regular = (9 * 60 + 30)..(16 * 60);
        assert!(!regular.contains(&mins), "extended bar inside the session: {bar:?}");
    }
}

#[tokio::test]
async fn daily_buckets_are_eastern_midnights_matching_intraday() {
    let daily = load(Timeframe::Daily).await;
    let minute = load(Timeframe::Min1).await;

    for bar in &daily {
        assert_eq!(bar.time.rem_euclid(86_400), 0, "daily bucket is not an ET midnight: {bar:?}");
        assert!(!bar.is_extended, "daily bars have no extended flag");
    }

    // The check the old UTC `DATE_TRUNC` failed: after-hours minutes (up to 01:00 UTC the
    // next day) must stay inside their own ET day.
    let day = et(&daily[0]).date_naive();
    let same_day: Vec<_> = minute.iter().filter(|b| et(b).date_naive() == day).collect();
    assert!(!same_day.is_empty());
    let high = same_day.iter().map(|b| b.high).fold(f64::MIN, f64::max);
    let low = same_day.iter().map(|b| b.low).fold(f64::MAX, f64::min);
    assert_eq!(daily[0].high, high, "daily high should be the ET day's intraday high");
    assert_eq!(daily[0].low, low, "daily low should be the ET day's intraday low");
    assert_eq!(daily[0].open, same_day.first().unwrap().open);
    assert_eq!(daily[0].close, same_day.last().unwrap().close);
}

#[tokio::test]
async fn weekly_buckets_are_eastern_mondays() {
    let weekly = load(Timeframe::Weekly).await;
    for bar in &weekly {
        let t = et(bar);
        assert_eq!(t.weekday(), chrono::Weekday::Mon, "weekly bucket is not a Monday: {t}");
        assert_eq!(bar.time.rem_euclid(86_400), 0, "weekly bucket is not midnight: {t}");
    }
    let daily = load(Timeframe::Daily).await;
    assert!(weekly.len() < daily.len(), "weekly should aggregate multiple daily bars");
    assert_eq!(
        weekly.iter().map(|b| b.volume).sum::<f64>(),
        daily.iter().map(|b| b.volume).sum::<f64>(),
        "weekly and daily should cover the same minutes",
    );
}

#[tokio::test]
async fn min5_buckets_align_to_five_minute_marks() {
    let bars = load(Timeframe::Min5).await;
    for bar in &bars {
        assert_eq!(et(bar).minute() % 5, 0, "5m bucket off-grid: {}", et(bar));
    }
}

// ── Indicators ────────────────────────────────────────────────────────────────

#[tokio::test]
async fn indicators_are_index_aligned_and_keyed_by_spec() {
    let (status, json) =
        get_json("/api/bars?symbol=AAPL&tf=daily&ind=ema:20,ema:50,macd:12:26:9").await;
    assert_eq!(status, StatusCode::OK);
    let n = json["bars"].as_array().unwrap().len();
    assert!(n > 0, "no data — check tests/fixtures or set DATA_DIR");

    let ind = &json["indicators"];
    // Same indicator, different periods: distinct keys with distinct values.
    let fast = ind["ema:20"]["ema"].as_array().expect("expected \"ema:20\"");
    let slow = ind["ema:50"]["ema"].as_array().expect("expected \"ema:50\"");
    assert_eq!(fast.len(), n);
    assert_eq!(slow.len(), n);
    assert_ne!(fast.last(), slow.last(), "ema:20 and ema:50 should not collide");

    for line in ["macd", "signal", "histogram"] {
        assert_eq!(ind["macd:12:26:9"][line].as_array().unwrap().len(), n, "{line} length");
    }
}

#[tokio::test]
async fn indicators_bbands_and_rsi_lines() {
    let (status, json) = get_json("/api/bars?symbol=AAPL&tf=daily&ind=bbands:20:2.0,rsi:14").await;
    assert_eq!(status, StatusCode::OK);
    let n = json["bars"].as_array().unwrap().len();
    for line in ["upper", "middle", "lower"] {
        assert_eq!(json["indicators"]["bbands:20:2.0"][line].as_array().unwrap().len(), n);
    }
    assert_eq!(json["indicators"]["rsi:14"]["rsi"].as_array().unwrap().len(), n);
}

#[tokio::test]
async fn indicators_unknown_spec_is_skipped() {
    let (status, json) = get_json("/api/bars?symbol=AAPL&tf=daily&ind=bogus:5,ema:20").await;
    assert_eq!(status, StatusCode::OK);
    let ind = json["indicators"].as_object().unwrap();
    assert_eq!(ind.len(), 1);
    assert!(ind.contains_key("ema:20"));
}

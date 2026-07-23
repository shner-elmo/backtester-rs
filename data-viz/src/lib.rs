use std::{collections::HashMap, sync::Arc};

use axum::{
    extract::{Query, State},
    http::StatusCode,
    response::{Html, IntoResponse, Json, Response},
    routing::get,
    Router,
};
use backtester::data::{load_ticker_map, sorted_parquet_files};
use chrono::{NaiveDate, TimeZone, Utc};
use datafusion::{
    arrow::array::{Float64Array, TimestampNanosecondArray},
    datasource::{
        file_format::parquet::ParquetFormat,
        listing::{ListingOptions, ListingTable, ListingTableConfig, ListingTableUrl},
    },
    prelude::*,
    scalar::ScalarValue,
};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use ta::{
    indicators::{
        BollingerBands, ExponentialMovingAverage, MovingAverageConvergenceDivergence,
        RelativeStrengthIndex, SimpleMovingAverage,
    },
    Next,
};

#[derive(Debug, Clone, Serialize)]
pub struct OhlcBar {
    pub time: String,
    pub open: f64,
    pub high: f64,
    pub low: f64,
    pub close: f64,
}

struct AppState {
    ctx: Arc<SessionContext>,
    ticker_map: HashMap<u16, String>,
}

async fn make_ctx_and_ticker_map(data_root: &str) -> (SessionContext, HashMap<u16, String>) {
    let ticker_map =
        load_ticker_map(data_root).unwrap_or_else(|e| panic!("failed to load ticker map: {e}"));
    let ctx = SessionContext::new();
    let minute_dir = format!("{}/minute", data_root);
    let files = sorted_parquet_files(&minute_dir);
    assert!(!files.is_empty(), "no parquet files found under {}", minute_dir);
    let table_urls: Vec<ListingTableUrl> =
        files.iter().map(|p| ListingTableUrl::parse(p.to_str().unwrap()).unwrap()).collect();
    let format = Arc::new(ParquetFormat::default());
    let listing_opts = ListingOptions::new(format).with_file_extension(".parquet");
    let schema = listing_opts
        .infer_schema(&ctx.state(), &table_urls[0])
        .await
        .unwrap_or_else(|e| panic!("schema inference failed: {}", e));
    let config = ListingTableConfig::new_with_multi_paths(table_urls)
        .with_listing_options(listing_opts)
        .with_schema(schema);
    let table = ListingTable::try_new(config).unwrap();
    ctx.register_table("bars", Arc::new(table))
        .unwrap_or_else(|e| panic!("failed to register table: {}", e));
    tracing::info!(
        "Registered parquet table 'bars' ({} tickers, {} files) from {}/minute",
        ticker_map.len(),
        files.len(),
        data_root,
    );
    (ctx, ticker_map)
}

pub async fn create_app(data_root: String) -> Router {
    let (ctx, ticker_map) = make_ctx_and_ticker_map(&data_root).await;
    let state = Arc::new(AppState { ctx: Arc::new(ctx), ticker_map });
    Router::new()
        .route("/", get(index))
        .route("/api/bars", get(bars))
        .route("/api/indicators", get(indicators))
        .with_state(state)
}

pub async fn load_daily_bars(
    data_root: &str,
    symbol: &str,
    start: Option<NaiveDate>,
    end: Option<NaiveDate>,
) -> Vec<OhlcBar> {
    let (ctx, ticker_map) = make_ctx_and_ticker_map(data_root).await;
    query_bars(&ctx, &ticker_map, symbol, start, end).await
}

async fn query_bars(
    ctx: &SessionContext,
    ticker_map: &HashMap<u16, String>,
    symbol: &str,
    start: Option<NaiveDate>,
    end: Option<NaiveDate>,
) -> Vec<OhlcBar> {
    let Some((&ticker_id, _)) = ticker_map.iter().find(|(_, v)| v.as_str() == symbol) else {
        tracing::warn!("symbol '{}' not found in ticker map", symbol);
        return vec![];
    };

    let mut df = ctx
        .table("bars")
        .await
        .unwrap()
        .filter(col("ticker").eq(lit(ScalarValue::UInt16(Some(ticker_id)))))
        .unwrap();

    if let Some(s) = start {
        let ts_ns = s.and_hms_opt(0, 0, 0).unwrap().and_utc().timestamp_nanos_opt().unwrap();
        df = df
            .filter(
                col("window_start").gt_eq(lit(ScalarValue::TimestampNanosecond(Some(ts_ns), None))),
            )
            .unwrap();
    }
    if let Some(e) = end {
        let ts_ns = e
            .and_hms_nano_opt(23, 59, 59, 999_999_999)
            .unwrap()
            .and_utc()
            .timestamp_nanos_opt()
            .unwrap();
        df = df
            .filter(
                col("window_start").lt_eq(lit(ScalarValue::TimestampNanosecond(Some(ts_ns), None))),
            )
            .unwrap();
    }

    let batches = df
        .select_columns(&["open", "high", "low", "close", "window_start"])
        .unwrap()
        // .sort(vec![col("window_start").sort(true, true)])
        // .unwrap()
        .collect()
        .await
        .unwrap();

    tracing::info!(
        "symbol='{}' got {} batches (start={:?} end={:?})",
        symbol,
        batches.len(),
        start,
        end,
    );

    batches
        .into_iter()
        .flat_map(|batch| {
            let open = batch
                .column_by_name("open")
                .unwrap()
                .as_any()
                .downcast_ref::<Float64Array>()
                .unwrap();
            let high = batch
                .column_by_name("high")
                .unwrap()
                .as_any()
                .downcast_ref::<Float64Array>()
                .unwrap();
            let low = batch
                .column_by_name("low")
                .unwrap()
                .as_any()
                .downcast_ref::<Float64Array>()
                .unwrap();
            let close = batch
                .column_by_name("close")
                .unwrap()
                .as_any()
                .downcast_ref::<Float64Array>()
                .unwrap();
            let ts = batch
                .column_by_name("window_start")
                .unwrap()
                .as_any()
                .downcast_ref::<TimestampNanosecondArray>()
                .unwrap();

            (0..batch.num_rows())
                .filter_map(|i| {
                    let ns = ts.value(i);
                    let dt = Utc
                        .timestamp_opt(ns / 1_000_000_000, (ns % 1_000_000_000) as u32)
                        .single()?;
                    Some(OhlcBar {
                        time: dt.to_rfc3339(),
                        open: open.value(i),
                        high: high.value(i),
                        low: low.value(i),
                        close: close.value(i),
                    })
                })
                .collect::<Vec<_>>()
        })
        .collect()
}

// ── Handlers ─────────────────────────────────────────────────────────────────

async fn index() -> Html<&'static str> {
    Html(include_str!("index.html"))
}

#[derive(Deserialize)]
struct BarsParams {
    symbol: Option<String>,
    start: Option<NaiveDate>,
    end: Option<NaiveDate>,
}

async fn bars(Query(params): Query<BarsParams>, State(state): State<Arc<AppState>>) -> Response {
    let Some(symbol) = params.symbol else {
        return (StatusCode::BAD_REQUEST, "missing required query param: symbol").into_response();
    };
    let bars = query_bars(&state.ctx, &state.ticker_map, &symbol, params.start, params.end).await;
    Json(bars).into_response()
}

#[derive(Deserialize)]
struct IndicatorParams {
    symbol: Option<String>,
    #[serde(rename = "type")]
    indicator_type: Option<String>,
    /// Lookback for `ema`/`sma` (default 20), `rsi` (14), and `bbands` (20).
    period: Option<usize>,
    /// MACD periods (defaults 12 / 26 / 9).
    fast: Option<usize>,
    slow: Option<usize>,
    signal: Option<usize>,
    /// Bollinger band width in standard deviations (default 2.0).
    mult: Option<f64>,
}

async fn indicators(
    Query(params): Query<IndicatorParams>,
    State(state): State<Arc<AppState>>,
) -> Response {
    let Some(symbol) = params.symbol.as_deref() else {
        return (StatusCode::BAD_REQUEST, "missing required query param: symbol").into_response();
    };
    let Some(indicator_type) = params.indicator_type.as_deref() else {
        return (StatusCode::BAD_REQUEST, "missing required query param: type").into_response();
    };
    let bars = query_bars(&state.ctx, &state.ticker_map, symbol, None, None).await;
    Json(compute_indicator(&bars, indicator_type, &params)).into_response()
}

/// A rejected parameter set (e.g. `period=0`) returns this payload instead of
/// panicking the handler.
fn invalid_params() -> Value {
    json!({ "error": "invalid indicator parameters" })
}

fn compute_indicator(bars: &[OhlcBar], indicator_type: &str, p: &IndicatorParams) -> Value {
    match indicator_type {
        "ema" => {
            let Ok(mut ind) = ExponentialMovingAverage::new(p.period.unwrap_or(20)) else {
                return invalid_params();
            };
            let pts: Vec<Value> =
                bars.iter().map(|b| json!({"time": b.time, "value": ind.next(b.close)})).collect();
            json!({ "ema": pts })
        }
        "sma" => {
            let Ok(mut ind) = SimpleMovingAverage::new(p.period.unwrap_or(20)) else {
                return invalid_params();
            };
            let pts: Vec<Value> =
                bars.iter().map(|b| json!({"time": b.time, "value": ind.next(b.close)})).collect();
            json!({ "sma": pts })
        }
        "rsi" => {
            let Ok(mut ind) = RelativeStrengthIndex::new(p.period.unwrap_or(14)) else {
                return invalid_params();
            };
            let pts: Vec<Value> =
                bars.iter().map(|b| json!({"time": b.time, "value": ind.next(b.close)})).collect();
            json!({ "rsi": pts })
        }
        "macd" => {
            let Ok(mut ind) = MovingAverageConvergenceDivergence::new(
                p.fast.unwrap_or(12),
                p.slow.unwrap_or(26),
                p.signal.unwrap_or(9),
            ) else {
                return invalid_params();
            };
            let (mut macd_pts, mut sig_pts, mut hist_pts) = (vec![], vec![], vec![]);
            for b in bars {
                let out = ind.next(b.close);
                macd_pts.push(json!({"time": b.time, "value": out.macd}));
                sig_pts.push(json!({"time": b.time, "value": out.signal}));
                hist_pts.push(json!({"time": b.time, "value": out.histogram}));
            }
            json!({ "macd": macd_pts, "signal": sig_pts, "histogram": hist_pts })
        }
        "bbands" => {
            let Ok(mut ind) = BollingerBands::new(p.period.unwrap_or(20), p.mult.unwrap_or(2.0))
            else {
                return invalid_params();
            };
            let (mut upper, mut middle, mut lower) = (vec![], vec![], vec![]);
            for b in bars {
                let out = ind.next(b.close);
                upper.push(json!({"time": b.time, "value": out.upper}));
                middle.push(json!({"time": b.time, "value": out.average}));
                lower.push(json!({"time": b.time, "value": out.lower}));
            }
            json!({ "upper": upper, "middle": middle, "lower": lower })
        }
        unknown => {
            tracing::warn!("unknown indicator type '{}'", unknown);
            json!({})
        }
    }
}

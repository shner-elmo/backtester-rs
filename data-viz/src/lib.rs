use std::{collections::HashMap, sync::Arc};

use axum::{
    extract::{Query, State},
    http::StatusCode,
    response::{
        sse::{Event, KeepAlive, Sse},
        Html, IntoResponse, Json, Response,
    },
    routing::get,
    Router,
};
use backtester::data::load_ticker_map;
use chrono::{Datelike, NaiveDate, TimeZone, Timelike, Utc};
use chrono_tz::US::Eastern;
use datafusion::{
    arrow::{
        array::{Float64Array, TimestampNanosecondArray},
        datatypes::DataType,
    },
    datasource::{
        file_format::parquet::ParquetFormat,
        listing::{ListingOptions, ListingTable, ListingTableConfig, ListingTableUrl},
    },
    execution::context::SessionConfig,
    prelude::*,
};
use futures_util::StreamExt;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use ta::{
    indicators::{
        BollingerBands, ExponentialMovingAverage, MovingAverageConvergenceDivergence,
        RelativeStrengthIndex, SimpleMovingAverage,
    },
    Next,
};

// ── Timeframe ─────────────────────────────────────────────────────────────────

#[derive(Debug, Clone, Copy, Deserialize, PartialEq, Eq, Default)]
#[serde(rename_all = "lowercase")]
pub enum Timeframe {
    #[default]
    Minute,
    Min5,
    Min15,
    Hour1,
    Hour4,
    Daily,
}

impl Timeframe {
    fn interval_sql(self) -> Option<&'static str> {
        match self {
            Timeframe::Min5 => Some("5 minutes"),
            Timeframe::Min15 => Some("15 minutes"),
            Timeframe::Hour1 => Some("1 hour"),
            Timeframe::Hour4 => Some("4 hours"),
            _ => None,
        }
    }

    fn is_aggregated(self) -> bool {
        !matches!(self, Timeframe::Minute)
    }

    fn has_extended_hours(self) -> bool {
        !matches!(self, Timeframe::Daily)
    }
}

// ── Bar type ──────────────────────────────────────────────────────────────────

#[derive(Debug, Clone, Serialize)]
pub struct OhlcBar {
    pub time: i64,
    pub open: f64,
    pub high: f64,
    pub low: f64,
    pub close: f64,
    pub volume: f64,
    pub is_extended: bool,
}

// ── Inline indicator state ────────────────────────────────────────────────────

// Encoded as "ema:20", "sma:20", "rsi:14", "macd:12:26:9", "bbands:20:2.0"
enum IndState {
    Ema(ExponentialMovingAverage),
    Sma(SimpleMovingAverage),
    Rsi(RelativeStrengthIndex),
    Macd(MovingAverageConvergenceDivergence),
    Bbands(BollingerBands),
}

impl IndState {
    fn from_spec(spec: &str) -> Option<Self> {
        let p: Vec<&str> = spec.splitn(5, ':').collect();
        let pu = |i: usize| -> usize { p.get(i).and_then(|s| s.parse().ok()).unwrap_or(0) };
        let pf = |i: usize| -> f64 { p.get(i).and_then(|s| s.parse().ok()).unwrap_or(0.0) };
        match *p.first()? {
            "ema" => ExponentialMovingAverage::new(pu(1).max(2)).ok().map(Self::Ema),
            "sma" => SimpleMovingAverage::new(pu(1).max(2)).ok().map(Self::Sma),
            "rsi" => RelativeStrengthIndex::new(pu(1).max(2)).ok().map(Self::Rsi),
            "macd" => {
                MovingAverageConvergenceDivergence::new(pu(1).max(2), pu(2).max(2), pu(3).max(2))
                    .ok()
                    .map(Self::Macd)
            }
            "bbands" => BollingerBands::new(pu(1).max(2), pf(2).max(0.1)).ok().map(Self::Bbands),
            _ => None,
        }
    }

    fn apply_to(&mut self, bars: &[OhlcBar], out: &mut serde_json::Map<String, Value>) {
        match self {
            Self::Ema(ind) => {
                let pts: Vec<Value> = bars
                    .iter()
                    .map(|b| json!({"time": b.time, "value": ind.next(b.close)}))
                    .collect();
                out.insert("ema".into(), Value::Array(pts));
            }
            Self::Sma(ind) => {
                let pts: Vec<Value> = bars
                    .iter()
                    .map(|b| json!({"time": b.time, "value": ind.next(b.close)}))
                    .collect();
                out.insert("sma".into(), Value::Array(pts));
            }
            Self::Rsi(ind) => {
                let pts: Vec<Value> = bars
                    .iter()
                    .map(|b| json!({"time": b.time, "value": ind.next(b.close)}))
                    .collect();
                out.insert("rsi".into(), Value::Array(pts));
            }
            Self::Macd(ind) => {
                let (mut ml, mut sl, mut hl) = (vec![], vec![], vec![]);
                for b in bars {
                    let o = ind.next(b.close);
                    ml.push(json!({"time": b.time, "value": o.macd}));
                    sl.push(json!({"time": b.time, "value": o.signal}));
                    hl.push(json!({"time": b.time, "value": o.histogram}));
                }
                out.insert("macd".into(), Value::Array(ml));
                out.insert("signal".into(), Value::Array(sl));
                out.insert("histogram".into(), Value::Array(hl));
            }
            Self::Bbands(ind) => {
                let (mut ul, mut ml, mut ll) = (vec![], vec![], vec![]);
                for b in bars {
                    let o = ind.next(b.close);
                    ul.push(json!({"time": b.time, "value": o.upper}));
                    ml.push(json!({"time": b.time, "value": o.average}));
                    ll.push(json!({"time": b.time, "value": o.lower}));
                }
                out.insert("upper".into(), Value::Array(ul));
                out.insert("middle".into(), Value::Array(ml));
                out.insert("lower".into(), Value::Array(ll));
            }
        }
    }
}

// ── App state ─────────────────────────────────────────────────────────────────

struct AppState {
    ctx: Arc<SessionContext>,
    ticker_map: Arc<HashMap<u16, String>>,
}

async fn make_ctx_and_ticker_map(data_root: &str) -> (SessionContext, HashMap<u16, String>) {
    let ticker_map = load_ticker_map(&format!("{data_root}/minute"))
        .unwrap_or_else(|e| panic!("failed to load ticker map: {e}"));

    // target_partitions(1): sequential reads preserve sort order for minute streaming (no ORDER BY).
    let config = SessionConfig::new().with_target_partitions(1);
    let ctx = SessionContext::new_with_config(config);

    let minute_url = ListingTableUrl::parse(format!("{}/minute/", data_root))
        .unwrap_or_else(|e| panic!("invalid minute dir URL: {e}"));
    let format = Arc::new(ParquetFormat::default());
    let listing_opts =
        ListingOptions::new(format).with_file_extension(".parquet").with_table_partition_cols(
            vec![("year".to_string(), DataType::Int32), ("month".to_string(), DataType::Int32)],
        );
    let cfg = ListingTableConfig::new(minute_url)
        .with_listing_options(listing_opts)
        .infer_schema(&ctx.state())
        .await
        .unwrap_or_else(|e| panic!("schema inference failed: {}", e));
    let table = ListingTable::try_new(cfg).unwrap();
    ctx.register_table("bars", Arc::new(table))
        .unwrap_or_else(|e| panic!("failed to register table: {}", e));
    tracing::info!(
        "Registered Hive-partitioned parquet table 'bars' ({} tickers) from {}/minute",
        ticker_map.len(),
        data_root,
    );
    (ctx, ticker_map)
}

pub async fn create_app(data_root: String) -> Router {
    let (ctx, ticker_map) = make_ctx_and_ticker_map(&data_root).await;
    let state = Arc::new(AppState { ctx: Arc::new(ctx), ticker_map: Arc::new(ticker_map) });
    Router::new()
        .route("/", get(index))
        .route("/api/bars", get(bars))
        .route("/api/stream/bars", get(stream_bars))
        .route("/api/indicators", get(indicators))
        .with_state(state)
}

// ── SQL building ──────────────────────────────────────────────────────────────

fn build_conditions(
    ticker_id: u16,
    start: Option<NaiveDate>,
    end: Option<NaiveDate>,
) -> Vec<String> {
    let mut conds = vec![format!("ticker = {ticker_id}")];
    if let Some(s) = start {
        let (y, m) = (s.year(), s.month() as i32);
        conds.push(format!("(year > {y} OR (year = {y} AND month >= {m}))"));
        conds.push(format!("window_start >= to_timestamp('{}')", s.format("%Y-%m-%d")));
    }
    if let Some(e) = end {
        let next = e.succ_opt().unwrap_or(e);
        let (y, m) = (e.year(), e.month() as i32);
        conds.push(format!("(year < {y} OR (year = {y} AND month <= {m}))"));
        conds.push(format!("window_start < to_timestamp('{}')", next.format("%Y-%m-%d")));
    }
    conds
}

fn build_sql(
    ticker_id: u16,
    start: Option<NaiveDate>,
    end: Option<NaiveDate>,
    tf: Timeframe,
) -> String {
    let where_clause = build_conditions(ticker_id, start, end).join(" AND ");
    match tf {
        Timeframe::Minute => format!(
            "SELECT window_start, open, high, low, close, CAST(volume AS DOUBLE) AS volume \
             FROM bars WHERE {where_clause}"
        ),
        Timeframe::Daily => format!(
            "SELECT DATE_TRUNC('day', window_start) AS bucket, \
               FIRST_VALUE(open ORDER BY window_start) AS open, \
               MAX(high) AS high, MIN(low) AS low, \
               LAST_VALUE(close ORDER BY window_start) AS close, \
               CAST(SUM(volume) AS DOUBLE) AS volume \
             FROM bars WHERE {where_clause} GROUP BY bucket ORDER BY bucket"
        ),
        _ => {
            let interval = tf.interval_sql().unwrap();
            format!(
                "SELECT date_bin(INTERVAL '{interval}', window_start, TIMESTAMP '1970-01-01T00:00:00') AS bucket, \
                   FIRST_VALUE(open ORDER BY window_start) AS open, \
                   MAX(high) AS high, MIN(low) AS low, \
                   LAST_VALUE(close ORDER BY window_start) AS close, \
                   CAST(SUM(volume) AS DOUBLE) AS volume \
                 FROM bars WHERE {where_clause} GROUP BY bucket ORDER BY bucket"
            )
        }
    }
}

// ── Batch → bars ──────────────────────────────────────────────────────────────

fn batch_to_bars(
    batch: &datafusion::arrow::record_batch::RecordBatch,
    tf: Timeframe,
) -> Vec<OhlcBar> {
    let time_col = if tf.is_aggregated() { "bucket" } else { "window_start" };

    let ts = batch
        .column_by_name(time_col)
        .unwrap()
        .as_any()
        .downcast_ref::<TimestampNanosecondArray>()
        .unwrap();
    let open =
        batch.column_by_name("open").unwrap().as_any().downcast_ref::<Float64Array>().unwrap();
    let high =
        batch.column_by_name("high").unwrap().as_any().downcast_ref::<Float64Array>().unwrap();
    let low = batch.column_by_name("low").unwrap().as_any().downcast_ref::<Float64Array>().unwrap();
    let close =
        batch.column_by_name("close").unwrap().as_any().downcast_ref::<Float64Array>().unwrap();
    let volume =
        batch.column_by_name("volume").unwrap().as_any().downcast_ref::<Float64Array>().unwrap();

    let check_ext = tf.has_extended_hours();

    (0..batch.num_rows())
        .filter_map(|i| {
            let ns = ts.value(i);
            let utc =
                Utc.timestamp_opt(ns / 1_000_000_000, (ns % 1_000_000_000) as u32).single()?;
            let is_extended = if check_ext {
                let et = utc.with_timezone(&Eastern);
                let mins = et.hour() * 60 + et.minute();
                mins < 9 * 60 + 30 || mins >= 16 * 60
            } else {
                false
            };
            Some(OhlcBar {
                time: utc.timestamp(),
                open: open.value(i),
                high: high.value(i),
                low: low.value(i),
                close: close.value(i),
                volume: volume.value(i),
                is_extended,
            })
        })
        .collect()
}

// ── Shared query helper ───────────────────────────────────────────────────────

async fn query_bars(
    ctx: &SessionContext,
    ticker_map: &HashMap<u16, String>,
    symbol: &str,
    start: Option<NaiveDate>,
    end: Option<NaiveDate>,
    tf: Timeframe,
) -> Vec<OhlcBar> {
    let Some((&ticker_id, _)) = ticker_map.iter().find(|(_, v)| v.as_str() == symbol) else {
        tracing::warn!("symbol '{}' not found in ticker map", symbol);
        return vec![];
    };
    let sql = build_sql(ticker_id, start, end, tf);
    tracing::debug!("query: {}", sql);
    match ctx.sql(&sql).await {
        Ok(df) => match df.collect().await {
            Ok(batches) => batches.iter().flat_map(|b| batch_to_bars(b, tf)).collect(),
            Err(e) => {
                tracing::error!("collect error: {e}");
                vec![]
            }
        },
        Err(e) => {
            tracing::error!("sql error: {e}");
            vec![]
        }
    }
}

/// Public helper for integration tests.
pub async fn load_bars(
    data_root: &str,
    symbol: &str,
    start: Option<NaiveDate>,
    end: Option<NaiveDate>,
    tf: Timeframe,
) -> Vec<OhlcBar> {
    let (ctx, ticker_map) = make_ctx_and_ticker_map(data_root).await;
    query_bars(&ctx, &ticker_map, symbol, start, end, tf).await
}

// ── SSE streaming ─────────────────────────────────────────────────────────────

#[derive(Deserialize)]
struct BarsParams {
    symbol: Option<String>,
    start: Option<NaiveDate>,
    end: Option<NaiveDate>,
    #[serde(default)]
    timeframe: Timeframe,
    /// Repeatable indicator specs, e.g. `&ind=ema:20&ind=rsi:14`
    #[serde(default)]
    ind: Vec<String>,
}

async fn stream_bars(Query(p): Query<BarsParams>, State(state): State<Arc<AppState>>) -> Response {
    let Some(symbol) = p.symbol else {
        return (StatusCode::BAD_REQUEST, "missing required query param: symbol").into_response();
    };

    let Some((&ticker_id, _)) =
        state.ticker_map.iter().find(|(_, v)| v.as_str() == symbol.as_str())
    else {
        let s = async_stream::stream! {
            yield Ok::<Event, axum::BoxError>(
                Event::default().event("done").data(r#"{"count":0}"#)
            );
        };
        return Sse::new(s).keep_alive(KeepAlive::default()).into_response();
    };

    let sql = build_sql(ticker_id, p.start, p.end, p.timeframe);
    tracing::debug!("stream query: {}", sql);

    let ctx = Arc::clone(&state.ctx);
    let tf = p.timeframe;
    let ind_specs = p.ind;

    let sse_stream = async_stream::stream! {
        let mut ind_states: Vec<IndState> = ind_specs
            .iter()
            .filter_map(|s| IndState::from_spec(s))
            .collect();

        let df = match ctx.sql(&sql).await {
            Ok(df) => df,
            Err(e) => {
                tracing::error!("sql error: {e}");
                yield Ok::<Event, axum::BoxError>(
                    Event::default().event("error").data(format!(r#"{{"message":"{e}"}}"#))
                );
                return;
            }
        };

        let mut total = 0usize;

        if tf.is_aggregated() {
            // GROUP BY + ORDER BY must buffer everything before emitting; collect() is equivalent
            // to execute_stream() here but avoids the streaming overhead.
            match df.collect().await {
                Err(e) => {
                    tracing::error!("collect error: {e}");
                    yield Ok(Event::default().event("error")
                        .data(format!(r#"{{"message":"{e}"}}"#)));
                }
                Ok(batches) => {
                    let bars: Vec<OhlcBar> = batches.iter()
                        .flat_map(|b| batch_to_bars(b, tf))
                        .collect();
                    if !bars.is_empty() {
                        total = bars.len();
                        let mut ev = serde_json::Map::new();
                        ev.insert("bars".into(), json!(bars));
                        for state in &mut ind_states { state.apply_to(&bars, &mut ev); }
                        match serde_json::to_string(&Value::Object(ev)) {
                            Ok(data) => yield Ok(Event::default().event("bars").data(data)),
                            Err(e) => yield Ok(Event::default().event("error")
                                .data(format!(r#"{{"message":"serialize: {e}"}}"#))),
                        }
                    }
                }
            }
        } else {
            // Minute: true streaming — first batch arrives after first row group is decoded.
            let mut record_stream = match df.execute_stream().await {
                Ok(s) => s,
                Err(e) => {
                    tracing::error!("execute_stream error: {e}");
                    yield Ok(Event::default().event("error")
                        .data(format!(r#"{{"message":"{e}"}}"#)));
                    return;
                }
            };

            while let Some(batch_result) = record_stream.next().await {
                match batch_result {
                    Ok(batch) => {
                        let bars = batch_to_bars(&batch, tf);
                        if bars.is_empty() { continue; }
                        total += bars.len();

                        let mut ev = serde_json::Map::new();
                        ev.insert("bars".into(), json!(bars));
                        for state in &mut ind_states { state.apply_to(&bars, &mut ev); }

                        match serde_json::to_string(&Value::Object(ev)) {
                            Ok(data) => yield Ok(Event::default().event("bars").data(data)),
                            Err(e) => yield Ok(Event::default().event("error")
                                .data(format!(r#"{{"message":"serialize: {e}"}}"#))),
                        }
                    }
                    Err(e) => {
                        tracing::error!("batch error: {e}");
                        yield Ok(Event::default().event("error")
                            .data(format!(r#"{{"message":"{e}"}}"#)));
                        break;
                    }
                }
            }
        }

        yield Ok(Event::default().event("done").data(format!(r#"{{"count":{total}}}"#)));
    };

    Sse::new(sse_stream).keep_alive(KeepAlive::default()).into_response()
}

// ── HTTP handlers ─────────────────────────────────────────────────────────────

async fn index() -> Html<&'static str> {
    Html(include_str!("index.html"))
}

async fn bars(Query(p): Query<BarsParams>, State(state): State<Arc<AppState>>) -> Response {
    let Some(symbol) = p.symbol else {
        return (StatusCode::BAD_REQUEST, "missing required query param: symbol").into_response();
    };
    let bars =
        query_bars(&state.ctx, &state.ticker_map, &symbol, p.start, p.end, p.timeframe).await;
    Json(bars).into_response()
}

// ── Indicators (kept for tests / direct API use) ──────────────────────────────

#[derive(Deserialize)]
struct IndicatorParams {
    symbol: Option<String>,
    #[serde(rename = "type")]
    indicator_type: Option<String>,
    start: Option<NaiveDate>,
    end: Option<NaiveDate>,
    #[serde(default)]
    timeframe: Timeframe,
    period: Option<usize>,
    fast: Option<usize>,
    slow: Option<usize>,
    signal: Option<usize>,
    mult: Option<f64>,
}

async fn indicators(
    Query(p): Query<IndicatorParams>,
    State(state): State<Arc<AppState>>,
) -> Response {
    let Some(symbol) = p.symbol.as_deref() else {
        return (StatusCode::BAD_REQUEST, "missing required query param: symbol").into_response();
    };
    let Some(indicator_type) = p.indicator_type.as_deref() else {
        return (StatusCode::BAD_REQUEST, "missing required query param: type").into_response();
    };
    let bars = query_bars(&state.ctx, &state.ticker_map, symbol, p.start, p.end, p.timeframe).await;
    Json(compute_indicator(&bars, indicator_type, &p)).into_response()
}

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

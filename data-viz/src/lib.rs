use std::{
    collections::{BTreeMap, HashMap},
    sync::Arc,
};

use axum::{
    extract::{Query, State},
    http::StatusCode,
    response::{Html, IntoResponse, Json, Response},
    routing::get,
    Router,
};
use backtester::data::load_ticker_map;
use chrono::{DateTime, Datelike, NaiveDate, TimeZone, Timelike, Utc};
use chrono_tz::US::Eastern;
use datafusion::{
    arrow::{
        array::{Float64Array, TimestampNanosecondArray},
        datatypes::DataType,
        record_batch::RecordBatch,
    },
    datasource::{
        file_format::parquet::ParquetFormat,
        listing::{ListingOptions, ListingTable, ListingTableConfig, ListingTableUrl},
    },
    prelude::*,
};
use serde::{Deserialize, Serialize};
use serde_json::json;
use ta::{
    indicators::{
        BollingerBands, ExponentialMovingAverage, MovingAverageConvergenceDivergence,
        RelativeStrengthIndex, SimpleMovingAverage,
    },
    Next,
};

/// `window_start` reinterpreted in US/Eastern, so `date_trunc` cuts days and weeks on
/// market-local boundaries rather than UTC ones.
const ET_WINDOW_START: &str =
    r#"arrow_cast(window_start, 'Timestamp(Nanosecond, Some("America/New_York"))')"#;

/// The regular US equity session, 09:30–16:00 ET, as minutes past ET midnight.
const REGULAR_SESSION: std::ops::Range<u32> = (9 * 60 + 30)..(16 * 60);

/// A range bound as an explicit UTC instant. The dataset stamps `window_start` with a
/// timezone (`Timestamp(ns, Some("US/Eastern"))`), and DataFusion coerces a *naive*
/// literal into the column's zone — which would silently shift every bound by the ET
/// offset. Pinning the literal to UTC keeps the comparison an instant comparison.
fn utc_literal(t: DateTime<Utc>) -> String {
    format!(
        r#"arrow_cast('{}Z', 'Timestamp(Nanosecond, Some("UTC"))')"#,
        t.format("%Y-%m-%dT%H:%M:%S")
    )
}

// ── Timeframe ─────────────────────────────────────────────────────────────────

#[derive(Debug, Clone, Copy, Deserialize, Serialize, PartialEq, Eq, Default)]
#[serde(rename_all = "lowercase")]
pub enum Timeframe {
    #[default]
    Min1,
    Min5,
    Daily,
    Weekly,
}

impl Timeframe {
    /// SQL expression for the bucket a minute row belongs to, or `None` for raw minutes.
    fn bucket_expr(self) -> Option<String> {
        match self {
            Timeframe::Min1 => None,
            // ET offsets are whole hours, so a 5-minute bin over UTC is already ET-aligned.
            Timeframe::Min5 => Some(
                "date_bin(INTERVAL '5 minutes', window_start, TIMESTAMP '1970-01-01T00:00:00')"
                    .to_string(),
            ),
            Timeframe::Daily => Some(format!("date_trunc('day', {ET_WINDOW_START})")),
            Timeframe::Weekly => Some(format!("date_trunc('week', {ET_WINDOW_START})")),
        }
    }

    fn has_extended_hours(self) -> bool {
        matches!(self, Timeframe::Min1 | Timeframe::Min5)
    }
}

// ── Bar type ──────────────────────────────────────────────────────────────────

#[derive(Debug, Clone, Serialize)]
pub struct OhlcBar {
    /// US/Eastern wall-clock seconds: the ET local time reinterpreted as a Unix
    /// timestamp. Lightweight Charts renders numeric times in UTC, so feeding it this
    /// shifted value makes the axis read market-local time. Never treat it as an instant.
    pub time: i64,
    pub open: f64,
    pub high: f64,
    pub low: f64,
    pub close: f64,
    pub volume: f64,
    pub is_extended: bool,
}

// ── App state ─────────────────────────────────────────────────────────────────

struct AppState {
    ctx: SessionContext,
    /// Symbol → encoded ticker id, the direction every request needs.
    ticker_ids: HashMap<String, u16>,
}

async fn make_ctx_and_ticker_ids(data_root: &str) -> (SessionContext, HashMap<String, u16>) {
    let ticker_map = load_ticker_map(&format!("{data_root}/minute"))
        .unwrap_or_else(|e| panic!("failed to load ticker map: {e}"));
    let ticker_ids: HashMap<String, u16> =
        ticker_map.into_iter().map(|(id, sym)| (sym, id)).collect();

    // Every month-file holds all ~19k tickers interleaved in time order, so row-group
    // stats can't prune on `ticker`. Filter pushdown (off by default) at least turns the
    // ticker predicate into a row filter, so OHLCV pages for other tickers are never
    // decoded — the difference between scanning and decoding ~38M rows per month.
    let config = SessionConfig::new()
        .set_bool("datafusion.execution.parquet.pushdown_filters", true)
        .set_bool("datafusion.execution.parquet.reorder_filters", true);
    let ctx = SessionContext::new_with_config(config);
    let minute_url = ListingTableUrl::parse(format!("{data_root}/minute/"))
        .unwrap_or_else(|e| panic!("invalid minute dir URL: {e}"));
    let listing_opts = ListingOptions::new(Arc::new(ParquetFormat::default()))
        .with_file_extension(".parquet")
        .with_table_partition_cols(vec![
            ("year".to_string(), DataType::Int32),
            ("month".to_string(), DataType::Int32),
        ]);
    let cfg = ListingTableConfig::new(minute_url)
        .with_listing_options(listing_opts)
        .infer_schema(&ctx.state())
        .await
        .unwrap_or_else(|e| panic!("schema inference failed: {e}"));
    let table = ListingTable::try_new(cfg).unwrap();
    ctx.register_table("bars", Arc::new(table))
        .unwrap_or_else(|e| panic!("failed to register table: {e}"));
    tracing::info!(
        "Registered Hive-partitioned parquet table 'bars' ({} tickers) from {data_root}/minute",
        ticker_ids.len(),
    );
    (ctx, ticker_ids)
}

pub async fn create_app(data_root: String) -> Router {
    let (ctx, ticker_ids) = make_ctx_and_ticker_ids(&data_root).await;
    Router::new()
        .route("/", get(index))
        .route("/api/bars", get(bars))
        .with_state(Arc::new(AppState { ctx, ticker_ids }))
}

// ── SQL building ──────────────────────────────────────────────────────────────

/// The UTC instant of midnight US/Eastern on `date` — the boundary a user means when
/// they type a date into the toolbar.
fn et_midnight_utc(date: NaiveDate) -> Option<DateTime<Utc>> {
    let naive = date.and_hms_opt(0, 0, 0)?;
    Eastern.from_local_datetime(&naive).earliest().map(|dt| dt.with_timezone(&Utc))
}

fn build_conditions(
    ticker_id: u16,
    start: Option<NaiveDate>,
    end: Option<NaiveDate>,
) -> Vec<String> {
    let mut conds = vec![format!("ticker = {ticker_id}")];
    // Hive year/month predicates prune whole month-files; the timestamp predicates then
    // trim the partial months at each edge.
    if let Some(s) = start.and_then(et_midnight_utc) {
        let (y, m) = (s.year(), s.month() as i32);
        conds.push(format!("(year > {y} OR (year = {y} AND month >= {m}))"));
        conds.push(format!("window_start >= {}", utc_literal(s)));
    }
    if let Some(e) = end.and_then(|d| d.succ_opt()).and_then(et_midnight_utc) {
        let (y, m) = (e.year(), e.month() as i32);
        conds.push(format!("(year < {y} OR (year = {y} AND month <= {m}))"));
        conds.push(format!("window_start < {}", utc_literal(e)));
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
    match tf.bucket_expr() {
        None => format!(
            "SELECT window_start AS bucket, open, high, low, close, \
               CAST(volume AS DOUBLE) AS volume \
             FROM bars WHERE {where_clause} ORDER BY bucket"
        ),
        Some(bucket) => format!(
            "SELECT {bucket} AS bucket, \
               FIRST_VALUE(open ORDER BY window_start) AS open, \
               MAX(high) AS high, MIN(low) AS low, \
               LAST_VALUE(close ORDER BY window_start) AS close, \
               CAST(SUM(volume) AS DOUBLE) AS volume \
             FROM bars WHERE {where_clause} GROUP BY bucket ORDER BY bucket"
        ),
    }
}

// ── Batch → bars ──────────────────────────────────────────────────────────────

fn column<'a, T: 'static>(batch: &'a RecordBatch, name: &str) -> Result<&'a T, String> {
    batch
        .column_by_name(name)
        .and_then(|c| c.as_any().downcast_ref::<T>())
        .ok_or_else(|| format!("column '{name}' missing or of unexpected type"))
}

fn batch_to_bars(batch: &RecordBatch, tf: Timeframe) -> Result<Vec<OhlcBar>, String> {
    let ts = column::<TimestampNanosecondArray>(batch, "bucket")?;
    let open = column::<Float64Array>(batch, "open")?;
    let high = column::<Float64Array>(batch, "high")?;
    let low = column::<Float64Array>(batch, "low")?;
    let close = column::<Float64Array>(batch, "close")?;
    let volume = column::<Float64Array>(batch, "volume")?;
    let check_ext = tf.has_extended_hours();

    Ok((0..batch.num_rows())
        .map(|i| {
            let et = Utc.timestamp_nanos(ts.value(i)).with_timezone(&Eastern);
            let mins = et.hour() * 60 + et.minute();
            OhlcBar {
                time: et.naive_local().and_utc().timestamp(),
                open: open.value(i),
                high: high.value(i),
                low: low.value(i),
                close: close.value(i),
                volume: volume.value(i),
                is_extended: check_ext && !REGULAR_SESSION.contains(&mins),
            }
        })
        .collect())
}

// ── Query ─────────────────────────────────────────────────────────────────────

async fn query_bars(
    ctx: &SessionContext,
    ticker_ids: &HashMap<String, u16>,
    symbol: &str,
    start: Option<NaiveDate>,
    end: Option<NaiveDate>,
    tf: Timeframe,
) -> Result<Vec<OhlcBar>, String> {
    let Some(&ticker_id) = ticker_ids.get(symbol) else {
        tracing::warn!("symbol '{symbol}' not found in ticker map");
        return Ok(vec![]);
    };
    let sql = build_sql(ticker_id, start, end, tf);
    tracing::debug!("query: {sql}");
    let df = ctx.sql(&sql).await.map_err(|e| e.to_string())?;
    let batches = df.collect().await.map_err(|e| e.to_string())?;
    batches.iter().try_fold(Vec::new(), |mut acc, b| {
        acc.extend(batch_to_bars(b, tf)?);
        Ok(acc)
    })
}

/// Bars straight from Parquet, without the HTTP layer.
pub async fn load_bars(
    data_root: &str,
    symbol: &str,
    start: Option<NaiveDate>,
    end: Option<NaiveDate>,
    tf: Timeframe,
) -> Result<Vec<OhlcBar>, String> {
    let (ctx, ticker_ids) = make_ctx_and_ticker_ids(data_root).await;
    query_bars(&ctx, &ticker_ids, symbol, start, end, tf).await
}

// ── Indicators ────────────────────────────────────────────────────────────────

/// One indicator's lines, each index-aligned with the bars it was computed over.
type IndLines = BTreeMap<&'static str, Vec<f64>>;

/// Computes one indicator from a spec: `ema:20`, `sma:20`, `rsi:14`, `macd:12:26:9`,
/// `bbands:20:2.0`. Returns `None` for an unknown or unbuildable spec.
fn indicator_lines(spec: &str, bars: &[OhlcBar]) -> Option<IndLines> {
    let p: Vec<&str> = spec.splitn(5, ':').collect();
    let period = |i: usize, default: usize| {
        p.get(i).and_then(|s| s.parse::<usize>().ok()).unwrap_or(default).max(2)
    };
    let mult = |i: usize, default: f64| {
        p.get(i).and_then(|s| s.parse::<f64>().ok()).unwrap_or(default).max(0.1)
    };

    let mut lines = IndLines::new();
    macro_rules! single {
        ($ind:expr, $name:literal) => {{
            let mut ind = $ind.ok()?;
            lines.insert($name, bars.iter().map(|b| ind.next(b.close)).collect());
        }};
    }

    match *p.first()? {
        "ema" => single!(ExponentialMovingAverage::new(period(1, 20)), "ema"),
        "sma" => single!(SimpleMovingAverage::new(period(1, 20)), "sma"),
        "rsi" => single!(RelativeStrengthIndex::new(period(1, 14)), "rsi"),
        "macd" => {
            let mut ind =
                MovingAverageConvergenceDivergence::new(period(1, 12), period(2, 26), period(3, 9))
                    .ok()?;
            let (mut macd, mut signal, mut histogram) = (vec![], vec![], vec![]);
            for b in bars {
                let out = ind.next(b.close);
                macd.push(out.macd);
                signal.push(out.signal);
                histogram.push(out.histogram);
            }
            lines.insert("macd", macd);
            lines.insert("signal", signal);
            lines.insert("histogram", histogram);
        }
        "bbands" => {
            let mut ind = BollingerBands::new(period(1, 20), mult(2, 2.0)).ok()?;
            let (mut upper, mut middle, mut lower) = (vec![], vec![], vec![]);
            for b in bars {
                let out = ind.next(b.close);
                upper.push(out.upper);
                middle.push(out.average);
                lower.push(out.lower);
            }
            lines.insert("upper", upper);
            lines.insert("middle", middle);
            lines.insert("lower", lower);
        }
        unknown => {
            tracing::warn!("unknown indicator spec '{unknown}'");
            return None;
        }
    }
    Some(lines)
}

// ── HTTP handlers ─────────────────────────────────────────────────────────────

async fn index() -> Html<&'static str> {
    Html(include_str!("index.html"))
}

#[derive(Deserialize)]
struct BarsParams {
    symbol: Option<String>,
    start: Option<NaiveDate>,
    end: Option<NaiveDate>,
    #[serde(default)]
    tf: Timeframe,
    /// Comma-separated indicator specs, e.g. `&ind=ema:20,rsi:14`. Repeated query keys
    /// are not an option here: axum's `Query` is `serde_urlencoded`, which has no
    /// sequence support.
    ind: Option<String>,
}

#[derive(Serialize)]
struct BarsResponse {
    symbol: String,
    timeframe: Timeframe,
    bars: Vec<OhlcBar>,
    /// Keyed by the requested spec, so `ema:20` and `ema:50` stay distinct.
    indicators: BTreeMap<String, IndLines>,
}

async fn bars(Query(p): Query<BarsParams>, State(state): State<Arc<AppState>>) -> Response {
    let Some(symbol) = p.symbol else {
        return (StatusCode::BAD_REQUEST, "missing required query param: symbol").into_response();
    };
    let symbol = symbol.trim().to_uppercase();

    let bars = match query_bars(&state.ctx, &state.ticker_ids, &symbol, p.start, p.end, p.tf).await
    {
        Ok(bars) => bars,
        Err(e) => {
            tracing::error!("query failed: {e}");
            return (StatusCode::INTERNAL_SERVER_ERROR, Json(json!({ "error": e })))
                .into_response();
        }
    };

    let indicators = p
        .ind
        .as_deref()
        .unwrap_or_default()
        .split(',')
        .map(str::trim)
        .filter(|spec| !spec.is_empty())
        .filter_map(|spec| Some((spec.to_string(), indicator_lines(spec, &bars)?)))
        .collect();

    Json(BarsResponse { symbol, timeframe: p.tf, bars, indicators }).into_response()
}

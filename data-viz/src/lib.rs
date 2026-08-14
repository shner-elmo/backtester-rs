use std::{collections::BTreeMap, sync::Arc};

use axum::{
    extract::{Query, State},
    http::StatusCode,
    response::{Html, IntoResponse, Json, Response},
    routing::get,
    Router,
};
use backtester::{consolidator::ConsolidatorPeriod, data::load_ticker_map};
use chrono::NaiveDate;
use rustc_hash::FxHashSet;
use serde::{Deserialize, Serialize};
use serde_json::json;
use ta::{
    indicators::{
        BollingerBands, ExponentialMovingAverage, MovingAverageConvergenceDivergence,
        RelativeStrengthIndex, SimpleMovingAverage,
    },
    Next,
};

mod viz_strategy;

use viz_strategy::run_viz;

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
    /// The backtester consolidator period that produces this timeframe, or
    /// `None` for raw minutes (which need no consolidation).
    fn to_period(self) -> Option<ConsolidatorPeriod> {
        match self {
            Timeframe::Min1 => None,
            Timeframe::Min5 => Some(ConsolidatorPeriod::Minutes(5)),
            Timeframe::Daily => Some(ConsolidatorPeriod::Daily),
            Timeframe::Weekly => Some(ConsolidatorPeriod::Weekly),
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
    /// The backtester data root — the `minute/` tree that holds the Parquet and
    /// `encoded_tickers.json`. Every request runs a strategy over it.
    data_path: String,
    /// The dataset's ticker names, so an unknown symbol is answered with an
    /// empty chart instead of paying for a whole scan that matches nothing.
    tickers: FxHashSet<String>,
}

/// The ticker names the dataset carries, upper-cased to match request symbols.
fn load_ticker_names(data_path: &str) -> FxHashSet<String> {
    load_ticker_map(data_path)
        .unwrap_or_else(|e| panic!("failed to load ticker map from {data_path}: {e}"))
        .into_values()
        .map(|name| name.to_uppercase())
        .collect()
}

pub async fn create_app(data_root: String) -> Router {
    let data_path = format!("{data_root}/minute");
    let tickers = load_ticker_names(&data_path);
    tracing::info!("Loaded {} tickers from {data_path}", tickers.len());
    Router::new()
        .route("/", get(index))
        .route("/api/bars", get(bars))
        .with_state(Arc::new(AppState { data_path, tickers }))
}

/// Bars straight from the engine, without the HTTP layer. Used by tests and the
/// examples.
pub async fn load_bars(
    data_root: &str,
    symbol: &str,
    start: Option<NaiveDate>,
    end: Option<NaiveDate>,
    tf: Timeframe,
) -> Result<Vec<OhlcBar>, String> {
    let data_path = format!("{data_root}/minute");
    let symbol = symbol.trim().to_uppercase();
    // The engine run is blocking and spawns decode threads, so keep it off the
    // async worker.
    tokio::task::spawn_blocking(move || run_viz(&data_path, &symbol, start, end, tf))
        .await
        .map_err(|e| e.to_string())?
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

    let bars: Vec<OhlcBar> = if state.tickers.contains(&symbol) {
        let (data_path, sym, tf) = (state.data_path.clone(), symbol.clone(), p.tf);
        let (start, end) = (p.start, p.end);
        match tokio::task::spawn_blocking(move || run_viz(&data_path, &sym, start, end, tf)).await {
            Ok(Ok(bars)) => bars,
            Ok(Err(e)) => {
                tracing::error!("chart run failed: {e}");
                return (StatusCode::INTERNAL_SERVER_ERROR, Json(json!({ "error": e })))
                    .into_response();
            }
            Err(e) => {
                tracing::error!("chart task failed: {e}");
                return (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    Json(json!({ "error": e.to_string() })),
                )
                    .into_response();
            }
        }
    } else {
        tracing::warn!("symbol '{symbol}' not found in ticker map");
        Vec::new()
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

use std::{
    collections::{BTreeMap, HashMap},
    sync::Arc,
};

use axum::{
    Router,
    extract::{Query, State},
    response::{Html, Json},
    routing::get,
};
use backtester::{
    bar::MarketSession,
    data::{iter_bars, load_ticker_map},
    indicators::{BollingerBands, Ema, Macd, Next, Rsi, Sma},
};
use chrono::NaiveDate;
use serde::{Deserialize, Serialize};

struct AppState {
    data_path: String,
}

#[derive(Serialize)]
struct OhlcvBar {
    time: String,
    open: f64,
    high: f64,
    low: f64,
    close: f64,
}

#[derive(Serialize, Clone)]
struct IndicatorPoint {
    time: String,
    value: f64,
}

#[derive(Deserialize)]
struct BarsQuery {
    symbol: String,
    start: Option<String>,
    end: Option<String>,
}

#[derive(Deserialize)]
struct IndicatorQuery {
    symbol: String,
    start: Option<String>,
    end: Option<String>,
    #[serde(rename = "type")]
    kind: String,
    period: Option<usize>,
    fast: Option<usize>,
    slow: Option<usize>,
    signal: Option<usize>,
}

fn parse_date(s: &str) -> Option<NaiveDate> {
    NaiveDate::parse_from_str(s, "%Y-%m-%d").ok()
}

fn load_daily_bars(
    data_path: &str,
    symbol: &str,
    start: Option<NaiveDate>,
    end: Option<NaiveDate>,
) -> Vec<(NaiveDate, f64, f64, f64, f64)> {
    let ticker_map = load_ticker_map(data_path);
    let mut daily: BTreeMap<NaiveDate, (f64, f64, f64, f64)> = BTreeMap::new();

    iter_bars(data_path, &ticker_map, |sym, bar| {
        if sym != symbol || bar.market_session != MarketSession::Main {
            return;
        }
        let date = bar.time.date_naive();
        if start.is_some_and(|s| date < s) || end.is_some_and(|e| date > e) {
            return;
        }
        daily
            .entry(date)
            .and_modify(|(_, h, l, c)| {
                *h = h.max(bar.high);
                *l = l.min(bar.low);
                *c = bar.close;
            })
            .or_insert((bar.open, bar.high, bar.low, bar.close));
    });

    daily.into_iter().map(|(d, (o, h, l, c))| (d, o, h, l, c)).collect()
}

async fn get_bars(
    State(state): State<Arc<AppState>>,
    Query(q): Query<BarsQuery>,
) -> Json<Vec<OhlcvBar>> {
    let start = q.start.as_deref().and_then(parse_date);
    let end = q.end.as_deref().and_then(parse_date);
    let bars = load_daily_bars(&state.data_path, &q.symbol, start, end)
        .into_iter()
        .map(|(date, o, h, l, c)| OhlcvBar { time: date.to_string(), open: o, high: h, low: l, close: c })
        .collect();
    Json(bars)
}

async fn get_indicators(
    State(state): State<Arc<AppState>>,
    Query(q): Query<IndicatorQuery>,
) -> Json<HashMap<String, Vec<IndicatorPoint>>> {
    let start = q.start.as_deref().and_then(parse_date);
    let end = q.end.as_deref().and_then(parse_date);
    let bars = load_daily_bars(&state.data_path, &q.symbol, start, end);
    let period = q.period.unwrap_or(20);
    let mut result: HashMap<String, Vec<IndicatorPoint>> = HashMap::new();

    match q.kind.as_str() {
        "ema" => {
            if let Ok(mut ind) = Ema::new(period) {
                result.insert(
                    "ema".into(),
                    bars.iter().map(|(d, _, _, _, c)| IndicatorPoint { time: d.to_string(), value: ind.next(*c) }).collect(),
                );
            }
        }
        "sma" => {
            if let Ok(mut ind) = Sma::new(period) {
                result.insert(
                    "sma".into(),
                    bars.iter().map(|(d, _, _, _, c)| IndicatorPoint { time: d.to_string(), value: ind.next(*c) }).collect(),
                );
            }
        }
        "rsi" => {
            if let Ok(mut ind) = Rsi::new(period) {
                result.insert(
                    "rsi".into(),
                    bars.iter().map(|(d, _, _, _, c)| IndicatorPoint { time: d.to_string(), value: ind.next(*c) }).collect(),
                );
            }
        }
        "macd" => {
            let fast = q.fast.unwrap_or(12);
            let slow = q.slow.unwrap_or(26);
            let signal = q.signal.unwrap_or(9);
            if let Ok(mut ind) = Macd::new(fast, slow, signal) {
                let mut macd_pts = Vec::new();
                let mut sig_pts = Vec::new();
                let mut hist_pts = Vec::new();
                for (d, _, _, _, c) in &bars {
                    let out = ind.next(*c);
                    let t = d.to_string();
                    macd_pts.push(IndicatorPoint { time: t.clone(), value: out.macd });
                    sig_pts.push(IndicatorPoint { time: t.clone(), value: out.signal });
                    hist_pts.push(IndicatorPoint { time: t, value: out.histogram });
                }
                result.insert("macd".into(), macd_pts);
                result.insert("signal".into(), sig_pts);
                result.insert("histogram".into(), hist_pts);
            }
        }
        "bbands" => {
            if let Ok(mut ind) = BollingerBands::new(period, 2.0) {
                let mut upper = Vec::new();
                let mut middle = Vec::new();
                let mut lower = Vec::new();
                for (d, _, _, _, c) in &bars {
                    let out = ind.next(*c);
                    let t = d.to_string();
                    upper.push(IndicatorPoint { time: t.clone(), value: out.upper });
                    middle.push(IndicatorPoint { time: t.clone(), value: out.average });
                    lower.push(IndicatorPoint { time: t, value: out.lower });
                }
                result.insert("upper".into(), upper);
                result.insert("middle".into(), middle);
                result.insert("lower".into(), lower);
            }
        }
        _ => {}
    }

    Json(result)
}

async fn index() -> Html<&'static str> {
    Html(HTML)
}

#[tokio::main]
async fn main() {
    let args: Vec<String> = std::env::args().collect();
    let data_path =
        args.get(1).cloned().unwrap_or_else(|| "../../data/output/minute".to_string());

    let state = Arc::new(AppState { data_path });
    let app = Router::new()
        .route("/", get(index))
        .route("/api/bars", get(get_bars))
        .route("/api/indicators", get(get_indicators))
        .with_state(state);

    let listener = tokio::net::TcpListener::bind("0.0.0.0:3001").await.unwrap();
    println!("data-viz listening on http://localhost:3001");
    axum::serve(listener, app).await.unwrap();
}

const HTML: &str = r#"<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title>data-viz</title>
  <style>
    * { box-sizing: border-box; margin: 0; padding: 0; }
    body { background: #0d1117; color: #c9d1d9; font-family: 'Courier New', monospace; height: 100vh; display: flex; flex-direction: column; }
    .controls { padding: 12px 16px; background: #161b22; border-bottom: 1px solid #30363d; display: flex; gap: 12px; align-items: center; flex-wrap: wrap; }
    .controls label { color: #8b949e; font-size: 12px; }
    .controls input, .controls select { background: #0d1117; color: #c9d1d9; border: 1px solid #30363d; padding: 5px 8px; border-radius: 6px; font-size: 13px; font-family: inherit; }
    .controls input:focus, .controls select:focus { outline: none; border-color: #58a6ff; }
    .controls input[type="date"] { color-scheme: dark; }
    button { background: #238636; color: #fff; border: none; padding: 6px 16px; border-radius: 6px; font-size: 13px; cursor: pointer; font-family: inherit; }
    button:hover { background: #2ea043; }
    .charts { flex: 1; display: flex; flex-direction: column; overflow: hidden; }
    #chart-main { flex: 1; min-height: 0; }
    #chart-osc { height: 180px; border-top: 1px solid #30363d; display: none; }
    .status { padding: 4px 16px; font-size: 11px; color: #8b949e; background: #161b22; border-top: 1px solid #30363d; }
  </style>
</head>
<body>
  <div class="controls">
    <label>Symbol</label>
    <input id="symbol" value="AAPL" style="width:80px">
    <label>Start</label>
    <input id="start" type="date" value="2023-01-01">
    <label>End</label>
    <input id="end" type="date" value="2023-12-31">
    <label>Indicator</label>
    <select id="indicator">
      <option value="">None</option>
      <option value="ema">EMA</option>
      <option value="sma">SMA</option>
      <option value="bbands">Bollinger Bands</option>
      <option value="rsi">RSI</option>
      <option value="macd">MACD</option>
    </select>
    <label>Period</label>
    <input id="period" type="number" value="20" style="width:60px" min="2">
    <button onclick="load()">Load</button>
  </div>
  <div class="charts">
    <div id="chart-main"></div>
    <div id="chart-osc"></div>
  </div>
  <div class="status" id="status">Ready</div>

  <script src="https://unpkg.com/lightweight-charts@4.1.0/dist/lightweight-charts.standalone.production.js"></script>
  <script>
    const THEME = {
      layout: { background: { color: '#0d1117' }, textColor: '#8b949e' },
      grid: { vertLines: { color: '#161b22' }, horzLines: { color: '#161b22' } },
      crosshair: { mode: 1 },
      rightPriceScale: { borderColor: '#30363d' },
      timeScale: { borderColor: '#30363d', timeVisible: true },
    };

    const mainDiv = document.getElementById('chart-main');
    const oscDiv = document.getElementById('chart-osc');
    const statusEl = document.getElementById('status');

    let mainChart = LightweightCharts.createChart(mainDiv, { ...THEME, autoSize: true });
    let oscChart = null;
    let candleSeries = mainChart.addCandlestickSeries({
      upColor: '#3fb950', downColor: '#f85149',
      borderUpColor: '#3fb950', borderDownColor: '#f85149',
      wickUpColor: '#3fb950', wickDownColor: '#f85149',
    });
    let overlaySeries = [];

    function clearOverlays() {
      overlaySeries.forEach(s => mainChart.removeSeries(s));
      overlaySeries = [];
      if (oscChart) {
        oscChart.remove();
        oscChart = null;
        oscDiv.style.display = 'none';
      }
    }

    function status(msg) { statusEl.textContent = msg; }

    async function load() {
      const symbol = document.getElementById('symbol').value.trim().toUpperCase();
      const start = document.getElementById('start').value;
      const end = document.getElementById('end').value;
      const indicator = document.getElementById('indicator').value;
      const period = document.getElementById('period').value;

      status('Loading bars...');
      clearOverlays();

      try {
        const resp = await fetch(`/api/bars?symbol=${symbol}&start=${start}&end=${end}`);
        const bars = await resp.json();
        if (!bars.length) { status('No data for the given symbol / date range.'); return; }
        candleSeries.setData(bars);
        mainChart.timeScale().fitContent();
        status(`Loaded ${bars.length} daily bars for ${symbol}`);

        if (!indicator) return;

        status(`Computing ${indicator}(${period})...`);
        const iResp = await fetch(`/api/indicators?symbol=${symbol}&start=${start}&end=${end}&type=${indicator}&period=${period}`);
        const iData = await iResp.json();

        const OVERLAY_COLORS = { ema: '#58a6ff', sma: '#f0883e', upper: '#3fb950', middle: '#a371f7', lower: '#3fb950' };
        const OSC_COLORS    = { macd: '#58a6ff', signal: '#f0883e', histogram: '#3fb950', rsi: '#f85149' };
        const OSC_KEYS = ['macd', 'signal', 'histogram', 'rsi'];

        const overlayKeys = Object.keys(iData).filter(k => !OSC_KEYS.includes(k));
        const oscKeys     = Object.keys(iData).filter(k => OSC_KEYS.includes(k));

        for (const key of overlayKeys) {
          const s = mainChart.addLineSeries({
            color: OVERLAY_COLORS[key] ?? '#fff',
            lineWidth: 1,
            priceLineVisible: false,
            lastValueVisible: false,
          });
          s.setData(iData[key]);
          overlaySeries.push(s);
        }

        if (oscKeys.length) {
          oscDiv.style.display = 'block';
          oscChart = LightweightCharts.createChart(oscDiv, { ...THEME, autoSize: true });

          for (const key of oscKeys) {
            let s;
            if (key === 'histogram') {
              s = oscChart.addHistogramSeries({ color: OSC_COLORS[key], priceLineVisible: false, lastValueVisible: false });
            } else {
              s = oscChart.addLineSeries({ color: OSC_COLORS[key] ?? '#fff', lineWidth: 1, priceLineVisible: false, lastValueVisible: false });
            }
            s.setData(iData[key]);
          }

          oscChart.timeScale().fitContent();

          // Sync time scales bidirectionally
          mainChart.timeScale().subscribeVisibleLogicalRangeChange(range => {
            if (range && oscChart) oscChart.timeScale().setVisibleLogicalRange(range);
          });
          oscChart.timeScale().subscribeVisibleLogicalRangeChange(range => {
            if (range) mainChart.timeScale().setVisibleLogicalRange(range);
          });
        }

        status(`Loaded ${bars.length} bars · ${indicator.toUpperCase()}(${period})`);
      } catch (e) {
        status(`Error: ${e.message}`);
      }
    }

    // Trigger initial load on Enter in any input
    document.querySelectorAll('.controls input').forEach(el => {
      el.addEventListener('keydown', e => { if (e.key === 'Enter') load(); });
    });

    load();
  </script>
</body>
</html>
"#;

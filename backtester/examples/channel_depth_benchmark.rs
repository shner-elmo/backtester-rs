//! Reusable channel-depth benchmark for the parallel Parquet reader.
//!
//! The process prints exactly one JSON object to stdout. Measure process-level
//! CPU and peak RSS by wrapping the release binary in GNU `time -v`.
//!
//! ```text
//! BACKTEST_DATA_DIR=/path/to/data \
//!   target/release/examples/channel_depth_benchmark \
//!   noop-wide --depth 8 --threads 8 --start 2024-01-01 --end 2024-03-31
//! ```

use std::{cell::Cell, env, process, rc::Rc, time::Instant};

use backtester::{
    consolidator::ConsolidatorPeriod,
    indicators::{Ema, Next, Rsi},
    run_backtest, Algorithm, Context, LogConfig, Slice, Symbol, SymbolMap,
};
use chrono::{Datelike, NaiveDate};
use serde::Serialize;

const STARTING_CASH: f64 = 100_000.0;
const DEFAULT_DEPTH: usize = 8;
const DEFAULT_THREADS: usize = 8;
const NARROW_SYMBOLS: usize = 100;

#[derive(Clone, Copy, Debug, Serialize)]
#[serde(rename_all = "kebab-case")]
enum Workload {
    NoopWide,
    IndicatorsWide,
    ConsolidatorWide,
    MixedNarrow,
}

impl Workload {
    fn parse(value: &str) -> Result<Self, String> {
        match value {
            "noop-wide" => Ok(Self::NoopWide),
            "indicators-wide" => Ok(Self::IndicatorsWide),
            "consolidator-wide" => Ok(Self::ConsolidatorWide),
            "mixed-narrow" => Ok(Self::MixedNarrow),
            _ => Err(format!(
                "unknown workload {value:?}; expected noop-wide, indicators-wide, \
                 consolidator-wide, or mixed-narrow"
            )),
        }
    }

    fn is_wide(self) -> bool {
        !matches!(self, Self::MixedNarrow)
    }

    fn uses_indicators(self) -> bool {
        matches!(self, Self::IndicatorsWide | Self::MixedNarrow)
    }
}

#[derive(Debug)]
struct Config {
    workload: Workload,
    depth: usize,
    threads: usize,
    start: Option<NaiveDate>,
    end: Option<NaiveDate>,
}

impl Config {
    fn parse() -> Result<Self, String> {
        let mut args = env::args().skip(1);
        let Some(workload) = args.next() else {
            return Err(usage());
        };
        if matches!(workload.as_str(), "-h" | "--help") {
            println!("{}", usage());
            process::exit(0);
        }

        let mut config = Self {
            workload: Workload::parse(&workload)?,
            depth: DEFAULT_DEPTH,
            threads: DEFAULT_THREADS,
            start: None,
            end: None,
        };
        while let Some(flag) = args.next() {
            let value = args.next().ok_or_else(|| format!("missing value after {flag:?}"))?;
            match flag.as_str() {
                "--depth" => {
                    config.depth = parse_usize(&flag, &value)?;
                    if config.depth == 0 {
                        return Err("--depth must be greater than zero".to_string());
                    }
                }
                "--threads" => config.threads = parse_usize(&flag, &value)?,
                "--start" => config.start = Some(parse_date(&flag, &value)?),
                "--end" => config.end = Some(parse_date(&flag, &value)?),
                _ => return Err(format!("unknown option {flag:?}\n{}", usage())),
            }
        }
        if let (Some(start), Some(end)) = (config.start, config.end) {
            if start > end {
                return Err(format!("--start {start} is after --end {end}"));
            }
        }
        Ok(config)
    }
}

fn usage() -> String {
    "usage: channel_depth_benchmark WORKLOAD [--depth N] [--threads N] \
     [--start YYYY-MM-DD] [--end YYYY-MM-DD]\n\
     workloads: noop-wide | indicators-wide | consolidator-wide | mixed-narrow"
        .to_string()
}

fn parse_usize(flag: &str, value: &str) -> Result<usize, String> {
    value.parse().map_err(|error| format!("invalid {flag} value {value:?}: {error}"))
}

fn parse_date(flag: &str, value: &str) -> Result<NaiveDate, String> {
    value.parse().map_err(|error| format!("invalid {flag} value {value:?}: {error}"))
}

#[derive(Default)]
struct Measurements {
    ticks: Cell<u64>,
    bars: Cell<u64>,
    checksum: Cell<u64>,
    consolidator_callbacks: Cell<u64>,
    symbols: Cell<usize>,
}

impl Measurements {
    fn fold(&self, value: u64) {
        self.checksum.set(fold_checksum(self.checksum.get(), value));
    }

    fn count_tick(&self, data: &Slice) {
        self.ticks.set(self.ticks.get() + 1);
        self.bars.set(self.bars.get() + data.bars.len() as u64);
        self.fold(data.time.timestamp_nanos_opt().expect("benchmark timestamp is in range") as u64);
        self.fold(data.bars.len() as u64);
    }

    fn count_consolidated(&self, symbol: Symbol, bar: &backtester::bar::Bar) {
        self.consolidator_callbacks.set(self.consolidator_callbacks.get() + 1);
        self.fold(symbol.ticker_id() as u64);
        self.fold(bar.time.timestamp_nanos_opt().expect("benchmark timestamp is in range") as u64);
        self.fold(bar.close.to_bits());
        self.fold(bar.volume);
    }
}

fn fold_checksum(seed: u64, value: u64) -> u64 {
    let mut mixed = value.wrapping_add(0x9e37_79b9_7f4a_7c15);
    mixed = (mixed ^ (mixed >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
    mixed = (mixed ^ (mixed >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
    seed.rotate_left(11) ^ mixed ^ (mixed >> 31)
}

struct IndicatorState {
    ema_10: Ema,
    ema_30: Ema,
    rsi_14: Rsi,
}

impl IndicatorState {
    fn new() -> Self {
        Self {
            ema_10: Ema::new(10).expect("valid EMA period"),
            ema_30: Ema::new(30).expect("valid EMA period"),
            rsi_14: Rsi::new(14).expect("valid RSI period"),
        }
    }
}

struct BenchmarkAlgorithm {
    config: Config,
    measurements: Rc<Measurements>,
    indicators: SymbolMap<IndicatorState>,
}

impl Algorithm for BenchmarkAlgorithm {
    fn initialize(&mut self, ctx: &mut Context) {
        ctx.set_cash(STARTING_CASH);
        ctx.set_read_threads(self.config.threads);
        ctx.set_read_channel_depth(self.config.depth);
        ctx.set_log_config(LogConfig::none());
        if let Some(date) = self.config.start {
            ctx.set_start_date(date.year(), date.month(), date.day());
        }
        if let Some(date) = self.config.end {
            ctx.set_end_date(date.year(), date.month(), date.day());
        }

        let mut symbols = ctx.dataset_symbols();
        if !self.config.workload.is_wide() {
            symbols.truncate(NARROW_SYMBOLS);
        }
        self.measurements.symbols.set(symbols.len());

        for symbol in symbols {
            ctx.add_symbol(symbol);
            if self.config.workload.uses_indicators() {
                self.indicators.insert(symbol, IndicatorState::new());
            }
            match self.config.workload {
                Workload::ConsolidatorWide => {
                    let measurements = Rc::clone(&self.measurements);
                    ctx.consolidate(symbol, ConsolidatorPeriod::Minutes(15), move |bar| {
                        measurements.count_consolidated(symbol, bar);
                    });
                }
                Workload::MixedNarrow => {
                    let measurements = Rc::clone(&self.measurements);
                    ctx.consolidate(symbol, ConsolidatorPeriod::Hours(1), move |bar| {
                        measurements.count_consolidated(symbol, bar);
                    });
                }
                Workload::NoopWide | Workload::IndicatorsWide => {}
            }
        }
    }

    fn on_data(&mut self, _ctx: &mut Context, data: &Slice) {
        self.measurements.count_tick(data);
        if !self.config.workload.uses_indicators() {
            return;
        }

        for (&symbol, bar) in &data.bars {
            let state = self
                .indicators
                .get_mut(&symbol)
                .expect("every subscribed benchmark symbol has indicator state");
            let ema_10 = state.ema_10.next(bar.close);
            let ema_30 = state.ema_30.next(bar.close);
            let rsi_14 = state.rsi_14.next(bar.close);
            self.measurements.fold(symbol.ticker_id() as u64);
            self.measurements.fold(ema_10.to_bits());
            self.measurements.fold(ema_30.to_bits());
            self.measurements.fold(rsi_14.to_bits());
        }
    }
}

#[derive(Serialize)]
struct Report<'a> {
    schema_version: u32,
    workload: Workload,
    depth: usize,
    reader_threads: usize,
    start_date: Option<NaiveDate>,
    end_date: Option<NaiveDate>,
    symbols: usize,
    elapsed_seconds: f64,
    elapsed_nanoseconds: u128,
    ticks: u64,
    bars: u64,
    bars_per_second: f64,
    checksum: String,
    consolidator_callbacks: u64,
    initial_equity: f64,
    final_equity: f64,
    trades: usize,
    open_positions: usize,
    status: &'a str,
}

fn main() {
    let config = Config::parse().unwrap_or_else(|message| {
        eprintln!("{message}");
        process::exit(2);
    });
    let measurements = Rc::new(Measurements::default());
    let workload = config.workload;
    let depth = config.depth;
    let threads = config.threads;
    let start_date = config.start;
    let end_date = config.end;
    let algorithm = BenchmarkAlgorithm {
        config,
        measurements: Rc::clone(&measurements),
        indicators: SymbolMap::default(),
    };

    let started = Instant::now();
    let result = run_backtest(algorithm).unwrap_or_else(|error| {
        eprintln!("backtest failed: {error}");
        process::exit(1);
    });
    let elapsed = started.elapsed();
    let seconds = elapsed.as_secs_f64();
    let report = Report {
        schema_version: 1,
        workload,
        depth,
        reader_threads: threads,
        start_date,
        end_date,
        symbols: measurements.symbols.get(),
        elapsed_seconds: seconds,
        elapsed_nanoseconds: elapsed.as_nanos(),
        ticks: measurements.ticks.get(),
        bars: measurements.bars.get(),
        bars_per_second: measurements.bars.get() as f64 / seconds,
        checksum: format!("{:016x}", measurements.checksum.get()),
        consolidator_callbacks: measurements.consolidator_callbacks.get(),
        initial_equity: result.initial_cash,
        final_equity: result.final_equity,
        trades: result.trades.len(),
        open_positions: result.open_positions.len(),
        status: "ok",
    };
    println!("{}", serde_json::to_string(&report).expect("benchmark report serializes"));
}

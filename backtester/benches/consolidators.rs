//! Consolidator dispatch throughput across a *wide* universe.
//!
//! `benches/engine.rs` replicates the committed AAPL fixture across months, so
//! every tick holds exactly one bar and the consolidator registry is never
//! walked more than once per tick. This bench does the symbol fan-out that one
//! notes as missing: replica `s` of the fixture is written under ticker id `s`
//! with the *same* timestamps, so a tick holds `n_symbols` bars and the cost of
//! matching each bar to its consolidators is the thing being measured.
//!
//! Four strategy shapes per universe size, all subscribing the whole universe
//! and doing no strategy work beyond the consolidator callbacks:
//!
//! * `no_consolidators` — control. Nothing registered; isolates the read +
//!   engine-loop floor that every other number sits on top of.
//! * `one_total` — a single consolidator on one symbol. The registry is
//!   length 1, so a per-bar scan is already cheap; this is the case an index
//!   cannot improve.
//! * `one_per_symbol` — the pattern `add_all_equities` invites: one
//!   consolidator per subscribed symbol.
//! * `four_per_symbol` — the same with a 5m/15m/60m/daily stack per symbol.

use std::sync::{
    atomic::{AtomicU64, Ordering},
    Arc,
};

use arrow::{
    array::{Float64Array, TimestampNanosecondArray, UInt16Array, UInt32Array},
    datatypes::{DataType, Field, Schema, TimeUnit},
    record_batch::RecordBatch,
};
use backtester::{
    consolidator::ConsolidatorPeriod, data::sorted_parquet_files, run_backtest, Algorithm, Context,
    Slice, Symbol,
};
use criterion::{
    black_box, criterion_group, criterion_main, BenchmarkId, Criterion, SamplingMode, Throughput,
};
use parquet::arrow::{arrow_reader::ParquetRecordBatchReaderBuilder, ArrowWriter};
use tempfile::TempDir;

/// Universe sizes to sweep. The per-bar matching cost the index removes grows
/// as `bars_per_tick x consolidators`, i.e. quadratically in this number for
/// the per-symbol shapes, so two points apart show the shape of the curve.
const UNIVERSES: [usize; 2] = [100, 500];

fn fixture_root() -> String {
    concat!(env!("CARGO_MANIFEST_DIR"), "/tests/fixtures").to_string()
}

fn schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("ticker", DataType::UInt16, false),
        Field::new("volume", DataType::UInt32, false),
        Field::new("open", DataType::Float64, false),
        Field::new("close", DataType::Float64, false),
        Field::new("high", DataType::Float64, false),
        Field::new("low", DataType::Float64, false),
        Field::new("window_start", DataType::Timestamp(TimeUnit::Nanosecond, None), false),
    ]))
}

struct FixtureColumns {
    volume: Vec<u32>,
    open: Vec<f64>,
    high: Vec<f64>,
    low: Vec<f64>,
    close: Vec<f64>,
    ts: Vec<i64>,
}

fn read_fixture() -> FixtureColumns {
    let file = sorted_parquet_files(&fixture_root()).into_iter().next().expect("fixture missing");
    let f = std::fs::File::open(file).unwrap();
    let reader = ParquetRecordBatchReaderBuilder::try_new(f).unwrap().build().unwrap();

    let mut cols = FixtureColumns {
        volume: Vec::new(),
        open: Vec::new(),
        high: Vec::new(),
        low: Vec::new(),
        close: Vec::new(),
        ts: Vec::new(),
    };

    for batch in reader {
        let batch = batch.unwrap();
        let u32_col = |name| {
            batch.column_by_name(name).unwrap().as_any().downcast_ref::<UInt32Array>().unwrap()
        };
        let f64_col = |name| {
            batch.column_by_name(name).unwrap().as_any().downcast_ref::<Float64Array>().unwrap()
        };
        let ts = batch
            .column_by_name("window_start")
            .unwrap()
            .as_any()
            .downcast_ref::<TimestampNanosecondArray>()
            .unwrap();

        let (volume, open, high, low, close) =
            (u32_col("volume"), f64_col("open"), f64_col("high"), f64_col("low"), f64_col("close"));

        for i in 0..batch.num_rows() {
            cols.volume.push(volume.value(i));
            cols.open.push(open.value(i));
            cols.high.push(high.value(i));
            cols.low.push(low.value(i));
            cols.close.push(close.value(i));
            cols.ts.push(ts.value(i));
        }
    }

    cols
}

/// Write the fixture fanned out across `n_symbols` ticker ids into a temp data
/// root. Rows are emitted timestamp-major (all symbols for bar 0, then all for
/// bar 1, ...) so the file stays time-sorted the way the engine requires, and
/// every tick carries `n_symbols` bars.
fn generate_wide_dataset(n_symbols: usize) -> (TempDir, u64) {
    assert!(n_symbols <= u16::MAX as usize);
    let fx = read_fixture();
    let rows = fx.ts.len();
    let total = rows * n_symbols;

    let mut ticker = Vec::with_capacity(total);
    let mut volume = Vec::with_capacity(total);
    let mut open = Vec::with_capacity(total);
    let mut close = Vec::with_capacity(total);
    let mut high = Vec::with_capacity(total);
    let mut low = Vec::with_capacity(total);
    let mut ts = Vec::with_capacity(total);

    for i in 0..rows {
        for s in 0..n_symbols {
            // Spread the symbols apart in price so they are not bit-identical
            // series; the scale is arbitrary and does not affect dispatch cost.
            let scale = 1.0 + s as f64 * 0.01;
            ticker.push(s as u16);
            volume.push(fx.volume[i]);
            open.push(fx.open[i] * scale);
            close.push(fx.close[i] * scale);
            high.push(fx.high[i] * scale);
            low.push(fx.low[i] * scale);
            ts.push(fx.ts[i]);
        }
    }

    let tmp = tempfile::tempdir().unwrap();
    let map: std::collections::BTreeMap<String, String> =
        (0..n_symbols).map(|s| (s.to_string(), format!("SYM{s}"))).collect();
    std::fs::write(tmp.path().join("encoded_tickers.json"), serde_json::to_string(&map).unwrap())
        .unwrap();

    let schema = schema();
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(UInt16Array::from(ticker)),
            Arc::new(UInt32Array::from(volume)),
            Arc::new(Float64Array::from(open)),
            Arc::new(Float64Array::from(close)),
            Arc::new(Float64Array::from(high)),
            Arc::new(Float64Array::from(low)),
            Arc::new(TimestampNanosecondArray::from(ts)),
        ],
    )
    .unwrap();

    let dir = tmp.path().join("minute/year=2023/month=1");
    std::fs::create_dir_all(&dir).unwrap();
    let file = std::fs::File::create(dir.join("part-0.parquet")).unwrap();
    let mut writer = ArrowWriter::try_new(file, schema, None).unwrap();
    writer.write(&batch).unwrap();
    writer.close().unwrap();

    (tmp, total as u64)
}

#[derive(Clone, Copy)]
enum Shape {
    None,
    OneTotal,
    PerSymbol(usize),
}

/// Subscribes the whole universe, registers consolidators according to `shape`,
/// and does nothing in `on_data` — so the delta between shapes is dispatch plus
/// aggregation, not strategy work.
struct ConsolidatorBench {
    shape: Shape,
    fired: Arc<AtomicU64>,
}

impl Algorithm for ConsolidatorBench {
    fn initialize(&mut self, ctx: &mut Context) {
        ctx.set_cash(100_000.0);
        let symbols: Vec<Symbol> = ctx.add_all_equities();

        let register = |ctx: &mut Context, symbol: Symbol, k: usize| {
            let period = match k % 4 {
                0 => ConsolidatorPeriod::Minutes(5),
                1 => ConsolidatorPeriod::Minutes(15),
                2 => ConsolidatorPeriod::Hours(1),
                _ => ConsolidatorPeriod::Daily,
            };
            let fired = self.fired.clone();
            ctx.consolidate(symbol, period, move |_bar| {
                fired.fetch_add(1, Ordering::Relaxed);
            });
        };
        match self.shape {
            Shape::None => {}
            Shape::OneTotal => register(ctx, symbols[0], 0),
            Shape::PerSymbol(n) => {
                for &symbol in &symbols {
                    for k in 0..n {
                        register(ctx, symbol, k);
                    }
                }
            }
        }
    }

    fn on_data(&mut self, _ctx: &mut Context, _data: &Slice) {}
}

fn run(path: &str, shape: Shape) -> u64 {
    let fired = Arc::new(AtomicU64::new(0));
    let algo = ConsolidatorBench { shape, fired: fired.clone() };
    black_box(run_backtest(algo, path).unwrap());
    fired.load(Ordering::Relaxed)
}

fn benches(c: &mut Criterion) {
    let mut group = c.benchmark_group("consolidators");
    group.sample_size(10);
    group.sampling_mode(SamplingMode::Flat);
    group.warm_up_time(std::time::Duration::from_secs(2));
    group.measurement_time(std::time::Duration::from_secs(10));

    // Keep every tempdir alive for the whole run.
    let datasets: Vec<(usize, TempDir, u64)> = UNIVERSES
        .iter()
        .map(|&n| {
            let (tmp, bars) = generate_wide_dataset(n);
            (n, tmp, bars)
        })
        .collect();

    for (n, tmp, bars) in &datasets {
        let path = tmp.path().to_str().unwrap().to_string();
        group.throughput(Throughput::Elements(*bars));

        let shapes: &[(&str, Shape)] = if *n <= 100 {
            &[
                ("no_consolidators", Shape::None),
                ("one_total", Shape::OneTotal),
                ("one_per_symbol", Shape::PerSymbol(1)),
                ("four_per_symbol", Shape::PerSymbol(4)),
            ]
        } else {
            // 4 x 500 consolidators against 500 bars/tick is ~10 s per
            // iteration before the index; enough of the curve without it.
            &[
                ("no_consolidators", Shape::None),
                ("one_total", Shape::OneTotal),
                ("one_per_symbol", Shape::PerSymbol(1)),
            ]
        };

        for (name, shape) in shapes {
            group.bench_with_input(BenchmarkId::new(*name, n), &path, |b, path| {
                b.iter(|| run(path, *shape));
            });
        }
    }

    group.finish();
}

criterion_group!(consolidators, benches);
criterion_main!(consolidators);

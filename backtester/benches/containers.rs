//! `SymbolVec` vs `SymbolMap`: the same per-bar access pattern through the
//! dense array and through the Fibonacci-hashed map, to show why the engine
//! reaches for one or the other.
//!
//! `SymbolVec` is a `Vec<T>` indexed straight by the ticker id — no hashing, no
//! keys stored, no collision probing — so the state every symbol touches on
//! every bar (marks, last-seen day) lives there. `SymbolMap` is a `HashMap`
//! (so it still hashes the key, probes, and stores it) and earns its keep only
//! for the state few symbols have (positions, resting orders). This bench makes
//! the gap for the *dense* case concrete; it is illustrative and is not part of
//! the CI baseline gate.
//!
//! Run with: `cargo bench -p backtester --bench containers`

use std::hint::black_box;

use backtester::{Symbol, SymbolMap, SymbolVec};
use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion};

/// Symbols printing a bar on one timestamp. ~1600 is a realistic wide-universe
/// tick; the small and large ends bracket it.
const WIDTHS: [u16; 3] = [256, 1600, 8192];

fn per_bar_marks(c: &mut Criterion) {
    let mut group = c.benchmark_group("per_bar_marks");

    for &n in &WIDTHS {
        let symbols: Vec<Symbol> = (0..n).map(Symbol::from_ticker_id).collect();

        // The dense table: write every symbol's mark, then read them all back —
        // exactly what mark-to-market does each bar.
        group.bench_with_input(BenchmarkId::new("symbolvec", n), &symbols, |b, symbols| {
            let mut table: SymbolVec<Option<f64>> = SymbolVec::with_len(n as usize);
            b.iter(|| {
                for (i, &s) in symbols.iter().enumerate() {
                    table.set(s, Some(i as f64));
                }
                let mut sum = 0.0;
                for &s in symbols {
                    sum += table.copied(s).unwrap_or(0.0);
                }
                black_box(sum)
            });
        });

        // The same write-all / read-all cycle through the hash map.
        group.bench_with_input(BenchmarkId::new("symbolmap", n), &symbols, |b, symbols| {
            let mut table: SymbolMap<f64> = SymbolMap::default();
            b.iter(|| {
                for (i, &s) in symbols.iter().enumerate() {
                    table.insert(s, i as f64);
                }
                let mut sum = 0.0;
                for &s in symbols {
                    sum += table.get(&s).copied().unwrap_or(0.0);
                }
                black_box(sum)
            });
        });
    }

    group.finish();
}

criterion_group!(benches, per_bar_marks);
criterion_main!(benches);

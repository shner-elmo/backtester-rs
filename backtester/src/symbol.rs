//! The engine's instrument identifier and the containers keyed by it.
//!
//! A [`Symbol`] is the dataset's encoded ticker id — the `ticker` column value
//! that every Parquet row already carries (see
//! [`TickerMap`](crate::data::TickerMap) for the id ↔ ticker naming). It is a
//! `Copy` integer, so the event loop moves instruments around without
//! allocating or hashing text: a bar's symbol comes straight off the column,
//! and per-symbol state is an array index away.
//!
//! Nothing in this module knows what a ticker is. Names enter the engine when
//! a strategy subscribes ([`Context::add_equity`](crate::Context::add_equity))
//! and leave it when results are written; in between there are only integers.
//!
//! ```no_run
//! # use backtester::{Algorithm, Context, Slice, Symbol};
//! struct MyAlgo {
//!     aapl: Option<Symbol>,
//! }
//!
//! impl Algorithm for MyAlgo {
//!     fn initialize(&mut self, ctx: &mut Context) {
//!         self.aapl = Some(ctx.add_equity("AAPL")); // the one ticker -> Symbol step
//!     }
//!
//!     fn on_data(&mut self, ctx: &mut Context, data: &Slice) {
//!         let Some(aapl) = self.aapl else { return };
//!         if data.bars.contains_key(&aapl) {
//!             ctx.set_holdings(aapl, 1.0);
//!         }
//!     }
//! }
//! ```

use std::{
    collections::{HashMap, HashSet},
    hash::{BuildHasherDefault, Hasher},
};

/// An instrument, identified by its encoded ticker id in the dataset.
///
/// Get one from [`Context::add_equity`](crate::Context::add_equity) (or
/// [`Context::symbol`](crate::Context::symbol)) and keep it — resolving it
/// back to a ticker string
/// ([`Context::symbol_name`](crate::Context::symbol_name)) is for logging and
/// reporting, not for the trading path.
///
/// Ids are only meaningful within one dataset: they come from that dataset's
/// `encoded_tickers.json`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Symbol(u16);

impl Symbol {
    /// The symbol an encoded ticker id stands for. The id is not validated —
    /// one the dataset's ticker map doesn't list simply has no name.
    pub fn from_ticker_id(ticker_id: u16) -> Self {
        Self(ticker_id)
    }

    /// The encoded ticker id, as it appears in the data's `ticker` column.
    pub fn ticker_id(self) -> u16 {
        self.0
    }

    /// The id as an index — its slot in any [`SymbolVec`].
    pub fn index(self) -> usize {
        self.0 as usize
    }
}

/// Hasher for [`Symbol`] keys. Ticker ids are small integers, so one multiply
/// by an odd 64-bit constant (Fibonacci hashing) spreads them across a table
/// for a fraction of SipHash's cost.
#[derive(Debug, Default, Clone, Copy)]
pub struct SymbolHasher(u64);

/// 2^64 / φ, rounded to an odd number.
const GOLDEN_RATIO: u64 = 0x9E37_79B9_7F4A_7C15;

impl Hasher for SymbolHasher {
    fn write_u16(&mut self, value: u16) {
        self.0 = (value as u64).wrapping_mul(GOLDEN_RATIO);
    }

    fn write_u32(&mut self, value: u32) {
        self.0 = (value as u64).wrapping_mul(GOLDEN_RATIO);
    }

    // Symbols hash through `write_u16`; this is only here to satisfy the
    // trait and stays correct (if unremarkable) for anything else.
    fn write(&mut self, bytes: &[u8]) {
        for &b in bytes {
            self.0 = (self.0 ^ b as u64).wrapping_mul(GOLDEN_RATIO);
        }
    }

    fn finish(&self) -> u64 {
        self.0
    }
}

/// A hash map keyed by [`Symbol`], hashed with [`SymbolHasher`]. Use it for
/// state only some symbols have (positions, order books); use a
/// [`SymbolVec`] for state every streaming symbol has.
pub type SymbolMap<V> = HashMap<Symbol, V, BuildHasherDefault<SymbolHasher>>;

/// A hash set of [`Symbol`]s, hashed with [`SymbolHasher`].
pub type SymbolSet = HashSet<Symbol, BuildHasherDefault<SymbolHasher>>;

/// A dense per-symbol table: one slot per ticker id, so a lookup is an array
/// index rather than a hash. Grows on demand, filling new slots with
/// `T::default()`.
///
/// This is what the per-bar state uses (marks, last-seen day) —
/// state that every streaming symbol touches on every bar.
#[derive(Debug)]
pub struct SymbolVec<T> {
    slots: Vec<T>,
}

impl<T> Default for SymbolVec<T> {
    fn default() -> Self {
        Self { slots: Vec::new() }
    }
}

impl<T: Default> SymbolVec<T> {
    pub fn new() -> Self {
        Self::default()
    }

    /// An empty table with a slot for every id below `len`, so the per-bar
    /// writes never reallocate.
    pub fn with_len(len: usize) -> Self {
        let mut slots = Vec::new();
        slots.resize_with(len, T::default);
        Self { slots }
    }

    /// The slot for `symbol`, or `None` if it was never written.
    pub fn get(&self, symbol: Symbol) -> Option<&T> {
        self.slots.get(symbol.index())
    }

    pub fn get_mut(&mut self, symbol: Symbol) -> Option<&mut T> {
        self.slots.get_mut(symbol.index())
    }

    /// The slot for `symbol`, defaulted if it was never written.
    pub fn copied(&self, symbol: Symbol) -> T
    where
        T: Copy,
    {
        self.slots.get(symbol.index()).copied().unwrap_or_default()
    }

    /// The slot for `symbol`, growing the table to reach it.
    pub fn slot(&mut self, symbol: Symbol) -> &mut T {
        if self.slots.len() <= symbol.index() {
            self.slots.resize_with(symbol.index() + 1, T::default);
        }
        &mut self.slots[symbol.index()]
    }

    pub fn set(&mut self, symbol: Symbol, value: T) {
        *self.slot(symbol) = value;
    }

    /// Reset `symbol`'s slot to the default, returning what was there.
    pub fn take(&mut self, symbol: Symbol) -> T {
        match self.slots.get_mut(symbol.index()) {
            Some(slot) => std::mem::take(slot),
            None => T::default(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_symbol_is_its_ticker_id() {
        let sym = Symbol::from_ticker_id(17_309);
        assert_eq!(sym.ticker_id(), 17_309);
        assert_eq!(sym.index(), 17_309);
    }

    #[test]
    fn symbol_vec_grows_and_defaults() {
        let (a, z) = (Symbol::from_ticker_id(1), Symbol::from_ticker_id(900));

        let mut prices: SymbolVec<Option<f64>> = SymbolVec::new();
        assert_eq!(prices.copied(z), None);
        prices.set(z, Some(10.0));
        assert_eq!(prices.copied(z), Some(10.0));
        assert_eq!(prices.copied(a), None, "untouched slots stay defaulted");
        assert_eq!(prices.take(z), Some(10.0));
        assert_eq!(prices.copied(z), None);
    }

    #[test]
    fn symbol_map_round_trips() {
        let mut map: SymbolMap<f64> = SymbolMap::default();
        for id in [0u16, 1, 47, 17_309, u16::MAX] {
            map.insert(Symbol::from_ticker_id(id), id as f64);
        }
        assert_eq!(map.len(), 5);
        assert_eq!(map.get(&Symbol::from_ticker_id(47)), Some(&47.0));
        assert_eq!(map.get(&Symbol::from_ticker_id(48)), None);
    }
}

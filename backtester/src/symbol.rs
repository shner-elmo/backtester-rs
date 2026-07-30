//! Interned symbols — the engine's identifier for an instrument.
//!
//! A [`Symbol`] is a `Copy` integer handle into the run's [`SymbolTable`],
//! which owns the single copy of each ticker string. Text is converted at the
//! edges only: on the way in when a strategy calls
//! [`Context::add_equity`](crate::Context::add_equity), and on the way out
//! when results are written. Everything in between — the tick stream, order
//! books, positions, history, corporate actions — moves integers, so no bar
//! costs a `String` clone or a hash of text.
//!
//! ```no_run
//! # use backtester::{Algorithm, Context, Slice, Symbol};
//! struct MyAlgo {
//!     aapl: Symbol,
//! }
//!
//! impl Algorithm for MyAlgo {
//!     fn initialize(&mut self, ctx: &mut Context) {
//!         self.aapl = ctx.add_equity("AAPL"); // the one string -> Symbol step
//!     }
//!
//!     fn on_data(&mut self, ctx: &mut Context, data: &Slice) {
//!         if data.bars.contains_key(&self.aapl) {
//!             ctx.set_holdings(self.aapl, 1.0);
//!         }
//!     }
//! }
//! ```

use std::{
    collections::{HashMap, HashSet},
    hash::{BuildHasherDefault, Hasher},
};

/// An interned instrument identifier: an index into the [`SymbolTable`] that
/// minted it. Cheap to copy, compare, and hash.
///
/// Obtain one from [`Context::add_equity`](crate::Context::add_equity) (or
/// [`Context::symbol`](crate::Context::symbol)) and keep it — resolving it
/// back to a ticker string ([`Context::symbol_name`](crate::Context::symbol_name))
/// is for logging and reporting, not for the trading path.
///
/// Symbols from different tables are unrelated; mixing them is a bug (a
/// backtest has exactly one table, owned by its `Context`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Symbol(u32);

impl Symbol {
    /// Position of this symbol in its table — its index in any
    /// [`SymbolVec`].
    pub fn index(self) -> usize {
        self.0 as usize
    }

    /// Rebuild a symbol from an index handed out by [`Symbol::index`]. Only
    /// for the lookup tables that cache ids (see
    /// [`TickerResolver`](crate::data::TickerResolver)) — an index no table
    /// minted has no name.
    pub(crate) fn from_index(index: usize) -> Self {
        Self(index as u32)
    }
}

/// The string ↔ [`Symbol`] mapping for one backtest. Owned by the
/// [`Context`](crate::Context); ids are handed out densely from zero in
/// interning order.
#[derive(Debug, Default)]
pub struct SymbolTable {
    names: Vec<Box<str>>,
    ids: HashMap<Box<str>, Symbol>,
}

impl SymbolTable {
    /// The symbol for `name`, interning it if this is the first time it is
    /// seen.
    pub fn intern(&mut self, name: &str) -> Symbol {
        if let Some(&symbol) = self.ids.get(name) {
            return symbol;
        }
        let symbol = Symbol(self.names.len() as u32);
        self.names.push(name.into());
        self.ids.insert(name.into(), symbol);
        symbol
    }

    /// The symbol for `name`, or `None` if it was never interned.
    pub fn get(&self, name: &str) -> Option<Symbol> {
        self.ids.get(name).copied()
    }

    /// The ticker string behind `symbol`.
    ///
    /// # Panics
    ///
    /// If `symbol` was minted by a different table.
    pub fn name(&self, symbol: Symbol) -> &str {
        &self.names[symbol.index()]
    }

    /// How many symbols have been interned.
    pub fn len(&self) -> usize {
        self.names.len()
    }

    pub fn is_empty(&self) -> bool {
        self.names.is_empty()
    }
}

/// Hasher for [`Symbol`] keys. Symbols are dense small integers, so one
/// multiply by an odd 64-bit constant (Fibonacci hashing) spreads them across
/// a table for a fraction of SipHash's cost — and unlike ticker strings there
/// is nothing to walk byte by byte.
#[derive(Debug, Default, Clone, Copy)]
pub struct SymbolHasher(u64);

/// 2^64 / φ, rounded to an odd number.
const GOLDEN_RATIO: u64 = 0x9E37_79B9_7F4A_7C15;

impl Hasher for SymbolHasher {
    fn write_u32(&mut self, value: u32) {
        self.0 = (value as u64).wrapping_mul(GOLDEN_RATIO);
    }

    // Symbols hash through `write_u32`; this is only here to satisfy the
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
/// state only a few symbols have (positions, order books); use a
/// [`SymbolVec`] for state every symbol has.
pub type SymbolMap<V> = HashMap<Symbol, V, BuildHasherDefault<SymbolHasher>>;

/// A hash set of [`Symbol`]s, hashed with [`SymbolHasher`].
pub type SymbolSet = HashSet<Symbol, BuildHasherDefault<SymbolHasher>>;

/// A dense per-symbol table: one slot per symbol, indexed by its id, so a
/// lookup is an array index rather than a hash. Grows on demand, filling new
/// slots with `T::default()`.
///
/// This is what the per-bar state uses (history, marks, last-seen day) —
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
        self.reserve(symbol);
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

    /// Grow so every symbol up to `symbol` has a slot.
    fn reserve(&mut self, symbol: Symbol) {
        if self.slots.len() <= symbol.index() {
            self.slots.resize_with(symbol.index() + 1, T::default);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn interning_is_stable_and_dense() {
        let mut table = SymbolTable::default();
        let a = table.intern("AAPL");
        let b = table.intern("MSFT");
        assert_eq!(table.intern("AAPL"), a);
        assert_eq!((a.index(), b.index()), (0, 1));
        assert_eq!(table.name(a), "AAPL");
        assert_eq!(table.get("MSFT"), Some(b));
        assert_eq!(table.get("NVDA"), None);
        assert_eq!(table.len(), 2);
    }

    #[test]
    fn symbol_vec_grows_and_defaults() {
        let mut table = SymbolTable::default();
        let a = table.intern("A");
        let z = table.intern("Z");

        let mut prices: SymbolVec<Option<f64>> = SymbolVec::new();
        assert_eq!(prices.copied(z), None);
        prices.set(z, Some(10.0));
        assert_eq!(prices.copied(z), Some(10.0));
        assert_eq!(prices.copied(a), None, "untouched slots stay defaulted");
        assert_eq!(prices.take(z), Some(10.0));
        assert_eq!(prices.copied(z), None);
    }
}

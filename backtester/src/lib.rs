//! An event-driven backtesting engine for minute-bar equity data stored as
//! Parquet.
//!
//! Implement [`Algorithm`], configure the [`Context`] in `initialize`
//! (symbols, dates, cash), trade in `on_data`, and hand it to [`run`] or
//! [`run_backtest`]. See `examples/ema_cross.rs` for a complete strategy.
//!
//! # Symbols
//!
//! Instruments are identified by [`Symbol`] — the dataset's encoded ticker
//! id, handed to you by [`Context::add_equity`]. Ticker strings are read
//! exactly twice: when you subscribe, and when results are written out.
//! Everything in between (slices, orders, positions, history, corporate
//! actions) is keyed by that integer, so streaming a bar costs no allocation
//! and no string hashing. See [`symbol`] for the details.
//!
//! # Execution models
//!
//! Fill friction and buying power are pluggable, and all three follow the
//! same pattern — pass a built-in model, your own trait impl, or a plain
//! closure to the corresponding `Context` setter: `set_slippage`
//! ([`slippage`]), `set_commission` ([`commission`]), and `set_margin_model`
//! ([`margin`]).

/// Tolerance for float comparisons on share quantities: anything smaller is
/// rounding noise, not a position.
pub(crate) const EPSILON: f64 = 1e-9;

pub mod algorithm;
pub mod bar;
pub mod broker;
pub mod commission;
pub mod consolidator;
pub mod context;
pub mod data;
pub mod engine;
pub mod error;
pub mod indicators;
pub mod logging;
pub mod margin;
pub mod slice;
pub mod slippage;
pub mod stats;
pub mod symbol;
#[cfg(test)]
mod test_util;
pub mod tick_stream;

pub use algorithm::Algorithm;
pub use commission::{CommissionModel, NoCommission, PerShareCommission, PercentCommission};
pub use context::{Context, FillTiming};
pub use engine::{
    run, run_backtest, run_backtest_with_ticker_map, run_with_ticker_map, BacktestResult,
};
pub use error::BacktestError;
pub use logging::LogConfig;
pub use margin::{MarginContext, MarginModel, MaxLeverage, NoMargin};
pub use slice::Slice;
pub use slippage::{FillContext, FixedSlippage, NoSlippage, PercentSlippage, SlippageModel};
pub use stats::{BacktestStats, EquityPoint, Trade};
pub use symbol::{Symbol, SymbolMap, SymbolSet, SymbolVec};

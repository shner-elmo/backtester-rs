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
pub mod margin;
pub mod slice;
pub mod slippage;
pub mod stats;

pub use algorithm::Algorithm;
pub use commission::{CommissionModel, NoCommission, PerShareCommission, PercentCommission};
pub use context::{Context, FillTiming};
pub use engine::{run, run_backtest, BacktestResult};
pub use error::BacktestError;
pub use margin::{MarginContext, MarginModel, MaxLeverage, NoMargin};
pub use slice::Slice;
pub use slippage::{FillContext, FixedSlippage, NoSlippage, PercentSlippage, SlippageModel};
pub use stats::{BacktestStats, EquityPoint, Trade};

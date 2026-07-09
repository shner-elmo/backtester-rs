//! Configurable engine logging: pick which categories of events the engine
//! reports (to stderr) as a backtest runs.

/// Which categories of engine events are logged to stderr while a backtest
/// runs. Set it via [`Context::set_log_config`](crate::Context::set_log_config).
///
/// Defaults to warnings only, so data-quality problems (a date range the data
/// doesn't cover, out-of-order timestamps) are never silent while the happy
/// path stays quiet. Logs go to stderr so they never mix with the result
/// summary `run` prints to stdout.
#[derive(Debug, Clone)]
pub struct LogConfig {
    /// The run header (date range, starting cash, warm-up, symbols), the
    /// warm-up-complete marker, and the run footer (trade count, final
    /// equity).
    pub run_summary: bool,
    /// Every order fill and every completed round-trip trade.
    pub trades: bool,
    /// One line at each day boundary: date, equity, cash, open positions.
    pub daily_recap: bool,
    /// Corporate events as they apply: splits, dividends, renames, delist
    /// liquidations.
    pub corporate_events: bool,
    /// Data-quality warnings: a configured date range the data source doesn't
    /// fully cover, bars dropped for running backwards in time.
    pub warnings: bool,
}

impl Default for LogConfig {
    fn default() -> Self {
        Self {
            run_summary: false,
            trades: false,
            daily_recap: false,
            corporate_events: false,
            warnings: true,
        }
    }
}

impl LogConfig {
    /// Every category on.
    pub fn all() -> Self {
        Self {
            run_summary: true,
            trades: true,
            daily_recap: true,
            corporate_events: true,
            warnings: true,
        }
    }

    /// Every category off, warnings included.
    pub fn none() -> Self {
        Self {
            run_summary: false,
            trades: false,
            daily_recap: false,
            corporate_events: false,
            warnings: false,
        }
    }
}

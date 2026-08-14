//! The backtest engine. [`run`] and [`run_backtest`] are the entry points;
//! the event loop and its state live in `run.rs`, order execution in
//! `orders.rs`, day-boundary corporate actions in `corporate_actions.rs`, and
//! the trade ledger in `ledger.rs`.

mod corporate_actions;
mod ledger;
mod orders;
mod run;

use run::{run_prepared, PendingActions};
use rustc_hash::FxHashSet;
use serde::{Deserialize, Serialize};

use crate::{
    algorithm::Algorithm,
    context::Context,
    data::{
        load_dividends_from, load_renames_from, load_splits_from, SubscriptionMask, TickerMap,
        DIVIDENDS_FILE, RENAMES_FILE, SPLITS_FILE, TICKER_MAP_FILE,
    },
    error::BacktestError,
    stats::{BacktestStats, EquityPoint, OpenPositionSummary, Trade},
};

/// Everything a finished backtest produced. `run` prints a summary of this and
/// writes it to disk; consume it directly (or via the JSON file) for custom
/// reporting and the results dashboard.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BacktestResult {
    pub initial_cash: f64,
    /// Cash plus open positions marked at the last known market price.
    pub final_equity: f64,
    pub total_commission: f64,
    pub stats: BacktestStats,
    /// Daily mark-to-market equity, one point per trading day.
    pub equity_curve: Vec<EquityPoint>,
    /// Per-bar mark-to-market equity (RFC 3339 timestamps), empty unless
    /// `Context::set_track_intraday_equity(true)` was set. Exposes intraday
    /// drawdown the daily `equity_curve` can't show.
    #[serde(default)]
    pub intraday_equity: Vec<EquityPoint>,
    /// Positions still open at the end, marked at the last known price.
    pub open_positions: Vec<OpenPositionSummary>,
    /// Completed round trips (rebalance fills netted per position lifetime).
    pub trades: Vec<Trade>,
}

/// Run a backtest and return its full results without printing anything.
pub fn run_backtest<A: Algorithm>(
    algo: A,
    data_path: &str,
) -> Result<BacktestResult, BacktestError> {
    run_backtest_with_ticker_map(algo, data_path, None)
}

/// [`run_backtest`] against a ticker map that isn't `encoded_tickers.json` in
/// the data root. A relative path is resolved against the data root, an
/// absolute one used as-is.
pub fn run_backtest_with_ticker_map<A: Algorithm>(
    mut algo: A,
    data_path: &str,
    ticker_map: Option<&std::path::Path>,
) -> Result<BacktestResult, BacktestError> {
    let mut ctx = prepare_context(data_path, ticker_map)?;
    algo.initialize(&mut ctx);
    run_prepared(algo, ctx, data_path)
}

/// Load the dataset's ticker map and build the context around it. This runs
/// *before* `initialize`, which is what lets `Context::add_equity` hand back
/// the ticker id the data itself uses instead of inventing one.
fn prepare_context(
    data_path: &str,
    ticker_map: Option<&std::path::Path>,
) -> Result<Context, BacktestError> {
    let (path, _) = resolve_data_file(data_path, &ticker_map.map(Into::into), TICKER_MAP_FILE);
    Ok(Context::with_tickers(TickerMap::load(&path)?))
}

/// Run a backtest, print a summary, and write the full result JSON
/// (`backtest_result_<timestamp>.json`) for the `ui` dashboard. The file goes
/// to the directory set via `Context::set_output_dir`, else to
/// `$BACKTEST_OUTPUT_DIR` when that is set, else to the current directory
/// (the directory is created if missing).
pub fn run<A: Algorithm>(algo: A, data_path: &str) -> Result<BacktestResult, BacktestError> {
    run_with_ticker_map(algo, data_path, None)
}

/// [`run`] against a ticker map that isn't `encoded_tickers.json` in the data
/// root. Same path resolution as
/// [`run_backtest_with_ticker_map`](run_backtest_with_ticker_map).
pub fn run_with_ticker_map<A: Algorithm>(
    mut algo: A,
    data_path: &str,
    ticker_map: Option<&std::path::Path>,
) -> Result<BacktestResult, BacktestError> {
    let mut ctx = prepare_context(data_path, ticker_map)?;
    algo.initialize(&mut ctx);
    // set_output_dir wins over the env var: the strategy author's explicit
    // choice shouldn't be silently redirected by the environment.
    let out_dir = ctx.output_dir.clone().or_else(|| {
        std::env::var("BACKTEST_OUTPUT_DIR").ok().filter(|d| !d.is_empty()).map(Into::into)
    });

    let result = run_prepared(algo, ctx, data_path)?;
    let stats = &result.stats;

    let ts = chrono::Local::now().format("%Y-%m-%dT%H-%M-%S");
    let file_name = format!("backtest_result_{ts}.json");
    let out_path = match out_dir {
        Some(dir) => {
            std::fs::create_dir_all(&dir)
                .map_err(|source| BacktestError::Io { path: dir.clone(), source })?;
            dir.join(file_name)
        }
        None => std::path::PathBuf::from(file_name),
    };
    let file = std::fs::File::create(&out_path)
        .map_err(|source| BacktestError::Io { path: out_path.clone(), source })?;
    serde_json::to_writer_pretty(file, &result)
        .map_err(|e| BacktestError::Json { path: out_path.clone(), message: e.to_string() })?;

    println!("=== Backtest Complete ===");
    println!("Result written to: {}  (view it with `cargo run -p ui`)", out_path.display());
    println!(
        "Trades: {}  |  Win Rate: {:.0}%  |  Total PnL: ${:.0}  |  Final Equity: ${:.0}",
        stats.trade_count,
        stats.win_rate * 100.0,
        stats.total_pnl,
        result.final_equity,
    );
    println!(
        "Profit Factor: {:.2}  |  Max Drawdown: {:.1}%  |  Sharpe: {:.2}  |  Commission: ${:.2}",
        stats.profit_factor,
        stats.max_drawdown * 100.0,
        stats.sharpe_ratio,
        result.total_commission,
    );
    if !result.open_positions.is_empty() {
        for p in &result.open_positions {
            println!(
                "Open: {} {:.2} @ {:.2} (last {:.2}, unrealized ${:.0})",
                p.symbol, p.quantity, p.avg_price, p.last_price, p.unrealized_pnl
            );
        }
    }

    Ok(result)
}

/// Resolve a metadata file location: an explicitly configured absolute path
/// is used as-is, a relative one is joined to the data root, and `None`
/// falls back to `default_name` in the data root. The bool reports whether
/// the path was explicitly configured — missing *optional* files are only
/// tolerated at the defaults; an explicitly set file must exist.
fn resolve_data_file(
    data_root: &str,
    custom: &Option<std::path::PathBuf>,
    default_name: &str,
) -> (std::path::PathBuf, bool) {
    match custom {
        Some(p) if p.is_absolute() => (p.clone(), true),
        Some(p) => (std::path::Path::new(data_root).join(p), true),
        None => (std::path::Path::new(data_root).join(default_name), false),
    }
}

/// Queue every corporate action (splits, dividends, renames) for the
/// subscribed symbols by date, and build the mask the bar stream filters on.
/// Rename targets are subscribed up front so their bars stream from the start
/// (the position is only transferred on the effective date); this also covers
/// a successor that begins trading in the same month-file as the rename.
///
/// This is the string boundary of a run: the metadata files are matched by
/// ticker name here, once, against the dataset's ticker map. Everything handed
/// back is keyed by [`Symbol`](crate::Symbol).
fn load_pending_actions(
    ctx: &mut Context,
    data_path: &str,
) -> Result<(SubscriptionMask, PendingActions), BacktestError> {
    // The metadata loaders filter by ticker name, so hand them the names of
    // what is subscribed.
    let mut names: FxHashSet<String> = ctx
        .subscribed_symbols
        .iter()
        .filter_map(|&s| ctx.tickers.name(s).map(str::to_string))
        .collect();

    let mut pending = PendingActions::default();

    let (renames_path, renames_required) =
        resolve_data_file(data_path, &ctx.renames_file, RENAMES_FILE);
    for (date, pairs) in load_renames_from(&renames_path, &names, renames_required)? {
        for (old, new) in pairs {
            // A successor the dataset has no id for can never print a bar, so
            // there is nothing to transfer the position to: leave it on the
            // old symbol, which the delist scan then handles.
            let (Some(old_symbol), Some(new_symbol)) =
                (ctx.tickers.symbol(&old), ctx.tickers.symbol(&new))
            else {
                if ctx.log_config.warnings {
                    eprintln!(
                        "[warn] rename {old} -> {new} on {date} skipped: {new} is not in the \
                         dataset's ticker map"
                    );
                }
                continue;
            };
            ctx.subscribe(new_symbol);
            names.insert(new);
            pending.renames.entry(date).or_default().push((old_symbol, new_symbol));
        }
    }

    let (splits_path, splits_required) =
        resolve_data_file(data_path, &ctx.splits_file, SPLITS_FILE);
    for (ticker, by_date) in load_splits_from(&splits_path, &names, splits_required)? {
        let Some(symbol) = ctx.tickers.symbol(&ticker) else { continue };
        for (date, ratio) in by_date {
            pending.splits.entry(date).or_default().push((symbol, ratio));
        }
    }

    let (dividends_path, dividends_required) =
        resolve_data_file(data_path, &ctx.dividends_file, DIVIDENDS_FILE);
    for (ticker, by_date) in load_dividends_from(&dividends_path, &names, dividends_required)? {
        let Some(symbol) = ctx.tickers.symbol(&ticker) else { continue };
        for (date, amount) in by_date {
            pending.dividends.entry(date).or_default().push((symbol, amount));
        }
    }

    // A row's ticker id is already its symbol, so the reader only needs to
    // know which ids this run wants.
    let mut subscribed = SubscriptionMask::with_id_space(ctx.id_space());
    for &symbol in &ctx.subscribed_symbols {
        subscribed.insert(symbol);
    }

    Ok((subscribed, pending))
}

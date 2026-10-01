use std::{
    ffi::OsString,
    path::PathBuf,
    sync::{Arc, Mutex},
};

use backtester::{
    bar::MarketSession, consolidator::ConsolidatorPeriod, run_backtest, run_backtest_with_data_dir,
    run_with_data_dir, Algorithm, BacktestError, Context, Slice,
};

const FIXTURE: &str = concat!(env!("CARGO_MANIFEST_DIR"), "/../test-data");
const DATA_DIR_ENV: &str = "BACKTEST_DATA_DIR";
static ENV_LOCK: Mutex<()> = Mutex::new(());

struct RestoreEnv(Option<OsString>);

impl Drop for RestoreEnv {
    fn drop(&mut self) {
        match self.0.take() {
            Some(value) => std::env::set_var(DATA_DIR_ENV, value),
            None => std::env::remove_var(DATA_DIR_ENV),
        }
    }
}

fn lock_env() -> (std::sync::MutexGuard<'static, ()>, RestoreEnv) {
    let guard = ENV_LOCK.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
    let restore = RestoreEnv(std::env::var_os(DATA_DIR_ENV));
    (guard, restore)
}

struct ObserveDataDir {
    observed: Arc<Mutex<Option<PathBuf>>>,
    output_dir: Option<PathBuf>,
}

impl Algorithm for ObserveDataDir {
    fn initialize(&mut self, ctx: &mut Context) {
        *self.observed.lock().unwrap() = Some(ctx.data_dir().to_path_buf());
        if let Some(output_dir) = &self.output_dir {
            ctx.set_output_dir(output_dir);
        }
        ctx.add_equity("AAPL");
    }

    fn on_data(&mut self, _ctx: &mut Context, _data: &Slice) {}
}

#[test]
fn run_backtest_requires_backtest_data_dir() {
    let (_guard, _restore) = lock_env();
    std::env::remove_var(DATA_DIR_ENV);

    let observed = Arc::new(Mutex::new(None));
    let err = run_backtest(ObserveDataDir { observed, output_dir: None }).unwrap_err();
    assert!(matches!(err, BacktestError::MissingConfiguration { variable: "BACKTEST_DATA_DIR" }));
}

#[test]
fn run_backtest_resolves_backtest_data_dir_into_context() {
    let (_guard, _restore) = lock_env();
    std::env::set_var(DATA_DIR_ENV, FIXTURE);

    let observed = Arc::new(Mutex::new(None));
    run_backtest(ObserveDataDir { observed: observed.clone(), output_dir: None }).unwrap();
    assert_eq!(*observed.lock().unwrap(), Some(PathBuf::from(FIXTURE)));
}

#[test]
fn explicit_data_dir_ignores_environment_state() {
    let (_guard, _restore) = lock_env();
    std::env::set_var(DATA_DIR_ENV, "/not/the/requested/dataset");

    let observed = Arc::new(Mutex::new(None));
    run_backtest_with_data_dir(
        ObserveDataDir { observed: observed.clone(), output_dir: None },
        FIXTURE,
    )
    .unwrap();
    assert_eq!(*observed.lock().unwrap(), Some(PathBuf::from(FIXTURE)));

    let output = tempfile::tempdir().unwrap();
    let observed = Arc::new(Mutex::new(None));
    run_with_data_dir(
        ObserveDataDir {
            observed: observed.clone(),
            output_dir: Some(output.path().to_path_buf()),
        },
        FIXTURE,
    )
    .unwrap();
    assert_eq!(*observed.lock().unwrap(), Some(PathBuf::from(FIXTURE)));
}

struct Idle;

impl Algorithm for Idle {
    fn initialize(&mut self, _ctx: &mut Context) {}
    fn on_data(&mut self, _ctx: &mut Context, _data: &Slice) {}
}

struct AlternateBars {
    dir: PathBuf,
    consolidated: Arc<Mutex<usize>>,
}

impl Algorithm for AlternateBars {
    fn initialize(&mut self, ctx: &mut Context) {
        ctx.set_bar_data_dir(&self.dir);
        let symbol = ctx.add_equity("AAPL");
        let consolidated = self.consolidated.clone();
        ctx.consolidate(symbol, ConsolidatorPeriod::Daily, move |_| {
            *consolidated.lock().unwrap() += 1;
        });
    }

    fn on_data(&mut self, _ctx: &mut Context, _data: &Slice) {}
}

fn copy_fixture_data(root: &std::path::Path) {
    std::fs::copy(
        PathBuf::from(FIXTURE).join("encoded_tickers.json"),
        root.join("encoded_tickers.json"),
    )
    .unwrap();
    let month = root.join("year=2023/month=1");
    std::fs::create_dir_all(&month).unwrap();
    std::fs::copy(
        PathBuf::from(FIXTURE).join("year=2023/month=1/part-0.parquet"),
        month.join("part-0.parquet"),
    )
    .unwrap();
}

struct ObserveSessions {
    regular_only: bool,
    sessions: Arc<Mutex<Vec<MarketSession>>>,
}

impl Algorithm for ObserveSessions {
    fn initialize(&mut self, ctx: &mut Context) {
        if self.regular_only {
            ctx.set_extended_market_hours(false);
        }
        ctx.add_equity("AAPL");
    }

    fn on_data(&mut self, _ctx: &mut Context, data: &Slice) {
        self.sessions.lock().unwrap().extend(data.bars.values().map(|bar| bar.session()));
    }
}

fn observed_sessions(regular_only: bool) -> Vec<MarketSession> {
    let sessions = Arc::new(Mutex::new(Vec::new()));
    run_backtest_with_data_dir(
        ObserveSessions { regular_only, sessions: sessions.clone() },
        FIXTURE,
    )
    .unwrap();
    let seen = sessions.lock().unwrap().clone();
    seen
}

#[test]
fn regular_session_filter_precedes_on_data() {
    let all = observed_sessions(false);
    let regular = observed_sessions(true);

    assert!(all.contains(&MarketSession::PreMarket));
    assert!(all.contains(&MarketSession::AfterMarket));
    assert!(regular.contains(&MarketSession::Main));
    assert!(regular.iter().all(|session| *session == MarketSession::Main));
    assert!(regular.len() < all.len());
}

#[test]
fn alternate_bar_dataset_still_feeds_consolidators() {
    let daily = tempfile::tempdir().unwrap();
    copy_fixture_data(daily.path());
    let consolidated = Arc::new(Mutex::new(0));

    run_backtest_with_data_dir(
        AlternateBars { dir: daily.path().to_path_buf(), consolidated: consolidated.clone() },
        FIXTURE,
    )
    .unwrap();

    assert!(*consolidated.lock().unwrap() > 0);
}

#[test]
fn alternate_bar_dataset_must_use_the_primary_ticker_ids() {
    let daily = tempfile::tempdir().unwrap();
    copy_fixture_data(daily.path());
    std::fs::write(daily.path().join("encoded_tickers.json"), r#"{"47":"MSFT"}"#).unwrap();

    let err = run_backtest_with_data_dir(
        AlternateBars { dir: daily.path().to_path_buf(), consolidated: Arc::new(Mutex::new(0)) },
        FIXTURE,
    )
    .unwrap_err();

    assert!(matches!(err, BacktestError::InvalidDataset { .. }));
    assert!(err.to_string().contains("does not match the primary dataset"));
}

#[test]
fn missing_ticker_map_is_an_invalid_dataset() {
    let tmp = tempfile::tempdir().unwrap();
    let err = run_backtest_with_data_dir(Idle, tmp.path()).unwrap_err();
    assert!(matches!(err, BacktestError::InvalidDataset { .. }));
    assert!(err.to_string().contains("encoded_tickers.json"));
}

#[test]
fn malformed_ticker_map_is_an_invalid_dataset() {
    let tmp = tempfile::tempdir().unwrap();
    std::fs::write(tmp.path().join("encoded_tickers.json"), "not json").unwrap();
    let err = run_backtest_with_data_dir(Idle, tmp.path()).unwrap_err();
    assert!(matches!(err, BacktestError::InvalidDataset { .. }));
    assert!(err.to_string().contains("encoded_tickers.json"));
}

#[test]
fn absent_optional_metadata_is_allowed() {
    let tmp = tempfile::tempdir().unwrap();
    std::fs::copy(
        PathBuf::from(FIXTURE).join("encoded_tickers.json"),
        tmp.path().join("encoded_tickers.json"),
    )
    .unwrap();
    std::fs::create_dir_all(tmp.path().join("year=2023/month=1")).unwrap();
    std::fs::copy(
        PathBuf::from(FIXTURE).join("year=2023/month=1/part-0.parquet"),
        tmp.path().join("year=2023/month=1/part-0.parquet"),
    )
    .unwrap();

    run_backtest_with_data_dir(
        ObserveDataDir { observed: Arc::new(Mutex::new(None)), output_dir: None },
        tmp.path(),
    )
    .unwrap();
}

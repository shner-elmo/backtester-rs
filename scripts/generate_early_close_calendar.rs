#!/usr/bin/env rust-script
//! Generate an early-close calendar by scanning every minute bar in the dataset.
//!
//! Requires: cargo install rust-script
//! Run:
//!   export BACKTEST_DATA_DIR=/path/to/minute-dataset
//!   rust-script scripts/generate_early_close_calendar.rs --output early_closes.json
//!
//! The output is a JSON object whose keys are US Eastern calendar dates and whose values are
//! market close times in `HH:MM` Eastern time. It finds the sustained market-wide activity drop
//! that separates regular trading from sparse post-close prints. Pre-market and after-market bars
//! are ignored.
//!
//! ```cargo
//! [dependencies]
//! arrow = "56"
//! parquet = "56"
//! chrono = "0.4"
//! chrono-tz = "0.9"
//! serde_json = "1"
//! walkdir = "2"
//! ```

use std::{
    collections::BTreeMap,
    env, fs,
    fs::File,
    io::Write,
    path::{Path, PathBuf},
    time::Instant,
};

use arrow::array::TimestampNanosecondArray;
use chrono::{DateTime, NaiveDate, Timelike, Utc};
use chrono_tz::US::Eastern;
use parquet::{arrow::arrow_reader::ParquetRecordBatchReaderBuilder, arrow::ProjectionMask};
use walkdir::WalkDir;

const OPEN_MINUTE: u32 = 9 * 60 + 30;
const NORMAL_CLOSE_MINUTE: u32 = 16 * 60;
const SESSION_MINUTES: usize = (NORMAL_CLOSE_MINUTE - OPEN_MINUTE) as usize;
const BATCH_SIZE: usize = 1_048_576;

type ActivityByDate = BTreeMap<NaiveDate, Vec<u32>>;

fn usage() -> &'static str {
    "Usage:\n    export BACKTEST_DATA_DIR=/path/to/minute-dataset\n    \
     generate_early_close_calendar --output <calendar.json>"
}

fn parse_output(args: impl IntoIterator<Item = String>) -> Result<Option<PathBuf>, String> {
    let mut output = None;
    let mut args = args.into_iter();
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--output" => {
                output = Some(PathBuf::from(args.next().ok_or("--output needs a value")?));
            }
            "--help" | "-h" => return Ok(None),
            other => return Err(format!("unknown argument {other:?}")),
        }
    }
    output.map(Some).ok_or_else(|| "--output is required".into())
}

fn main() {
    let input = env::var_os("BACKTEST_DATA_DIR").filter(|value| !value.is_empty());
    let result = match (parse_output(env::args().skip(1)), input) {
        (Ok(None), _) => {
            println!("{}", usage());
            return;
        }
        (Ok(Some(output)), Some(input)) => generate(Path::new(&input), &output),
        (Ok(Some(_)), None) => {
            Err("BACKTEST_DATA_DIR must point to the minute dataset root".into())
        }
        (Err(error), _) => Err(error),
    };
    if let Err(error) = result {
        eprintln!("error: {error}");
        eprintln!("{}", usage());
        std::process::exit(1);
    }
}

fn generate(input: &Path, output: &Path) -> Result<(), String> {
    let files = parquet_files(input)?;
    if files.is_empty() {
        return Err(format!("no Parquet files found under {}", input.display()));
    }

    let started = Instant::now();
    let mut activity_by_date = BTreeMap::new();
    let mut rows = 0_u64;
    let mut regular_rows = 0_u64;

    for (index, path) in files.iter().enumerate() {
        let (file_rows, file_regular_rows) = scan_file(path, &mut activity_by_date)
            .map_err(|e| format!("{}: {e}", path.display()))?;
        rows += file_rows;
        regular_rows += file_regular_rows;
        eprintln!("[{}/{}] {}: {file_rows} rows", index + 1, files.len(), path.display());
    }

    if regular_rows == 0 {
        return Err("the dataset contains no bars during 09:30-16:00 US Eastern; is this a minute-bar dataset?".into());
    }

    let calendar = early_closes(&activity_by_date);
    write_json(output, &calendar)?;
    eprintln!(
        "\n{} files, {rows} rows, {} trading dates, {} early closes  ({:.1}s)\nwrote {}",
        files.len(),
        activity_by_date.len(),
        calendar.len(),
        started.elapsed().as_secs_f32(),
        output.display()
    );
    Ok(())
}

fn parquet_files(root: &Path) -> Result<Vec<PathBuf>, String> {
    let mut files = Vec::new();
    for entry in WalkDir::new(root) {
        let entry = entry.map_err(|e| format!("walk {}: {e}", root.display()))?;
        if entry.file_type().is_file()
            && entry.path().extension().is_some_and(|extension| extension == "parquet")
        {
            files.push(entry.into_path());
        }
    }
    files.sort();
    Ok(files)
}

fn scan_file(path: &Path, activity_by_date: &mut ActivityByDate) -> Result<(u64, u64), String> {
    let file = File::open(path).map_err(|e| e.to_string())?;
    let builder =
        ParquetRecordBatchReaderBuilder::try_new(file).map_err(|e| format!("open Parquet: {e}"))?;
    let projection = ProjectionMask::columns(builder.parquet_schema(), ["window_start"]);
    let reader = builder
        .with_projection(projection)
        .with_batch_size(BATCH_SIZE)
        .build()
        .map_err(|e| format!("build reader: {e}"))?;

    let mut rows = 0_u64;
    let mut regular_rows = 0_u64;
    let mut previous_timestamp = None;
    let mut previous_session_minute = None;
    for batch in reader {
        let batch = batch.map_err(|e| format!("read batch: {e}"))?;
        let timestamps = batch
            .column_by_name("window_start")
            .ok_or("missing window_start column")?
            .as_any()
            .downcast_ref::<TimestampNanosecondArray>()
            .ok_or("window_start is not a nanosecond timestamp")?;
        rows += timestamps.len() as u64;

        for timestamp in timestamps.values() {
            // A full-universe dataset repeats each minute once per ticker. We still visit every
            // row, but only do timezone conversion when the timestamp changes.
            if previous_timestamp != Some(*timestamp) {
                previous_timestamp = Some(*timestamp);
                previous_session_minute = regular_session_minute(*timestamp);
            }
            if let Some((date, minute)) = previous_session_minute {
                regular_rows += 1;
                activity_by_date.entry(date).or_insert_with(|| vec![0; SESSION_MINUTES])
                    [(minute - OPEN_MINUTE) as usize] += 1;
            }
        }
    }
    Ok((rows, regular_rows))
}

fn regular_session_minute(timestamp_ns: i64) -> Option<(NaiveDate, u32)> {
    let local = DateTime::<Utc>::from_timestamp_nanos(timestamp_ns).with_timezone(&Eastern);
    let minute = local.hour() * 60 + local.minute();
    (OPEN_MINUTE..NORMAL_CLOSE_MINUTE).contains(&minute).then(|| (local.date_naive(), minute))
}

fn early_closes(activity_by_date: &ActivityByDate) -> BTreeMap<String, String> {
    activity_by_date
        .iter()
        .filter_map(|(date, counts)| {
            inferred_early_close(counts)
                .map(|close| (date.to_string(), format!("{:02}:{:02}", close / 60, close % 60)))
        })
        .collect()
}

/// Infer the boundary between broad regular-session activity and sparse post-close prints.
///
/// The source schema has no market-session column, and bars after an early close can still appear
/// before 16:00. The morning median gives a scale-independent estimate of normal whole-market
/// activity. The close is the final active (auction-print) minute before a sustained ten-minute
/// fall below 20% of that baseline. Requiring that final minute to remain active prevents a short
/// quiet patch inside a normal session from looking like a close.
fn inferred_early_close(counts: &[u32]) -> Option<u32> {
    if counts.len() != SESSION_MINUTES {
        return None;
    }
    let mut morning = counts[..120].to_vec();
    morning.sort_unstable();
    let baseline = morning[morning.len() / 2];
    let active_threshold = baseline.div_ceil(5);
    if active_threshold == 0 {
        return None;
    }

    // No US-equity early close occurs before 11:00. Starting there also keeps opening volatility
    // and isolated missing early minutes from becoming candidates.
    let first_candidate = (11 * 60 - OPEN_MINUTE) as usize;
    for boundary in first_candidate..counts.len() - 10 {
        if counts[boundary - 1] >= active_threshold
            && counts[boundary..boundary + 10].iter().all(|count| *count < active_threshold)
        {
            return Some(OPEN_MINUTE + boundary as u32 - 1);
        }
    }
    None
}

fn write_json(path: &Path, calendar: &BTreeMap<String, String>) -> Result<(), String> {
    if let Some(parent) = path.parent().filter(|parent| !parent.as_os_str().is_empty()) {
        fs::create_dir_all(parent).map_err(|e| format!("create {}: {e}", parent.display()))?;
    }
    let temporary = path.with_extension("json.tmp");
    let json = serde_json::to_string_pretty(calendar).map_err(|e| e.to_string())?;
    let mut file =
        File::create(&temporary).map_err(|e| format!("create {}: {e}", temporary.display()))?;
    writeln!(file, "{json}").map_err(|e| format!("write {}: {e}", temporary.display()))?;
    file.sync_all().map_err(|e| format!("sync {}: {e}", temporary.display()))?;
    fs::rename(&temporary, path)
        .map_err(|e| format!("rename {} to {}: {e}", temporary.display(), path.display()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::{NaiveDate, TimeZone};

    fn timestamp(year: i32, month: u32, day: u32, hour: u32, minute: u32) -> i64 {
        Eastern
            .with_ymd_and_hms(year, month, day, hour, minute, 0)
            .single()
            .unwrap()
            .timestamp_nanos_opt()
            .unwrap()
    }

    #[test]
    fn excludes_extended_hours() {
        assert_eq!(regular_session_minute(timestamp(2024, 11, 29, 9, 29)), None);
        assert_eq!(
            regular_session_minute(timestamp(2024, 11, 29, 9, 30)),
            Some((NaiveDate::from_ymd_opt(2024, 11, 29).unwrap(), 9 * 60 + 30))
        );
        assert_eq!(regular_session_minute(timestamp(2024, 11, 29, 16, 0)), None);
    }

    #[test]
    fn reports_the_activity_drop_only_for_early_closes() {
        let mut early = vec![2_000; SESSION_MINUTES];
        early[(13 * 60 + 1 - OPEN_MINUTE) as usize..].fill(25);
        let normal = vec![2_000; SESSION_MINUTES];
        let days = BTreeMap::from([
            (NaiveDate::from_ymd_opt(2024, 11, 29).unwrap(), early),
            (NaiveDate::from_ymd_opt(2024, 12, 2).unwrap(), normal),
        ]);

        assert_eq!(early_closes(&days), BTreeMap::from([("2024-11-29".into(), "13:00".into())]));
    }

    #[test]
    fn ignores_a_short_intraday_drop_in_activity() {
        let mut counts = vec![2_000; SESSION_MINUTES];
        counts[180..185].fill(10);
        assert_eq!(inferred_early_close(&counts), None);
    }

    #[test]
    fn output_is_sorted_json() {
        let calendar = BTreeMap::from([
            ("2024-12-24".to_owned(), "13:00".to_owned()),
            ("2024-07-03".to_owned(), "13:00".to_owned()),
        ]);
        assert_eq!(
            serde_json::to_string_pretty(&calendar).unwrap(),
            "{\n  \"2024-07-03\": \"13:00\",\n  \"2024-12-24\": \"13:00\"\n}"
        );
    }
}

use chrono::{DateTime, Datelike, Duration, TimeZone, Utc};
use chrono_tz::US::Eastern;

use crate::bar::Bar;

pub enum ConsolidatorPeriod {
    Minutes(u32),
    Hours(u32),
    Daily,
    Weekly,
}

impl ConsolidatorPeriod {
    /// The instant the bucket containing `time` opens. Buckets are aligned to
    /// the wall-clock grid, not to the first bar seen, so the emitted bars land
    /// on the same boundaries regardless of where the stream starts or which
    /// minutes are missing:
    ///
    /// - `Minutes`/`Hours`: floored to the period grid from the Unix epoch. ET
    ///   offsets are whole hours, so an epoch-aligned intraday bucket is also
    ///   aligned to ET wall-clock marks.
    /// - `Daily`: US Eastern midnight of the bar's trading date, matching the
    ///   engine's day boundaries so after-market bars (which cross UTC
    ///   midnight) stay on the day they belong to.
    /// - `Weekly`: US Eastern midnight of the Monday of the bar's trading week.
    fn bucket_start(&self, time: DateTime<Utc>) -> DateTime<Utc> {
        match self {
            ConsolidatorPeriod::Minutes(m) => floor_to(time, *m as i64 * 60),
            ConsolidatorPeriod::Hours(h) => floor_to(time, *h as i64 * 3_600),
            ConsolidatorPeriod::Daily => et_midnight(time.with_timezone(&Eastern).date_naive()),
            ConsolidatorPeriod::Weekly => {
                let date = time.with_timezone(&Eastern).date_naive();
                let monday = date - Duration::days(date.weekday().num_days_from_monday() as i64);
                et_midnight(monday)
            }
        }
    }
}

/// Floor `time` to a multiple of `secs` from the Unix epoch.
fn floor_to(time: DateTime<Utc>, secs: i64) -> DateTime<Utc> {
    let ts = time.timestamp();
    DateTime::from_timestamp(ts - ts.rem_euclid(secs), 0).expect("floored timestamp is in range")
}

/// The UTC instant of US Eastern midnight on `date`.
fn et_midnight(date: chrono::NaiveDate) -> DateTime<Utc> {
    let naive = date.and_hms_opt(0, 0, 0).expect("midnight is a valid time");
    Eastern
        .from_local_datetime(&naive)
        .earliest()
        .expect("ET midnight is unambiguous")
        .with_timezone(&Utc)
}

pub(crate) struct ConsolidatorEntry {
    /// The symbol this consumes is the key it is indexed under in
    /// `Context::consolidators_by_symbol`, so it is not repeated here.
    period: ConsolidatorPeriod,
    /// The open instant of the bucket `current_bar` is accumulating.
    bucket_start: Option<DateTime<Utc>>,
    current_bar: Option<Bar>,
    callback: Box<dyn FnMut(&Bar)>,
}

impl ConsolidatorEntry {
    pub fn new(period: ConsolidatorPeriod, callback: Box<dyn FnMut(&Bar)>) -> Self {
        Self { period, bucket_start: None, current_bar: None, callback }
    }

    pub fn feed(&mut self, bar: &Bar) {
        let start = self.period.bucket_start(bar.time);
        match self.current_bar.as_mut() {
            None => self.open_bucket(bar, start),
            Some(cb) if Some(start) != self.bucket_start => {
                // First bar of a new bucket: emit the finished one, then seed
                // the next. The emitted bar carries the bucket-open timestamp,
                // not the last bar's, so consumers see a bar per grid slot.
                let fired = cb.clone();
                (self.callback)(&fired);
                self.open_bucket(bar, start);
            }
            Some(cb) => {
                cb.high = cb.high.max(bar.high);
                cb.low = cb.low.min(bar.low);
                cb.close = bar.close;
                cb.volume += bar.volume;
                // cb.time stays the bucket open; cb.open stays the first bar's.
            }
        }
    }

    fn open_bucket(&mut self, bar: &Bar, start: DateTime<Utc>) {
        let mut seed = bar.clone();
        seed.time = start;
        self.current_bar = Some(seed);
        self.bucket_start = Some(start);
    }

    pub fn flush(&mut self) {
        if let Some(bar) = &self.current_bar {
            (self.callback)(bar);
        }
        self.current_bar = None;
        self.bucket_start = None;
    }
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Mutex};

    use chrono::{Timelike, Weekday};

    use super::*;

    fn bar_at_et(y: i32, mo: u32, d: u32, h: u32, m: u32, close: f64, volume: u64) -> Bar {
        let time = Eastern.with_ymd_and_hms(y, mo, d, h, m, 0).unwrap().with_timezone(&Utc);
        Bar { time, open: close, high: close, low: close, close, volume }
    }

    fn collect(period: ConsolidatorPeriod, bars: &[Bar]) -> Vec<Bar> {
        let fired: Arc<Mutex<Vec<Bar>>> = Arc::new(Mutex::new(Vec::new()));
        let f = fired.clone();
        let mut c = ConsolidatorEntry::new(
            period,
            Box::new(move |b: &Bar| f.lock().unwrap().push(b.clone())),
        );
        for b in bars {
            c.feed(b);
        }
        c.flush();
        let bars = fired.lock().unwrap().clone();
        bars
    }

    #[test]
    fn daily_period_uses_eastern_trading_dates() {
        // 2023-01-03 is in EST (UTC-5), so the 19:30 ET after-market bar sits
        // past midnight UTC. It must still consolidate into Jan 3's daily bar,
        // which fires when the first Jan 4 (ET) bar arrives.
        let fired = collect(
            ConsolidatorPeriod::Daily,
            &[
                bar_at_et(2023, 1, 3, 10, 0, 1.0, 10),
                bar_at_et(2023, 1, 3, 19, 30, 2.0, 20), // crosses UTC midnight
                bar_at_et(2023, 1, 4, 10, 0, 3.0, 30),
            ],
        );
        // Jan 3's daily bar spans both its bars (after-market close, summed
        // volume) and is stamped at Jan 3 ET midnight; Jan 4's flushes at end.
        assert_eq!(fired.len(), 2);
        assert_eq!(fired[0].close, 2.0);
        assert_eq!(fired[0].volume, 30);
        let open_et = fired[0].time.with_timezone(&Eastern);
        assert_eq!((open_et.hour(), open_et.minute()), (0, 0));
        assert_eq!(open_et.date_naive(), chrono::NaiveDate::from_ymd_opt(2023, 1, 3).unwrap());
    }

    #[test]
    fn minute_buckets_align_to_the_grid() {
        // A stream starting off-grid (09:31) still emits buckets on the 5-min
        // marks, not anchored to the first bar.
        let fired = collect(
            ConsolidatorPeriod::Minutes(5),
            &[
                bar_at_et(2023, 1, 3, 9, 31, 1.0, 1),
                bar_at_et(2023, 1, 3, 9, 34, 2.0, 1),
                bar_at_et(2023, 1, 3, 9, 36, 3.0, 1),
            ],
        );
        assert_eq!(fired.len(), 2);
        assert_eq!(fired[0].time.with_timezone(&Eastern).minute(), 30);
        assert_eq!(fired[0].volume, 2, "09:31 and 09:34 fall in the 09:30 bucket");
        assert_eq!(fired[1].time.with_timezone(&Eastern).minute(), 35);
    }

    #[test]
    fn weekly_buckets_open_on_eastern_mondays() {
        // Jan 4 2023 is a Wednesday; Jan 9 is the next Monday.
        let fired = collect(
            ConsolidatorPeriod::Weekly,
            &[
                bar_at_et(2023, 1, 4, 10, 0, 1.0, 5),
                bar_at_et(2023, 1, 6, 10, 0, 2.0, 5),
                bar_at_et(2023, 1, 9, 10, 0, 3.0, 5),
            ],
        );
        assert_eq!(fired.len(), 2);
        assert_eq!(fired[0].volume, 10, "Wed + Fri land in the same week");
        let monday = fired[0].time.with_timezone(&Eastern);
        assert_eq!(monday.weekday(), Weekday::Mon);
        assert_eq!(monday.date_naive(), chrono::NaiveDate::from_ymd_opt(2023, 1, 2).unwrap());
    }
}

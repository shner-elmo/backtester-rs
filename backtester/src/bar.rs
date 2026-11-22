use std::ops::Range;

use chrono::{DateTime, Datelike, NaiveDate, TimeZone, Timelike, Utc, Weekday};
use chrono_tz::US::Eastern;

#[derive(Debug, Clone, PartialEq)]
pub struct Bar {
    pub time: DateTime<Utc>,
    pub open: f64,
    pub high: f64,
    pub low: f64,
    pub close: f64,
    pub volume: u64,
}

impl Bar {
    /// Trading session this bar belongs to, derived from its US Eastern
    /// timestamp and the standard 13:00 early-close rules.
    ///
    /// Algorithms can use [`Slice::session`](crate::Slice::session) instead:
    /// the engine computes it once for the whole tick from cached daily
    /// boundaries, while this convenience method converts the timestamp on
    /// each call.
    pub fn session(&self) -> MarketSession {
        let time = self.time.with_timezone(&Eastern);
        let minute = time.hour() * 60 + time.minute();
        match minute {
            m if m < 9 * 60 + 30 => MarketSession::PreMarket,
            m if m < close_hour(time.date_naive()) * 60 => MarketSession::Main,
            _ => MarketSession::AfterMarket,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MarketSession {
    PreMarket,
    /// The exchange's regular trading session.
    Main,
    AfterMarket,
}

impl MarketSession {
    pub(crate) fn at(time: DateTime<Utc>, regular: &Range<DateTime<Utc>>) -> Self {
        if time < regular.start {
            Self::PreMarket
        } else if time < regular.end {
            Self::Main
        } else {
            Self::AfterMarket
        }
    }
}

pub(crate) fn eastern_time(date: NaiveDate, hour: u32, minute: u32) -> DateTime<Utc> {
    Eastern
        .with_ymd_and_hms(date.year(), date.month(), date.day(), hour, minute, 0)
        .single()
        .expect("US Eastern market time is unambiguous")
        .with_timezone(&Utc)
}

fn is_early_close(date: NaiveDate) -> bool {
    // July 3, the Friday after Thanksgiving, and Christmas Eve.
    matches!(
        (date.month(), date.day(), date.weekday()),
        (7, 3, Weekday::Mon | Weekday::Tue | Weekday::Wed | Weekday::Thu)
            | (11, 23..=29, Weekday::Fri)
            | (12, 24, Weekday::Mon | Weekday::Tue | Weekday::Wed | Weekday::Thu)
    )
}

fn close_hour(date: NaiveDate) -> u32 {
    if is_early_close(date) {
        13
    } else {
        16
    }
}

pub(crate) fn regular_session(date: NaiveDate) -> Range<DateTime<Utc>> {
    eastern_time(date, 9, 30)..eastern_time(date, close_hour(date), 0)
}

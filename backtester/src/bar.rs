use chrono::{DateTime, Timelike, Utc};
use chrono_tz::US::Eastern;

#[derive(Debug, Clone)]
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
    /// time-of-day: before 9:30 is pre-market, before 16:00 is the main
    /// session, later bars are after-market.
    pub fn session(&self) -> MarketSession {
        let et = self.time.with_timezone(&Eastern);
        let minutes = et.hour() * 60 + et.minute();
        match minutes {
            m if m < 9 * 60 + 30 => MarketSession::PreMarket,
            m if m < 16 * 60 => MarketSession::Main,
            _ => MarketSession::AfterMarket,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MarketSession {
    PreMarket,
    Main,
    AfterMarket,
}

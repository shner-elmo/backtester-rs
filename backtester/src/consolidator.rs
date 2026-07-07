use chrono::{DateTime, Duration, Utc};
use chrono_tz::US::Eastern;

use crate::bar::Bar;

pub enum ConsolidatorPeriod {
    Minutes(u32),
    Hours(u32),
    Daily,
}

pub(crate) struct ConsolidatorEntry {
    pub symbol: String,
    period: ConsolidatorPeriod,
    period_start: Option<DateTime<Utc>>,
    current_bar: Option<Bar>,
    callback: Box<dyn FnMut(&Bar)>,
}

impl ConsolidatorEntry {
    pub fn new(symbol: String, period: ConsolidatorPeriod, callback: Box<dyn FnMut(&Bar)>) -> Self {
        Self { symbol, period, period_start: None, current_bar: None, callback }
    }

    pub fn feed(&mut self, bar: &Bar) {
        match &self.period {
            ConsolidatorPeriod::Minutes(_) | ConsolidatorPeriod::Hours(_) => {
                let duration = match &self.period {
                    ConsolidatorPeriod::Hours(h) => Duration::hours(*h as i64),
                    ConsolidatorPeriod::Minutes(m) => Duration::minutes(*m as i64),
                    _ => unreachable!(),
                };
                match (self.period_start, self.current_bar.as_mut()) {
                    (None, _) => {
                        self.period_start = Some(bar.time);
                        self.current_bar = Some(bar.clone());
                    }
                    (Some(start), Some(cb)) => {
                        if bar.time >= start + duration {
                            // Fire on the first bar that crosses the boundary so the
                            // fired bar's timestamp is the period open, not the close.
                            // This bar then seeds the next period.
                            let fired = cb.clone();
                            (self.callback)(&fired);
                            self.period_start = Some(bar.time);
                            self.current_bar = Some(bar.clone());
                        } else {
                            cb.high = cb.high.max(bar.high);
                            cb.low = cb.low.min(bar.low);
                            cb.close = bar.close;
                            cb.volume += bar.volume;
                            // cb.time is not updated: it stays as the period-open timestamp.
                        }
                    }
                    _ => unreachable!(),
                }
            }
            ConsolidatorPeriod::Daily => {
                // Trading date in US Eastern, matching the engine's day
                // boundaries, so after-market bars that cross UTC midnight
                // stay on the day they belong to.
                let bar_date = bar.time.with_timezone(&Eastern).date_naive();
                match self.current_bar.as_mut() {
                    None => {
                        self.current_bar = Some(bar.clone());
                    }
                    Some(cb) => {
                        if bar_date != cb.time.with_timezone(&Eastern).date_naive() {
                            let fired = cb.clone();
                            (self.callback)(&fired);
                            self.current_bar = Some(bar.clone());
                        } else {
                            cb.high = cb.high.max(bar.high);
                            cb.low = cb.low.min(bar.low);
                            cb.close = bar.close;
                            cb.volume += bar.volume;
                            cb.time = bar.time;
                        }
                    }
                }
            }
        }
    }

    pub fn flush(&mut self) {
        if let Some(bar) = &self.current_bar {
            (self.callback)(bar);
        }
        self.current_bar = None;
        self.period_start = None;
    }
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Mutex};

    use chrono::TimeZone;

    use super::*;
    use crate::bar::MarketSession;

    fn bar_at_et(y: i32, mo: u32, d: u32, h: u32, m: u32, close: f64, volume: u64) -> Bar {
        let time = Eastern.with_ymd_and_hms(y, mo, d, h, m, 0).unwrap().with_timezone(&Utc);
        Bar {
            time,
            open: close,
            high: close,
            low: close,
            close,
            volume,
            market_session: MarketSession::Main,
        }
    }

    #[test]
    fn daily_period_uses_eastern_trading_dates() {
        // 2023-01-03 is in EST (UTC-5), so the 19:30 ET after-market bar sits
        // past midnight UTC. It must still consolidate into Jan 3's daily bar,
        // which fires when the first Jan 4 (ET) bar arrives.
        let fired: Arc<Mutex<Vec<Bar>>> = Arc::new(Mutex::new(Vec::new()));
        let f = fired.clone();
        let mut c = ConsolidatorEntry::new(
            "SYM".into(),
            ConsolidatorPeriod::Daily,
            Box::new(move |b: &Bar| f.lock().unwrap().push(b.clone())),
        );

        c.feed(&bar_at_et(2023, 1, 3, 10, 0, 1.0, 10));
        c.feed(&bar_at_et(2023, 1, 3, 19, 30, 2.0, 20)); // crosses UTC midnight
        assert!(fired.lock().unwrap().is_empty(), "period fired mid trading day");

        c.feed(&bar_at_et(2023, 1, 4, 10, 0, 3.0, 30));
        let fired = fired.lock().unwrap();
        assert_eq!(fired.len(), 1);
        // Jan 3's daily bar spans both its bars: the after-market close and
        // the summed volume.
        assert_eq!(fired[0].close, 2.0);
        assert_eq!(fired[0].volume, 30);
    }
}

use chrono::{Duration, DateTime, Utc};

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
    callback: Box<dyn Fn(&Bar)>,
}

impl ConsolidatorEntry {
    pub fn new(symbol: String, period: ConsolidatorPeriod, callback: Box<dyn Fn(&Bar)>) -> Self {
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
                            // cb.time is not updated: it stays as the period-open timestamp.
                        }
                    }
                    _ => unreachable!(),
                }
            }
            ConsolidatorPeriod::Daily => {
                let bar_date = bar.time.date_naive();
                match self.current_bar.as_mut() {
                    None => {
                        self.current_bar = Some(bar.clone());
                    }
                    Some(cb) => {
                        if bar_date != cb.time.date_naive() {
                            let fired = cb.clone();
                            (self.callback)(&fired);
                            self.current_bar = Some(bar.clone());
                        } else {
                            cb.high = cb.high.max(bar.high);
                            cb.low = cb.low.min(bar.low);
                            cb.close = bar.close;
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

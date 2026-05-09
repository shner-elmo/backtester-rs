use crate::bar::Bar;

pub enum ConsolidatorPeriod {
    Minutes(u32),
    Hours(u32),
    Daily,
}

pub(crate) struct ConsolidatorEntry {
    pub symbol: String,
    period: ConsolidatorPeriod,
    bar_count: u32,
    current_bar: Option<Bar>,
    callback: Box<dyn Fn(&Bar)>,
}

impl ConsolidatorEntry {
    pub fn new(symbol: String, period: ConsolidatorPeriod, callback: Box<dyn Fn(&Bar)>) -> Self {
        Self { symbol, period, bar_count: 0, current_bar: None, callback }
    }

    pub fn feed(&mut self, bar: &Bar) {
        match &self.period {
            ConsolidatorPeriod::Minutes(_) | ConsolidatorPeriod::Hours(_) => {
                let target = match &self.period {
                    ConsolidatorPeriod::Hours(h) => h * 60,
                    ConsolidatorPeriod::Minutes(m) => *m,
                    _ => unreachable!(),
                };
                match self.current_bar.as_mut() {
                    None => {
                        self.current_bar = Some(bar.clone());
                        self.bar_count = 1;
                    }
                    Some(cb) => {
                        cb.close = bar.close;
                        cb.time = bar.time;
                        self.bar_count += 1;
                    }
                }
                if self.bar_count >= target {
                    if let Some(consolidated) = &self.current_bar {
                        (self.callback)(consolidated);
                    }
                    self.current_bar = None;
                    self.bar_count = 0;
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
                            cb.close = bar.close;
                            cb.time = bar.time;
                        }
                    }
                }
            }
        }
    }
}

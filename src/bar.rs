use chrono::{DateTime, Utc};

#[derive(Debug, Clone)]
pub struct Bar {
    pub time: DateTime<Utc>,
    pub open: f64,
    pub high: f64,
    pub low: f64,
    pub close: f64,
    pub market_session: MarketSession,
}

#[derive(Debug, Clone, PartialEq)]
pub enum MarketSession {
    PreMarket,
    Main,
    AfterMarket,
}

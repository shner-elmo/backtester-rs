use chrono::{DateTime, Utc};

use crate::{
    bar::{Bar, MarketSession},
    symbol::SymbolMap,
};

pub struct Slice {
    pub time: DateTime<Utc>,
    /// The US-equity market session containing this tick. Unlike calling
    /// [`Bar::session`](crate::bar::Bar::session) for every bar, this is
    /// computed once from the engine's cached boundaries for the current day.
    pub session: MarketSession,
    /// This tick's bars, keyed by the [`Symbol`](crate::Symbol) that
    /// `Context::add_equity` handed out.
    pub bars: SymbolMap<Bar>,
}

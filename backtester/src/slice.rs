use chrono::{DateTime, Utc};

use crate::{bar::Bar, symbol::SymbolMap};

pub struct Slice {
    pub time: DateTime<Utc>,
    /// This tick's bars, keyed by the [`Symbol`](crate::Symbol) that
    /// `Context::add_equity` handed out.
    pub bars: SymbolMap<Bar>,
}

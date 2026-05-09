use std::collections::HashMap;
use chrono::{DateTime, Utc};
use crate::bar::Bar;

pub struct Slice {
    pub time: DateTime<Utc>,
    pub bars: HashMap<String, Bar>,
}

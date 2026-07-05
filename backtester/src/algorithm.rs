use crate::{context::Context, slice::Slice};

pub trait Algorithm {
    fn initialize(&mut self, ctx: &mut Context);
    fn on_data(&mut self, ctx: &mut Context, data: &Slice);
    fn on_end_of_day(&mut self, _ctx: &mut Context) {}

    /// Fired when a stock split executes on a subscribed symbol. The engine
    /// has already adjusted any held position (quantity × ratio, basis ÷
    /// ratio) and rescaled `ctx.history` into post-split terms; bar prices in
    /// slices are never adjusted. Use this to reset indicators you own.
    /// `ratio` is `split_to / split_from` (3.0 for a 1→3 forward split).
    fn on_split(&mut self, _ctx: &mut Context, _symbol: &str, _ratio: f64) {}

    /// Fired after a held symbol produced no bars for the configured number
    /// of trading days (see `Context::set_delist_after_days`) and the engine
    /// force-liquidated the position at its last known price.
    fn on_delisted(&mut self, _ctx: &mut Context, _symbol: &str) {}
}

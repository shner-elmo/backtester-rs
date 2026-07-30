use crate::{context::Context, slice::Slice, symbol::Symbol};

pub trait Algorithm {
    fn initialize(&mut self, ctx: &mut Context);
    fn on_data(&mut self, ctx: &mut Context, data: &Slice);

    /// Fired once per trading day after its last bar: on the first bar of the
    /// following day (before that bar touches any state), and once at the end
    /// of data for the final day. Orders placed here fill on the next day's
    /// first bar — except on the final call, where no bar remains and they are
    /// dropped.
    fn on_end_of_day(&mut self, _ctx: &mut Context) {}

    /// Fired when a stock split executes on a subscribed symbol. The engine
    /// has already adjusted any held position (quantity × ratio, basis ÷
    /// ratio) and rescaled `ctx.history` into post-split terms; bar prices in
    /// slices are never adjusted. Use this to reset indicators you own.
    /// `ratio` is `split_to / split_from` (3.0 for a 1→3 forward split).
    fn on_split(&mut self, _ctx: &mut Context, _symbol: Symbol, _ratio: f64) {}

    /// Fired after a held symbol produced no bars for the configured number
    /// of trading days (see `Context::set_delist_after_days`) and the engine
    /// force-liquidated the position at its last known price.
    fn on_delisted(&mut self, _ctx: &mut Context, _symbol: Symbol) {}

    /// Fired when a cash dividend goes ex on a subscribed symbol you hold. The
    /// engine has already credited `quantity * amount` to portfolio cash
    /// (debited it for a short) and attributed that income to the position's
    /// PnL, so a round trip's PnL is its total return including dividends.
    /// `amount` is the cash dividend per share.
    fn on_dividend(&mut self, _ctx: &mut Context, _symbol: Symbol, _amount: f64) {}

    /// Fired when a subscribed symbol is renamed (e.g. FB → META). The engine
    /// has already transferred any held position, PnL ledger, and history from
    /// `old` to `new`, and subscribed `new` so its bars flow — but your own
    /// state still refers to `old`. Update any symbol strings and indicators
    /// you key on it here.
    fn on_rename(&mut self, _ctx: &mut Context, _old: Symbol, _new: Symbol) {}
}

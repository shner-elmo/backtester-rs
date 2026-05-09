use crate::context::Context;
use crate::slice::Slice;

pub trait Algorithm {
    fn initialize(&mut self, ctx: &mut Context);
    fn on_data(&mut self, ctx: &mut Context, data: &Slice);
    fn on_end_of_day(&mut self, _ctx: &mut Context) {}
}

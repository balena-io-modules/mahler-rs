mod target;

pub use target::*;

pub struct Context<S>
where
    S: Clone,
{
    pub target: S,
}

pub trait FromContext<S>
where
    S: Clone,
{
    fn from_context(context: &Context<S>) -> Self;
}

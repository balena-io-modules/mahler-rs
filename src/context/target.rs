use super::{Context, FromContext};

pub struct Target<S>(pub S);
impl<'system, S> FromContext<S> for Target<S>
where
    S: Clone,
{
    fn from_context(context: &Context<S>) -> Self {
        Target(context.target.clone())
    }
}

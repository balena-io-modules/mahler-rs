use super::Entity;
use crate::state::{FromState, State};
use std::ops::Deref;

pub struct Target<E>(E);

impl<E> FromState<E> for Target<E>
where
    E: Entity + Clone + 'static,
{
    fn from_state(_: &State, target: &E) -> Self {
        Self(target.clone())
    }
}

impl<R> Deref for Target<R> {
    type Target = R;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

use crate::state::{Context, FromState};
use std::ops::{Deref, DerefMut};

pub struct State<'system, S>(&'system mut S);

impl<'system, S> FromState<'system, S> for State<'system, S> {
    fn from_state(state: &'system mut S, _: &Context<S>) -> Self {
        State(state)
    }
}

impl<'system, S> Deref for State<'system, S> {
    type Target = S;

    fn deref(&self) -> &Self::Target {
        self.0
    }
}

impl<'system, S> DerefMut for State<'system, S> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.0
    }
}

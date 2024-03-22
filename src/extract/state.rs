use crate::state::{Context, FromState};
use std::ops::{Deref, DerefMut};

pub struct State<'system, S: crate::state::State>(&'system mut S);

impl<'system, S: crate::state::State> FromState<'system, S> for State<'system, S> {
    fn from_state(state: &'system mut S, _: Context<S>) -> Self {
        State(state)
    }
}

impl<'system, S: crate::state::State> Deref for State<'system, S> {
    type Target = S;

    fn deref(&self) -> &Self::Target {
        self.0
    }
}

impl<'system, S: crate::state::State> DerefMut for State<'system, S> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.0
    }
}

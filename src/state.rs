use std::{
    fmt::{Display, Formatter},
    ops::Deref,
    ops::DerefMut,
};

pub trait FromState<'system, S> {
    fn from_state(state: &'system mut S) -> Self;
}

pub struct State<'system, S>(&'system mut S);

impl<'system, S> DerefMut for State<'system, S> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.0
    }
}

impl<'system, S> Deref for State<'system, S> {
    type Target = S;

    fn deref(&self) -> &Self::Target {
        self.0
    }
}

impl<S> Display for State<'_, S>
where
    S: Display,
{
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl<'system, S> FromState<'system, S> for State<'system, S> {
    fn from_state(state: &'system mut S) -> Self {
        State(state)
    }
}

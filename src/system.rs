use std::{
    fmt::{Display, Formatter},
    ops::Deref,
    ops::DerefMut,
};

pub trait FromSystem<'system, S> {
    fn from_state(state: &'system mut S) -> Self;
}

pub struct System<'system, S>(&'system mut S);

impl<'system, S> DerefMut for System<'system, S> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.0
    }
}

impl<'system, S> Deref for System<'system, S> {
    type Target = S;

    fn deref(&self) -> &Self::Target {
        self.0
    }
}

impl<S> Display for System<'_, S>
where
    S: Display,
{
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl<'system, S> FromSystem<'system, S> for System<'system, S> {
    fn from_state(state: &'system mut S) -> Self {
        System(state)
    }
}

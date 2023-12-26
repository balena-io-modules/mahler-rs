use super::Resource;
use crate::state::{FromStateMut, State};
use std::ops::{Deref, DerefMut};

pub struct Update<'system, R>(&'system mut R);

impl<'system, R> FromStateMut<'system, R> for Update<'system, R>
where
    R: Resource + 'static,
{
    fn from_state_mut(state: &'system mut State, _: &R) -> Self {
        if let Some(entity) = state.get_resource_mut::<R>() {
            Self(entity)
        } else {
            // TODO
            panic!("Resource not found")
        }
    }
}

impl<'system, E> DerefMut for Update<'system, E> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.0
    }
}

impl<'system, E> Deref for Update<'system, E> {
    type Target = E;

    fn deref(&self) -> &Self::Target {
        self.0
    }
}

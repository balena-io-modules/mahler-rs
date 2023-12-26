use super::Entity;
use crate::state::{FromState, State};
use std::ops::Deref;

pub struct Read<E>(E);

impl<E> FromState<E> for Read<E>
where
    E: Entity + Clone + 'static,
{
    fn from_state(state: &State, target: &E) -> Self {
        if let Some(entity) = state.get_entity::<E>(&target.id()) {
            Self(entity.clone())
        } else {
            panic!("Entity not found")
        }
    }
}

impl<E> Deref for Read<E> {
    type Target = E;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

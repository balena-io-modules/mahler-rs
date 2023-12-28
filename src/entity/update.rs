use super::Entity;
use crate::system::{FromSystemMut, System};
use std::ops::{Deref, DerefMut};

pub struct Update<'system, T>(&'system mut T);

impl<'system, E> FromSystemMut<'system, E> for Update<'system, E>
where
    E: Entity + 'static,
{
    fn from_system_mut(system: &'system mut System, target: &E) -> Self {
        if let Some(entity) = system.get_entity_mut::<E>(&target.id()) {
            Self(entity)
        } else {
            // TODO
            panic!("Entity not found")
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

use super::Entity;
use crate::system::{FromSystem, System};
use std::ops::Deref;

pub struct Read<E>(E);

impl<E> FromSystem<E> for Read<E>
where
    E: Entity + Clone + 'static,
{
    fn from_system(system: &System, target: &E) -> Self {
        if let Some(entity) = system.get_entity::<E>(&target.id()) {
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

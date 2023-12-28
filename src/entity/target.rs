use super::Entity;
use crate::system::{FromSystem, System};
use std::ops::Deref;

pub struct Target<E>(E);

impl<E> FromSystem<E> for Target<E>
where
    E: Entity + Clone + 'static,
{
    fn from_system(_: &System, target: &E) -> Self {
        Self(target.clone())
    }
}

impl<R> Deref for Target<R> {
    type Target = R;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

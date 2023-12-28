use super::Resource;
use crate::system::{FromSystem, System};
use std::ops::Deref;

pub struct Read<R>(R);

impl<R> FromSystem<R> for Read<R>
where
    R: Resource + Clone + 'static,
{
    fn from_system(state: &System, _: &R) -> Self {
        if let Some(entity) = state.get_resource::<R>() {
            Self(entity.clone())
        } else {
            panic!("Resource not found")
        }
    }
}

impl<R> Deref for Read<R> {
    type Target = R;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

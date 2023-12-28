use super::Resource;
use crate::system::{FromSystem, System};
use std::ops::Deref;

pub struct Target<R>(R);

impl<R> FromSystem<R> for Target<R>
where
    R: Resource + Clone + 'static,
{
    fn from_system(_: &System, target: &R) -> Self {
        Self(target.clone())
    }
}

impl<R> Deref for Target<R> {
    type Target = R;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

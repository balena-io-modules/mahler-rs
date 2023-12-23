use super::{Entity, Indexable, WithParent};

use std::any::{Any, TypeId};

pub trait Resource {}

pub(crate) struct BoxedResource {
    id: TypeId,
    contents: Box<dyn Any>,
}

impl BoxedResource {
    pub fn new<T>(value: T) -> Self
    where
        T: Resource + 'static,
    {
        Self {
            id: TypeId::of::<T>(),
            contents: Box::new(value),
        }
    }

    pub fn as_resource<T>(&self) -> &T
    where
        T: Resource + 'static,
    {
        self.contents
            .downcast_ref::<T>()
            .expect("could not downcast from BoxedResource")
    }

    pub fn as_resource_mut<T>(&mut self) -> &mut T
    where
        T: Resource + 'static,
    {
        self.contents.downcast_mut::<T>().unwrap()
    }
}

impl Indexable for BoxedResource {
    type Id = TypeId;

    fn id(&self) -> Self::Id {
        self.id
    }
}

impl WithParent for BoxedResource {
    type Parent = ();

    fn pid(&self) {
        ()
    }
}

impl Entity for BoxedResource {}

macro_rules! impl_resource_for_basic_types {
    ($($type:ty),*) => {
        $(
            impl Resource for $type {}
        )*
    };
}

impl_resource_for_basic_types!(
    i8, i16, i32, i64, i128, isize, u8, u16, u32, u64, u128, usize, f32, f64, bool, char, str,
    String
);

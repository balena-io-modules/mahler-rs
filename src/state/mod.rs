mod index;
mod indexable;

pub use indexable::*;

use index::Index;

use std::{
    any::{Any, TypeId},
    collections::HashMap,
};

use crate::{
    entity::Entity,
    resource::{BoxedResource, Resource},
};

pub struct State {
    db: HashMap<TypeId, Box<dyn Any>>,
}

pub trait FromStateMut<'system, E>
where
    E: Entity,
{
    fn from_state_mut(state: &'system mut State, target: &E) -> Self;
}

pub trait FromState<E>
where
    E: Entity,
{
    fn from_state(state: &State, target: &E) -> Self;
}

impl State {
    pub fn new() -> Self {
        State { db: HashMap::new() }
    }

    fn init<I>(&mut self)
    where
        I: Indexable + 'static,
    {
        self.db
            .insert(TypeId::of::<I>(), Box::new(Index::<I>::new()));
    }

    pub fn insert_entity<E>(&mut self, entity: E)
    where
        E: Entity + 'static,
    {
        if !self.db.contains_key(&TypeId::of::<E>()) {
            self.init::<E>();
        }
        self.db
            .get_mut(&TypeId::of::<E>())
            .map(|index| index.downcast_mut::<Index<E>>().expect("downcast failed"))
            .unwrap()
            .insert(entity);
    }

    pub fn insert_resource<R>(&mut self, resource: R)
    where
        R: Resource + 'static,
    {
        self.insert_entity(BoxedResource::new(resource))
    }

    pub fn get_entity<E>(&self, id: &<E as Indexable>::Id) -> Option<&E>
    where
        E: Entity + 'static,
    {
        self.db
            .get(&TypeId::of::<E>())
            .map(|index| index.downcast_ref::<Index<E>>().expect("downcast failed"))
            .unwrap()
            .get(id)
    }

    pub fn get_entity_mut<E>(&mut self, id: &<E as Indexable>::Id) -> Option<&mut E>
    where
        E: Entity + 'static,
    {
        self.db
            .get_mut(&TypeId::of::<E>())
            .map(|index| index.downcast_mut::<Index<E>>().expect("downcast failed"))
            .unwrap()
            .get_mut(id)
    }

    pub fn get_resource<R>(&self) -> Option<&R>
    where
        R: Resource + 'static,
    {
        self.get_entity::<BoxedResource>(&TypeId::of::<R>())
            .map(|res| res.as_resource::<R>().expect("downcast failed"))
    }

    pub fn get_resource_mut<R>(&mut self) -> Option<&mut R>
    where
        R: Resource + 'static,
    {
        self.get_entity_mut::<BoxedResource>(&TypeId::of::<R>())
            .map(|res| res.as_resource_mut::<R>().expect("downcast failed"))
    }
}

#[cfg(test)]
mod tests {
    use crate::entity::WithParent;

    use super::*;

    #[test]
    fn it_insertion_and_retrieval_of_entities() {
        #[derive(PartialEq, Clone, Debug)]
        struct Directory {
            name: String,
        }

        impl Entity for Directory {}

        impl Indexable for Directory {
            type Id = String;

            fn id(&self) -> Self::Id {
                self.name.clone()
            }
        }

        impl WithParent for Directory {
            type Parent = ();

            fn pid(&self) {}
        }

        let mut system = State::new();

        system.insert_entity(Directory {
            name: "test".to_string(),
        });

        let dir = Directory {
            name: "test".to_string(),
        };

        assert_eq!(
            system.get_entity::<Directory>(&"test".to_string()),
            Some(&dir)
        );
    }

    #[test]
    fn it_allows_insertion_and_retrieval_of_resources() {
        #[derive(PartialEq, Clone, Debug)]
        struct Counter(u32);

        impl Resource for Counter {}

        let mut db = State::new();
        db.insert_resource(Counter(11));
        db.insert_resource("Hello world!!".to_string());

        assert_eq!(db.get_resource::<Counter>(), Some(&Counter(11)));
        assert_eq!(
            db.get_resource::<String>(),
            Some(&"Hello world!!".to_string())
        );
    }
}

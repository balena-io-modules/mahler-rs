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

    pub fn insert_entity<E>(&mut self, entity: E) -> &E
    where
        E: Entity + 'static,
    {
        if !self.db.contains_key(&TypeId::of::<E>()) {
            self.init::<E>();
        }
        let id = entity.id();
        self.db
            .get_mut(&TypeId::of::<E>())
            .map(|index| index.downcast_mut::<Index<E>>().expect("downcast failed"))
            .unwrap()
            .insert(entity);
        self.get_entity::<E>(&id).unwrap()
    }

    pub fn insert_resource<R>(&mut self, resource: R) -> &R
    where
        R: Resource + 'static,
    {
        self.insert_entity(BoxedResource::new(resource));
        self.get_resource::<R>().unwrap()
    }

    pub fn create_resource<R>(&mut self) -> &R
    where
        R: Resource + Default + 'static,
    {
        self.insert_resource(R::default());
        self.get_resource::<R>().unwrap()
    }

    pub fn create_resource_mut<R>(&mut self) -> &mut R
    where
        R: Resource + Default + 'static,
    {
        self.insert_resource(R::default());
        self.get_resource_mut::<R>().unwrap()
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

            fn parent_id(&self) {}
        }

        #[derive(PartialEq, Clone, Debug)]
        struct File<'dir> {
            name: String,
            contents: String,
            directory: &'dir Directory,
        }

        impl<'dir> Indexable for File<'dir> {
            type Id = (<Directory as Indexable>::Id, String);

            fn id(&self) -> Self::Id {
                (self.directory.id(), self.name.clone())
            }
        }

        impl<'dir> WithParent for File<'dir> {
            type Parent = Directory;

            fn parent_id(&self) -> String {
                self.directory.id()
            }
        }

        impl<'dir> Entity for File<'dir> {}

        let mut state = State::new();

        state.insert_entity(Directory {
            name: "test".to_string(),
        });

        assert_eq!(
            state.get_entity::<Directory>(&"test".to_string()),
            Some(&Directory {
                name: "test".to_string()
            })
        );

        // This doesn't work
        // let directory = state.get_entity::<Directory>(&"test".to_string()).unwrap();
        // state.insert_entity(File {
        //     name: "test.txt".to_string(),
        //     contents: "Hello world!!".to_string(),
        //     directory,
        // });
        //
        // assert_eq!(
        //     state.get_entity::<File>(&("/".to_string(), "test.txt".to_string())),
        //     Some(&File {
        //         name: "test.txt".to_string(),
        //         contents: "Hello world!!".to_string(),
        //         directory: &Directory {
        //             name: "test".to_string()
        //         }
        //     })
        // );
    }

    #[test]
    fn it_allows_insertion_and_retrieval_of_resources() {
        #[derive(PartialEq, Clone, Debug, Default)]
        struct Counter(u32);

        impl Resource for Counter {}

        let mut db = State::new();
        db.create_resource::<Counter>();
        db.insert_resource("Hello world!!".to_string());

        assert_eq!(db.get_resource::<Counter>(), Some(&Counter(0)));
        assert_eq!(
            db.get_resource::<String>(),
            Some(&"Hello world!!".to_string())
        );
    }
}

mod entity;
mod resource;
mod traits;

pub use entity::*;
pub use resource::*;
pub use traits::*;

use std::any::{Any, TypeId};
use std::collections::HashMap;

pub struct Database {
    tables: HashMap<TypeId, Box<dyn Any>>,
}

impl Database {
    pub fn new() -> Self {
        let mut this = Self {
            tables: HashMap::new(),
        };

        // Initialize the database with a table for BoxedResource
        this.tables.insert(
            TypeId::of::<BoxedResource>(),
            Box::new(Table::<BoxedResource>::new()),
        );

        this
    }

    pub fn init<E>(&mut self)
    where
        E: Entity + 'static,
    {
        self.tables
            .insert(TypeId::of::<E>(), Box::new(Table::<E>::new()));
    }

    pub fn insert<E>(&mut self, value: E)
    where
        E: Entity + 'static,
    {
        self.tables
            .get_mut(&TypeId::of::<E>())
            .map(|table| table.downcast_mut::<Table<E>>().expect("downcast failed"))
            .unwrap()
            .insert(value);
    }

    pub fn insert_resource<R>(&mut self, value: R)
    where
        R: Resource + 'static,
    {
        self.insert(BoxedResource::new(value));
    }

    pub fn get<E>(&self, id: &E::Id) -> Option<&E>
    where
        E: Entity + 'static,
    {
        self.tables
            .get(&TypeId::of::<E>())
            .map(|table| table.downcast_ref::<Table<E>>().expect("downcast failed"))
            .unwrap()
            .get(id)
    }

    pub fn get_resource<R>(&self) -> Option<&R>
    where
        R: Resource + 'static,
    {
        self.get::<BoxedResource>(&TypeId::of::<R>())
            .map(|res| res.as_resource::<R>())
    }

    pub fn get_mut<E>(&mut self, id: &E::Id) -> Option<&mut E>
    where
        E: Entity + 'static,
    {
        self.tables
            .get_mut(&TypeId::of::<E>())
            .map(|table| table.downcast_mut::<Table<E>>().expect("downcast failed"))
            .unwrap()
            .get_mut(id)
    }

    pub fn get_resource_mut<R>(&mut self) -> Option<&mut R>
    where
        R: Resource + 'static,
    {
        self.get_mut::<BoxedResource>(&TypeId::of::<R>())
            .map(|res| res.as_resource_mut::<R>())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn it_allows_insertion_and_retrieval_of_entities() {
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

        let mut db = Database::new();
        db.init::<Directory>();

        db.insert(Directory {
            name: "test".to_string(),
        });

        let dir = Directory {
            name: "test".to_string(),
        };

        assert_eq!(db.get::<Directory>(&"test".to_string()), Some(&dir));
    }

    #[test]
    fn it_allows_insertion_and_retrieval_of_resources() {
        #[derive(PartialEq, Clone, Debug)]
        struct Counter(u32);

        impl Resource for Counter {}

        let mut db = Database::new();
        db.insert_resource(Counter(11));
        db.insert_resource("Hello world!!".to_string());

        assert_eq!(db.get_resource::<Counter>(), Some(&Counter(11)));
        assert_eq!(
            db.get_resource::<String>(),
            Some(&"Hello world!!".to_string())
        );
    }
}

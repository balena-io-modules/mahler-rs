use std::hash::Hash;

pub trait Indexable {
    type Id: Hash + Eq;

    fn id(&self) -> Self::Id;
}

impl Indexable for () {
    type Id = ();

    fn id(&self) -> Self::Id {
        ()
    }
}

impl<T> Indexable for Option<T>
where
    T: Indexable,
{
    type Id = Option<T::Id>;

    fn id(&self) -> Self::Id
    where
        T: Indexable,
    {
        self.as_ref().map(|t| t.id())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::any::TypeId;
    use std::collections::HashMap;

    #[derive(PartialEq, Clone, Debug)]
    struct Counter(i32);

    impl Indexable for Counter {
        type Id = TypeId;

        fn id(&self) -> Self::Id {
            TypeId::of::<Self>()
        }
    }

    #[derive(PartialEq, Clone, Debug)]
    struct File {
        name: String,
        contents: String,
    }

    impl Indexable for File {
        type Id = String;

        fn id(&self) -> Self::Id {
            self.name.clone()
        }
    }

    #[test]
    fn it_allows_indexing_by_the_type_id() {
        let mut map = HashMap::new();
        let counter = Counter(0);
        map.insert(counter.id(), counter.clone());
        assert_eq!(map.get(&counter.id()), Some(&counter));

        // Counter is indexed by type id so inserting a new counter
        // replaces the previous one
        let counter2 = Counter(1);
        map.insert(counter2.id(), counter2.clone());
        assert_eq!(map.get(&counter.id()), Some(&counter2));
    }

    #[test]
    fn it_allows_indexing_by_explicit_id_type() {
        let mut map = HashMap::new();
        let file = File {
            name: "test.txt".to_string(),
            contents: "Hello, world!".to_string(),
        };
        map.insert(file.id(), file.clone());

        assert_eq!(map.get(&file.id()), Some(&file));
        let file2 = File {
            name: "test.txt".to_string(),
            contents: "Goodbye, world!".to_string(),
        };

        // Inserting a new file with the same name replaces the file
        map.insert(file2.id(), file2.clone());
        assert_eq!(map.get(&file.id()), Some(&file2));

        // Inserting a new file with a different name does not replace the file
        let file3 = File {
            name: "test2.txt".to_string(),
            contents: "Goodbye, world!".to_string(),
        };

        map.insert(file3.id(), file3.clone());
        assert_eq!(map.get(&file.id()), Some(&file2));
        assert_eq!(map.get(&file3.id()), Some(&file3));
    }
}

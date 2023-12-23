use super::traits::{Indexable, WithParent};

pub trait Entity: Indexable + WithParent {}

use std::collections::HashMap;

pub(crate) struct Table<E>
where
    E: Entity,
{
    rows: HashMap<E::Id, E>,
}

impl<E> Table<E>
where
    E: Entity,
{
    pub fn new() -> Self {
        Self {
            rows: HashMap::new(),
        }
    }

    pub fn insert(&mut self, value: E) {
        self.rows.insert(value.id(), value);
    }

    pub fn get(&self, id: &E::Id) -> Option<&E> {
        self.rows.get(id)
    }

    pub fn get_mut(&mut self, id: &E::Id) -> Option<&mut E> {
        self.rows.get_mut(id)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(PartialEq, Clone, Debug)]
    struct Directory {
        name: String,
    }

    // These implementations could happen via a derive
    // macro with arguments
    impl Indexable for Directory {
        type Id = String;

        fn id(&self) -> String {
            self.name.clone()
        }
    }

    impl WithParent for Directory {
        type Parent = ();

        fn pid(&self) {
            ()
        }
    }

    impl Entity for Directory {}

    #[derive(PartialEq, Clone, Debug)]
    struct File<'a> {
        name: String,
        contents: String,
        directory: &'a Directory,
    }

    // These implementations could also happen via a derive
    impl<'a> Indexable for File<'a> {
        type Id = (<Directory as Indexable>::Id, String);

        fn id(&self) -> Self::Id {
            (self.directory.id(), self.name.clone())
        }
    }

    impl<'a> WithParent for File<'a> {
        type Parent = Directory;

        fn pid(&self) -> String {
            self.directory.id()
        }
    }
    impl<'a> Entity for File<'a> {}

    #[test]
    fn it_allows_indexing_using_ids() {
        let mut directories = Table::new();
        directories.insert(Directory {
            name: "/".to_string(),
        });
        let directory = directories.get(&"/".to_string()).unwrap();

        let mut files = Table::new();
        files.insert(File {
            name: "file".to_string(),
            contents: "contents".to_string(),
            directory,
        });
        let file = files.get(&("/".to_string(), "file".to_string())).unwrap();

        assert_eq!(directory.id(), "/");
        assert_eq!(file.id(), ("/".to_string(), "file".to_string()));

        assert_eq!(
            files.get(&("/".to_string(), "file".to_string())),
            Some(file)
        );
    }
}

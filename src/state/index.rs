use std::collections::HashMap;

use super::Indexable;

pub(crate) struct Index<I>
where
    I: Indexable,
{
    rows: HashMap<I::Id, I>,
}

impl<I> Index<I>
where
    I: Indexable,
{
    pub fn new() -> Self {
        Self {
            rows: HashMap::new(),
        }
    }

    pub fn insert(&mut self, value: I) {
        self.rows.insert(value.id(), value);
    }

    pub fn get(&self, id: &I::Id) -> Option<&I> {
        self.rows.get(id)
    }

    pub fn get_mut(&mut self, id: &I::Id) -> Option<&mut I> {
        self.rows.get_mut(id)
    }
}

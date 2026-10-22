use std::{
    any::{Any, TypeId},
    collections::HashMap,
    sync::Arc,
};

#[derive(Clone)]
pub struct Resources(Arc<HashMap<TypeId, Arc<dyn Any + Send + Sync>>>);

impl Default for Resources {
    fn default() -> Self {
        Self::new()
    }
}

impl Resources {
    pub fn new() -> Self {
        Self(Arc::new(HashMap::new()))
    }

    pub fn insert<R>(&mut self, res: R)
    where
        R: Send + Sync + 'static,
    {
        let type_id = TypeId::of::<R>();
        Arc::make_mut(&mut self.0).insert(type_id, Arc::new(res));
    }

    pub fn get<R>(&self) -> Option<Arc<R>>
    where
        R: Send + Sync + 'static,
    {
        let type_id = TypeId::of::<R>();
        self.0.get(&type_id).and_then(|res| {
            // Clone the Arc<dyn Any + Send + Sync> first
            let arc = Arc::clone(res);

            // Then downcast it into Arc<E>
            arc.downcast::<R>().ok()
        })
    }
}

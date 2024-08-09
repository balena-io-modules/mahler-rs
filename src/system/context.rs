pub struct Context<S> {
    pub target: S,
}

impl<S: Clone> Clone for Context<S> {
    fn clone(&self) -> Self {
        Context {
            target: self.target.clone(),
        }
    }
}

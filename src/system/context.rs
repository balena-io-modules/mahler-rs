use crate::path::Path;

pub struct Context<S> {
    pub target: S,
    pub path: Path,
}

impl<S> Context<S> {
    pub fn from(target: S) -> Self {
        Self {
            target,
            path: Path::default(),
        }
    }

    pub fn with_path(self, path: Path) -> Self {
        Self {
            target: self.target,
            path,
        }
    }
}

impl<S: Clone> Clone for Context<S> {
    fn clone(&self) -> Self {
        Context {
            target: self.target.clone(),
            path: self.path.clone(),
        }
    }
}

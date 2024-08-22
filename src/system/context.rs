use crate::path::Path;

pub struct Context<S> {
    pub target: Option<S>,
    pub path: Path,
}

impl<S> Default for Context<S> {
    fn default() -> Self {
        Context {
            target: None,
            path: Path::default(),
        }
    }
}

impl<S> Context<S> {
    pub fn from(target: S) -> Self {
        Self {
            target: Some(target),
            path: Path::default(),
        }
    }

    pub fn with_path(self, path: &'static str) -> Self {
        Self {
            target: self.target,
            path: Path::from_static(path),
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

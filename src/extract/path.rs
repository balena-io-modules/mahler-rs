use std::fmt::{Display, Formatter};

use crate::errors::ExtractionError;
use crate::system::{FromSystem, System};
use crate::task::{Context, FromContext};

pub struct Path(pub String);

impl FromContext for Path {
    type Error = ExtractionError;

    fn from_context(context: &Context) -> Result<Self, Self::Error> {
        let path = context.path.to_str();

        Ok(Path(String::from(path)))
    }
}

impl Display for Path {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl FromSystem for Path {
    type Error = ExtractionError;

    fn from_system(_: &System, context: &Context) -> Result<Self, Self::Error> {
        Self::from_context(context)
    }
}

impl From<Path> for String {
    fn from(path: Path) -> Self {
        path.0
    }
}

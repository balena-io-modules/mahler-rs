use std::fmt;
use std::ops::Deref;

#[derive(Debug, Clone, Eq, PartialEq, Hash, Copy, PartialOrd, Ord)]
pub struct Id(&'static str);

impl fmt::Display for Id {
    fn fmt(&self, fmt: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt.write_str(self.as_ref())
    }
}

impl From<&'static str> for Id {
    fn from(s: &'static str) -> Self {
        Id(s)
    }
}

impl AsRef<str> for Id {
    fn as_ref(&self) -> &str {
        self.0
    }
}

impl Deref for Id {
    type Target = str;

    fn deref(&self) -> &str {
        self.0
    }
}

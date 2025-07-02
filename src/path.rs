use jsonptr::{Pointer, PointerBuf};
use std::cmp::Ordering;
use std::fmt::Display;
use std::ops::{Deref, DerefMut};
use std::sync::Arc;

#[derive(Clone, Default, PartialEq, Eq, Debug)]
pub struct Path(PointerBuf);

impl PartialOrd for Path {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for Path {
    fn cmp(&self, other: &Self) -> Ordering {
        // Compare first by path length (number of tokens)
        // shorter paths have higher ordering
        other
            .0
            .count()
            .cmp(&self.0.count())
            // Then lexicographically
            .then(self.0.cmp(&other.0))
    }
}
impl Path {
    pub(crate) fn new(pointer: &Pointer) -> Self {
        Self(pointer.to_buf())
    }

    /// Create a static path from a string
    ///
    /// # Panics
    ///
    /// This will panic if the string is not a valid path
    pub fn from_static(s: &'static str) -> Self {
        Path(Pointer::from_static(s).to_buf())
    }

    /// Get the internal string representation for the Path
    pub fn as_str(&self) -> &str {
        self.0.as_str()
    }
}

impl Display for Path {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl From<Path> for String {
    fn from(path: Path) -> String {
        path.0.to_string()
    }
}

impl From<Path> for PointerBuf {
    fn from(path: Path) -> Self {
        path.0
    }
}

impl AsRef<Pointer> for Path {
    fn as_ref(&self) -> &Pointer {
        &self.0
    }
}

impl Deref for Path {
    type Target = str;

    fn deref(&self) -> &Self::Target {
        self.as_str()
    }
}

// Structure to store path arguments when matching
// against a lens
#[derive(Clone, Default, Debug, PartialEq, Eq)]
pub(crate) struct PathArgs(pub Vec<(Arc<str>, String)>);

impl PathArgs {
    pub fn iter(&self) -> impl Iterator<Item = &(Arc<str>, String)> {
        self.0.iter()
    }

    pub fn insert(&mut self, key: impl AsRef<str>, value: impl Into<String>) -> Option<String> {
        let existing = self.0.iter_mut().find(|(k, _)| k.as_ref() == key.as_ref());
        if let Some((_, v)) = existing {
            let old = v.clone();
            *v = value.into();
            return Some(old);
        } else {
            self.0.push((Arc::from(key.as_ref()), value.into()))
        }
        None
    }
}

impl Display for PathArgs {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let entries = self
            .0
            .iter()
            .map(|(key, value)| format!("\"{key}\": \"{value}\""))
            .collect::<Vec<_>>()
            .join(", ");

        write!(f, "{{{entries}}}")
    }
}

impl Deref for PathArgs {
    type Target = Vec<(Arc<str>, String)>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl DerefMut for PathArgs {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

impl From<matchit::Params<'_, '_>> for PathArgs {
    fn from(params: matchit::Params) -> PathArgs {
        let params: Vec<(Arc<str>, String)> = params
            .iter()
            .map(|(k, v)| (Arc::from(k), String::from(v)))
            .collect();

        PathArgs(params)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn it_converts_a_path_to_string() {
        assert_eq!(Path::from_static("/a/b/c").to_string(), "/a/b/c");
        assert_eq!(Path::from_static("/").to_string(), "/");
        assert_eq!(Path::from_static("").to_string(), "");
    }

    #[test]
    fn it_converts_a_str_to_a_path() {
        let path = Path::from_static("/a/b/c");
        assert_eq!(String::from(path), "/a/b/c");
    }

    #[test]
    #[should_panic]
    fn it_panics_on_an_invalid_path() {
        Path::from_static("a/b/c");
    }

    #[test]
    fn it_replaces_existing_key_with_insert() {
        let mut args = PathArgs::default();
        args.insert("one", "a");
        args.insert("two", "b");

        assert_eq!(
            args.0,
            vec![
                (Arc::from("one"), "a".to_string()),
                (Arc::from("two"), "b".to_string())
            ]
        );

        // Replace the key
        let old = args.insert("one", "c");
        assert_eq!(
            args.0,
            vec![
                (Arc::from("one"), "c".to_string()),
                (Arc::from("two"), "b".to_string())
            ]
        );
        assert_eq!(old, Some("a".to_string()))
    }
}

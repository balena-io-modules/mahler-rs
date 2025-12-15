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
        // shorter paths come before longer paths no matter the
        // lexicographic order
        self.0
            .count()
            .cmp(&other.0.count())
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

    /// Returns true if this path is a prefix of the other path.
    /// A path is a prefix of another if all its tokens match the beginning of the other's tokens.
    /// For example, "/a" is a prefix of "/a/b", but "/a" is NOT a prefix of "/aa".
    pub fn is_prefix_of(&self, other: &Path) -> bool {
        let self_tokens: Vec<_> = self.0.tokens().collect();
        let other_tokens: Vec<_> = other.0.tokens().collect();

        // Empty path is a prefix of everything
        if self_tokens.is_empty() {
            return true;
        }

        // Can't be a prefix if we have more tokens
        if self_tokens.len() > other_tokens.len() {
            return false;
        }

        // Check if all our tokens match the beginning of other's tokens
        self_tokens
            .iter()
            .zip(other_tokens.iter())
            .all(|(a, b)| a == b)
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
pub struct PathArgs(Vec<(Arc<str>, String)>);

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

    pub fn contains_key(&self, key: impl AsRef<str>) -> bool {
        self.0.iter().any(|(k, _)| k.as_ref() == key.as_ref())
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

impl<K: AsRef<str>, V: Into<String>> From<Vec<(K, V)>> for PathArgs {
    fn from(args: Vec<(K, V)>) -> Self {
        PathArgs(
            args.into_iter()
                .map(|(k, v)| (Arc::from(k.as_ref()), v.into()))
                .collect(),
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn it_sorts_path_by_length_and_then_lexicographically() {
        let mut paths = vec![
            Path::from_static("/a/b/c"),
            Path::from_static(""),
            Path::from_static("/a/b"),
            Path::from_static("/"),
            Path::from_static("/a/f"),
            Path::from_static("/x"),
            Path::from_static("/a/b/d"),
        ];

        paths.sort();
        assert_eq!(
            paths,
            vec![
                Path::from_static(""),
                Path::from_static("/"),
                Path::from_static("/x"),
                Path::from_static("/a/b"),
                Path::from_static("/a/f"),
                Path::from_static("/a/b/c"),
                Path::from_static("/a/b/d"),
            ]
        )
    }

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

    #[test]
    fn test_is_prefix_of() {
        // Basic prefix relationship
        let path_a = Path::from_static("/a");
        let path_a_b = Path::from_static("/a/b");
        let path_a_b_c = Path::from_static("/a/b/c");
        let path_aa = Path::from_static("/aa");
        let path_b = Path::from_static("/b");
        let root = Path::from_static("");

        // True cases - proper path prefixes
        assert!(path_a.is_prefix_of(&path_a_b));
        assert!(path_a.is_prefix_of(&path_a_b_c));
        assert!(path_a_b.is_prefix_of(&path_a_b_c));
        assert!(root.is_prefix_of(&path_a));
        assert!(root.is_prefix_of(&path_a_b));
        assert!(path_a.is_prefix_of(&path_a)); // Self prefix

        // False cases - not path prefixes
        assert!(!path_a.is_prefix_of(&path_aa)); // "/a" is NOT a prefix of "/aa"
        assert!(!path_a.is_prefix_of(&path_b));
        assert!(!path_a_b.is_prefix_of(&path_a)); // Longer path cannot be prefix of shorter
        assert!(!path_aa.is_prefix_of(&path_a));
    }
}

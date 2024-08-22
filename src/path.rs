use crate::error::Error;
use std::fmt::Display;

#[derive(Clone, Default, PartialEq, Debug)]
pub struct Path(String);

fn encode<S: AsRef<str>>(s: S) -> String {
    s.as_ref().replace("~1", "/").replace("~0", "~")
}

fn decode<S: AsRef<str>>(s: S) -> String {
    s.as_ref().replace('~', "~0").replace('/', "~1")
}

impl Path {
    pub fn split(&self) -> Vec<String> {
        self.0.split('/').skip(1).map(encode).collect()
    }

    fn parse<S: AsRef<str>>(s: S) -> Result<Path, Error> {
        let pointer = s.as_ref();
        if !pointer.is_empty() && !pointer.starts_with('/') {
            return Err(Error::InvalidPath(String::from(pointer)));
        }

        Ok(Path(pointer.to_string()))
    }

    pub fn from_static(s: &'static str) -> Path {
        Path::parse(s).unwrap()
    }

    pub fn is_root(&self) -> bool {
        self.0.is_empty()
    }

    pub fn parent(&self) -> Path {
        let mut parts = self.split();
        parts.pop();
        parts.into()
    }

    pub fn basename(&self) -> Option<String> {
        if self.0 == "/" {
            return Some(String::from(""));
        }
        self.split().pop()
    }
}

impl Display for Path {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl From<Path> for String {
    fn from(path: Path) -> String {
        path.0
    }
}

impl AsRef<str> for Path {
    fn as_ref(&self) -> &str {
        &self.0
    }
}

impl<S> From<Vec<S>> for Path
where
    S: AsRef<str>,
{
    fn from(path: Vec<S>) -> Self {
        let parts = path.iter().map(decode).collect::<Vec<String>>();

        if parts.is_empty() {
            return Path(String::from(""));
        }

        Path(format!("/{}", parts.join("/")))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn it_converts_a_path_to_string() {
        let path = Path("/a/b/c".to_string());
        assert_eq!(String::from(path), "/a/b/c");
        assert_eq!(Path::from_static("/").to_string(), "/")
    }

    #[test]
    fn it_converts_a_vec_of_strings_to_a_path() {
        let path: Path = vec!["a", "~b", "c/d"].into();
        assert_eq!(String::from(path), "/a/~0b/c~1d");
    }

    #[test]
    fn it_converts_a_path_to_a_vec_of_strings() {
        let path = Path("/a/~0b/c~1d".to_string());
        assert_eq!(path.split(), vec!["a", "~b", "c/d"]);
    }

    #[test]
    fn it_converts_a_str_to_a_path() {
        let path = Path::from_static("/a/b/c");
        assert_eq!(String::from(path), "/a/b/c");
    }

    #[test]
    fn it_errors_on_an_invalid_path() {
        let path = Path::parse("a/b/c");
        assert!(path.is_err());

        let path = Path::parse("").unwrap();
        assert_eq!(path.as_ref(), "")
    }

    #[test]
    fn it_allows_to_get_the_parent_of_a_path() {
        assert_eq!(
            Path::from_static("/a/b/c").parent(),
            Path::from_static("/a/b")
        );
        assert_eq!(Path::from_static("/a").parent(), Path::from_static(""));
        assert_eq!(Path::from_static("/").parent(), Path::from_static(""));
        assert_eq!(Path::from_static("").parent(), Path::from_static(""));
    }

    #[test]
    fn it_allows_to_get_the_basename_of_a_path() {
        assert_eq!(
            Path::from_static("/a/b/c").basename(),
            Some("c".to_string())
        );
        assert_eq!(Path::from_static("/a").basename(), Some("a".to_string()));
        assert_eq!(Path::from_static("/").basename(), Some("".to_string()));
        assert_eq!(Path::from_static("").basename(), None);
    }
}

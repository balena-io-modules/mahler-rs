use crate::error::Error;
use std::fmt::Display;

#[derive(Clone, Default)]
pub struct Path(String);

impl Path {
    pub fn split(&self) -> Vec<String> {
        self.0
            .split('/')
            .skip(1)
            .map(|x| x.replace("~1", "/").replace("~0", "~"))
            .collect()
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
        let parts = path
            .iter()
            .map(|x| x.as_ref().replace('~', "~0").replace('/', "~1"))
            .collect::<Vec<String>>();

        if parts.is_empty() {
            return Path(String::from(""));
        }

        Path(format!("/{}", parts.join("/")))
    }
}

impl TryFrom<&str> for Path {
    type Error = Error;

    fn try_from(pointer: &str) -> Result<Self, Self::Error> {
        if !pointer.is_empty() && !pointer.starts_with('/') {
            return Err(Error::InvalidPath(String::from(pointer)));
        }

        Ok(Path(pointer.to_string()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn it_converts_a_path_to_string() {
        let path = Path("/a/b/c".to_string());
        assert_eq!(String::from(path), "/a/b/c");
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
        let path = Path::try_from("/a/b/c").unwrap();
        assert_eq!(String::from(path), "/a/b/c");
    }

    #[test]
    fn it_errors_on_an_invalid_path() {
        let path = Path::try_from("a/b/c");
        assert!(path.is_err());

        let path = Path::try_from("").unwrap();
        assert_eq!(path.as_ref(), "")
    }
}

use jsonptr::Pointer;
use std::fmt::Display;

#[derive(Clone, Default, PartialEq, Debug)]
pub struct Path(&'static Pointer);

impl Path {
    pub fn from_static(s: &'static str) -> Path {
        Path(Pointer::from_static(s))
    }

    pub fn to_str(&self) -> &str {
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

impl AsRef<Pointer> for Path {
    fn as_ref(&self) -> &Pointer {
        self.0
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
}

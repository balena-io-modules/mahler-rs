use std::any::TypeId;
use std::fmt;
use std::hash::{Hash, Hasher};
use std::ops::Deref;

/// A unique, human-readable identifier for a handler.
///
/// Combines [`TypeId`] (for guaranteed uniqueness per type) with
/// [`type_name`](std::any::type_name) (for human-readable display in logs).
/// Equality and hashing use `TypeId` only, so two different closure types
/// always produce distinct `Id`s even if `type_name` collides.
///
/// Note that, like with [`TypeId`], ordering between ids may vary between Rust
/// releases, you should avoid relying on order remaining consistent.
#[derive(Clone, Copy)]
pub struct Id {
    type_id: TypeId,
    name: &'static str,
}

impl Id {
    /// Create an `Id` from the type `T`.
    pub fn of<T: 'static>() -> Self {
        Id {
            type_id: TypeId::of::<T>(),
            name: std::any::type_name::<T>(),
        }
    }
}

impl PartialEq for Id {
    fn eq(&self, other: &Self) -> bool {
        self.type_id == other.type_id
    }
}

impl Eq for Id {}

impl Hash for Id {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.type_id.hash(state);
    }
}

impl Ord for Id {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.type_id.cmp(&other.type_id)
    }
}

impl PartialOrd for Id {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl fmt::Display for Id {
    fn fmt(&self, fmt: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt.write_str(self.name)
    }
}

impl fmt::Debug for Id {
    fn fmt(&self, fmt: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt.write_str(self.name)
    }
}

impl AsRef<str> for Id {
    fn as_ref(&self) -> &str {
        self.name
    }
}

impl Deref for Id {
    type Target = str;

    fn deref(&self) -> &str {
        self.name
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn different_types_have_different_ids() {
        let id_a = Id::of::<fn() -> bool>();
        let id_b = Id::of::<fn() -> i32>();
        assert_ne!(id_a, id_b);
    }

    #[test]
    fn same_type_has_same_id() {
        let id_a = Id::of::<fn() -> bool>();
        let id_b = Id::of::<fn() -> bool>();
        assert_eq!(id_a, id_b);
    }

    #[test]
    fn id_displays_type_name() {
        let id = Id::of::<String>();
        assert!(id.to_string().contains("String"));
    }
}

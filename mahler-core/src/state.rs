//! Derive traits for Mahler
//!
//! This module provides the [`State`] trait and primitive type implementations.
//!
//! While the trait is not necessary (yet) for performing [Worker](`crate::worker::Worker`)
//! operations, it is useful (along with the derive implementation), to ensure that the current and
//! target state types are structurally compatible to avoid planning issues.
//!
//! # The Problem
//!
//! When using different types for current and target states, one can tipically define them in
//! the following way.
//!
//! ```rust
//! use serde::{Serialize, Deserialize};
//!
//! #[derive(Serialize, Deserialize, Debug, Clone)]
//! struct Current {
//!     #[serde(skip_serializing_if = "Option::is_none")]
//!     name: Option<String>,
//! }
//!
//! #[derive(Serialize, Deserialize, Debug, Clone)]
//! struct Target {
//!     name: Option<String>,
//! }
//! ```
//!
//! However, this can lead to subtle bugs because the planner normalizes the current
//! state using the target type's serialization rules. If the serialization differs
//! (like `skip_serializing_if` above), the planner might see different operations
//! than expected.
//!
//! # The Solution
//!
//! The [`State`] trait and `#[derive(State)]` macro solve this by ensuring structural
//! compatibility between current and target types:
//!
//! ```rust
//! use mahler::State;
//! use serde::{Serialize, Deserialize};
//!
//! #[derive(State, Serialize, Deserialize, Debug, Clone)]
//! struct Service {
//!     // Serialization rules apply to both current and target
//!     #[serde(skip_serializing_if = "Option::is_none")]
//!     name: Option<String>,
//!
//!     // This field only exists in current state, not in targets
//!     #[mahler(internal)]
//!     created_at: Option<String>,
//! }
//! ```
//!
//! This generates a compatible `MyStateTarget` type automatically.

use serde::{Deserialize, Serialize};

/// A trait for types that can be used as mahler state models.
///
/// This trait links a state type with its target type. When used with the `#derive[(State)]`
/// feature, this allows to avoid issues by ensuring structural compatibility between the current
/// and target types..
///
/// ```rust
/// use mahler::State;
/// use serde::{Serialize, Deserialize};
///
/// #[derive(State, Serialize, Deserialize, Debug, Clone)]
/// struct Service {
///     name: String,
///
///     #[serde(skip_serializing_if = "Option::is_none")]
///     image: Option<String>,
///
///     // Only present in current state, not targets
///     #[mahler(internal)]
///     container_id: Option<String>,
/// }
/// ```
///
/// This automatically generates a `ServiceTarget` type that excludes
/// `#[mahler(internal)]` fields and preserves all serde attributes.
pub trait State: Serialize {
    /// The corresponding target type for this model.
    ///
    /// This type should be structurally compatible with the current type,
    /// typically generated automatically by the derive macro.
    type Target: Serialize + for<'de> Deserialize<'de>;
}

// Implement State for primitive types with Target = Self
// This allows the derive macro to consistently use ::Target for all field types

macro_rules! primitive_impl {
    ($($t:ty),*) => {
        $(
            impl State for $t {
                type Target = Self;
            }
        )*
    };
}

primitive_impl!(
    bool, char, i8, i16, i32, i64, i128, isize, u8, u16, u32, u64, u128, usize, f32, f64
);

impl State for String {
    type Target = Self;
}

impl<T: State> State for Option<T> {
    type Target = Option<T::Target>;
}

impl<T: State> State for Vec<T> {
    type Target = Vec<T::Target>;
}

impl<T: State, E: State> State for Result<T, E> {
    type Target = Result<T::Target, E::Target>;
}

impl<T: State> State for Box<T> {
    type Target = Box<T::Target>;
}

// HashMap and BTreeMap implementations
impl<K: State, V: State> State for std::collections::HashMap<K, V>
where
    K::Target: std::hash::Hash + Eq,
{
    type Target = std::collections::HashMap<K::Target, V::Target>;
}

impl<K: State, V: State> State for std::collections::BTreeMap<K, V>
where
    K::Target: Ord,
{
    type Target = std::collections::BTreeMap<K::Target, V::Target>;
}

// Tuple implementations (up to 8 values)
macro_rules! tuple_impl {
    () => {
        impl State for () {
            type Target = ();
        }
    };
    ($($name:ident)+) => {
        impl<$($name: State),+> State for ($($name,)+) {
            type Target = ($($name::Target,)+);
        }
    };
}

tuple_impl!();
tuple_impl!(T0);
tuple_impl!(T0 T1);
tuple_impl!(T0 T1 T2);
tuple_impl!(T0 T1 T2 T3);
tuple_impl!(T0 T1 T2 T3 T4);
tuple_impl!(T0 T1 T2 T3 T4 T5);
tuple_impl!(T0 T1 T2 T3 T4 T5 T6);
tuple_impl!(T0 T1 T2 T3 T4 T5 T6 T7);

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json;

    // Manual implementation example for testing
    #[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
    struct TestModel {
        pub name: String,
        #[serde(skip_serializing_if = "Option::is_none")]
        pub optional_field: Option<String>,
        // This would be marked as #[mahler(internal)] in real usage
        pub internal_field: Option<String>,
    }

    #[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
    struct TargetTestModel {
        pub name: String,
        #[serde(skip_serializing_if = "Option::is_none")]
        pub optional_field: Option<String>,
        // internal_field is excluded
    }

    impl State for TestModel {
        type Target = TargetTestModel;
    }

    fn to_target<T: State>(t: &T) -> Result<T::Target, serde_json::Error> {
        let value = serde_json::to_value(t)?;
        let target = serde_json::from_value(value)?;
        Ok(target)
    }

    #[test]
    fn test_model_conversion() {
        let model = TestModel {
            name: "test".to_string(),
            optional_field: Some("value".to_string()),
            internal_field: Some("internal".to_string()),
        };

        let target = to_target(&model).unwrap();
        assert_eq!(target.name, "test");
        assert_eq!(target.optional_field, Some("value".to_string()));
    }

    #[test]
    fn test_serialization_compatibility() {
        let model = TestModel {
            name: "test".to_string(),
            optional_field: None,
            internal_field: Some("internal".to_string()),
        };

        let target = to_target(&model).unwrap();

        // Both should serialize the same way for optional_field when None
        let model_json = serde_json::to_value(&model).unwrap();
        let target_json = serde_json::to_value(&target).unwrap();

        // Remove internal_field from model_json for comparison
        let mut model_json_comparable = model_json.as_object().unwrap().clone();
        model_json_comparable.remove("internal_field");

        assert_eq!(
            serde_json::Value::Object(model_json_comparable),
            target_json
        );
    }
}

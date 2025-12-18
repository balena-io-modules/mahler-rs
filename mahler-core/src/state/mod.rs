//! State trait and standardized serialization.
//!
//! This module provides the State trait which defines how types are serialized
//! and deserialized in a standardized way, including the halted state.

use std::marker::PhantomData;

use crate::serde::{self, Deserialize, Serialize};

mod collections;

// Re-export collection types
pub use collections::{List, Map, Set};

/// The State trait defines types that can be used in Mahler's state management.
///
/// State provides standardized serialization that always includes halted information
/// and uses custom serialization for nested State values.
///
/// # Implementation
///
/// You should use the `#[derive(State)]` macro to implement this trait.
///
/// # Example
///
/// ```
/// use mahler::state::State;
///
/// #[derive(State)]
/// struct Service {
///     name: String,
///     port: u16,
///
///     // Internal fields are excluded from Target
///     #[mahler(internal)]
///     container_id: Option<String>,
/// }
/// ```
///
/// This generates a `ServiceTarget` type that excludes internal fields.
pub trait State: Serialize + for<'de> Deserialize<'de> {
    /// The corresponding target type for this state.
    ///
    /// For primitive types and collections, Target = Self.
    /// For structs, a separate Target type is generated excluding internal fields.
    type Target: Serialize + for<'de> Deserialize<'de>;

    /// Returns whether this state is halted and doesn't admit changes.
    ///
    /// When a state is halted, the planner will skip it during planning.
    ///
    /// The default implementation returns false (not halted).
    fn is_halted(&self) -> bool {
        false
    }

    /// Serialize using State's standardized serialization.
    ///
    /// If State is derived using the procedural macro, the internal state
    /// will include information about the `halted` state.
    ///
    /// This is used internally by Mahler and is not meant be called directly by users.
    ///
    /// The default implementation delegates to serde's Serialize, which works for
    /// primitives and types that implement Serialize.
    fn as_internal<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        Serialize::serialize(self, serializer)
    }
}

/// Wrapper for serializing State using its standardized serialization.
///
/// This wrapper is used internally by Mahler to serialize State types
/// using their `as_internal` method instead of the regular Serialize trait.
///
/// Users should not need to use this directly.
#[doc(hidden)]
pub struct AsInternal<'a, T: State>(pub &'a T);

impl<'a, T: State> Serialize for AsInternal<'a, T> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        self.0.as_internal(serializer)
    }
}

/// Helper for deserializing State values using DeserializeSeed.
///
/// This allows deserialization of State values using their `from_internal` method
/// without needing the type to implement `Deserialize`.
struct StateDeserializer<T>(pub PhantomData<T>);

impl<'de, T: serde::Deserialize<'de>> serde::de::DeserializeSeed<'de> for StateDeserializer<T> {
    type Value = T;

    fn deserialize<D>(self, deserializer: D) -> Result<Self::Value, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        T::deserialize(deserializer)
    }
}

// Primitive type implementations
// These delegate to serde since primitives serialize the same way

macro_rules! impl_state_for_primitive {
    ($($t:ty),*) => {
        $(
            impl State for $t {
                type Target = Self;
            }
        )*
    };
}

impl_state_for_primitive!(
    bool, i8, i16, i32, i64, i128, isize, u8, u16, u32, u64, u128, usize, f32, f64, char, String
);

// Option<T> implementation
// Must use custom visitor to deserialize T using State deserialization

impl<T: State> State for Option<T> {
    type Target = Option<T::Target>;

    fn as_internal<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        match self {
            Some(value) => serializer.serialize_some(&AsInternal(value)),
            None => serializer.serialize_none(),
        }
    }
}

// Box<T> implementation

impl<T: State> State for Box<T> {
    type Target = Box<T::Target>;

    fn as_internal<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        AsInternal(self.as_ref()).serialize(serializer)
    }
}

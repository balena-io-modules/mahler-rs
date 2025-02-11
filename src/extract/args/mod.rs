use serde::de::DeserializeOwned;
use std::ops::Deref;

use crate::error::{Error, IntoError};
use crate::system::{FromSystem, System};
use crate::task::Context;

mod de;
mod error;

pub use error::ArgsDeserializationError;

impl IntoError for ArgsDeserializationError {
    fn into_error(self) -> Error {
        Error::ArgsExtractFailed(self)
    }
}

#[derive(Debug)]
pub struct Args<T>(pub T);

impl<T: DeserializeOwned + Send> FromSystem for Args<T> {
    type Error = ArgsDeserializationError;

    fn from_system(_: &System, context: &Context) -> Result<Self, Self::Error> {
        let args = &context.args;
        T::deserialize(de::PathDeserializer::new(args)).map(Args)
    }
}

impl<S> Deref for Args<S> {
    type Target = S;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::task::*;
    use serde::{Deserialize, Serialize};
    use std::collections::HashMap;

    // The state model
    #[derive(Serialize, Deserialize, Debug, Clone)]
    struct State {
        numbers: HashMap<String, i32>,
    }

    #[test]
    fn deserializes_simple_path_args() {
        let mut numbers = HashMap::new();
        numbers.insert("one".to_string(), 1);
        numbers.insert("two".to_string(), 2);

        let state = State { numbers };

        let system = System::from(state);

        let Args(name): Args<String> =
            Args::from_system(&system, &Context::new().with_arg("name", "one")).unwrap();

        assert_eq!(name, "one");
    }

    #[test]
    fn deserializes_tuple_args() {
        let mut numbers = HashMap::new();
        numbers.insert("one".to_string(), 1);
        numbers.insert("two".to_string(), 2);

        let state = State { numbers };

        let system = System::from(state);

        let Args((first, second)): Args<(String, String)> = Args::from_system(
            &system,
            &Context::new()
                .with_arg("first", "one")
                .with_arg("second", "two"),
        )
        .unwrap();

        assert_eq!(first, "one");
        assert_eq!(second, "two");
    }

    #[test]
    fn deserializes_hashmap_args() {
        let mut numbers = HashMap::new();
        numbers.insert("one".to_string(), 1);
        numbers.insert("two".to_string(), 2);

        let state = State { numbers };

        let system = System::from(state);

        let Args(map): Args<HashMap<String, String>> = Args::from_system(
            &system,
            &Context::new()
                .with_arg("first", "one")
                .with_arg("second", "two"),
        )
        .unwrap();

        assert_eq!(
            map,
            HashMap::from([
                ("first".to_string(), "one".to_string()),
                ("second".to_string(), "two".to_string())
            ])
        );
    }
}

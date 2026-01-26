//! Sensor handler trait and implementations

use std::fmt;
use std::pin::Pin;
use std::sync::Arc;

use matchit::Router;
use tokio_stream::{Stream, StreamExt};

use crate::error::Error;
use crate::json::{PathArgs, Value};
use crate::result::Result;
use crate::runtime::{Channel, Context, FromSystem, System};
use crate::serde::Serialize;

/// A boxed stream of JSON values that sensors produce
pub type SensorStream = Pin<Box<dyn Stream<Item = Result<Value>> + Send + 'static>>;

/// Trait for functions that can be converted into sensors
///
/// A SensorHandler is any function that accepts zero or more "[system extractors](`FromSystem`)" as
/// arguments and returns something that can be converted into a [`Stream`] of values.
///
/// Note that the handler is called when the sensor is initialized, where system extractors
/// contain the state of the system at that point in time.
pub trait SensorHandler<T, U>: Clone + Sync + Send + 'static {
    /// Create the sensor stream from the given context
    ///
    /// This is called when subscribing to a sensor for a specific path.
    fn call(&self, system: &System, context: &Context) -> Result<SensorStream>;
}

/// Helper trait for types that can be converted into a SensorStream
trait IntoSensorStream<T> {
    fn into_sensor_stream(self) -> SensorStream;
}

trait IntoSensorResult<T> {
    fn into_sensor_result(self) -> Result<T>;
}

impl<T> IntoSensorResult<T> for T {
    fn into_sensor_result(self) -> Result<T> {
        Ok(self)
    }
}

impl<T, E> IntoSensorResult<T> for std::result::Result<T, E>
where
    E: std::error::Error + Sync + Send + 'static,
{
    fn into_sensor_result(self) -> Result<T> {
        self.map_err(Error::runtime)
    }
}

impl<S, T, U> IntoSensorStream<U> for S
where
    S: Stream<Item = T> + Send + 'static,
    T: IntoSensorResult<U> + Send + 'static,
    U: Serialize + Send + 'static,
{
    fn into_sensor_stream(self) -> SensorStream {
        Box::pin(self.map(move |item| {
            item.into_sensor_result()
                .and_then(|input| serde_json::to_value(input).map_err(Error::from))
        }))
    }
}

// Macro to implement SensorHandler for functions with varying numbers of FromContext extractors
macro_rules! impl_sensor_handler {
    (
        $($ty:ident),*
    ) => {
        #[allow(non_snake_case, unused)]
        impl<F, $($ty,)* U, S> SensorHandler<($($ty,)*), U> for F
        where
            F: Fn($($ty,)*) -> S + Clone + Send + Sync + 'static,
            S: IntoSensorStream<U>,
            $($ty: FromSystem + Send,)*
        {
            fn call(&self, system: &System, context: &Context) -> Result<SensorStream> {
                $(
                    let $ty = $ty::from_system(system, context, &Channel::detached())?;
                )*

                let stream = (self)($($ty,)*);
                Ok(stream.into_sensor_stream())
            }
        }
    };
}

// Implement for 0-8 extractors
impl_sensor_handler!();
impl_sensor_handler!(T1);
impl_sensor_handler!(T1, T2);
impl_sensor_handler!(T1, T2, T3);
impl_sensor_handler!(T1, T2, T3, T4);
impl_sensor_handler!(T1, T2, T3, T4, T5);
impl_sensor_handler!(T1, T2, T3, T4, T5, T6);
impl_sensor_handler!(T1, T2, T3, T4, T5, T6, T7);
impl_sensor_handler!(T1, T2, T3, T4, T5, T6, T7, T8);

/// Type alias for the sensor factory function
type SensorFactory = Arc<dyn Fn(&System, &Context) -> Result<SensorStream> + Send + Sync>;

/// A sensor that monitors external state and pushes updates
///
/// Sensors are created using the [`sensor`] function and registered with a Worker.
#[derive(Clone)]
pub(crate) struct Sensor {
    pub id: &'static str,
    factory: SensorFactory,
}

impl Sensor {
    /// Create a new sensor from a handler
    pub fn new<H, T, U>(handler: H) -> Self
    where
        H: SensorHandler<T, U>,
    {
        Sensor {
            id: std::any::type_name::<H>(),
            factory: Arc::new(move |system, context| handler.call(system, context)),
        }
    }

    /// Create a sensor stream for the given context
    pub fn create_stream(&self, system: &System, context: &Context) -> Result<SensorStream> {
        (self.factory)(system, context)
    }
}

impl fmt::Debug for Sensor {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Sensor").field("id", &self.id).finish()
    }
}

impl PartialEq for Sensor {
    fn eq(&self, other: &Self) -> bool {
        self.id == other.id
    }
}

impl Eq for Sensor {}

/// Registry of sensors mapped to routes
#[derive(Default, Debug, Clone)]
pub(crate) struct SensorRouter(Router<Sensor>);

impl SensorRouter {
    /// Create a new empty sensor registry
    pub fn new() -> Self {
        Self(Router::new())
    }

    /// Add a sensor to the registry
    ///
    /// # Panics
    ///
    /// This function will panic if the route is not a valid path
    pub fn insert(&mut self, route: &'static str, sensor: Sensor) -> Option<Sensor> {
        // remove the route if it already exists
        let old_sensor = self.0.remove(route);

        // (re)insert the queue to the router
        self.0.insert(route, sensor).expect("route should be valid");

        old_sensor
    }

    /// Find a sensor matching the given route
    pub fn at(&self, path: &str) -> Option<(PathArgs, &Sensor)> {
        self.0
            .at(path)
            .map(|matched| (PathArgs::from(matched.params), matched.value))
            .ok()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn dummy_sensor() -> impl Stream<Item = i32> {
        tokio_stream::iter(vec![1, 2, 3])
    }

    #[test]
    fn test_registry_find_matching_exact_path() {
        let mut registry = SensorRouter::new();
        registry.insert("/temperature", Sensor::new(dummy_sensor));

        let result = registry.at("/temperature");
        assert!(result.is_some());

        // should not panic
        result.unwrap();
    }

    #[test]
    fn test_registry_find_matching_with_parameter() {
        let mut registry = SensorRouter::new();
        registry.insert("/rooms/{room}/temperature", Sensor::new(dummy_sensor));

        let result = registry.at("/rooms/kitchen/temperature");
        assert!(result.is_some());

        let (args, _) = result.unwrap();
        assert_eq!(args.len(), 1);
        assert_eq!(args[0].0.as_ref(), "room");
        assert_eq!(args[0].1, "kitchen");
    }

    #[test]
    fn test_registry_find_matching_multiple_parameters() {
        let mut registry = SensorRouter::new();
        registry.insert(
            "/buildings/{building}/rooms/{room}/temperature",
            Sensor::new(dummy_sensor),
        );

        let result = registry.at("/buildings/office/rooms/conference/temperature");
        assert!(result.is_some());

        let (args, _) = result.unwrap();
        assert_eq!(args.len(), 2);

        // Check args contain both parameters
        let args_map: std::collections::HashMap<_, _> =
            args.iter().map(|(k, v)| (k.as_ref(), v.as_str())).collect();
        assert_eq!(args_map.get("building"), Some(&"office"));
        assert_eq!(args_map.get("room"), Some(&"conference"));
    }

    #[test]
    fn test_registry_find_matching_no_match() {
        let mut registry = SensorRouter::new();
        registry.insert("/rooms/{room}/temperature", Sensor::new(dummy_sensor));

        // Path doesn't match the route pattern
        let result = registry.at("/rooms/kitchen/humidity");
        assert!(result.is_none());
    }

    #[test]
    fn test_registry_find_matching_partial_path_no_match() {
        let mut registry = SensorRouter::new();
        registry.insert("/rooms/{room}/temperature", Sensor::new(dummy_sensor));

        // Partial path should not match
        let result = registry.at("/rooms/kitchen");
        assert!(result.is_none());
    }

    #[test]
    fn test_registry_find_matching_longer_path_no_match() {
        let mut registry = SensorRouter::new();
        registry.insert("/rooms/{room}/temperature", Sensor::new(dummy_sensor));

        // Longer path should not match
        let result = registry.at("/rooms/kitchen/temperature/celsius");
        assert!(result.is_none());
    }

    #[test]
    fn test_registry_empty_returns_none() {
        let registry = SensorRouter::new();

        let result = registry.at("/any/path");
        assert!(result.is_none());
    }

    #[test]
    fn test_registry_root_path() {
        let mut registry = SensorRouter::new();
        registry.insert("", Sensor::new(dummy_sensor));

        let result = registry.at("");
        assert!(result.is_some());

        let (args, _) = result.unwrap();
        assert!(args.is_empty());
    }
}

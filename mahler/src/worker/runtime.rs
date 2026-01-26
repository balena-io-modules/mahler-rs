//! Worker runtime for handling system state updates

use jsonptr::{Pointer, PointerBuf};
use tokio::sync::{mpsc, watch};
use tokio_stream::{StreamExt, StreamMap};
use tracing::{span, trace, Instrument, Level};

use crate::json::{Patch, PatchOperation, ReplaceOperation, Value};
use crate::result::Result;
use crate::runtime::{Context, Resources, System};
use crate::sensor::{SensorRouter, SensorStream};
use crate::sync::{channel, rw_lock, Reader, Sender, WithAck};
use crate::system_ext::SystemExt;

#[derive(Clone)]
pub struct OnStateChange;

type Error = String;

/// Worker runtime that manages system state updates
///
/// The runtime spawns a dedicated task with exclusive write access
/// to the system state. All state modifications are serialized through this task.
#[derive(Clone)]
pub struct Handle {
    resource_tx: mpsc::Sender<Resources>,
    system_reader: Reader<System>,
    update_tx: watch::Sender<OnStateChange>,
    closed_rx: watch::Receiver<Option<Error>>,
}

impl Handle {
    /// Get a reader for the system state
    pub fn reader(&self) -> &Reader<System> {
        &self.system_reader
    }

    /// Subscribe to state update notifications
    ///
    /// Returns a receiver that gets notified after each state change.
    pub fn on_change(&self) -> watch::Receiver<OnStateChange> {
        self.update_tx.subscribe()
    }

    /// Return true if the runtime is running
    pub fn is_running(&self) -> bool {
        self.closed_rx.has_changed().is_ok() || self.closed_rx.borrow().is_none()
    }

    /// Wait for the runtime to terminate
    ///
    /// This method blocks until the runtime task exits. Under normal operation,
    /// the runtime only exits when all senders have closed.
    ///
    /// If this method returns
    /// while the worker is still active, it indicates an error occurred.
    pub async fn closed(&self) -> Option<Error> {
        let mut rx = self.closed_rx.clone();

        loop {
            // if the channel has a message, return immediately
            if let Some(err) = &*rx.borrow() {
                return Some(err.clone());
            }

            if rx.changed().await.is_err() {
                return None;
            }

            // if the channel doesn't have a value at this point there is a bug
            debug_assert!(rx.borrow().is_some())
        }
    }

    /// Update the system resources
    pub async fn set_resources(&self, resources: Resources) {
        let _ = self.resource_tx.send(resources).await;
    }
}

enum Command {
    Patch(WithAck<Patch>),
    SetResources(Resources),
    SensorValue(PointerBuf, Result<Value>),
}

/// Helper function to extract all paths from a JSON value
fn collect_paths(value: &Value, current_path: &Pointer, paths: &mut Vec<PointerBuf>) {
    match value {
        Value::Object(map) => {
            for (key, v) in map {
                // create a new path and add it to the list
                let mut path = current_path.to_buf();
                path.push_back(key);
                paths.push(path.clone());

                collect_paths(v, &path, paths);
            }
        }
        Value::Array(arr) => {
            for (i, v) in arr.iter().enumerate() {
                // create a new path and add it to the list
                let mut path = current_path.to_buf().clone();
                path.push_back(i);
                paths.push(path.clone());

                collect_paths(v, &path, paths);
            }
        }
        _ => {}
    }
}

/// Try to create a sensor subscription for a path if it matches any sensor route
fn try_subscribe(
    system: &System,
    pointer: &Pointer,
    sensors: &SensorRouter,
    sensor_streams: &mut StreamMap<PointerBuf, SensorStream>,
) -> Result<()> {
    let path = pointer.to_buf();
    if sensor_streams.contains_key(&path) {
        return Ok(());
    }

    if let Some((args, sensor)) = sensors.at(pointer.as_str()) {
        // try create the stream using the path and args
        let context = Context::new().with_path(pointer).with_args(args);
        let stream = sensor.create_stream(system, &context)?;

        sensor_streams.insert(path, stream);
        trace!(path = %pointer, "sensor subscription created");
    }

    Ok(())
}

/// Spawn a new runtime for the given system state
///
/// Returns the runtime and a sender for patches (used by workflows).
pub fn spawn(system: System, sensors: SensorRouter) -> (Handle, Sender<Patch>) {
    let initial_system = system.clone();
    let (system_reader, mut system_writer) = rw_lock(system);
    let (patch_tx, mut patch_rx) = channel::<Patch>(100);
    let (resource_tx, mut resource_rx) = mpsc::channel::<Resources>(10);
    let (closed_tx, closed_rx) = watch::channel(None);
    let (update_tx, _) = watch::channel(OnStateChange);
    let update_tx_clone = update_tx.clone();

    tokio::spawn(
        async move {
            let mut sensor_streams: StreamMap<PointerBuf, SensorStream> = StreamMap::new();

            let res = {
                // Create initial subscriptions for existing paths
                let mut all_paths = Vec::new();
                collect_paths(
                    initial_system.inner_state(),
                    Pointer::root(),
                    &mut all_paths,
                );

                for path in all_paths {
                    try_subscribe(&initial_system, &path, &sensors, &mut sensor_streams)?;
                }

                // If the worker is dropped, then patch_tx will get dropped and
                // this task will terminate, causing update_event_tx to get dropped
                // and notifying the broadcast channel followers. That will also drop closed_tx
                // that was moved into this thread
                loop {
                    let cmd = tokio::select! {
                        biased;

                        Some(msg) = patch_rx.recv() => Command::Patch(msg),
                        Some(resources) = resource_rx.recv() => Command::SetResources(resources),
                        Some((path, op)) = sensor_streams.next() => Command::SensorValue(path, op),

                        else => break,
                    };

                    match cmd {
                        Command::Patch(mut msg) => {
                            let changes = std::mem::take(&mut msg.data);
                            trace!(received = %changes);

                            let system = {
                                let mut system = system_writer.write().await;
                                system.patch(&changes)?;
                                trace!("patch successful");

                                // return a copy of the system to release the write lock
                                system.clone()
                            };

                            // Remove subscriptions for deleted paths
                            let paths_to_remove: Vec<PointerBuf> = sensor_streams
                                .keys()
                                .filter(|path| path.resolve(system.inner_state()).is_err())
                                .cloned()
                                .collect();

                            for path in paths_to_remove {
                                sensor_streams.remove(&path);
                                trace!(path = %path, "sensor subscription removed");
                            }

                            // Create subscriptions for paths affected by the patch
                            for patch_op in changes.0.iter() {
                                // we already remove paths
                                if matches!(patch_op, &PatchOperation::Remove { .. }) {
                                    continue;
                                }
                                let change_path = patch_op.path();

                                // Get the value at this path to find all nested paths
                                if let Ok(value) = change_path.resolve(system.inner_state()) {
                                    // Collect all paths under this change
                                    let mut new_paths = Vec::new();
                                    collect_paths(value, change_path, &mut new_paths);

                                    // Also include the change path itself
                                    new_paths.push(change_path.to_buf());

                                    // Check each path against sensor routes
                                    for path in new_paths {
                                        try_subscribe(
                                            &system,
                                            &path,
                                            &sensors,
                                            &mut sensor_streams,
                                        )?;
                                    }
                                }
                            }

                            let _ = update_tx_clone.send(OnStateChange);
                            msg.ack();
                        }
                        Command::SetResources(resources) => {
                            let mut system = system_writer.write().await;
                            system.set_resources(resources);
                        }
                        Command::SensorValue(path, result) => {
                            let value = result?;
                            let op = PatchOperation::Replace(ReplaceOperation { path, value });

                            let mut system = system_writer.write().await;
                            system.patch(&Patch(vec![op]))?;
                            let _ = update_tx_clone.send(OnStateChange);
                        }
                    }
                }

                Ok(()) as Result<()>
            };

            if let Err(err) = res {
                let _ = closed_tx.send(Some(err.to_string()));
            }

            Ok(()) as Result<()>
        }
        .instrument(span!(Level::TRACE, "runtime")),
    );

    (
        Handle {
            resource_tx,
            system_reader,
            update_tx,
            closed_rx,
        },
        patch_tx,
    )
}

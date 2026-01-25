//! Worker runtime for handling system state updates

use tokio::sync::{mpsc, watch};
use tracing::{span, trace, Instrument, Level};

use crate::json::Patch;
use crate::runtime::{Resources, System};
use crate::sync::{channel, rw_lock, Reader, Sender};
use crate::system_ext::SystemExt;

#[derive(Clone)]
pub struct OnStateChange;

pub type Error = String;

/// Worker runtime that manages system state updates
///
/// The runtime spawns a dedicated task with exclusive write access
/// to the system state. All state modifications are serialized through this task.
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

    /// Wait for the runtime to terminate due
    ///
    /// This method blocks until the runtime task exits. Under normal operation,
    /// the runtime only exits when all senders have closed.
    ///
    /// If this method returns
    /// while the worker is still active, it indicates an error occurred.
    pub async fn closed(&self) -> Option<String> {
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

/// Spawn a new runtime for the given system state
///
/// Returns the runtime and a sender for patches (used by workflows).
pub fn spawn(system: System) -> (Handle, Sender<Patch>) {
    let (system_reader, mut system_writer) = rw_lock(system);
    let (patch_tx, mut patch_rx) = channel::<Patch>(100);
    let (resource_tx, mut resource_rx) = mpsc::channel::<Resources>(10);
    let (closed_tx, closed_rx) = watch::channel(None);
    let (update_tx, _) = watch::channel(OnStateChange);
    let update_tx_clone = update_tx.clone();

    tokio::spawn(
        async move {
            // If the worker is dropped, then patch_tx will get dropped and
            // this task will terminate, causing update_event_tx to get dropped
            // and notifying the broadcast channel followers. That will also drop closed_tx
            // that was moved into this thread
            loop {
                tokio::select! {
                    biased;

                    Some(mut msg) = patch_rx.recv() => {
                        let changes = std::mem::take(&mut msg.data);
                        trace!(received = %changes);

                        let mut system = system_writer.write().await;
                        if let Err(e) = system.patch(changes) {
                            let _ = closed_tx.send(Some(format!("patch failed {e}")));
                            break;
                        }
                        trace!("patch successful");

                        let _ = update_tx_clone.send(OnStateChange);
                        msg.ack();
                    }

                    Some(resources) = resource_rx.recv() => {
                        let mut system = system_writer.write().await;
                        system.set_resources(resources);
                    }

                    else => break,
                }
            }
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

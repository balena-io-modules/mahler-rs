use std::ops::Deref;
use std::sync::Arc;

use tokio::sync::Mutex;

use crate::error::Error;
use crate::json::{Patch, Value};
use crate::result::Result;
use crate::sync::Sender;

#[derive(Clone)]
struct Checkpoint<T>(Arc<Mutex<Option<T>>>);

impl<T> Default for Checkpoint<T> {
    fn default() -> Self {
        Self(Arc::new(Mutex::new(None)))
    }
}

impl<T> Deref for Checkpoint<T> {
    type Target = Mutex<Option<T>>;

    fn deref(&self) -> &Self::Target {
        self.0.as_ref()
    }
}

/// A channel to communicate state changes at runtime
///
/// The `Channel` allows tasks to send state changes and rollback checkpoints back to the worker during execution,
/// enabling real-time progress updates. This can be used by extractors to propagate changes
/// during a long operation.
#[derive(Clone)]
pub struct Channel {
    sender: Option<Sender<Patch>>,
    checkpoint: Checkpoint<Value>,
}

impl std::fmt::Debug for Channel {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut dbg = f.debug_struct("Channel");
        dbg.field(
            "sender",
            if self.sender.is_some() {
                &"attached"
            } else {
                &"detached"
            },
        );
        match self.checkpoint.try_lock() {
            Ok(guard) => dbg.field("checkpoint", &*guard),
            Err(_) => dbg.field("checkpoint", &"locked"),
        };
        dbg.finish()
    }
}

impl Channel {
    /// Return true if the channel is detached
    pub fn is_detached(&self) -> bool {
        self.sender.is_none()
    }

    /// Create a detached channel. A detached channel is not connected to a worker
    pub fn detached() -> Self {
        Self {
            sender: None,
            checkpoint: Checkpoint::default(),
        }
    }

    /// Communicate the changes to the global state
    pub async fn send(&self, changes: Patch) -> Result<()> {
        if let Some(sender) = self.sender.as_ref() {
            sender.send(changes).await.map_err(Error::internal)?;
        }
        Ok(())
    }

    /// Communicate changes and update the checkpoint
    ///
    /// Like [`send`](Channel::send), but also records the given value as the
    /// current checkpoint. The checkpoint can be read by the caller via
    /// [`checkpoint`](Channel::checkpoint) after the task completes.
    pub async fn send_and_commit(&self, changes: Patch, new_checkpoint: Value) -> Result<()> {
        if let Some(sender) = self.sender.as_ref() {
            sender.send(changes).await.map_err(Error::internal)?;
            *self.checkpoint.lock().await = Some(new_checkpoint);
        }
        Ok(())
    }

    /// Return the last checkpointed value, if any.
    ///
    /// Returns `Some(value)` if [`send_and_commit`](Channel::send_and_commit) was called
    /// at least once, `None` otherwise.
    pub async fn checkpoint(&self) -> Option<Value> {
        self.checkpoint.lock().await.clone()
    }
}

impl From<Sender<Patch>> for Channel {
    /// Create a channel with a checkpoint for tracking committed state.
    ///
    /// The checkpoint allows [`send_and_commit`](Channel::send_and_commit) to record
    /// state snapshots, retrievable later via [`checkpoint`](Channel::checkpoint).
    fn from(sender: Sender<Patch>) -> Self {
        Self {
            sender: Some(sender),
            checkpoint: Checkpoint::default(),
        }
    }
}

use crate::error::Error;
use crate::json::{Patch, Value};
use crate::result::Result;
use crate::sync::Sender;

/// A channel to communicate state changes at runtime
///
/// The `Channel` allows tasks to send state changes and rollback checkpoints back to the worker during execution,
/// enabling real-time progress updates. This can be used by extractors to propagate changes
/// during a long operation.
#[derive(Clone)]
pub struct Channel {
    sender: Option<Sender<(Patch, Option<Value>)>>,
}

impl std::fmt::Debug for Channel {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Channel")
            .field(
                "sender",
                if self.sender.is_some() {
                    &"attached"
                } else {
                    &"detached"
                },
            )
            .finish()
    }
}

impl Channel {
    /// Return true if the channel is detached
    pub fn is_detached(&self) -> bool {
        self.sender.is_none()
    }

    /// Create a detached channel. A detached channel is not connected to a worker
    pub fn detached() -> Self {
        Self { sender: None }
    }

    /// Communicate the changes to the global state
    pub async fn send(&self, changes: Patch) -> Result<()> {
        if let Some(sender) = self.sender.as_ref() {
            sender
                .send((changes, None))
                .await
                .map_err(Error::internal)?;
        }
        Ok(())
    }

    /// Communicate changes and update the checkpoint
    ///
    /// Like [`send`](Channel::send), but also sends the given value as the
    /// current checkpoint alongside the patch.
    pub async fn send_and_commit(&self, changes: Patch, checkpoint: Value) -> Result<()> {
        if let Some(sender) = self.sender.as_ref() {
            sender
                .send((changes, Some(checkpoint)))
                .await
                .map_err(Error::internal)?;
        }
        Ok(())
    }
}

impl From<Sender<(Patch, Option<Value>)>> for Channel {
    /// Create an attached channel from a sender.
    fn from(sender: Sender<(Patch, Option<Value>)>) -> Self {
        Self {
            sender: Some(sender),
        }
    }
}

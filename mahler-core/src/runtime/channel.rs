use crate::error::Error;
use crate::json::Patch;
use crate::result::Result;
use crate::sync::Sender;

/// A channel to communicate state changes at runtime
///
/// The `Channel` allows tasks to send state changes back to the worker during execution,
/// enabling real-time progress updates. This can be used by extractors to propagate changes
/// during operation.
#[derive(Clone)]
pub struct Channel(Option<Sender<Patch>>);

impl std::fmt::Debug for Channel {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Channel")
            .field(
                "0",
                if self.0.is_some() {
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
        self.0.is_none()
    }

    /// Create a detached channel. A detached channel is not connected to a worker
    pub fn detached() -> Self {
        Self(None)
    }

    /// Communicate the changes to the global state
    ///
    /// Returns `Ok(true)` if the channel is attached and the changes were sent successfully,
    /// `Ok(false)` if the channel is detached, or an error if sending fails.
    pub async fn send(&self, changes: Patch) -> Result<()> {
        if let Some(sender) = self.0.as_ref() {
            sender.send(changes).await.map_err(Error::internal)?;
        }
        Ok(())
    }
}

impl From<Sender<Patch>> for Channel {
    fn from(sender: Sender<Patch>) -> Self {
        Self(Some(sender))
    }
}

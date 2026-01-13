use crate::error::Error;
use crate::json::Patch;
use crate::result::Result;
use crate::sync::Sender;

/// A channel communicate changes at runtime
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
    /// Create a detached channel
    pub fn detached() -> Self {
        Self(None)
    }

    /// Communicate the changes to the global state
    pub async fn send(&self, changes: Patch) -> Result<()> {
        if let Some(sender) = self.0.as_ref() {
            sender.send(changes).await.map_err(Error::runtime)?;
        }
        Ok(())
    }
}

impl From<Sender<Patch>> for Channel {
    fn from(sender: Sender<Patch>) -> Self {
        Self(Some(sender))
    }
}

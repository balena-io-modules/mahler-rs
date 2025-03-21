use thiserror::Error;

#[derive(Debug, Error)]
#[error("condition failed: {0}")]
pub struct ConditionFailed(String);

impl Default for ConditionFailed {
    fn default() -> Self {
        ConditionFailed::new("unknown")
    }
}

impl ConditionFailed {
    pub fn new(msg: impl Into<String>) -> Self {
        Self(msg.into())
    }
}

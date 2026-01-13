use crate::error::Error;
use crate::json::Patch;
use crate::result::Result;
use crate::runtime::System;

use json_patch::patch;

pub trait SystemExt {
    /// Update the internal copy of the system state
    fn patch(&mut self, changes: Patch) -> Result<()>;
}

impl SystemExt for System {
    fn patch(&mut self, changes: Patch) -> Result<()> {
        patch(self.inner_state_mut(), &changes).map_err(Error::internal)?;
        Ok(())
    }
}

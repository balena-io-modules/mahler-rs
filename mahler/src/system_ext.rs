use crate::error::Error;
use crate::result::Result;
use crate::runtime::System;

use json_patch::{patch, Patch};

pub trait SystemExt {
    fn patch(&mut self, changes: Patch) -> Result<()>;
}

impl SystemExt for System {
    fn patch(&mut self, changes: Patch) -> Result<()> {
        patch(self.inner_state_mut(), &changes).map_err(Error::internal)?;
        Ok(())
    }
}

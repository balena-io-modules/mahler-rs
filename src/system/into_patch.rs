use super::System;

use json_patch::Patch;

pub(crate) trait IntoPatch {
    fn into_patch(self, system: &System) -> Patch;
}

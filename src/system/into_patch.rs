use super::System;

use json_patch::Patch;

pub(crate) trait IntoPatch {
    fn into_patch(self, system: &System) -> Patch;
}

impl<T> IntoPatch for Option<T>
where
    T: IntoPatch,
{
    fn into_patch(self, system: &System) -> Patch {
        match self {
            None => Patch(vec![]),
            Some(t) => t.into_patch(system),
        }
    }
}

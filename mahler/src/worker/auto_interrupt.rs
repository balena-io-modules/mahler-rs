use std::ops::Deref;

use crate::sync::Interrupt;

/// An interrupt that gets triggered on drop
///
/// This purposely doesn't implement Clone, but implements Deref, meaning
/// `.clone()` calls will return an interrupt. This means that interrupt copies will
/// only get dropped when the original AutoInterrupt gets dropped
pub struct AutoInterrupt(Interrupt);

impl From<Interrupt> for AutoInterrupt {
    fn from(interrupt: Interrupt) -> Self {
        Self(interrupt)
    }
}

impl Drop for AutoInterrupt {
    fn drop(&mut self) {
        // Trigger the interrupt flag when the worker is dropped
        self.0.trigger();
    }
}

impl Deref for AutoInterrupt {
    type Target = Interrupt;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

use std::{
    ops::Deref,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
};
use tokio::sync::Notify;

#[derive(Clone, Default)]
/// An interrupt flag with notification capabilities
///  
/// The `Interrupt` struct provides a thread-safe way to signal cancellation
/// across async tasks. When triggered, it notifies all waiting tasks to terminate
/// their operations gracefully.
///
/// # Examples
///
/// ```rust
/// use mahler::workflow::Interrupt;
/// use std::time::Duration;
/// use tokio::time::timeout;
///
/// #[tokio::main]
/// async fn main() {
///     let interrupt = Interrupt::new();
///     let interrupt_clone = interrupt.clone();
///
///     // Spawn a task that will be interrupted
///     let handle = tokio::spawn(async move {
///         interrupt_clone.wait().await;
///         println!("Task was interrupted!");
///     });
///
///     // Trigger the interrupt after a delay
///     tokio::spawn(async move {
///         tokio::time::sleep(Duration::from_millis(100)).await;
///         interrupt.trigger();
///     });
///
///     handle.await.unwrap();
/// }
/// ```
pub struct Interrupt {
    flag: Arc<AtomicBool>,
    notify: Arc<Notify>,
}

impl Interrupt {
    pub fn new() -> Self {
        Self::default()
    }

    /// Triggers the interrupt flag and notifies all waiting tasks
    ///
    /// This method sets the internal flag to `true` and immediately notifies
    /// all tasks waiting on this interrupt to proceed with their cancellation logic.
    ///
    /// Once triggered, the interrupt remains in the triggered state and cannot be reset.
    pub fn trigger(&self) {
        self.flag.store(true, Ordering::SeqCst);
        self.notify.notify_waiters();
    }

    /// Checks if the interrupt has been set
    pub fn is_set(&self) -> bool {
        self.flag.load(Ordering::SeqCst)
    }

    /// Waits asynchronously until the interrupt is set
    pub async fn wait(&self) {
        if self.is_set() {
            return;
        }

        // Otherwise wait to be notified
        self.notify.notified().await;
    }
}

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

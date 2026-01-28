use std::{
    ops::{Deref, DerefMut},
    sync::Arc,
};

use tokio::sync::{RwLock, RwLockReadGuard, RwLockWriteGuard};

/// RAII structure used to release the shared read access of a lock when dropped.
///
/// This structure is created by the [read](`Reader::read`) method on [`Reader`].
pub struct ReadGuard<'a, T: ?Sized>(RwLockReadGuard<'a, T>);

impl<'a, T> Deref for ReadGuard<'a, T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        self.0.deref()
    }
}

/// The reader component for a RwLock
///
/// This is a wrapper over [`tokio::sync::RwLock`] which exposes
/// only a read() method.
///
/// A Reader can be cloned, meaning there can be multiple reader tasks
#[derive(Debug, Clone)]
pub struct Reader<T>(Arc<RwLock<T>>);

impl<T> Reader<T> {
    /// Locks the RwLock with shared read access, causing the current task to yield until the lock
    /// has been acquired.
    ///
    /// The calling task will yield until there are no writers which hold the lock.
    /// There may be other readers inside the lock when the task resumes.
    ///
    /// Returns an RAII guard which will drop this read access of the RwLock when dropped.
    pub async fn read(&self) -> ReadGuard<'_, T> {
        ReadGuard(self.0.read().await)
    }
}

/// RAII structure used to release the exclusive write access of a lock when dropped.
///
/// This structure is created by the [write](`Writer::write`) method on [`Writer`].
pub struct WriteGuard<'a, T: ?Sized>(RwLockWriteGuard<'a, T>);

impl<'a, T> Deref for WriteGuard<'a, T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        self.0.deref()
    }
}

impl<'a, T> DerefMut for WriteGuard<'a, T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.0.deref_mut()
    }
}

/// The writer component for a RwLock
///
/// This is a wrapper over [`tokio::sync::RwLock`] which exposes
/// only a write() method.
///
/// Writer is not Clone and write() takes &mut self to enforce single-task ownership.
/// This prevents wrapping Writer in Arc for sharing across tasks since Arc only
/// provides &T, not &mut T.
#[derive(Debug)]
pub struct Writer<T>(Arc<RwLock<T>>);

impl<T> Writer<T> {
    /// Locks this RwLock with exclusive write access, causing the current task to
    /// yield until the lock has been acquired.
    ///
    /// The calling task will yield while other writers or readers currently have access to the lock.
    ///
    /// Takes `&mut self` to prevent `Arc<Writer>` from being usable for sharing across tasks,
    /// since `Arc` only provides `&T` access.
    ///
    /// Returns an RAII guard which will drop the write access of this RwLock when dropped.
    pub async fn write(&mut self) -> WriteGuard<'_, T> {
        WriteGuard(self.0.write().await)
    }
}

/// Create a read write lock returning a [`Reader`] and [`Writer`] components
pub fn rw_lock<T>(value: T) -> (Reader<T>, Writer<T>) {
    let lock = Arc::new(RwLock::new(value));

    (Reader(lock.clone()), Writer(lock))
}

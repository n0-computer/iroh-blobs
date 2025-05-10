//! This module defines a wrapper around a [`tokio::sync::RwLock`] that runs a callback
//! After any write operation occurs

use std::future::Future;

/// A wrapper over a [`tokio::sync::RwLock`] that executes a callback function after
/// the write guard is dropped
#[derive(derive_more::Debug)]
pub struct CallbackLock<T, F> {
    inner: tokio::sync::RwLock<T>,
    #[debug(skip)]
    callback: F,
}

/// the wrapper type over a [tokio::sync::RwLockWriteGuard]
#[derive(Debug)]
pub struct CallbackLockWriteGuard<'a, T, F: Fn(&T)> {
    inner: tokio::sync::RwLockWriteGuard<'a, T>,
    callback: &'a F,
}

impl<T, F: Fn(&T)> std::ops::Deref for CallbackLockWriteGuard<'_, T, F> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

impl<T, F: Fn(&T)> std::ops::DerefMut for CallbackLockWriteGuard<'_, T, F> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.inner
    }
}

impl<T, F: Fn(&T)> Drop for CallbackLockWriteGuard<'_, T, F> {
    fn drop(&mut self) {
        (self.callback)(&*self.inner);
    }
}

impl<T, F> CallbackLock<T, F>
where
    F: Fn(&T),
{
    /// create a new instance of the lock from a value
    /// and the callback to evaluate when a write guard is dropped
    pub fn new(val: T, callback: F) -> Self {
        CallbackLock {
            inner: tokio::sync::RwLock::new(val),
            callback,
        }
    }

    /// return an instance of the write guard
    pub async fn write(&self) -> CallbackLockWriteGuard<'_, T, F> {
        let guard = self.inner.write().await;

        CallbackLockWriteGuard {
            inner: guard,
            callback: &self.callback,
        }
    }

    /// return the [tokio::sync::RwLockReadGuard]
    /// this will not invoke the callback
    pub fn read(&self) -> impl Future<Output = tokio::sync::RwLockReadGuard<'_, T>> {
        self.inner.read()
    }

    /// try to synchronously acquire a read lock
    pub fn try_read(
        &self,
    ) -> Result<tokio::sync::RwLockReadGuard<'_, T>, tokio::sync::TryLockError> {
        self.inner.try_read()
    }
}

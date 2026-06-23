use std::{ops::Deref, sync::Arc};

use atomic_refcell::{AtomicRef, AtomicRefCell};

#[derive(Debug, Default)]
struct State<T> {
    value: T,
    dropped: bool,
}

#[derive(Debug, Default)]
struct Shared<T> {
    value: AtomicRefCell<State<T>>,
    notify: tokio::sync::Notify,
}

#[derive(Debug, Default)]
pub struct Sender<T>(Arc<Shared<T>>);

impl<T> Clone for Sender<T> {
    fn clone(&self) -> Self {
        Self(self.0.clone())
    }
}

pub struct Receiver<T>(Arc<Shared<T>>);

impl<T> Sender<T> {

    pub fn send_modify<F>(&self, modify: F)
    where
        F: FnOnce(&mut T),
    {
        self.send_if_modified(|value| {
            modify(value);
            true
        });
    }

    pub fn send_replace(&self, mut value: T) -> T {
        // swap old watched value with the new one
        self.send_modify(|old| std::mem::swap(old, &mut value));

        value
    }

    pub fn send_if_modified<F>(&self, modify: F) -> bool
    where
        F: FnOnce(&mut T) -> bool,
    {
        let mut state = self.0.value.borrow_mut();
        let modified = modify(&mut state.value);
        if modified {
            self.0.notify.notify_waiters();
        }
        modified
    }

    pub fn borrow(&self) -> impl Deref<Target = T> + '_ {
        AtomicRef::map(self.0.value.borrow(), |state| &state.value)
    }

    pub fn subscribe(&self) -> Receiver<T> {
        Receiver(self.0.clone())
    }
}

impl<T> Drop for Sender<T> {
    fn drop(&mut self) {
        self.0.value.borrow_mut().dropped = true;
        self.0.notify.notify_waiters();
    }
}

impl<T> Receiver<T> {
    pub async fn changed(&self) -> Result<(), error::RecvError> {
        self.0.notify.notified().await;
        if self.0.value.borrow().dropped {
            Err(error::RecvError(()))
        } else {
            Ok(())
        }
    }

    pub fn borrow(&self) -> impl Deref<Target = T> + '_ {
        AtomicRef::map(self.0.value.borrow(), |state| &state.value)
    }
}

pub mod error {
    use std::{error::Error, fmt};

    /// Error produced when receiving a change notification.
    #[derive(Debug, Clone)]
    pub struct RecvError(pub(super) ());

    impl fmt::Display for RecvError {
        fn fmt(&self, fmt: &mut fmt::Formatter<'_>) -> fmt::Result {
            write!(fmt, "channel closed")
        }
    }

    impl Error for RecvError {}
}

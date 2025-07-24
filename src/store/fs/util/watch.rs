use std::{ops::Deref, sync::Arc};

use atomic_refcell::{AtomicRef, AtomicRefCell};

struct State<T> {
    value: T,
    dropped: bool,
}

struct Shared<T> {
    value: AtomicRefCell<State<T>>,
    notify: tokio::sync::Notify,
}

pub struct Sender<T>(Arc<Shared<T>>);

pub struct Receiver<T>(Arc<Shared<T>>);

impl<T> Sender<T> {
    pub fn new(value: T) -> Self {
        Self(Arc::new(Shared {
            value: AtomicRefCell::new(State {
                value,
                dropped: false,
            }),
            notify: tokio::sync::Notify::new(),
        }))
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

use std::{ops::Deref, sync::Arc};

use atomic_refcell::AtomicRefCell;

struct Shared<T> {
    value: AtomicRefCell<T>,
    notify: tokio::sync::Notify,
}

pub struct Sender<T>(Arc<Shared<T>>);

pub struct Receiver<T>(Arc<Shared<T>>);

impl<T> Sender<T> {
    pub fn new(value: T) -> Self {
        Self(Arc::new(Shared {
            value: AtomicRefCell::new(value),
            notify: tokio::sync::Notify::new(),
        }))
    }

    pub fn send_if_modified<F>(&self, modify: F) -> bool
    where
        F: FnOnce(&mut T) -> bool,
    {
        let mut value = self.0.value.borrow_mut();
        let modified = modify(&mut value);
        if modified {
            self.0.notify.notify_waiters();
        }
        modified
    }

    pub fn borrow(&self) -> impl Deref<Target = T> + '_ {
        self.0.value.borrow()
    }

    pub fn subscribe(&self) -> Receiver<T> {
        Receiver(self.0.clone())
    }
}

impl<T> Receiver<T> {
    pub async fn changed(&self) {
        self.0.notify.notified().await;
    }

    pub fn borrow(&self) -> impl Deref<Target = T> + '_ {
        self.0.value.borrow()
    }
}

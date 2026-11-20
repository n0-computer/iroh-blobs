//! The lifecycle of an entity's entry, for [`EntityApi`](super::EntityApi) implementations to build on.
//!
//! A [`StateCell`] holds an entry's [`Phase`] and its generation in a `watch`,
//! so tasks of the entity can wait for it and observers wake when it changes.
//! It knows nothing about how the entry is stored: that is the settled state
//! `S`, which the implementation defines.
//!
//! `Initial`, `Loading` and `Deleting` are transient: tasks that load or change
//! the entry wait while it is in one of them. Otherwise the entry is settled,
//! and either writable or frozen for a gc session.
use std::{
    fmt,
    ops::{Deref, DerefMut},
};

use irpc::channel::mpsc;
use n0_error::StdResultExt;
use tokio::sync::watch;

use crate::api::blobs::Bitfield;

/// Where an entry is in its lifecycle, around its settled state `S`.
#[derive(Default)]
pub(crate) enum Phase<S> {
    /// Not loaded yet.
    #[default]
    Initial,
    /// The entry is being loaded from the metadata.
    Loading,
    /// An unconditional delete is awaiting its post-commit cleanup. Readers see
    /// the entry as absent; mutations wait until the cleanup finishes.
    Deleting,
    /// Settled, and open for reads and writes.
    ReadWrite(S),
    /// Settled, and frozen by a gc session: reads go on, writes, imports and
    /// deletes wait until the session ends.
    ReadOnly(S),
}

impl<S> Phase<S> {
    /// The settled entry for a reader, or the error for a phase in transition.
    pub(crate) fn try_settled(&self) -> std::io::Result<&S> {
        match self {
            Self::ReadWrite(s) | Self::ReadOnly(s) => Ok(s),
            Self::Initial => Err(std::io::Error::other("initial")),
            Self::Loading => Err(std::io::Error::other("loading")),
            // A delete in progress: readers see no entry.
            Self::Deleting => Err(std::io::ErrorKind::NotFound.into()),
        }
    }
}

impl<S: fmt::Debug> fmt::Debug for Phase<S> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Initial => f.debug_struct("Initial").finish(),
            Self::Loading => f.debug_struct("Loading").finish(),
            Self::Deleting => f.debug_struct("Deleting").finish(),
            Self::ReadWrite(x) => f.debug_tuple("ReadWrite").field(x).finish(),
            Self::ReadOnly(x) => f.debug_tuple("ReadOnly").field(x).finish(),
        }
    }
}

/// An entry's phase, and the generation of the entry.
///
/// Derefs to the phase, which is what almost all code looks at.
#[derive(Debug)]
pub(crate) struct CellState<S> {
    /// Starts a new generation with every delete of the entry. Observers end
    /// when it changes, even if they wake only after the entry was filled
    /// again. Only ever grows, also across recycling.
    pub generation: u64,
    pub storage: Phase<S>,
    /// Writes in flight, each holding a [`WriteGuard`].
    writers: usize,
    /// A freeze or delete waits for the writes in flight to end. New writes
    /// wait for it, and the last write out wakes it.
    exclusive_waiting: bool,
}

impl<S> Default for CellState<S> {
    fn default() -> Self {
        Self {
            generation: 0,
            storage: Phase::Initial,
            writers: 0,
            exclusive_waiting: false,
        }
    }
}

/// Whether the phase allows changes: settled and not frozen, or not loaded yet.
fn writable<S>(phase: &Phase<S>) -> bool {
    !matches!(phase, Phase::Loading | Phase::Deleting | Phase::ReadOnly(_))
}

/// A write in flight. A freeze or delete waits until all are dropped.
#[must_use = "the write is only protected while the guard is held"]
pub(crate) struct WriteGuard<'a, S>(&'a StateCell<S>);

impl<S> Drop for WriteGuard<'_, S> {
    fn drop(&mut self) {
        self.0.send_if_modified(|state| {
            state.writers -= 1;
            // Wake only a waiting freeze or delete, not every observer.
            state.writers == 0 && state.exclusive_waiting
        });
    }
}

impl<S> Deref for CellState<S> {
    type Target = Phase<S>;

    fn deref(&self) -> &Phase<S> {
        &self.storage
    }
}

impl<S> DerefMut for CellState<S> {
    fn deref_mut(&mut self) -> &mut Phase<S> {
        &mut self.storage
    }
}

/// A cheaply cloneable handle to an entry's state.
#[derive(Debug, derive_more::Deref)]
pub(crate) struct StateCell<S>(pub(crate) watch::Sender<CellState<S>>);

impl<S> Clone for StateCell<S> {
    fn clone(&self) -> Self {
        Self(self.0.clone())
    }
}

impl<S> Default for StateCell<S> {
    fn default() -> Self {
        Self(watch::Sender::new(CellState::default()))
    }
}

impl<S> StateCell<S> {
    /// Replaces the phase, keeping the generation.
    pub(crate) fn set_storage(&self, storage: Phase<S>) {
        self.send_modify(|state| state.storage = storage);
    }

    /// Makes the state `Initial` again for a recycled entity, keeping the generation.
    ///
    /// The entity is idle, so no write is in flight. A freeze or delete that
    /// was dropped while waiting leaves its flag behind, which is cleared here.
    pub(crate) fn reset(&self) {
        self.send_modify(|state| {
            debug_assert_eq!(state.writers, 0, "reset with writes in flight");
            state.storage = Phase::Initial;
            state.exclusive_waiting = false;
        });
    }

    /// Runs `f` once the entry may be changed and no write is in flight.
    ///
    /// For changes that exclude writes: a freeze or a delete. While it waits
    /// for writes in flight, new writes wait for it, so it cannot starve. `f`
    /// returns whether to wake waiters, and its result.
    async fn exclusive<R>(&self, f: impl FnOnce(&mut CellState<S>) -> (bool, R)) -> R {
        let mut f = Some(f);
        loop {
            let mut changes = self.subscribe();
            let mut out = None;
            self.send_if_modified(|state| {
                if !writable(&state.storage) {
                    return false;
                }
                if state.writers > 0 {
                    state.exclusive_waiting = true;
                    return false;
                }
                // Writes waiting for the flag must wake even if `f` does not change anything.
                let was_waiting = std::mem::take(&mut state.exclusive_waiting);
                let f = f.take().expect("runs once");
                let (wake, res) = f(state);
                out = Some(res);
                wake || was_waiting
            });
            if let Some(res) = out {
                return res;
            }
            changes.changed().await.ok();
        }
    }

    /// Loads the entry, unless it is loaded already.
    ///
    /// The first task to find it `Initial` opens it, and the others wait for
    /// it. `open` always gives a state, so a failure must map to one that
    /// marks the entry invalid: waiting tasks would stall otherwise.
    pub(crate) async fn load<F>(&self, open: impl FnOnce() -> F)
    where
        F: std::future::Future<Output = S>,
    {
        self.wait_settled().await;
        let mut claimed = false;
        self.send_if_modified(|state| {
            if matches!(state.storage, Phase::Initial) {
                state.storage = Phase::Loading;
                claimed = true;
            }
            claimed
        });
        if claimed {
            let settled = open().await;
            self.set_storage(Phase::ReadWrite(settled));
        }
    }

    /// Deletes the loaded entry, starting a new generation.
    ///
    /// Waits until the entry is writable, then hands its settled state to
    /// `delete`, which commits the delete and cleans up after it. Meanwhile the
    /// entry is `Deleting`: readers see no entry, and changes wait. The new
    /// generation ends current observers, even if they next wake only after
    /// the entry was filled again. `delete` returns the state to settle on,
    /// and the result.
    pub(crate) async fn delete<F, T>(&self, delete: impl FnOnce(S) -> F) -> std::io::Result<T>
    where
        F: std::future::Future<Output = (S, std::io::Result<T>)>,
    {
        let old = self
            .exclusive(|state| {
                state.generation += 1;
                match std::mem::replace(&mut state.storage, Phase::Deleting) {
                    Phase::ReadWrite(settled) => (true, Some(settled)),
                    // Not loaded: put it back, the generation may grow anyway.
                    other => {
                        state.storage = other;
                        (true, None)
                    }
                }
            })
            .await;
        let Some(old) = old else {
            debug_assert!(false, "delete of an entry that is not loaded");
            return Err(std::io::Error::other(
                "delete of an entry that is not loaded",
            ));
        };
        let (settled, res) = delete(old).await;
        self.set_storage(Phase::ReadWrite(settled));
        res
    }

    /// Waits until the entry is neither loading nor deleting.
    ///
    /// Loading may create files, and deleting resets the state, so both must be
    /// done before a task may look at the state or touch the entry's data.
    pub(crate) async fn wait_settled(&self) {
        // Checking and subscribing is one synchronous step, so no change can
        // slip in between: tasks of an entity only interleave at awaits.
        while matches!(&self.borrow().storage, Phase::Loading | Phase::Deleting) {
            self.0.subscribe().changed().await.ok();
        }
    }

    /// Waits until the entry may be changed: settled, and not frozen.
    ///
    /// A freeze or delete waits until the returned guard is dropped, so a
    /// write that awaits IO holds it until its change is in the state.
    pub(crate) async fn wait_writable(&self) -> WriteGuard<'_, S> {
        loop {
            let mut changes = self.0.subscribe();
            let mut acquired = false;
            self.send_if_modified(|state| {
                if writable(&state.storage) && !state.exclusive_waiting {
                    state.writers += 1;
                    acquired = true;
                }
                false
            });
            if acquired {
                return WriteGuard(self);
            }
            changes.changed().await.ok();
        }
    }
}

impl<S> StateCell<S> {
    /// Freezes the settled entry until the sender of `release` is dropped, then restores it.
    ///
    /// Call it once the entry is loaded. It waits for writes and for an earlier
    /// freeze to end, so no two freezes overlap and a freeze needs no owner id.
    /// The caller keeps the entity alive while it holds the freeze. If the task
    /// is dropped instead, the entity is shutting down, and persisting has to
    /// look through the freeze.
    pub(crate) async fn freeze(
        &self,
        ack: tokio::sync::oneshot::Sender<()>,
        mut release: watch::Receiver<()>,
    ) {
        // Settled, and not in a delete or another freeze, see above.
        let frozen = self
            .exclusive(
                |state| match std::mem::replace(&mut state.storage, Phase::Initial) {
                    Phase::ReadWrite(settled) => {
                        state.storage = Phase::ReadOnly(settled);
                        (true, true)
                    }
                    other => {
                        state.storage = other;
                        (false, false)
                    }
                },
            )
            .await;
        if !frozen {
            debug_assert!(false, "freeze of an entry that is not writable");
            // Without an ack, the gc session fails instead of trusting the freeze.
            return;
        }
        if ack.send(()).is_ok() {
            // Nothing is ever sent, so this returns once the sender is dropped.
            release.changed().await.ok();
        }
        self.unfreeze();
    }

    /// Makes a frozen entry writable again.
    fn unfreeze(&self) {
        self.send_if_modified(|state| {
            match std::mem::replace(&mut state.storage, Phase::Initial) {
                Phase::ReadOnly(settled) => {
                    state.storage = Phase::ReadWrite(settled);
                    true
                }
                other => {
                    state.storage = other;
                    false
                }
            }
        });
    }

    /// An observer of the entry's bitfield, for the current generation.
    pub(crate) fn observer(&self) -> Observer<S> {
        let receiver = self.subscribe();
        let generation = receiver.borrow().generation;
        Observer {
            receiver,
            generation,
        }
    }
}

/// Follows an entry's bitfield until the entry is deleted or its data shrinks.
pub(crate) struct Observer<S> {
    receiver: watch::Receiver<CellState<S>>,
    /// The entry's generation when the subscription started.
    generation: u64,
}

impl<S> Observer<S> {
    /// Forwards the bitfield each time it grows.
    ///
    /// `bitfield` gives the bitfield of a settled entry, or `None` if it is
    /// invalid. The stream ends on `None`, on a new generation, or when the
    /// ranges shrink, since observers only ever see them grow. Returns an error
    /// if sending fails or the state's sender is dropped.
    pub(crate) async fn forward(
        mut self,
        mut tx: mpsc::Sender<Bitfield>,
        bitfield: impl Fn(&S) -> Option<Bitfield>,
    ) -> n0_error::Result<()> {
        let Some(value) = self.bitfield(&bitfield) else {
            return Ok(());
        };
        let mut last = value.clone();
        tx.send(value).await?;
        loop {
            self.update_or_closed(&mut tx).await?;
            let Some(value) = self.bitfield(&bitfield) else {
                return Ok(());
            };
            if !value.ranges.is_superset(&last.ranges) {
                return Ok(());
            }
            // Skip notifications that do not change the observed bitfield.
            if value == last {
                continue;
            }
            last = value.clone();
            tx.send(value).await?;
        }
    }

    /// The current bitfield, or `None` if the stream must end.
    fn bitfield(&self, bitfield: &impl Fn(&S) -> Option<Bitfield>) -> Option<Bitfield> {
        // The generation and the phase come from the same value.
        let state = self.receiver.borrow();
        if state.generation != self.generation {
            return None;
        }
        match &state.storage {
            Phase::ReadWrite(s) | Phase::ReadOnly(s) => bitfield(s),
            Phase::Deleting => Some(Bitfield::empty()),
            Phase::Initial | Phase::Loading => None,
        }
    }

    async fn update_or_closed(&mut self, tx: &mut mpsc::Sender<Bitfield>) -> n0_error::Result<()> {
        tokio::select! {
            _ = tx.closed() => {
                // the receiver is gone, we are done
                Err(n0_error::e!(irpc::channel::SendError::ReceiverClosed).into())
            }
            e = self.receiver.changed() => Ok(e.anyerr()?),
        }
    }
}

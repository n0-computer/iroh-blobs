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

use tokio::sync::watch;

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
    /// The settled entry, writable or frozen, if the phase is not transient.
    pub(crate) fn settled(&self) -> Option<&S> {
        match self {
            Self::ReadWrite(s) | Self::ReadOnly(s) => Some(s),
            Self::Initial | Self::Loading | Self::Deleting => None,
        }
    }

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
}

impl<S> Default for CellState<S> {
    fn default() -> Self {
        Self {
            generation: 0,
            storage: Phase::Initial,
        }
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
    pub(crate) async fn wait_writable(&self) {
        loop {
            let mut changes = self.0.subscribe();
            if !matches!(
                &self.borrow().storage,
                Phase::Loading | Phase::Deleting | Phase::ReadOnly(_)
            ) {
                return;
            }
            changes.changed().await.ok();
        }
    }
}

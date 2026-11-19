//! The store engine: what a store is built from, and the generic part that runs it.
//!
//! A store is the generic store actor plus a [`Backend`], which names the
//! per-hash entity type and starts the metadata store. The engine owns
//! everything that is the same for every store: routing requests by hash,
//! temp tags, gc sessions, imports, and the order of operations between
//! entities and the metadata store. A backend owns how entries are stored.
//!
//! What an implementation of a backend needs is re-exported here: the traits
//! ([`Backend`], [`EntityApi`], [`Reset`]), what their methods get
//! ([`EntityGlobal`], [`TaskContext`], the import types), the metadata
//! protocol ([`metadata`]), and optional helpers ([`bao`]). A store is started
//! with [`start`] and then driven through the irpc client it returns.
mod actor;
pub(crate) mod bao;
mod entity;
mod entity_manager;
mod import;
#[cfg(test)]
pub(crate) mod mem_backend;
pub(crate) mod metadata;
mod options;

pub(crate) use actor::{start, TaskContext};
pub(crate) use entity::{Backend, EntityApi, EntityGlobal};
pub(crate) use entity_manager::Reset;
pub(crate) use import::{reflink_or_copy_with_progress, ImportEntry, ImportEntryMsg, ImportSource};
pub(crate) use options::{temp_name, StoreOptions};

/// The engine's internals, for tests that build a store actor by hand.
#[cfg(test)]
pub(crate) mod testing {
    pub(crate) use super::{
        actor::{Actor, ClearScope, RtWrapper},
        entity_manager::{EntityManagerState, SpawnArg},
    };
}

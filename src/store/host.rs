//! The store host: the generic part of a store, which a backend plugs into.
//!
//! The host is one opinionated way to build a store, not the only one. Anything
//! that answers [`crate::api::proto::Request`] is a store. You can implement
//! the protocol yourself and wrap it with
//! [`Store::from_client`](crate::api::Store::from_client), or wrap another
//! store and forward what you don't handle. That is the better choice for a
//! store that only serves part of the protocol, or works very differently.
//!
//! The host does everything that is the same for every store: routing requests
//! by hash, temp tags, gc sessions, imports, and the order of operations between
//! handlers and the metadata store. The backend decides how entries are stored.
//! You start a store with [`start`] and use it through the irpc client it
//! returns, like any other store.
//!
//! # Handlers
//!
//! The most useful part of the host is how it runs handlers. Every request for a
//! hash goes to that hash's handler, and runs as a task on the handler's own
//! actor. Tasks of one handler are concurrent, but only interleave at `.await`
//! points, so the handler is the single place that decides about its hash. The
//! host creates handlers on demand, keeps them alive while they have tasks, and
//! recycles idle ones for other hashes, allocations included.
//!
//! # Building a backend
//!
//! You implement [`Backend`], which names the handler type and starts the
//! metadata store, and [`HashHandler`], which has one async method per request.
//! The metadata store answers the [`metadata`] protocol, with whatever storage
//! you like.
//!
//! Two optional helpers do most of the work of a handler. [`StateCell`] handles
//! the lifecycle of an entry: loading, writes behind a [`WriteGuard`], freezes,
//! deletes and observers. You only define the loaded state. [`bao::BaoAccess`]
//! gives you bao import and export, if your reads are synchronous.
//!
//! Data outside the store is named by a [`Location`], bytes that only the
//! backend interprets. The fs backend keeps a path there.
//!
//! # Design
//!
//! The metadata store owns the metadata, each handler the data of its hash. A
//! handler's state is a cache of its entry, valid because the handler is the
//! only writer. Entries only grow. A delete is the one exception, so it goes through
//! the handler as well.
//!
//! Besides running the handlers, the store actor holds the temp tags and forwards
//! everything else to the metadata store. The metadata store never touches
//! entry data, except for recovery when the store opens. All its senders share
//! one channel, so it sees requests in the order they were sent.
//!
//! # Metadata
//!
//! A handler talks to the metadata store through `EntryMeta`:
//!
//! | Method | Waits | Meaning |
//! |---|---|---|
//! | `get()` | for the reply | get the entry state |
//! | `set(state)` | until applied | replace the entry state |
//! | `update(state)` | no | merge into the entry state |
//! | `update_await(state)` | until applied | merge into the entry state |
//! | `delete()` | until committed | ensure the entry is absent |
//!
//! Applied means visible to later requests, not durable. Only `delete()` waits
//! for the commit, since the handler removes the data right after.
//!
//! After a commit, the metadata store reports parts that a hash no longer
//! references. That doesn't make them garbage, since the handler may still use
//! them. So the store actor hands them to the hash's handler, waking one if
//! needed, and the handler decides from its loaded state.
//!
//! # Deletes
//!
//! The store actor sends each delete to the hash's handler, so it is ordered with
//! everything else for that hash. A delete that is not forced skips content with
//! a temp tag. The handler starts a new generation, which ends observers, deletes
//! the entry, waits for the commit, and then removes the data.
//!
//! A metadata error aborts the whole store. There is no rollback.
//!
//! # GC sessions
//!
//! `GcRun` opens an exclusive session. The store sends a snapshot of candidates
//! and roots, gc sends back its sweep deletes, and closing the channel ends the
//! session. While it lasts, the store holds back all tag changes, including temp
//! tag creation and import completion, and queues them until the end.
//!
//! Before sending the snapshot, the store freezes every HashSeq root. A frozen
//! entry can be read, but writes and deletes wait. So no new reference can show
//! up between traversal and sweep, and blobs added after the snapshot are never
//! swept.
//!
//! The protect callback runs while the session is held. It must not wait for
//! anything the session holds back, or the gc run deadlocks. External systems
//! must protect content before they publish references to it. A tag does not
//! bring back data that gc already deleted.
//!
//! # Tasks
//!
//! Tasks don't return a result; they send errors to the requester themselves.
//! Usually an impl fn returns a result, so it can use `?`, and a thin `..._task`
//! wrapper forwards the error.
mod actor;
pub(crate) mod bao;
mod entity_manager;
mod handler;
mod import;
#[cfg(test)]
pub(crate) mod mem_backend;
pub(crate) mod metadata;
pub(crate) mod options;
mod state;

pub(crate) use actor::{start, TaskContext};
pub(crate) use handler::{Backend, HandlerGlobal, HashHandler};
pub(crate) use import::{reflink_or_copy_with_progress, ImportEntry, ImportEntryMsg, ImportSource};
pub(crate) use metadata::entry_state::Location;
pub(crate) use options::{temp_name, StoreOptions};
pub(crate) use state::{Phase, StateCell, WriteGuard};

/// The host's internals, for tests that build a store actor by hand.
#[cfg(test)]
pub(crate) mod testing {
    pub(crate) use super::{
        actor::{Actor, ClearScope, RtWrapper},
        entity_manager::{EntityManagerState, SpawnArg},
    };
}

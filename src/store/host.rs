//! The store host: the generic part that runs a store, and what a backend plugs into it.
//!
//! The host is one opinionated way to build a store, not the only one. A
//! store is anything that answers the store protocol ([`crate::api::proto::Request`]):
//! a store can implement the protocol itself and be wrapped with
//! [`Store::from_client`](crate::api::Store::from_client), or wrap another
//! store and forward what it does not handle. That is the way to go for a
//! store that only serves part of the protocol, or that works very differently.
//!
//! With the host, a store is the host plus a [`Backend`]. The host owns
//! everything that is the same for every store: routing requests by hash, temp
//! tags, gc sessions, imports, and the order of operations between entities
//! and the metadata store. A backend owns how entries are stored. A store is
//! started with [`start`] and then driven through the irpc client it returns,
//! like any other store.
//!
//! # The entity manager
//!
//! The biggest thing the host gives a backend is its entity manager. Every
//! request for a hash goes to that hash's entity, which runs it as a task. The
//! tasks of one entity run concurrently on one entity actor, and only
//! interleave at `.await` points. So the entity is the single place that
//! decides about its hash, and needs no locks against other code. The manager:
//!
//! - creates an entity when the first request for its hash arrives,
//! - runs a set of tasks per entity, and keeps the entity alive while any runs,
//! - gives each live entity its own actor, a task on the store's runtime, so
//!   different hashes are worked on in parallel,
//! - persists an idle entity and recycles it and its actor for another hash,
//!   keeping their allocations.
//!
//! # Building a backend
//!
//! A backend implements two traits, and answers one protocol:
//!
//! - [`Backend`] names the entity type and starts the metadata store. Its value
//!   is the entities' configuration, shared through [`EntityGlobal`].
//! - [`EntityApi`] is one hash's entity: created and recycled by the host, with
//!   one async handler per request.
//! - The metadata store answers the [`metadata`] protocol, a flat request enum,
//!   in whatever way the backend keeps its entries.
//!
//! Two helpers do most of the work of an entity. Both are optional, and a
//! backend with unusual needs can do without them:
//!
//! - [`StateCell`] is the entry's lifecycle: loading once, writes behind a
//!   [`WriteGuard`], freezes for gc sessions, deletes that start a new
//!   generation, and observers. The backend only defines the loaded state,
//!   the type the cell wraps in a [`Phase`].
//! - [`bao::BaoAccess`] gives the generic bao import and exports, for backends
//!   with synchronous reads.
//!
//! Data outside the store is named by a [`Location`], bytes that only the
//! backend interprets: the fs backend keeps a path there.
//!
//! Not yet up to the backend: the host builds its own runtime, and it stages
//! imports in temp files under [`StoreOptions`]' temp path.
//!
//! # Design
//!
//! The metadata store owns the metadata, and each entity owns the data of its
//! hash. They talk only by messages. An entity's in-memory state is a cache of
//! its entry, valid as long as the entity is the only writer of that entry.
//! Entries only grow, which is why a write to a complete entry can be a no-op.
//! A delete is the one change that does not grow an entry, so it goes through
//! the entity too.
//!
//! # Actors
//!
//! - **Store actor**: handles user commands, runs the entity manager, holds the
//!   temp tags, forwards tag, status, listing and lifecycle commands to the
//!   metadata store, fans deletes out to the entities, and routes unreferenced
//!   parts.
//! - **Entity actors**, one per hash that is currently being worked on: run the
//!   per-hash tasks (import, export, observe, persist, delete). While an entity
//!   exists, it is the only one that writes and deletes the data of its hash.
//! - **Metadata store**: one actor that answers the [`metadata`] protocol. It
//!   never touches entry data, except for recovery when the store opens, before
//!   any entity exists.
//!
//! All senders to the metadata store share one channel, so it sees requests in
//! the order the sends completed.
//!
//! # Metadata protocol
//!
//! An entity talks to the metadata store through `EntryMeta`, scoped to its
//! hash:
//!
//! | Method | Waits | Meaning |
//! |---|---|---|
//! | `get()` | for the reply | get the entry state |
//! | `set(state)` | until applied | replace the entry state |
//! | `update(state)` | no | merge into the entry state |
//! | `update_await(state)` | until applied | merge into the entry state |
//! | `delete()` | until committed | ensure the entry is absent |
//!
//! "Applied" means visible to later requests, not durable. Durability comes
//! with the commit. Only `delete()` is answered after the commit, since its
//! entity removes the data right after. The full contract is in [`metadata`].
//!
//! After each commit, the metadata store sends `UnreferencedParts` to the
//! store actor for each hash that is not deleted and no longer references some
//! of its parts: the parts it referenced before or during the commit and no
//! longer does. The channel is unbounded, so the metadata store never waits on
//! the store actor, which waits on it.
//!
//! # Unreferenced parts
//!
//! A committed state no longer referencing a part does not make it garbage: the
//! entity may use it again, since its state is never behind its entry. So the
//! store actor hands the parts to the hash's entity, into its unbounded inbox,
//! also while it shuts down. If there is no entity, one is woken for this, so
//! only entities touch their data. A notice is only as recent as its commit,
//! so an entity decides only from its loaded state.
//!
//! At shutdown, the store actor hands the parts of the last batch to the
//! entities before they shut down. Parts that arrive after that stay, as after
//! a crash.
//!
//! # Deletes
//!
//! The store actor fans `DeleteBlobs` out to the entity of each hash, creating
//! it if needed. That orders a delete with concurrent requests even if no
//! entity existed when it arrived. A delete that is not forced skips content
//! with a current or retained temp tag; that check happens in the store actor,
//! before the fan-out. The entity starts a new generation of the entry, which
//! ends observers, deletes the entry in the metadata store, waits for the
//! commit, and then removes its data. Writes wait meanwhile.
//!
//! The store actor answers `DeleteBlobs` once every entity is done. Shutdown
//! waits for running deletes before it shuts down the entities, since an
//! entity in shutdown takes no new tasks.
//!
//! A metadata error aborts the whole store. The store actor cancels the
//! workers and releases pending callers. A delete in a failed batch gets no
//! answer, and fails instead of removing data. There is no per-entity
//! rollback.
//!
//! # GC sessions
//!
//! `GcRun` is a bidi streaming request that enters an exclusive gc session. The
//! store first sends a snapshot of candidates and roots. GC then sends its sweep
//! deletes on the session's update channel, and gets one reply per delete, in
//! order. Closing the update channel ends the session. A second `GcRun` waits
//! until the current session ends.
//!
//! While a session lasts, the store actor holds back all tag changes: setting,
//! creating, deleting and renaming tags, creating temp tags, and import
//! completion, which creates a temp tag. It queues them with their reply
//! channels and keeps handling reads and other blob writes. Temp tag drops and
//! scope ends cannot wait; `TempTags` keeps their protection until the session
//! ends. A batch scope that ended meanwhile is not recreated: its queued tag and
//! import requests are rejected.
//!
//! To build the snapshot, the store actor sends `GcSnapshot` to the metadata actor,
//! a top-level command, so tag changes queued before it are committed. It takes
//! all blobs as candidates, and the named tags plus current and retained temp
//! tags as roots. Then it freezes each HashSeq root through its entity, and
//! sends the snapshot only once every freeze is acknowledged. It keeps running
//! meanwhile, handling entity restarts and unreferenced parts. The mem store
//! takes the same view and freezes its handles in place.
//!
//! A freeze makes an entry read-only: reads go on, while writes, replacing
//! imports and deletes wait. A freeze in turn waits for writes in flight. Complete
//! and invalid entries are frozen too, since a delete would turn them into an
//! entry that can grow again. A freeze task keeps the entity alive. All freezes
//! of a session wait on one release channel; ending the session drops it, and
//! each freeze task makes its entry writable again itself. A freeze waits for
//! an earlier one to end, so freezes never overlap. If a freeze task is dropped
//! at shutdown instead, `persist` looks through the freeze.
//!
//! GC traverses exactly the snapshot's roots, calls the protect callback, and
//! sweeps only the snapshot's candidates. Incomplete HashSeqs contribute the
//! children in their available chunks, and missing chunks never hold up a run.
//! Their writes stay frozen until the sweep ends, so no new reference can appear
//! between traversal and deletion. Blobs added after the snapshot are not
//! candidates, so a run never sweeps them.
//!
//! Sweep deletes travel on the session's own channel, so none can arrive after
//! its session ended. The session task finishes each delete before it reads the
//! next update, and the session ends only after that, even if the GC client or
//! its connection disappears. So a held-back tag change can never overtake a
//! sweep delete. Sweeping a frozen root is rejected, since it would wait on the
//! sweep's own freeze. A plain `DeleteBlobs` is independent of the session: it
//! can be sent at any time, and during a session it skips protected content and
//! waits on frozen roots.
//!
//! Shutdown stops accepting work, rejects held-back tag changes, and cancels a
//! running session without waiting for its client. It lets deletes that were
//! already sent to entities finish before it closes the metadata actor.
//!
//! The protect callback runs in the GC task while the session is held. It must
//! only add protected hashes. It must not await, directly or through another
//! task, anything the session holds back: tag changes, temp tag creation, import
//! completion, and writes to or deletes of frozen HashSeq roots. Those wait for
//! the session to end, which waits for the callback, so the GC run and
//! everything queued behind it would deadlock. External systems must protect
//! content in the store before they publish references to it. If they protect
//! content that was unprotected before, they must check it is still there
//! afterwards: a tag does not bring back data that GC already deleted.
//!
//! # Tasks
//!
//! Tasks do not return a result. They are responsible for sending an error
//! to the requester if possible. Otherwise, just dropping the sender will
//! also fail the receiver, but without a descriptive error message.
//!
//! Tasks are usually implemented as an impl fn that does return a result,
//! and a wrapper (named `..._task`) that just forwards the error, if any.
//!
//! That way you can use `?` syntax in the task implementation. The impl fns
//! are also easier to test.
//!
//! # Context
//!
//! The store actor holds a [`TaskContext`] that is needed for almost all
//! tasks, such as the store's options and the client of the metadata store.
//! Entities get it with their backend in [`EntityGlobal`].
mod actor;
pub(crate) mod bao;
mod entity;
mod entity_manager;
mod import;
#[cfg(test)]
pub(crate) mod mem_backend;
pub(crate) mod metadata;
mod options;
mod state;

pub(crate) use actor::{start, TaskContext};
pub(crate) use entity::{Backend, EntityApi, EntityGlobal};
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

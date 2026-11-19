//! What an entity implements, so the store can run it on its entity manager.
//!
//! An entity is the state of one hash. The entity manager creates it with
//! [`Reset::new`] when a request for the hash arrives, and recycles it with
//! [`Reset::reset`] for another hash once it is idle, keeping its allocations.
//! Neither may do IO: loading is lazy, in the first request that needs it.
//!
//! Each request runs as a task on the entity's actor. Tasks of one entity
//! interleave at `.await` points but never run in parallel. Every method gets
//! the store-wide [`EntityGlobal`], which the manager holds once per store.
//!
//! # Contract
//!
//! - The entity is the only writer of its entry's metadata and the only one
//!   that touches its files.
//! - It decides about files only from loaded state: a delete or an
//!   unreferenced-parts notice loads first.
//! - A delete starts a new generation, which ends observers, and is answered
//!   once committed. Writes wait until it is done.
//! - While frozen, writes and deletes wait; reads go on.
//! - [`EntityApi::persist`] runs when the entity unloads, idle or at shutdown.
use std::{fmt, future::Future, marker::PhantomData, sync::Arc};

use super::{
    file_deletes::UnreferencedParts,
    import::ImportEntryMsg,
    util::entity_manager::{self, ActiveEntityState, Reset},
    TaskContext,
};
use crate::{
    api::proto::{ExportBaoMsg, ExportPathMsg, ExportRangesMsg, ImportBaoMsg, ObserveMsg},
    util::temp_tag::TempTag,
    Hash,
};

/// What every entity of a store shares.
#[derive(Debug)]
pub(super) struct EntityGlobal<C> {
    /// The store: options, and the client of the metadata store.
    pub store: Arc<TaskContext>,
    /// The implementation's own configuration.
    pub config: C,
}

/// The state of one hash, which handles its requests.
pub(super) trait EntityApi:
    Reset<Hash> + fmt::Debug + Clone + Send + Sync + 'static
{
    /// The implementation's configuration, shared by all its entities.
    type Config: fmt::Debug + Send + Sync + 'static;

    /// Import from a stream of n0 bao encoded data.
    fn import_bao(
        &self,
        global: &EntityGlobal<Self::Config>,
        cmd: ImportBaoMsg,
    ) -> impl Future<Output = ()> + Send;

    /// Finish an import from a local file or memory.
    fn finish_import(
        &self,
        global: &EntityGlobal<Self::Config>,
        cmd: ImportEntryMsg,
        tt: TempTag,
    ) -> impl Future<Output = ()> + Send;

    /// Observe the bitfield of the entry, until it is deleted.
    fn observe(
        &self,
        global: &EntityGlobal<Self::Config>,
        cmd: ObserveMsg,
    ) -> impl Future<Output = ()> + Send;

    /// Export byte ranges of the entry.
    fn export_ranges(
        &self,
        global: &EntityGlobal<Self::Config>,
        cmd: ExportRangesMsg,
    ) -> impl Future<Output = ()> + Send;

    /// Export chunk ranges of the entry as a n0 bao encoded stream.
    fn export_bao(
        &self,
        global: &EntityGlobal<Self::Config>,
        cmd: ExportBaoMsg,
    ) -> impl Future<Output = ()> + Send;

    /// Export the entry to a local file.
    fn export_path(
        &self,
        global: &EntityGlobal<Self::Config>,
        cmd: ExportPathMsg,
    ) -> impl Future<Output = ()> + Send;

    /// Delete the entry, for a delete request or a gc sweep.
    fn delete(
        &self,
        global: &EntityGlobal<Self::Config>,
    ) -> impl Future<Output = std::io::Result<()>> + Send;

    /// Hold off writes and deletes, acknowledging through `ack`, until `release` ends.
    fn freeze(
        &self,
        global: &EntityGlobal<Self::Config>,
        ack: tokio::sync::oneshot::Sender<()>,
        release: tokio::sync::watch::Receiver<()>,
    ) -> impl Future<Output = ()> + Send;

    /// Delete the parts a commit stopped referencing, unless the loaded state uses them.
    fn delete_unreferenced(
        &self,
        global: &EntityGlobal<Self::Config>,
        parts: UnreferencedParts,
    ) -> impl Future<Output = ()> + Send;

    /// Persist the entry when it unloads.
    fn persist(&self, global: &EntityGlobal<Self::Config>) -> impl Future<Output = ()> + Send;
}

/// Runs entities of type `E` on the entity manager.
pub(super) struct EmParams<E>(PhantomData<fn() -> E>);

impl<E> fmt::Debug for EmParams<E> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("EmParams")
    }
}

impl<E: EntityApi> entity_manager::Params for EmParams<E> {
    type EntityId = Hash;

    type GlobalState = Arc<EntityGlobal<E::Config>>;

    type EntityState = E;

    async fn on_shutdown(
        state: entity_manager::ActiveEntityState<Self>,
        cause: entity_manager::ShutdownCause,
    ) {
        tracing::trace!("persist {:?} due to {cause:?}", state.id);
        state.state.persist(&state.global).await;
    }
}

/// An entity as the manager hands it to a task: its hash, the shared state, and the entity.
pub(super) type EntityCtx<E> = ActiveEntityState<EmParams<E>>;

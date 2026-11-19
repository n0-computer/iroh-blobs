//! What a store is built from: a [`Backend`], and the entities it runs.
//!
//! A backend names the types that belong together and starts the parts the
//! store does not run itself. Today that is the entity type and the metadata
//! store; import staging and the location type are meant to follow, without
//! changing how a store is built from it.
//!
//! An entity is the state of one hash. The entity manager creates it with
//! [`Reset::new`] when a request for the hash arrives, and recycles it with
//! [`Reset::reset`] for another hash once it is idle, keeping its allocations.
//! Neither may do IO: loading is lazy, in the first request that needs it.
//!
//! Each request runs as a task on the entity's actor. Tasks of one entity
//! interleave at `.await` points but never run in parallel. Every method gets
//! the store-wide [`EntityGlobal`], which the manager holds once per store: the
//! store's context, and the backend value, which is the entities' configuration.
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

use super::metadata::parts::UnreferencedParts;
use super::{
    entity_manager::{self, ActiveEntityState, Reset},
    metadata::{MetaChannels, MetaTask},
};
use crate::store::fs::{import::ImportEntryMsg, TaskContext};
use crate::{
    api::proto::{ExportBaoMsg, ExportPathMsg, ExportRangesMsg, ImportBaoMsg, ObserveMsg},
    util::temp_tag::TempTag,
    Hash,
};

/// The parts a store is built from, beyond its own options.
pub(crate) trait Backend: fmt::Debug + Send + Sync + 'static {
    /// The state of one hash, which handles its requests.
    ///
    /// The entity manager creates it with [`Reset::new`], so this type is the
    /// entity factory.
    type Entity: EntityApi<Backend = Self>;

    /// Starts the metadata store with its channels, on the store's runtime.
    ///
    /// Returns it running. It ends with an error to fail the store.
    fn start_meta(&self, channels: MetaChannels) -> n0_error::Result<impl MetaTask>;
}

/// What every entity of a store shares.
#[derive(Debug)]
pub(crate) struct EntityGlobal<B> {
    /// The store: options, and the client of the metadata store.
    pub store: Arc<TaskContext>,
    /// The backend, which is the entities' configuration.
    pub backend: B,
}

/// The state of one hash, which handles its requests.
pub(crate) trait EntityApi:
    Reset<Hash> + fmt::Debug + Clone + Send + Sync + 'static
{
    /// The backend this entity belongs to.
    type Backend: Backend<Entity = Self>;

    /// Import from a stream of n0 bao encoded data.
    fn import_bao(
        &self,
        global: &EntityGlobal<Self::Backend>,
        cmd: ImportBaoMsg,
    ) -> impl Future<Output = ()> + Send;

    /// Finish an import from a local file or memory.
    fn finish_import(
        &self,
        global: &EntityGlobal<Self::Backend>,
        cmd: ImportEntryMsg,
        tt: TempTag,
    ) -> impl Future<Output = ()> + Send;

    /// Observe the bitfield of the entry, until it is deleted.
    fn observe(
        &self,
        global: &EntityGlobal<Self::Backend>,
        cmd: ObserveMsg,
    ) -> impl Future<Output = ()> + Send;

    /// Export byte ranges of the entry.
    fn export_ranges(
        &self,
        global: &EntityGlobal<Self::Backend>,
        cmd: ExportRangesMsg,
    ) -> impl Future<Output = ()> + Send;

    /// Export chunk ranges of the entry as a n0 bao encoded stream.
    fn export_bao(
        &self,
        global: &EntityGlobal<Self::Backend>,
        cmd: ExportBaoMsg,
    ) -> impl Future<Output = ()> + Send;

    /// Export the entry to a local file.
    fn export_path(
        &self,
        global: &EntityGlobal<Self::Backend>,
        cmd: ExportPathMsg,
    ) -> impl Future<Output = ()> + Send;

    /// Delete the entry, for a delete request or a gc sweep.
    fn delete(
        &self,
        global: &EntityGlobal<Self::Backend>,
    ) -> impl Future<Output = std::io::Result<()>> + Send;

    /// Hold off writes and deletes, acknowledging through `ack`, until `release` ends.
    fn freeze(
        &self,
        global: &EntityGlobal<Self::Backend>,
        ack: tokio::sync::oneshot::Sender<()>,
        release: tokio::sync::watch::Receiver<()>,
    ) -> impl Future<Output = ()> + Send;

    /// Delete the parts a commit stopped referencing, unless the loaded state uses them.
    fn delete_unreferenced(
        &self,
        global: &EntityGlobal<Self::Backend>,
        parts: UnreferencedParts,
    ) -> impl Future<Output = ()> + Send;

    /// Persist the entry when it unloads.
    fn persist(&self, global: &EntityGlobal<Self::Backend>) -> impl Future<Output = ()> + Send;
}

/// Runs the entities of backend `B` on the entity manager.
pub(crate) struct EmParams<B>(PhantomData<fn() -> B>);

impl<B> fmt::Debug for EmParams<B> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("EmParams")
    }
}

impl<B: Backend> entity_manager::Params for EmParams<B> {
    type EntityId = Hash;

    type GlobalState = Arc<EntityGlobal<B>>;

    type EntityState = B::Entity;

    async fn on_shutdown(
        state: entity_manager::ActiveEntityState<Self>,
        cause: entity_manager::ShutdownCause,
    ) {
        tracing::trace!("persist {:?} due to {cause:?}", state.id);
        state.state.persist(&state.global).await;
    }
}

/// An entity as the manager hands it to a task: its hash, the shared state, and the entity.
pub(crate) type EntityCtx<B> = ActiveEntityState<EmParams<B>>;

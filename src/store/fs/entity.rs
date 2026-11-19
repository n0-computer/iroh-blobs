//! Generic entity coordination, lifecycle state, readers, and observers.
use super::{
    entity_storage::EntityStorage,
    export::export_path_impl,
    file_deletes::{BaoFilePart, BaoFileParts, UnreferencedParts, ALL_PARTS},
    import::{ImportEntry, ImportEntryMsg},
    metadata::{EntryMeta, Metadata},
    options::Options,
    util::entity_manager::{self, ActiveEntityState},
};
use crate::{
    api::{
        self,
        blobs::{AddProgressItem, Bitfield},
        proto::{
            bitfield::is_validated, ExportBaoMsg, ExportBaoRequest, ExportPathMsg,
            ExportRangesItem, ExportRangesMsg, ExportRangesRequest, HashSpecific, ImportBaoMsg,
            ImportBaoRequest, ObserveMsg,
        },
    },
    protocol::ChunkRangesExt,
    store::{util::BaoTreeSender, IROH_BLOCK_SIZE},
    util::temp_tag::TempTag,
    Hash,
};
use bao_tree::{
    blake3,
    io::{
        mixed::{traverse_ranges_validated, EncodedItem, ReadBytesAt},
        outboard::PreOrderOutboard,
        sync::ReadAt,
        BaoContentItem, Leaf,
    },
    BaoTree, ChunkNum, ChunkRanges,
};
use bytes::Bytes;
use irpc::channel::mpsc;
use n0_error::{Result, StdResultExt};
use range_collections::range_set::RangeSetRange;
use std::{
    fmt, io,
    num::NonZeroU64,
    ops::{Deref, DerefMut},
    sync::Arc,
};
use tokio::sync::watch;
use tracing::{error, instrument, trace};

/// The state of an entity's entry, as the entity sees it.
///
/// `Initial`, `Loading` and `Deleting` are transient: tasks that load or change
/// the entry wait while it is in one of them. Otherwise the entry is settled,
/// and either writable or frozen for a gc session.
#[derive(Default)]
pub(super) enum StorageState<S: EntityStorage> {
    /// Not loaded yet.
    #[default]
    Initial,
    /// The entry is being loaded from the metadata.
    Loading,
    /// An unconditional delete is awaiting its post-commit file cleanup.
    /// Readers see the entry as absent; mutations wait until cleanup finishes.
    /// Metadata failure aborts the store instead of restoring the old state.
    Deleting,
    /// Loaded, and open for reads and writes. None means invalid storage.
    ReadWrite(Option<S>),
    /// Loaded, and frozen by a gc session: reads go on, writes, imports and
    /// deletes wait until the session ends. None means invalid storage.
    ReadOnly(Option<S>),
}

impl<S: EntityStorage> fmt::Debug for StorageState<S> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            StorageState::Initial => f.debug_struct("Initial").finish(),
            StorageState::Loading => f.debug_struct("Loading").finish(),
            StorageState::Deleting => f.debug_struct("Deleting").finish(),
            StorageState::ReadWrite(x) => f.debug_tuple("ReadWrite").field(x).finish(),
            StorageState::ReadOnly(x) => f.debug_tuple("ReadOnly").field(x).finish(),
        }
    }
}

impl<S: EntityStorage> StorageState<S> {
    /// The loaded entry for a reader, or the error for a state in transition.
    pub(super) fn try_settled(&self) -> io::Result<&S> {
        match self {
            Self::ReadWrite(s) | Self::ReadOnly(s) => s
                .as_ref()
                .ok_or_else(|| io::Error::other("invalid storage")),
            Self::Initial => Err(io::Error::other("initial")),
            Self::Loading => Err(io::Error::other("loading")),
            // A delete in progress: readers see no entry.
            Self::Deleting => Err(io::ErrorKind::NotFound.into()),
        }
    }

    pub fn bitfield(&self) -> Option<Bitfield> {
        match self {
            Self::Initial => panic!("initial storage should not be used"),
            Self::Loading => panic!("loading storage should not be used"),
            Self::Deleting => Some(Bitfield::empty()),
            Self::ReadWrite(s) | Self::ReadOnly(s) => match s.as_ref()?.bitfield() {
                Ok(bitfield) => Some(bitfield),
                Err(cause) if cause.kind() == io::ErrorKind::NotFound => Some(Bitfield::empty()),
                Err(_) => None,
            },
        }
    }

    /// Whether this state uses the given file of its entry.
    ///
    /// Says yes when it cannot tell, since a file kept too long only leaks,
    /// while a file deleted too early loses data.
    pub(super) fn references(&self, part: BaoFilePart) -> bool {
        match self {
            // Not loaded: the entity knows nothing about its files.
            Self::Initial | Self::Loading | Self::Deleting => true,
            Self::ReadWrite(s) | Self::ReadOnly(s) => s.as_ref().is_none_or(|s| s.references(part)),
        }
    }

    /// Takes the state, leaving an invalid one behind.
    pub fn take(&mut self) -> Self {
        std::mem::replace(self, Self::ReadWrite(None))
    }
}

/// The state of an entity: its storage, and the generation of its entry.
///
/// Derefs to the storage, which is what almost all code looks at.
#[derive(Debug)]
pub(super) struct EntityState<S: EntityStorage> {
    /// Starts a new generation with every delete of the entry. Observers end
    /// when it changes, even if they wake only after the entry was filled
    /// again. Only ever grows, also across recycling.
    pub generation: u64,
    pub storage: StorageState<S>,
}

impl<S: EntityStorage> Deref for EntityState<S> {
    type Target = StorageState<S>;

    fn deref(&self) -> &StorageState<S> {
        &self.storage
    }
}

impl<S: EntityStorage> DerefMut for EntityState<S> {
    fn deref_mut(&mut self) -> &mut StorageState<S> {
        &mut self.storage
    }
}

/// A cheaply cloneable handle to an entity.
///
/// The coordinator persists storage before releasing it, letting the backend
/// save its bitfield cache or other state needed on the next load.
#[derive(Debug, derive_more::Deref)]
pub(super) struct EntityHandle<S: EntityStorage>(pub(super) watch::Sender<EntityState<S>>);

impl<S: EntityStorage> EntityHandle<S> {
    /// Replaces the storage, keeping the generation.
    pub(super) fn set_storage(&self, storage: StorageState<S>) {
        self.send_modify(|state| state.storage = storage);
    }
}

impl<S: EntityStorage> entity_manager::Reset for EntityHandle<S> {
    fn reset(&mut self) {
        // Keeps the generation.
        self.send_modify(|state| state.storage = StorageState::Initial);
    }
}

/// A reader for a bao file, reading just the data.
#[derive(Debug)]
pub(super) struct DataReader<S: EntityStorage>(pub(super) EntityHandle<S>);

impl<S: EntityStorage> ReadBytesAt for DataReader<S> {
    fn read_bytes_at(&self, offset: u64, size: usize) -> std::io::Result<Bytes> {
        let guard = self.0.borrow();
        guard.try_settled()?.read_data_at(offset, size)
    }
}

/// A reader for the outboard part of a bao file.
#[derive(Debug)]
pub(super) struct OutboardReader<S: EntityStorage>(pub(super) EntityHandle<S>);

impl<S: EntityStorage> ReadAt for OutboardReader<S> {
    fn read_at(&self, offset: u64, buf: &mut [u8]) -> io::Result<usize> {
        let guard = self.0.borrow();
        guard.try_settled()?.read_outboard_at(offset, buf)
    }
}

impl<S: EntityStorage> EntityHandle<S> {
    /// Install completed storage, preserving an already complete entry.
    pub fn complete(&self, storage: S) {
        debug_assert!(storage.is_complete());
        self.send_if_modified(|guard| {
            // A finished import can replace invalid storage as well.
            let replace = matches!(
                &guard.storage,
                StorageState::ReadWrite(s) if !s.as_ref().is_some_and(|s| s.is_complete())
            );
            if replace {
                guard.storage = StorageState::ReadWrite(Some(storage));
            }
            true
        });
    }
}

pub(super) struct EntitySubscriber<S: EntityStorage> {
    receiver: watch::Receiver<EntityState<S>>,
    /// The entry's generation when the subscription started.
    generation: u64,
}

impl<S: EntityStorage> EntitySubscriber<S> {
    pub fn new(handle: &EntityHandle<S>) -> Self {
        let receiver = handle.subscribe();
        let generation = receiver.borrow().generation;
        Self {
            receiver,
            generation,
        }
    }

    /// Forward observed *values* to the given sender
    ///
    /// Returns an error if sending fails, or if the last sender is dropped
    ///
    /// Ends the stream if the ranges shrink, for example when the entry was
    /// deleted while observed, since observers only ever see them grow.
    pub async fn forward(mut self, mut tx: mpsc::Sender<Bitfield>) -> Result<()> {
        let Some(value) = self.bitfield() else {
            return Ok(());
        };
        let mut last = value.clone();
        tx.send(value).await?;
        loop {
            self.update_or_closed(&mut tx).await?;
            let Some(value) = self.bitfield() else {
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

    /// Forward observed *deltas* to the given sender
    ///
    /// Returns an error if sending fails, or if the last sender is dropped
    #[allow(dead_code)]
    pub async fn forward_delta(mut self, mut tx: mpsc::Sender<Bitfield>) -> Result<()> {
        let Some(value) = self.bitfield() else {
            return Ok(());
        };
        let mut old = value.clone();
        tx.send(value).await?;
        loop {
            self.update_or_closed(&mut tx).await?;
            let Some(new) = self.bitfield() else {
                return Ok(());
            };
            let diff = old.diff(&new);
            if diff.is_empty() {
                continue;
            }
            tx.send(diff).await?;
            old = new;
        }
    }

    /// The current bitfield, or `None` if the storage is invalid or the entry was deleted.
    ///
    /// Observers end the stream on `None`, since there is no error channel,
    /// and since observed ranges must only ever grow.
    fn bitfield(&self) -> Option<Bitfield> {
        // The generation and the storage come from the same value.
        let state = self.receiver.borrow();
        if state.generation != self.generation {
            return None;
        }
        state.storage.bitfield()
    }

    async fn update_or_closed(&mut self, tx: &mut mpsc::Sender<Bitfield>) -> Result<()> {
        tokio::select! {
            _ = tx.closed() => {
                // the sender is closed, we are done
                Err(n0_error::e!(irpc::channel::SendError::ReceiverClosed).into())
            }
            e = self.receiver.changed() => Ok(e.anyerr()?),
        }
    }
}

impl<S: EntityStorage> Default for EntityState<S> {
    fn default() -> Self {
        Self {
            generation: 0,
            storage: StorageState::Initial,
        }
    }
}
impl<S: EntityStorage> Default for EntityHandle<S> {
    fn default() -> Self {
        Self(watch::Sender::new(EntityState::default()))
    }
}
impl<S: EntityStorage> Clone for EntityHandle<S> {
    fn clone(&self) -> Self {
        Self(self.0.clone())
    }
}

#[derive(Debug)]
pub(super) struct EmParams<S: EntityStorage>(std::marker::PhantomData<S>);

#[derive(Debug)]
pub(super) struct EntityContext<S: EntityStorage> {
    pub(super) metadata: Metadata,
    pub(super) options: Arc<Options>,
    pub(super) storage_config: Arc<S::Config>,
}

impl<S: EntityStorage> entity_manager::Params for EmParams<S> {
    type EntityId = Hash;

    type GlobalState = Arc<EntityContext<S>>;

    type EntityState = EntityHandle<S>;

    async fn on_shutdown(
        state: entity_manager::ActiveEntityState<Self>,
        cause: entity_manager::ShutdownCause,
    ) {
        trace!("persist {:?} due to {cause:?}", state.id);
        state.persist().await;
    }
}

/// Waits until the entity is neither loading nor deleting.
///
/// Loading may create files, and deleting resets the state, so both must be
/// done before a task may look at the state or touch files.
async fn wait_settled<S: EntityStorage>(ctx: &HashContext<S>) {
    // Checking and subscribing is one synchronous step, so no change can slip
    // in between: tasks of an entity only interleave at awaits.
    while matches!(
        &ctx.state.borrow().storage,
        StorageState::Loading | StorageState::Deleting
    ) {
        ctx.state.0.subscribe().changed().await.ok();
    }
}

/// Mutations also wait for a GC freeze. Readers only need `wait_settled`.
pub(super) async fn wait_writable<S: EntityStorage>(ctx: &HashContext<S>) {
    loop {
        let mut changes = ctx.state.0.subscribe();
        if !matches!(
            &ctx.state.borrow().storage,
            StorageState::Loading | StorageState::Deleting | StorageState::ReadOnly(_)
        ) {
            return;
        }
        changes.changed().await.ok();
    }
}

/// Freezes the entry until the sender of `release` is dropped, then restores it.
///
/// The task is the only one that ends its freeze, and a freeze waits in
/// `wait_writable` for an earlier one to end. So no two freezes overlap, and a
/// freeze needs no owner id. The task keeps the entity alive while it holds the
/// freeze. If the task is dropped instead, the entity is shutting down, and
/// `persist` looks through the freeze.
async fn freeze_entry<S: EntityStorage>(
    ctx: &HashContext<S>,
    ack: tokio::sync::oneshot::Sender<()>,
    mut release: tokio::sync::watch::Receiver<()>,
) {
    ctx.load().await;
    wait_writable(ctx).await;
    // Loaded, and not in a delete or another freeze, see above.
    let frozen = ctx.state.send_if_modified(|state| match state.take() {
        StorageState::ReadWrite(settled) => {
            state.storage = StorageState::ReadOnly(settled);
            true
        }
        other => {
            state.storage = other;
            false
        }
    });
    if !frozen {
        debug_assert!(false, "freeze of an entry that is not writable");
        // Without an ack, the gc session fails instead of trusting the freeze.
        return;
    }
    if ack.send(()).is_ok() {
        // Nothing is ever sent, so this returns once the sender is dropped.
        release.changed().await.ok();
    }
    unfreeze_entry(ctx);
}

fn unfreeze_entry<S: EntityStorage>(ctx: &HashContext<S>) {
    ctx.state.send_if_modified(|state| {
        if !matches!(state.storage, StorageState::ReadOnly(_)) {
            return false;
        }
        let StorageState::ReadOnly(settled) = state.take() else {
            unreachable!()
        };
        state.storage = StorageState::ReadWrite(settled);
        true
    });
}

/// Loads the entity's state from the metadata, unless it is loaded already.
async fn load_entity<S: EntityStorage>(ctx: &HashContext<S>) {
    wait_settled(ctx).await;
    let mut load = false;
    ctx.state.send_if_modified(|guard| {
        if matches!(guard.storage, StorageState::Initial) {
            guard.storage = StorageState::Loading;
            load = true;
        }
        load
    });
    if !load {
        return;
    }
    // we must assign a new state even in the error case, otherwise
    // tasks waiting for loading would stall!
    let loaded = async {
        let entry = ctx.meta().get().await?;
        let (storage, correction) = S::open(&ctx.global.storage_config, &ctx.id, entry)?;
        if let Some(correction) = correction {
            ctx.meta().set(correction).await?;
        }
        Ok::<_, io::Error>(storage)
    }
    .await;
    let state = match loaded {
        Ok(storage) => Some(storage),
        Err(cause) => {
            error!("failed to load {}, marking it invalid: {cause}", ctx.id);
            None
        }
    };
    ctx.state.set_storage(StorageState::ReadWrite(state));
}

/// Delete unconditionally, keeping writes suspended until committed cleanup.
/// Metadata failure aborts the store, so there is no rollback state to retain.
async fn delete_entry<S: EntityStorage>(ctx: &HashContext<S>) -> io::Result<()> {
    // Loaded, the entity's state knows all its files: those of its entry, and
    // those it wrote that the entry does not reference yet.
    ctx.load().await;
    wait_writable(ctx).await;
    let parts: BaoFileParts = {
        let state = ctx.state.borrow();
        ALL_PARTS
            .into_iter()
            .filter(|part| state.references(*part))
            .collect()
    };
    // A new generation ends observers, even if they next wake only after this
    // hash was re-imported.
    ctx.state.send_modify(|state| {
        state.generation += 1;
        state.storage = StorageState::Deleting;
    });
    // Answered once the delete is committed.
    if let Err(cause) = ctx.meta().delete().await {
        // Metadata failure aborts the store. A command racing a graceful
        // shutdown can also be rejected, without a failure to trigger abort.
        // Release waiters with an error instead of restoring the old state.
        ctx.state.set_storage(StorageState::ReadWrite(None));
        return Err(cause);
    }
    // No durable state references the files anymore.
    S::delete_parts(&ctx.global.storage_config, &ctx.id, &parts);
    ctx.state
        .set_storage(StorageState::ReadWrite(Some(S::empty())));
    Ok(())
}

/// Deletes the parts a committed batch stopped referencing, unless the entry still uses them.
///
/// The notice is only as recent as its commit: by the time it arrives, the
/// entry may have been deleted and created again, and its entity unloaded. So
/// the parts are checked against the loaded state, which is never behind the
/// entry. An entity woken just for this loads first.
async fn delete_unreferenced<S: EntityStorage>(ctx: &HashContext<S>, parts: UnreferencedParts) {
    ctx.load().await;
    wait_writable(ctx).await;
    let parts: BaoFileParts = {
        let state = ctx.state.borrow();
        parts
            .0
            .into_iter()
            .filter(|part| !state.references(*part))
            .collect()
    };
    S::delete_parts(&ctx.global.storage_config, &ctx.id, &parts);
}

pub(super) type HashContext<S> = ActiveEntityState<EmParams<S>>;

impl<S: EntityStorage> SyncEntityApi for HashContext<S> {
    /// Load the state from the database.
    ///
    /// If the state is Initial, this will start the load.
    /// If it is Loading, it will wait until loading is done.
    /// If it is any other state, it will be a noop.
    async fn load(&self) {
        load_entity(self).await
    }

    /// Write a batch and notify the db
    async fn write_batch(&self, batch: &[BaoContentItem], bitfield: &Bitfield) -> io::Result<()> {
        trace!("write_batch bitfield={:?} batch={}", bitfield, batch.len());
        // No await between this and the closure, so the closure never sees a
        // delete in progress.
        wait_writable(self).await;
        let mut res = Ok(None);
        self.state.send_if_modified(|state| {
            // Imports load the entity first, and wait_writable leaves it
            // loaded and writable.
            let settled = match state.take() {
                StorageState::ReadWrite(Some(settled)) => settled,
                StorageState::ReadWrite(None) => {
                    res = Err(io::Error::other("invalid storage"));
                    return false;
                }
                other => {
                    debug_assert!(false, "write to an entry that is not writable");
                    state.storage = other;
                    return false;
                }
            };
            let (settled, update) =
                match settled.write_batch(batch, bitfield, &self.global.storage_config, &self.id) {
                    Ok(written) => written,
                    Err(cause) => {
                        error!(
                            "failed to write batch for {}, marking it invalid: {cause}",
                            self.id
                        );
                        res = Err(cause);
                        // the entry is invalid now, wake up observers so they can end
                        return true;
                    }
                };
            res = Ok(update);
            state.storage = StorageState::ReadWrite(Some(settled));
            true
        });
        if let Some(update) = res? {
            self.meta().update(update).await?;
        }
        Ok(())
    }

    /// An AsyncSliceReader for the data file.
    ///
    /// Caution: this is a reader for the unvalidated data file. Reading this
    /// can produce data that does not match the hash.
    #[allow(refining_impl_trait_internal)]
    fn data_reader(&self) -> DataReader<S> {
        DataReader(self.state.clone())
    }

    /// An AsyncSliceReader for the outboard file.
    ///
    /// The outboard file is used to validate the data file. It is not guaranteed
    /// to be complete.
    #[allow(refining_impl_trait_internal)]
    fn outboard_reader(&self) -> OutboardReader<S> {
        OutboardReader(self.state.clone())
    }

    /// The most precise known total size of the data file.
    fn current_size(&self) -> io::Result<u64> {
        let state = self.state.borrow();
        state.try_settled()?.current_size()
    }

    /// The most precise known total size of the data file.
    fn bitfield(&self) -> io::Result<Bitfield> {
        let state = self.state.borrow();
        state.try_settled()?.bitfield()
    }
}

/// The minimal API you need to implement for an entity for a store to work.
pub(super) trait EntityApi {
    /// The metadata of the entry.
    fn meta(&self) -> EntryMeta<'_>;
    /// Import from a stream of n0 bao encoded data.
    async fn import_bao(&self, cmd: ImportBaoMsg);
    /// Finish an import from a local file or memory.
    async fn finish_import(&self, cmd: ImportEntryMsg, tt: TempTag);
    /// Observe the bitfield of the entry.
    async fn observe(&self, cmd: ObserveMsg);
    /// Export byte ranges of the entry as data
    async fn export_ranges(&self, cmd: ExportRangesMsg);
    /// Export chunk ranges of the entry as a n0 bao encoded stream.
    async fn export_bao(&self, cmd: ExportBaoMsg);
    /// Export the entry to a local file.
    async fn export_path(&self, cmd: ExportPathMsg);
    /// Delete the entry, for a delete request or a gc sweep.
    async fn delete(&self) -> io::Result<()>;
    /// Hold off writes during a gc run, acknowledging through `ack`, until `release` ends.
    async fn freeze(
        &self,
        ack: tokio::sync::oneshot::Sender<()>,
        release: tokio::sync::watch::Receiver<()>,
    );
    /// Delete the parts a committed batch stopped referencing, after checking each against the loaded state.
    ///
    /// The notice may be stale, so the entity must load before deciding.
    async fn delete_unreferenced(&self, parts: UnreferencedParts);
    /// Persist the entry at the end of its lifecycle.
    async fn persist(&self);
}

/// A more opinionated API that can be used as a helper to save implementation
/// effort when implementing the EntityApi trait.
pub(super) trait SyncEntityApi: EntityApi {
    /// Load the entry state from the database. This must make sure that it is
    /// not run concurrently, so if load is called multiple times, all but one
    /// must wait. You can use a tokio::sync::OnceCell or similar to achieve this.
    async fn load(&self);

    /// Get a synchronous reader for the data file.
    fn data_reader(&self) -> impl ReadBytesAt;

    /// Get a synchronous reader for the outboard file.
    fn outboard_reader(&self) -> impl ReadAt;

    /// Get the best known size of the data file.
    fn current_size(&self) -> io::Result<u64>;

    /// Get the bitfield of the entry.
    fn bitfield(&self) -> io::Result<Bitfield>;

    /// Write a batch of content items to the entry.
    async fn write_batch(&self, batch: &[BaoContentItem], bitfield: &Bitfield) -> io::Result<()>;

    /// The outboard for the entry.
    fn outboard(&self) -> io::Result<PreOrderOutboard<impl ReadAt>> {
        let tree = BaoTree::new(self.current_size()?, IROH_BLOCK_SIZE);
        let outboard = self.outboard_reader();
        Ok(PreOrderOutboard {
            root: blake3::Hash::from(self.meta().hash()),
            tree,
            data: outboard,
        })
    }
}

/// The high level entry point per entry.
impl<S: EntityStorage> EntityApi for HashContext<S> {
    fn meta(&self) -> EntryMeta<'_> {
        self.global.metadata.entry(self.id)
    }

    #[instrument(skip_all, fields(hash = %cmd.hash_short()))]
    async fn import_bao(&self, cmd: ImportBaoMsg) {
        trace!("{cmd:?}");
        self.load().await;
        let ImportBaoMsg {
            inner: ImportBaoRequest { size, .. },
            mut rx,
            tx,
            ..
        } = cmd;
        let res = import_bao_impl(self, size, &mut rx).await;
        trace!("{res:?}");
        // Answer before dropping `rx`, so the sender sees our error rather
        // than just a closed channel.
        tx.send(res).await.ok();
        drop(rx);
    }

    #[instrument(skip_all, fields(hash = %cmd.hash_short()))]
    async fn observe(&self, cmd: ObserveMsg) {
        trace!("{cmd:?}");
        self.load().await;
        EntitySubscriber::new(&self.state)
            .forward(cmd.tx)
            .await
            .ok();
    }

    #[instrument(skip_all, fields(hash = %cmd.hash_short()))]
    async fn export_ranges(&self, mut cmd: ExportRangesMsg) {
        trace!("{cmd:?}");
        self.load().await;
        if let Err(cause) = export_ranges_impl(self, cmd.inner, &mut cmd.tx).await {
            cmd.tx
                .send(ExportRangesItem::Error(cause.into()))
                .await
                .ok();
        }
    }

    #[instrument(skip_all, fields(hash = %cmd.hash_short()))]
    async fn export_bao(&self, mut cmd: ExportBaoMsg) {
        trace!("{cmd:?}");
        self.load().await;
        if let Err(cause) = export_bao_impl(self, cmd.inner, &mut cmd.tx).await {
            // if the entry is in state NonExisting, this will be an io error with
            // kind NotFound. So we must not wrap this somehow but pass it on directly.
            cmd.tx
                .send(bao_tree::io::EncodeError::Io(cause).into())
                .await
                .ok();
        }
    }

    #[instrument(skip_all, fields(hash = %cmd.hash_short()))]
    async fn export_path(&self, cmd: ExportPathMsg) {
        trace!("{cmd:?}");
        self.load().await;
        let ExportPathMsg { inner, mut tx, .. } = cmd;
        if let Err(cause) = export_path_impl(self, inner, &mut tx).await {
            tx.send(cause.into()).await.ok();
        }
    }

    #[instrument(skip_all, fields(hash = %cmd.hash_short()))]
    async fn finish_import(&self, cmd: ImportEntryMsg, mut tt: TempTag) {
        trace!("{cmd:?}");
        self.load().await;
        let res = match finish_import_impl(self, cmd.inner).await {
            Ok(()) => {
                // for a remote call, we can't have the on_drop callback, so we have to leak the temp tag
                // it will be cleaned up when either the process exits or scope ends
                if cmd.tx.is_rpc() {
                    trace!("leaking temp tag {}", tt.hash_and_format());
                    tt.leak();
                }
                AddProgressItem::Done(tt)
            }
            Err(cause) => AddProgressItem::Error(cause),
        };
        cmd.tx.send(res).await.ok();
    }

    async fn delete(&self) -> io::Result<()> {
        delete_entry(self).await
    }

    async fn freeze(
        &self,
        ack: tokio::sync::oneshot::Sender<()>,
        release: tokio::sync::watch::Receiver<()>,
    ) {
        freeze_entry(self, ack, release).await
    }

    async fn delete_unreferenced(&self, parts: UnreferencedParts) {
        delete_unreferenced(self, parts).await
    }

    #[instrument(skip_all, fields(hash = %self.id.fmt_short()))]
    async fn persist(&self) {
        self.state.send_if_modified(|guard| {
            let hash = &self.id;
            // A freeze task dropped at shutdown leaves its freeze in place.
            let (StorageState::ReadWrite(Some(storage)) | StorageState::ReadOnly(Some(storage))) =
                guard.take()
            else {
                return false;
            };
            if let Err(cause) = storage.persist(&self.global.storage_config, hash) {
                error!("failed to persist {hash}: {cause}");
            }
            false
        });
    }
}

async fn finish_import_impl<S: EntityStorage>(
    ctx: &HashContext<S>,
    import_data: ImportEntry,
) -> io::Result<()> {
    ctx.load().await;
    wait_writable(ctx).await;
    let (storage, update) = S::finish_import(&ctx.global.storage_config, &ctx.id, import_data)?;
    ctx.state.complete(storage);
    ctx.meta().update_await(update).await?;
    Ok(())
}

fn chunk_range(leaf: &Leaf) -> ChunkRanges {
    let start = ChunkNum::chunks(leaf.offset);
    let end = ChunkNum::chunks(leaf.offset + leaf.data.len() as u64);
    (start..end).into()
}

async fn import_bao_impl(
    ctx: &impl SyncEntityApi,
    size: NonZeroU64,
    rx: &mut mpsc::Receiver<BaoContentItem>,
) -> api::Result<()> {
    trace!(
        "importing bao: {} {} bytes",
        ctx.meta().hash().fmt_short(),
        size
    );
    let mut batch = Vec::<BaoContentItem>::new();
    let mut ranges = ChunkRanges::empty();
    while let Some(item) = rx.recv().await? {
        // if the batch is not empty, the last item is a leaf and the current item is a parent, write the batch
        if !batch.is_empty() && batch[batch.len() - 1].is_leaf() && item.is_parent() {
            let bitfield = Bitfield::new_unchecked(ranges, size.into());
            ctx.write_batch(&batch, &bitfield).await?;
            batch.clear();
            ranges = ChunkRanges::empty();
        }
        if let BaoContentItem::Leaf(leaf) = &item {
            let leaf_range = chunk_range(leaf);
            if is_validated(size, &leaf_range) && size.get() != leaf.offset + leaf.data.len() as u64
            {
                return Err(api::Error::io(io::ErrorKind::InvalidData, "invalid size"));
            }
            ranges |= leaf_range;
        }
        batch.push(item);
    }
    if !batch.is_empty() {
        let bitfield = Bitfield::new_unchecked(ranges, size.into());
        ctx.write_batch(&batch, &bitfield).await?;
    }
    Ok(())
}

async fn export_ranges_impl(
    ctx: &impl SyncEntityApi,
    cmd: ExportRangesRequest,
    tx: &mut mpsc::Sender<ExportRangesItem>,
) -> io::Result<()> {
    let ExportRangesRequest { ranges, hash } = cmd;
    trace!(
        "exporting ranges: {hash} {ranges:?} size={}",
        ctx.current_size()?
    );
    let bitfield = ctx.bitfield()?;
    let data = ctx.data_reader();
    let size = bitfield.size();
    tx.send(ExportRangesItem::Size(size)).await?;
    for range in ranges.iter() {
        let range = match range {
            RangeSetRange::Range(range) => size.min(*range.start)..size.min(*range.end),
            RangeSetRange::RangeFrom(range) => size.min(*range.start)..size,
        };
        let requested = ChunkRanges::bytes(range.start..range.end);
        if !bitfield.ranges.is_superset(&requested) {
            return Err(io::Error::new(
                io::ErrorKind::NotFound,
                format!("missing range: {requested:?}, present: {bitfield:?}"),
            ));
        }
        let bs = 1024;
        let mut offset = range.start;
        loop {
            let end: u64 = (offset + bs).min(range.end);
            let size = (end - offset) as usize;
            let res = data.read_bytes_at(offset, size);
            tx.send(ExportRangesItem::Data(Leaf { offset, data: res? }))
                .await?;
            offset = end;
            if offset >= range.end {
                break;
            }
        }
    }
    Ok(())
}

async fn export_bao_impl(
    ctx: &impl SyncEntityApi,
    cmd: ExportBaoRequest,
    tx: &mut mpsc::Sender<EncodedItem>,
) -> io::Result<()> {
    let ExportBaoRequest { ranges, hash, .. } = cmd;
    let outboard = ctx.outboard()?;
    let size = outboard.tree.size();
    if size == 0 {
        // we have no data whatsoever, so we stop here
        return Ok(());
    }
    trace!("exporting bao: {hash} {ranges:?} size={size}",);
    let data = ctx.data_reader();
    let tx = BaoTreeSender::new(tx);
    traverse_ranges_validated(data, outboard, &ranges, tx).await?;
    Ok(())
}

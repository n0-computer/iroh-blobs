//! The fs store's entity: an entry's data and outboard in files, read and written through file descriptors.
//!
//! See the store engine's docs for how entities, the metadata store and the
//! store actor work together.
//!
//! # States
//!
//! An entity's state is `Initial`, `Loading` or `Deleting` while it is in
//! transition, and otherwise a loaded `BaoStorage` entry: `NonExisting`,
//! `Partial`, `Complete` or `Invalid`. A settled entry is `ReadWrite`, or
//! `ReadOnly` while a gc session freezes it.
//!
//! An entity's tasks are futures polled by one actor, so they only interleave at
//! `.await` points. Every change of an entity's state or files runs after
//! `wait_writable`, which waits while the state is `Loading`, `Deleting` or
//! `ReadOnly`, with no `.await` between the check and the change. Readers use
//! `wait_settled`, which only waits for `Loading` and `Deleting`. Both wait on
//! the entity's state `watch`.
//!
//! # Unreferenced parts
//!
//! The entity checks each part against its own state, and only unlinks those
//! it does not use. An entity that is not loaded, like one woken for the
//! notice, loads first. It is also told about parts while it shuts down,
//! since its shutdown may write the bitfield.
//!
//! # Deletes
//!
//! 1. Loads, and notes the files its state references: those of its entry, and
//!    those it wrote that the entry does not reference yet. Then it starts a
//!    new generation of the entry, which ends observers, and switches to
//!    `Deleting`. The cached state is dropped: readers see no entry, and
//!    changes wait from here on.
//! 2. Sends `delete` to the metadata store. Deletes that arrive together still
//!    land in one transaction.
//! 3. Waits for the answer, which comes once the delete is committed, even if
//!    there was no entry. Waiting keeps the entity alive, so it cannot unload
//!    and come back with new files that the unlink would take.
//! 4. Unlinks the files it noted, so an inline entry costs no unlink, and
//!    switches to `NonExisting`. Waiting writes continue on an empty entry: a
//!    running download keeps going with the ranges that arrive from then on.
//!
//! Unlink failures are logged. A delete in a failed metadata batch fails
//! instead of unlinking.
//!
//! # Context
//!
//! For one request, a `HashContext` combines the store's context, the fs
//! entity's configuration, and the entity's state.
use std::{fmt::Debug, fs, io::Write, path::Path};

use bao_tree::io::BaoContentItem;
use irpc::channel::mpsc;
use n0_future::io;
use tracing::{error, instrument, trace};

use super::options::FsConfig;
use crate::store::engine::{
    bao::{export_bao_impl, export_ranges_impl, import_bao_impl, BaoAccess},
    metadata::{
        self,
        entry_state::{DataLocation, EntryState, OutboardLocation},
        parts::{BaoFileParts, UnreferencedParts, ALL_PARTS},
    },
    reflink_or_copy_with_progress, EntityApi, EntityGlobal, ImportEntry, ImportEntryMsg,
    ImportSource, Phase, Reset, StateCell, StoreOptions, TaskContext, WriteGuard,
};
use crate::{
    api::proto::{
        Bitfield, ExportBaoMsg, ExportPathMsg, ExportPathRequest, ExportRangesItem,
        ExportRangesMsg, HashSpecific, ImportBaoMsg, ImportBaoRequest, ObserveMsg,
    },
    store::util::{FixedSize, MemOrFile},
    util::temp_tag::TempTag,
    Hash,
};

use crate::api::{
    self,
    blobs::{AddProgressItem, ExportMode, ExportProgressItem},
};

mod bao_file;
mod file_deletes;
use bao_file::{complete, BaoStorage, DataReader, OutboardReader};

use super::FsBackend;
use file_deletes::delete_files;

/// Maximum number of external paths we track per blob.
const MAX_EXTERNAL_PATHS: usize = 8;

/// Waits until the entity is neither loading nor deleting.
///
/// Mutations also wait for a GC freeze. Readers only need `wait_settled`.
async fn wait_writable<'a>(ctx: &HashContext<'a>) -> WriteGuard<'a, BaoStorage> {
    ctx.state.wait_writable().await
}

/// Loads the entry and freezes it, see [`StateCell::freeze`](crate::store::engine::StateCell::freeze).
async fn freeze_entry(
    ctx: &HashContext<'_>,
    ack: tokio::sync::oneshot::Sender<()>,
    release: tokio::sync::watch::Receiver<()>,
) {
    ctx.load().await;
    ctx.state.freeze(ack, release).await
}

/// Opens the entry from the metadata, marking it invalid if that fails.
async fn open_entry(ctx: &HashContext<'_>) -> BaoStorage {
    let state = match ctx.meta().get().await {
        Ok(state) => state,
        Err(cause) => {
            error!(
                "failed to get {} from db, marking it invalid: {cause}",
                ctx.id
            );
            return BaoStorage::Invalid;
        }
    };
    match BaoStorage::open(state, ctx).await {
        Ok(settled) => settled,
        Err(cause) => {
            error!("failed to open {}, marking it invalid: {cause}", ctx.id);
            BaoStorage::Invalid
        }
    }
}

/// Delete unconditionally, keeping writes suspended until committed cleanup.
/// Metadata failure aborts the store, so there is no rollback state to retain.
async fn delete_entry(ctx: &HashContext<'_>) -> io::Result<()> {
    // Loaded, the entity's state knows all its files: those of its entry, and
    // those it wrote that the entry does not reference yet.
    ctx.load().await;
    ctx.state
        .delete(|old| async move {
            let parts: BaoFileParts = ALL_PARTS
                .into_iter()
                .filter(|part| old.references(*part))
                .collect();
            // Close the files before unlinking them.
            drop(old);
            // Answered once the delete is committed.
            if let Err(cause) = ctx.meta().delete().await {
                // Metadata failure aborts the store. A command racing a graceful
                // shutdown can also be rejected, without a failure to trigger abort.
                // Release waiters with an error instead of restoring the old state.
                return (BaoStorage::Invalid, Err(cause));
            }
            // No durable state references the files anymore.
            delete_files(&ctx.config.path, &ctx.id, &parts);
            (BaoStorage::NonExisting, Ok(()))
        })
        .await
}

/// Deletes the parts a committed batch stopped referencing, unless the entry still uses them.
///
/// The notice is only as recent as its commit: by the time it arrives, the
/// entry may have been deleted and created again, and its entity unloaded. So
/// the parts are checked against the loaded state, which is never behind the
/// entry. An entity woken just for this loads first.
async fn delete_unreferenced(ctx: &HashContext<'_>, parts: UnreferencedParts) {
    ctx.load().await;
    let _write = wait_writable(ctx).await;
    let parts: BaoFileParts = ctx.state.peek(|state| {
        parts
            .0
            .into_iter()
            .filter(|part| !state.references(*part))
            .collect()
    });
    delete_files(&ctx.config.path, &ctx.id, &parts);
}

/// What the fs entity's code works on: the hash, the store, and the entity's state.
#[derive(Debug, Clone, Copy)]
pub(super) struct HashContext<'a> {
    id: Hash,
    global: &'a TaskContext,
    config: &'a FsConfig,
    state: &'a StateCell<BaoStorage>,
}

/// The fs store's entity: the state of a hash's files.
#[derive(Debug, Clone)]
pub(super) struct FsEntity {
    id: Hash,
    handle: StateCell<BaoStorage>,
}

impl FsEntity {
    /// The context its code runs in, for one request.
    fn ctx<'a>(&'a self, global: &'a EntityGlobal<FsBackend>) -> HashContext<'a> {
        HashContext {
            id: self.id,
            global: &global.store,
            config: &global.backend.config,
            state: &self.handle,
        }
    }
}

impl Reset<Hash> for FsEntity {
    fn new(id: &Hash) -> Self {
        Self {
            id: *id,
            handle: StateCell::default(),
        }
    }

    fn reset(&mut self, id: &Hash) {
        self.id = *id;
        // Keeps the generation.
        self.handle.reset();
    }
}

impl EntityApi for FsEntity {
    type Backend = FsBackend;

    #[instrument(skip_all, fields(hash = %cmd.hash_short()))]
    async fn import_bao(&self, global: &EntityGlobal<FsBackend>, cmd: ImportBaoMsg) {
        trace!("{cmd:?}");
        let ctx = self.ctx(global);
        ctx.load().await;
        let ImportBaoMsg {
            inner: ImportBaoRequest { size, .. },
            mut rx,
            tx,
            ..
        } = cmd;
        let res = import_bao_impl(&ctx, size, &mut rx).await;
        trace!("{res:?}");
        // Answer before dropping `rx`, so the sender sees our error rather
        // than just a closed channel.
        tx.send(res).await.ok();
        drop(rx);
    }

    #[instrument(skip_all, fields(hash = %cmd.hash_short()))]
    async fn finish_import(
        &self,
        global: &EntityGlobal<FsBackend>,
        cmd: ImportEntryMsg,
        mut tt: TempTag,
    ) {
        trace!("{cmd:?}");
        let ctx = self.ctx(global);
        ctx.load().await;
        let res = match finish_import_impl(&ctx, cmd.inner).await {
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

    #[instrument(skip_all, fields(hash = %cmd.hash_short()))]
    async fn observe(&self, global: &EntityGlobal<FsBackend>, cmd: ObserveMsg) {
        trace!("{cmd:?}");
        self.ctx(global).load().await;
        self.handle
            .observer()
            .forward(cmd.tx, BaoStorage::observed_bitfield)
            .await
            .ok();
    }

    #[instrument(skip_all, fields(hash = %cmd.hash_short()))]
    async fn export_ranges(&self, global: &EntityGlobal<FsBackend>, mut cmd: ExportRangesMsg) {
        trace!("{cmd:?}");
        let ctx = self.ctx(global);
        ctx.load().await;
        if let Err(cause) = export_ranges_impl(&ctx, cmd.inner, &mut cmd.tx).await {
            cmd.tx
                .send(ExportRangesItem::Error(cause.into()))
                .await
                .ok();
        }
    }

    #[instrument(skip_all, fields(hash = %cmd.hash_short()))]
    async fn export_bao(&self, global: &EntityGlobal<FsBackend>, mut cmd: ExportBaoMsg) {
        trace!("{cmd:?}");
        let ctx = self.ctx(global);
        ctx.load().await;
        if let Err(cause) = export_bao_impl(&ctx, cmd.inner, &mut cmd.tx).await {
            // if the entry is in state NonExisting, this will be an io error with
            // kind NotFound. So we must not wrap this somehow but pass it on directly.
            cmd.tx
                .send(bao_tree::io::EncodeError::Io(cause).into())
                .await
                .ok();
        }
    }

    #[instrument(skip_all, fields(hash = %cmd.hash_short()))]
    async fn export_path(&self, global: &EntityGlobal<FsBackend>, cmd: ExportPathMsg) {
        trace!("{cmd:?}");
        let ctx = self.ctx(global);
        ctx.load().await;
        let ExportPathMsg { inner, mut tx, .. } = cmd;
        if let Err(cause) = export_path_impl(&ctx, inner, &mut tx).await {
            tx.send(cause.into()).await.ok();
        }
    }

    async fn delete(&self, global: &EntityGlobal<FsBackend>) -> io::Result<()> {
        delete_entry(&self.ctx(global)).await
    }

    async fn freeze(
        &self,
        global: &EntityGlobal<FsBackend>,
        ack: tokio::sync::oneshot::Sender<()>,
        release: tokio::sync::watch::Receiver<()>,
    ) {
        freeze_entry(&self.ctx(global), ack, release).await
    }

    async fn delete_unreferenced(
        &self,
        global: &EntityGlobal<FsBackend>,
        parts: UnreferencedParts,
    ) {
        delete_unreferenced(&self.ctx(global), parts).await
    }

    #[instrument(skip_all, fields(hash = %self.id.fmt_short()))]
    async fn persist(&self, global: &EntityGlobal<FsBackend>) {
        let config = &global.backend.config;
        self.handle.retire(|state| {
            let hash = &self.id;
            // A freeze task dropped at shutdown leaves its freeze in place.
            let (Phase::ReadWrite(BaoStorage::Partial(fs))
            | Phase::ReadOnly(BaoStorage::Partial(fs))) = state.take()
            else {
                return;
            };
            let path = config.path.bitfield_path(hash);
            trace!("writing bitfield for hash {} to {}", hash, path.display());
            if let Err(cause) = fs.write_bitfield(&path, config.syncs_files()) {
                error!(
                    "failed to write bitfield for {} at {}: {:?}",
                    hash,
                    path.display(),
                    cause
                );
            }
        });
    }
}

impl BaoAccess for HashContext<'_> {
    fn id(&self) -> Hash {
        self.id
    }

    /// Write a batch and notify the db
    async fn write_batch(&self, batch: &[BaoContentItem], bitfield: &Bitfield) -> io::Result<()> {
        trace!("write_batch bitfield={:?} batch={}", bitfield, batch.len());
        // No await between this and the closure, so the closure never sees a
        // delete in progress.
        let write = wait_writable(self).await;
        let mut res = Ok(None);
        write.modify(|state| {
            // Imports load the entity first, and wait_writable leaves it
            // loaded and writable.
            let settled = match state.take() {
                Phase::ReadWrite(settled) => settled,
                other => {
                    debug_assert!(false, "write to an entry that is not writable");
                    *state = other;
                    return false;
                }
            };
            let (settled, update) = match settled.write_batch(batch, bitfield, self) {
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
            *state = Phase::ReadWrite(settled);
            true
        });
        drop(write);
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
    fn data_reader(&self) -> DataReader {
        DataReader(self.state.clone())
    }

    /// An AsyncSliceReader for the outboard file.
    ///
    /// The outboard file is used to validate the data file. It is not guaranteed
    /// to be complete.
    #[allow(refining_impl_trait_internal)]
    fn outboard_reader(&self) -> OutboardReader {
        OutboardReader(self.state.clone())
    }

    /// The most precise known total size of the data file.
    fn current_size(&self) -> io::Result<u64> {
        self.state.peek(|state| match state.try_settled()? {
            BaoStorage::Complete(mem) => Ok(mem.size()),
            BaoStorage::Partial(file) => file.current_size(),
            BaoStorage::Invalid => Err(io::Error::other("invalid storage")),
            BaoStorage::NonExisting => Err(io::ErrorKind::NotFound.into()),
        })
    }

    /// The most precise known total size of the data file.
    fn bitfield(&self) -> io::Result<Bitfield> {
        self.state.peek(|state| match state.try_settled()? {
            BaoStorage::Complete(mem) => Ok(mem.bitfield()),
            BaoStorage::Partial(file) => Ok(file.bitfield().clone()),
            BaoStorage::Invalid => Err(io::Error::other("invalid storage")),
            BaoStorage::NonExisting => Err(io::ErrorKind::NotFound.into()),
        })
    }
}

impl HashContext<'_> {
    pub fn options(&self) -> &StoreOptions {
        &self.global.options
    }

    /// Loads the entry from the metadata, unless it is loaded already.
    async fn load(&self) {
        self.state.load(|| open_entry(self)).await
    }

    /// The metadata of the entry.
    fn meta(&self) -> metadata::EntryMeta<'_> {
        self.global.db.entry(self.id)
    }
}

/// Syncs the contents of a file the store owns to disk.
fn sync_file(path: &Path) -> io::Result<()> {
    #[cfg(not(windows))]
    let file = fs::File::open(path)?;
    // windows only syncs handles with write access, and a copied
    // read-only source keeps its read-only attribute
    #[cfg(windows)]
    let file = {
        let mut perms = fs::metadata(path)?.permissions();
        if perms.readonly() {
            #[allow(clippy::permissions_set_readonly_false)]
            perms.set_readonly(false);
            fs::set_permissions(path, perms)?;
        }
        fs::OpenOptions::new().write(true).open(path)?
    };
    file.sync_data()
}

/// Syncs the directory at `path`, where the platform allows it.
fn sync_dir(path: &Path) -> io::Result<()> {
    #[cfg(unix)]
    fs::File::open(path)?.sync_all()?;
    #[cfg(not(unix))]
    let _ = path;
    Ok(())
}

async fn finish_import_impl(ctx: &HashContext<'_>, import_data: ImportEntry) -> io::Result<()> {
    let ImportEntry {
        source,
        hash,
        outboard,
        ..
    } = import_data;
    let options = ctx.options();
    let sync = ctx.config.syncs_files();
    match &source {
        ImportSource::Memory(data) => {
            debug_assert!(options.is_inlined_data(data.len() as u64));
        }
        ImportSource::External(_, _, size) => {
            debug_assert!(!options.is_inlined_data(*size));
        }
        ImportSource::TempFile(_, _, size) => {
            debug_assert!(!options.is_inlined_data(*size));
        }
    }
    ctx.load().await;
    let write = wait_writable(ctx).await;
    // if I do have an existing handle, I have to possibly deal with observers.
    // if I don't have an existing handle, there are 2 cases:
    //   the entry exists in the db, but we don't have a handle
    //   the entry does not exist at all.
    // convert the import source to a data location and drop the open files
    let data_location = match source {
        ImportSource::Memory(data) => DataLocation::Inline(data),
        ImportSource::External(path, _file, size) => DataLocation::External(vec![path], size),
        ImportSource::TempFile(path, _file, size) => {
            if sync {
                sync_file(path.as_ref())?;
            }
            // this will always work on any unix, but on windows there might be an issue if the target file is open!
            // possibly open with FILE_SHARE_DELETE on windows?
            let target = ctx.config.path.data_path(&hash);
            trace!(
                "moving temp file to owned data location: {path:?} -> {}",
                target.display()
            );
            if let Err(cause) = path.persist(&target) {
                error!(
                    "failed to move temp file to owned data location {}: {cause}",
                    target.display()
                );
            }
            DataLocation::Owned(size)
        }
    };
    let outboard_location = match outboard {
        MemOrFile::Mem(bytes) if bytes.is_empty() => OutboardLocation::NotNeeded,
        MemOrFile::Mem(bytes) => OutboardLocation::Inline(bytes),
        MemOrFile::File(path) => {
            if sync {
                sync_file(path.as_ref())?;
            }
            // the same caveat as above applies here
            let target = ctx.config.path.outboard_path(&hash);
            trace!(
                "moving temp file to owned outboard location: {path:?} -> {}",
                target.display()
            );
            if let Err(cause) = path.persist(&target) {
                error!(
                    "failed to move temp file to owned outboard location {}: {cause}",
                    target.display()
                );
            }
            OutboardLocation::Owned
        }
    };
    let moved = matches!(data_location, DataLocation::Owned(_))
        || matches!(outboard_location, OutboardLocation::Owned);
    if sync && moved {
        // A rename survives a crash only once its directory is synced.
        sync_dir(&ctx.config.path.data_path)?;
    }
    let data = match &data_location {
        DataLocation::Inline(data) => MemOrFile::Mem(data.clone()),
        DataLocation::Owned(size) => {
            let path = ctx.config.path.data_path(&hash);
            let file = fs::File::open(&path)?;
            MemOrFile::File(FixedSize::new(file, *size))
        }
        DataLocation::External(paths, size) => {
            let Some(path) = paths.iter().next() else {
                return Err(io::Error::other("no external data path"));
            };
            let file = fs::File::open(path)?;
            MemOrFile::File(FixedSize::new(file, *size))
        }
    };
    let outboard = match &outboard_location {
        OutboardLocation::NotNeeded => MemOrFile::empty(),
        OutboardLocation::Inline(data) => MemOrFile::Mem(data.clone()),
        OutboardLocation::Owned => {
            let path = ctx.config.path.outboard_path(&hash);
            let file = fs::File::open(&path)?;
            MemOrFile::File(file)
        }
    };
    complete(&write, data, outboard);
    drop(write);
    let state = EntryState::Complete {
        data_location,
        outboard_location,
    };
    ctx.meta().update_await(state).await?;
    Ok(())
}

async fn export_path_impl(
    ctx: &HashContext<'_>,
    cmd: ExportPathRequest,
    tx: &mut mpsc::Sender<ExportProgressItem>,
) -> api::Result<()> {
    let ExportPathRequest { mode, target, .. } = cmd;
    if !target.is_absolute() {
        return Err(api::Error::io(
            io::ErrorKind::InvalidInput,
            "path is not absolute",
        ));
    }
    if let Some(parent) = target.parent() {
        fs::create_dir_all(parent)?;
    }
    let state = ctx.meta().get().await?;
    let (data_location, outboard_location) = match state {
        Some(EntryState::Complete {
            data_location,
            outboard_location,
        }) => (data_location, outboard_location),
        Some(EntryState::Partial { .. }) => {
            return Err(api::Error::io(
                io::ErrorKind::InvalidInput,
                "cannot export partial entry",
            ));
        }
        None => {
            return Err(api::Error::io(io::ErrorKind::NotFound, "no entry found"));
        }
    };
    trace!("exporting {} to {}", cmd.hash.to_hex(), target.display());
    let (data, mut external) = match data_location {
        DataLocation::Inline(data) => (MemOrFile::Mem(data), vec![]),
        DataLocation::Owned(size) => (
            MemOrFile::File((ctx.config.path.data_path(&cmd.hash), size)),
            vec![],
        ),
        DataLocation::External(paths, size) => (
            MemOrFile::File((
                paths.first().cloned().ok_or_else(|| {
                    io::Error::new(io::ErrorKind::NotFound, "no external data path")
                })?,
                size,
            )),
            paths,
        ),
    };
    let size = match &data {
        MemOrFile::Mem(data) => data.len() as u64,
        MemOrFile::File((_, size)) => *size,
    };
    tx.send(ExportProgressItem::Size(size)).await?;
    match data {
        MemOrFile::Mem(data) => {
            let mut target = fs::File::create(&target)?;
            target.write_all(&data)?;
        }
        MemOrFile::File((source_path, size)) => match mode {
            ExportMode::Copy => {
                let res = reflink_or_copy_with_progress(&source_path, &target, size, tx).await?;
                trace!(
                    "exported {} to {}, {res:?}",
                    source_path.display(),
                    target.display()
                );
            }
            ExportMode::TryReference => {
                // Only waits: the copy below may take long, and must not hold
                // off a freeze or delete.
                drop(wait_writable(ctx).await);
                if !external.is_empty() {
                    // the file already exists externally, so we need to copy it.
                    // if the OS supports reflink, we might as well use that.
                    let res =
                        reflink_or_copy_with_progress(&source_path, &target, size, tx).await?;
                    trace!(
                        "exported {} also to {}, {res:?}",
                        source_path.display(),
                        target.display()
                    );
                    external.push(target);
                    external.sort();
                    external.dedup();
                    external.truncate(MAX_EXTERNAL_PATHS);
                } else {
                    // the file was previously owned, so we can just move it.
                    // if the target is on another file system or drive, we
                    // fall back to copy: EXDEV on unix, ERROR_NOT_SAME_DEVICE
                    // on windows.
                    match std::fs::rename(&source_path, &target) {
                        Ok(()) => {}
                        Err(cause) => {
                            if cause.kind() == io::ErrorKind::CrossesDevices {
                                reflink_or_copy_with_progress(&source_path, &target, size, tx)
                                    .await?;
                            } else {
                                return Err(cause.into());
                            }
                        }
                    }
                    external.push(target);
                };
                // The copy took a while, so the entry may have been deleted
                // meanwhile. Then it stays deleted, and this was just a copy.
                // No await between the check and sending the set.
                let write = wait_writable(ctx).await;
                let complete = ctx
                    .state
                    .peek(|state| matches!(state, Phase::ReadWrite(BaoStorage::Complete(_))));
                drop(write);
                if complete {
                    // After the commit, the owned data file comes back as an
                    // unreferenced part, and this entity deletes it unless its
                    // state still uses it.
                    // TODO: after a copy across file systems it does, so the
                    // owned file leaks.
                    ctx.meta()
                        .set(EntryState::Complete {
                            data_location: DataLocation::External(external, size),
                            outboard_location,
                        })
                        .await?;
                }
            }
        },
    }
    tx.send(ExportProgressItem::Done).await?;
    Ok(())
}

//! The metadata database
#![allow(clippy::result_large_err)]
use std::{
    collections::HashSet,
    io,
    ops::{Bound, Deref, DerefMut},
    path::PathBuf,
    sync::Arc,
    time::SystemTime,
};

use bao_tree::BaoTree;
use bytes::Bytes;
use irpc::channel::mpsc;
use n0_error::{anyerr, e, stack_error, AnyError, StdResultExt};
use redb::{Database, DatabaseError, ReadableDatabase, ReadableTable};
use tokio::pin;

use crate::{
    api::{
        self,
        blobs::BlobStatus,
        proto::{
            BlobDeleteRequest, BlobStatusMsg, BlobStatusRequest, ClearProtectedMsg,
            CreateTagRequest, DeleteBlobsMsg, DeleteTagsRequest, ListBlobsItem, ListBlobsMsg,
            ListRequest, ListTagsItem, ListTagsMsg, ListTagsRequest, RenameTagRequest,
            SetTagRequest, ShutdownMsg, SyncDbMsg,
        },
        tags::TagInfo,
        Tag,
    },
    util::channel::oneshot,
    Hash,
};
mod proto;
mod recovery;
mod run;
pub use proto::*;
pub(crate) mod tables;
use run::{LastShutdown, Run};
use tables::{ReadOnlyTables, ReadableTables, Tables, META_TABLE};
use tracing::{debug, error, info, info_span, trace, warn, Span};

use super::{
    boot_id,
    entry_state::{DataLocation, EntryState, OutboardLocation},
    file_deletes::{delete_files, unreferenced_parts, BaoFilePart, LiveEntities},
    options::{BatchOptions, PathOptions},
    util::PeekableReceiver,
};
use crate::store::IROH_BLOCK_SIZE;

/// Error type for message handler functions of the redb actor.
///
/// What can go wrong are various things with redb, as well as io errors related
/// to files other than redb.
#[allow(missing_docs)]
#[non_exhaustive]
#[stack_error(derive, add_meta, from_sources)]
pub enum ActorError {
    #[error("table error: {source}")]
    Table {
        #[error(std_err)]
        source: redb::TableError,
    },
    #[error("database error: {source}")]
    Database {
        #[error(std_err)]
        source: redb::DatabaseError,
    },
    #[error("transaction error: {source}")]
    Transaction {
        #[error(std_err)]
        source: redb::TransactionError,
    },
    #[error("commit error: {source}")]
    Commit {
        #[error(std_err)]
        source: redb::CommitError,
    },
    #[error("storage error: {source}")]
    Storage {
        #[error(std_err)]
        source: redb::StorageError,
    },
    #[error("inconsistent database state: {msg}")]
    Inconsistent { msg: String },
    #[error(transparent)]
    Other { source: AnyError },
}

impl From<ActorError> for io::Error {
    fn from(e: ActorError) -> Self {
        io::Error::other(e)
    }
}

impl ActorError {
    pub(super) fn inconsistent(msg: String) -> Self {
        e!(ActorError::Inconsistent { msg })
    }
}

pub type ActorResult<T> = std::result::Result<T, ActorError>;

#[derive(Debug, Clone)]
pub struct Db {
    sender: tokio::sync::mpsc::Sender<Command>,
}

impl Db {
    pub fn new(sender: tokio::sync::mpsc::Sender<Command>) -> Self {
        Self { sender }
    }

    pub async fn snapshot(&self, span: tracing::Span) -> io::Result<ReadOnlyTables> {
        let (tx, rx) = tokio::sync::oneshot::channel();
        self.sender
            .send(Snapshot { tx, span }.into())
            .await
            .map_err(|_| io::Error::other("send snapshot"))?;
        rx.await.map_err(|_| io::Error::other("receive snapshot"))
    }

    pub async fn update_await(&self, hash: Hash, state: EntryState<Bytes>) -> io::Result<()> {
        let (tx, rx) = oneshot::channel();
        self.sender
            .send(
                Update {
                    hash,
                    state,
                    tx: Some(tx),
                    span: tracing::Span::current(),
                }
                .into(),
            )
            .await
            .map_err(|_| io::Error::other("send update"))?;
        rx.await
            .map_err(|_e| io::Error::other("receive update"))??;
        Ok(())
    }

    /// Update the entry state for a hash, without awaiting completion.
    pub async fn update(&self, hash: Hash, state: EntryState<Bytes>) -> io::Result<()> {
        self.sender
            .send(
                Update {
                    hash,
                    state,
                    tx: None,
                    span: Span::current(),
                }
                .into(),
            )
            .await
            .map_err(|_| io::Error::other("send update"))
    }

    /// Set the entry state and await completion.
    pub async fn set(&self, hash: Hash, entry_state: EntryState<Bytes>) -> io::Result<()> {
        let (tx, rx) = oneshot::channel();
        self.sender
            .send(
                Set {
                    hash,
                    state: entry_state,
                    tx,
                    span: Span::current(),
                }
                .into(),
            )
            .await
            .map_err(|_| io::Error::other("send update"))?;
        rx.await.map_err(|_| io::Error::other("receive update"))??;
        Ok(())
    }

    /// Get the entry state for a hash, if any.
    pub async fn get(&self, hash: Hash) -> io::Result<Option<EntryState<Bytes>>> {
        Ok(self.get_impl(hash, GetMode::Lookup).await?.0)
    }

    /// Get the entry state for a hash, and whether this marked the hash live.
    pub async fn load(
        &self,
        hash: Hash,
        mode: GetMode,
    ) -> io::Result<(Option<EntryState<Bytes>>, bool)> {
        self.get_impl(hash, mode).await
    }

    /// Mark the hash as no longer live. This does not wait for the db to process it.
    pub async fn inactive(&self, hash: Hash) -> io::Result<()> {
        let span = Span::current();
        self.sender
            .send(Inactive { hash, span }.into())
            .await
            .map_err(|_| io::Error::other("send inactive"))
    }

    async fn get_impl(
        &self,
        hash: Hash,
        mode: GetMode,
    ) -> io::Result<(Option<EntryState<Bytes>>, bool)> {
        let (tx, rx) = oneshot::channel();
        self.sender
            .send(
                Get {
                    hash,
                    mode,
                    tx,
                    span: tracing::Span::current(),
                }
                .into(),
            )
            .await
            .map_err(|_| io::Error::other("send get"))?;
        let res = rx.await.map_err(|_| io::Error::other("receive get"))?;
        Ok((res.state?, res.live))
    }

    /// A view of this database scoped to a single entry.
    pub fn entry(&self, hash: Hash) -> EntryMeta<'_> {
        EntryMeta { db: self, hash }
    }

    /// Send a command. This exists so the main actor can directly forward commands.
    ///
    /// This will fail only if the database actor is dead. In that case the main
    /// actor should probably also shut down.
    pub async fn send(&self, cmd: Command) -> io::Result<()> {
        self.sender
            .send(cmd)
            .await
            .map_err(|_e| io::Error::other("actor down"))?;
        Ok(())
    }
}

/// The metadata of a single entry.
///
/// This is all an entity gets to see of the database.
#[derive(Debug, Clone, Copy)]
pub struct EntryMeta<'a> {
    db: &'a Db,
    hash: Hash,
}

impl EntryMeta<'_> {
    /// Get the entry state for the entity, and whether this marked it live.
    ///
    /// The entity must not touch any files of the entry before a load marked
    /// it live: a [`GetMode::Read`] load only does that for a partial entry.
    pub async fn load(&self, mode: GetMode) -> io::Result<(Option<EntryState<Bytes>>, bool)> {
        self.db.load(self.hash, mode).await
    }

    /// Mark the entity as no longer live.
    ///
    /// Only an entity that a load marked live sends this, and it must not
    /// touch any files of the entry after calling this.
    pub async fn inactive(&self) -> io::Result<()> {
        self.db.inactive(self.hash).await
    }

    /// Get the entry state.
    pub async fn get(&self) -> io::Result<Option<EntryState<Bytes>>> {
        self.db.get(self.hash).await
    }

    /// Replace the entry state, and wait for completion.
    pub async fn set(&self, state: EntryState<Bytes>) -> io::Result<()> {
        self.db.set(self.hash, state).await
    }

    /// Merge into the entry state, without waiting for completion.
    pub async fn update(&self, state: EntryState<Bytes>) -> io::Result<()> {
        self.db.update(self.hash, state).await
    }

    /// Merge into the entry state, and wait for completion.
    pub async fn update_await(&self, state: EntryState<Bytes>) -> io::Result<()> {
        self.db.update_await(self.hash, state).await
    }
}

fn handle_get(cmd: Get, tables: &impl ReadableTables, live: &mut LiveEntities) -> ActorResult<()> {
    trace!("{cmd:?}");
    let Get { hash, mode, tx, .. } = cmd;
    let Some(entry) = tables.blobs().get(hash)? else {
        let marked = mode == GetMode::Write;
        if marked {
            live.load(hash);
        }
        tx.send(GetResult {
            state: Ok(None),
            live: marked,
        });
        return Ok(());
    };
    let entry = entry.value();
    let entry = match entry {
        EntryState::Complete {
            data_location,
            outboard_location,
        } => {
            let data_location = load_data(tables, data_location, &hash)?;
            let outboard_location = load_outboard(tables, outboard_location, &hash)?;
            EntryState::Complete {
                data_location,
                outboard_location,
            }
        }
        EntryState::Partial { size } => EntryState::Partial { size },
    };
    // Marked only once the get succeeded, so an entity that sees an error
    // never leaves its hash live without knowing it has to send Inactive.
    let marked = match mode {
        GetMode::Lookup => false,
        GetMode::Read => matches!(entry, EntryState::Partial { .. }),
        GetMode::Write => true,
    };
    if marked {
        live.load(hash);
    }
    tx.send(GetResult {
        state: Ok(Some(entry)),
        live: marked,
    });
    Ok(())
}

fn handle_dump(cmd: Dump, tables: &impl ReadableTables) -> ActorResult<()> {
    trace!("{cmd:?}");
    trace!("dumping database");
    for e in tables
        .blobs()
        .iter()
        .map_err(|e| e!(ActorError::Storage, e))?
    {
        let (k, v) = e.map_err(|e| e!(ActorError::Storage, e))?;
        let k = k.value();
        let v = v.value();
        println!("blobs: {} -> {:?}", k.to_hex(), v);
    }
    for e in tables
        .tags()
        .iter()
        .map_err(|e| e!(ActorError::Storage, e))?
    {
        let (k, v) = e.map_err(|e| e!(ActorError::Storage, e))?;
        let k = k.value();
        let v = v.value();
        println!("tags: {k} -> {v:?}");
    }
    for e in tables
        .inline_data()
        .iter()
        .map_err(|e| e!(ActorError::Storage, e))?
    {
        let (k, v) = e.map_err(|e| e!(ActorError::Storage, e))?;
        let k = k.value();
        let v = v.value();
        println!("inline_data: {} -> {:?}", k.to_hex(), v.len());
    }
    for e in tables
        .inline_outboard()
        .iter()
        .map_err(|e| e!(ActorError::Storage, e))?
    {
        let (k, v) = e.map_err(|e| e!(ActorError::Storage, e))?;
        let k = k.value();
        let v = v.value();
        println!("inline_outboard: {} -> {:?}", k.to_hex(), v.len());
    }
    cmd.tx.send(Ok(()));
    Ok(())
}

async fn handle_clear_protected(
    cmd: ClearProtectedMsg,
    protected: &mut HashSet<Hash>,
) -> ActorResult<()> {
    trace!("{cmd:?}");
    protected.clear();
    cmd.tx.send(Ok(())).await.ok();
    Ok(())
}

async fn handle_get_blob_status(
    msg: BlobStatusMsg,
    tables: &impl ReadableTables,
) -> ActorResult<()> {
    trace!("{msg:?}");
    let BlobStatusMsg {
        inner: BlobStatusRequest { hash },
        tx,
        ..
    } = msg;
    let res = match tables
        .blobs()
        .get(hash)
        .map_err(|e| e!(ActorError::Storage, e))?
    {
        Some(entry) => match entry.value() {
            EntryState::Complete { data_location, .. } => match data_location {
                DataLocation::Inline(_) => {
                    let Some(data) = tables
                        .inline_data()
                        .get(hash)
                        .map_err(|e| e!(ActorError::Storage, e))?
                    else {
                        return Err(ActorError::inconsistent(format!(
                            "inconsistent database state: {} not found",
                            hash.to_hex()
                        )));
                    };
                    BlobStatus::Complete {
                        size: data.value().len() as u64,
                    }
                }
                DataLocation::Owned(size) => BlobStatus::Complete { size },
                DataLocation::External(_, size) => BlobStatus::Complete { size },
            },
            EntryState::Partial { size } => BlobStatus::Partial { size },
        },
        None => BlobStatus::NotFound,
    };
    tx.send(res).await.ok();
    Ok(())
}

/// Streams a snapshot without making the metadata actor wait for the reader.
pub async fn list_tags(snapshot: ReadOnlyTables, cmd: ListTagsMsg) {
    let ListTagsMsg { mut tx, inner, .. } = cmd;
    if let Err(cause) = list_tags_impl(snapshot, inner, &mut tx).await {
        tx.send(ListTagsItem::Error(cause)).await.ok();
    }
}

async fn list_tags_impl(
    snapshot: ReadOnlyTables,
    request: ListTagsRequest,
    tx: &mut mpsc::Sender<ListTagsItem>,
) -> api::Result<()> {
    let ListTagsRequest {
        from,
        to,
        raw,
        hash_seq,
    } = request;
    let from = from.map(Bound::Included).unwrap_or(Bound::Unbounded);
    let to = to.map(Bound::Excluded).unwrap_or(Bound::Unbounded);
    for item in snapshot
        .tags()
        .range((from, to))
        .map_err(api_error_from_storage_error)?
    {
        let (k, v) = item.map_err(api_error_from_storage_error)?;
        let v = v.value();
        if raw && v.format.is_raw() || hash_seq && v.format.is_hash_seq() {
            let info = TagInfo {
                name: k.value(),
                hash: v.hash,
                format: v.format,
            };
            if tx.send(ListTagsItem::Tag(info)).await.is_err() {
                return Ok(());
            }
        }
    }
    tx.send(ListTagsItem::Done).await.ok();
    Ok(())
}

fn handle_update(
    cmd: Update,
    bookkeeping: &mut Bookkeeping,
    tables: &mut Tables,
) -> ActorResult<()> {
    trace!("{cmd:?}");
    let Update {
        hash, state, tx, ..
    } = cmd;
    bookkeeping.written(hash);
    trace!("updating hash {} to {}", hash.to_hex(), state.fmt_short());
    let old_entry_opt = tables.blobs.get(hash)?.map(|e| e.value());
    let (state, data, outboard): (_, Option<Bytes>, Option<Bytes>) = match state {
        EntryState::Complete {
            data_location,
            outboard_location,
        } => {
            let (data_location, data) = data_location.split_inline_data();
            let (outboard_location, outboard) = outboard_location.split_inline_data();
            (
                EntryState::Complete {
                    data_location,
                    outboard_location,
                },
                data,
                outboard,
            )
        }
        EntryState::Partial { size } => (EntryState::Partial { size }, None, None),
    };
    let state = match old_entry_opt {
        Some(old) => EntryState::union(old, state)?,
        None => state,
    };
    tables
        .blobs
        .insert(hash, state)
        .map_err(|e| e!(ActorError::Storage, e))?;
    if let Some(data) = data {
        tables
            .inline_data
            .insert(hash, data.as_ref())
            .map_err(|e| e!(ActorError::Storage, e))?;
    }
    if let Some(outboard) = outboard {
        tables
            .inline_outboard
            .insert(hash, outboard.as_ref())
            .map_err(|e| e!(ActorError::Storage, e))?;
    }
    if let Some(tx) = tx {
        tx.send(Ok(()));
    }
    Ok(())
}

fn handle_set(cmd: Set, bookkeeping: &mut Bookkeeping, tables: &mut Tables) -> ActorResult<()> {
    trace!("{cmd:?}");
    let Set {
        state, hash, tx, ..
    } = cmd;
    bookkeeping.written(hash);
    let (state, data, outboard): (_, Option<Bytes>, Option<Bytes>) = match state {
        EntryState::Complete {
            data_location,
            outboard_location,
        } => {
            let (data_location, data) = data_location.split_inline_data();
            let (outboard_location, outboard) = outboard_location.split_inline_data();
            (
                EntryState::Complete {
                    data_location,
                    outboard_location,
                },
                data,
                outboard,
            )
        }
        EntryState::Partial { size } => (EntryState::Partial { size }, None, None),
    };
    tables
        .blobs
        .insert(hash, &state)
        .map_err(|e| e!(ActorError::Storage, e))?;
    if let Some(data) = data {
        tables
            .inline_data
            .insert(hash, data.as_ref())
            .map_err(|e| e!(ActorError::Storage, e))?;
    }
    if let Some(outboard) = outboard {
        tables
            .inline_outboard
            .insert(hash, outboard.as_ref())
            .map_err(|e| e!(ActorError::Storage, e))?;
    }
    tx.send(Ok(()));
    Ok(())
}

/// In-memory bookkeeping of the metadata actor, next to the database.
#[derive(Debug, Default)]
struct Bookkeeping {
    /// Hashes written since the last `ClearProtected`, which gc must not delete.
    protected: HashSet<Hash>,
    live: LiveEntities,
    /// Hashes whose entry was written or removed in the current write batch.
    written: HashSet<Hash>,
    /// Hashes whose file deletes wait until their entity is no longer live.
    parked: HashSet<Hash>,
}

impl Bookkeeping {
    /// Note a write to the entry of a hash.
    fn written(&mut self, hash: Hash) {
        self.protected.insert(hash);
        self.written.insert(hash);
    }

    /// Decide which owned files to delete after a batch.
    ///
    /// The candidates are the hashes written in the batch, and those parked
    /// earlier. A candidate whose entity is live at the end of the batch is
    /// parked, since the entity may have written files the metadata does not
    /// reference yet. All others lose the owned files their state at the end of
    /// the batch does not reference. This may name files that do not exist.
    fn plan_file_deletes(
        &mut self,
        tables: &impl ReadableTables,
    ) -> ActorResult<Vec<(Hash, Vec<BaoFilePart>)>> {
        let mut candidates = std::mem::take(&mut self.parked);
        candidates.extend(self.written.drain());
        let mut res = Vec::new();
        for hash in candidates {
            if self.live.is_live(&hash) {
                self.parked.insert(hash);
                continue;
            }
            let state = tables.blobs().get(hash)?.map(|e| e.value());
            let parts = unreferenced_parts(state.as_ref());
            if !parts.is_empty() {
                res.push((hash, parts));
            }
        }
        Ok(res)
    }
}

#[derive(Clone, Copy)]
enum TxnNum {
    Read(u64),
    Write(u64),
    TopLevel(u64),
}

impl std::fmt::Debug for TxnNum {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            TxnNum::Read(n) => write!(f, "r{n}"),
            TxnNum::Write(n) => write!(f, "w{n}"),
            TxnNum::TopLevel(n) => write!(f, "t{n}"),
        }
    }
}

#[derive(Debug)]
pub struct Actor {
    db: redb::Database,
    cmds: PeekableReceiver<Command>,
    paths: Arc<PathOptions>,
    options: BatchOptions,
    state: Bookkeeping,
}

impl Actor {
    pub fn new(
        db_path: PathBuf,
        cmds: tokio::sync::mpsc::Receiver<Command>,
        paths: Arc<PathOptions>,
        options: BatchOptions,
        syncs_files: bool,
    ) -> Result<Self, ActorError> {
        debug!("creating or opening meta database at {}", db_path.display());
        let db = match redb::Database::create(db_path) {
            Ok(db) => db,
            Err(DatabaseError::UpgradeRequired(v)) => {
                return Err(anyerr!(
                    "migration from redb v{v} no longer supported; \
                     upgrade with an older redb version first"
                )
                .into());
            }
            Err(err) => return Err(err.into()),
        };
        let tx = db.begin_write()?;
        let mut tables = Tables::new(&tx)?;
        let mut meta = tx.open_table(META_TABLE)?;
        let run = Run {
            boot_id: boot_id::current(),
            syncs_files,
        };
        // Recovery commits together with the new run, so if the process dies
        // during it, the next open still finds the old one.
        match run::record(&mut meta, &run)? {
            LastShutdown::Clean => {}
            LastShutdown::ProcessCrash => info!(
                "the blob store was not shut down cleanly, but the machine kept running, so \
                 nothing that was written got lost"
            ),
            LastShutdown::MaybeOsCrash { synced: true, .. } => info!(
                "the blob store was not shut down cleanly, but its last run synced its files, \
                 so an OS crash cannot have damaged entries"
            ),
            LastShutdown::MaybeOsCrash { synced: false } => {
                warn!(
                    "the blob store was not shut down cleanly, and the machine may have gone \
                     down too, so entries are checked again in case an OS crash lost data"
                );
                report(&recovery::recover(&mut tables, &paths)?);
            }
        }
        // A PowerLoss run must also cover writes inherited from earlier runs.
        // Sync after recovery so its writes and bitfield deletions are included,
        // and before committing the new run so a failed sync leaves the old one.
        if syncs_files {
            super::sync_fs::sync(&paths.data_path)
                .std_context("syncing the data filesystem on open")?;
        }
        drop(meta);
        drop(tables);
        tx.commit()?;
        let cmds = PeekableReceiver::new(cmds);
        Ok(Self {
            db,
            cmds,
            paths,
            options,
            state: Default::default(),
        })
    }

    async fn handle_readonly(
        state: &mut Bookkeeping,
        tables: &impl ReadableTables,
        cmd: ReadOnlyCommand,
        op: TxnNum,
    ) -> ActorResult<()> {
        let span = info_span!(
            parent: cmd.parent_span(),
            "tx",
            op = tracing::field::debug(op),
        );
        let _guard = span.enter();
        match cmd {
            ReadOnlyCommand::Get(cmd) => handle_get(cmd, tables, &mut state.live),
            ReadOnlyCommand::Inactive(cmd) => {
                trace!("{cmd:?}");
                state.live.inactive(cmd.hash);
                Ok(())
            }
            ReadOnlyCommand::Dump(cmd) => handle_dump(cmd, tables),
            ReadOnlyCommand::ClearProtected(cmd) => {
                handle_clear_protected(cmd, &mut state.protected).await
            }
            ReadOnlyCommand::GetBlobStatus(cmd) => handle_get_blob_status(cmd, tables).await,
        }
    }

    async fn delete(
        state: &mut Bookkeeping,
        tables: &mut Tables<'_>,
        cmd: DeleteBlobsMsg,
    ) -> ActorResult<()> {
        let DeleteBlobsMsg {
            inner: BlobDeleteRequest { hashes, force },
            ..
        } = cmd;
        for hash in hashes {
            if !force && state.protected.contains(&hash) {
                trace!("delete {hash}: skip (protected)");
                continue;
            }
            if let Some(entry) = tables.blobs.remove(hash)? {
                trace!("delete {hash}");
                // owned files are deleted after commit, see plan_file_deletes
                state.written.insert(hash);
                if let EntryState::Complete {
                    data_location,
                    outboard_location,
                } = entry.value()
                {
                    if let DataLocation::Inline(_) = data_location {
                        tables.inline_data.remove(hash)?;
                    }
                    if let OutboardLocation::Inline(_) = outboard_location {
                        tables.inline_outboard.remove(hash)?;
                    }
                }
            }
        }
        cmd.tx.send(Ok(())).await.ok();
        Ok(())
    }

    async fn set_tag(tables: &mut Tables<'_>, cmd: SetTagMsg) -> ActorResult<()> {
        trace!("{cmd:?}");
        let SetTagMsg {
            inner: SetTagRequest { name: tag, value },
            tx,
            ..
        } = cmd;
        let res = tables
            .tags
            .insert(tag, value)
            .map_err(api_error_from_storage_error)
            .map(|_| ());
        tx.send(res).await.ok();
        Ok(())
    }

    async fn create_tag(tables: &mut Tables<'_>, cmd: CreateTagMsg) -> ActorResult<()> {
        trace!("{cmd:?}");
        let CreateTagMsg {
            inner: CreateTagRequest { value },
            tx,
            ..
        } = cmd;
        let tag = {
            let tag = Tag::auto(SystemTime::now(), |x| {
                matches!(tables.tags.get(Tag(Bytes::copy_from_slice(x))), Ok(Some(_)))
            });
            tables.tags.insert(tag.clone(), value)?;
            tag
        };
        tx.send(Ok(tag.clone())).await.ok();
        Ok(())
    }

    async fn delete_tags(tables: &mut Tables<'_>, cmd: DeleteTagsMsg) -> ActorResult<()> {
        trace!("{cmd:?}");
        let DeleteTagsMsg {
            inner: DeleteTagsRequest { from, to },
            tx,
            ..
        } = cmd;
        let from = from.map(Bound::Included).unwrap_or(Bound::Unbounded);
        let to = to.map(Bound::Excluded).unwrap_or(Bound::Unbounded);
        let removing = tables.tags.extract_from_if((from, to), |_, _| true)?;
        // drain the iterator to actually remove the tags
        let mut deleted = 0;
        for res in removing {
            res?;
            deleted += 1;
        }
        tx.send(Ok(deleted)).await.ok();
        Ok(())
    }

    async fn rename_tag(tables: &mut Tables<'_>, cmd: RenameTagMsg) -> ActorResult<()> {
        trace!("{cmd:?}");
        let RenameTagMsg {
            inner: RenameTagRequest { from, to },
            tx,
            ..
        } = cmd;
        let value = match tables.tags.remove(from)? {
            Some(value) => value.value(),
            None => {
                tx.send(Err(api::Error::io(
                    io::ErrorKind::NotFound,
                    "tag not found",
                )))
                .await
                .ok();
                return Ok(());
            }
        };
        tables.tags.insert(to, value)?;
        tx.send(Ok(())).await.ok();
        Ok(())
    }

    async fn handle_readwrite(
        state: &mut Bookkeeping,
        tables: &mut Tables<'_>,
        cmd: ReadWriteCommand,
        op: TxnNum,
    ) -> ActorResult<()> {
        let span = info_span!(
            parent: cmd.parent_span(),
            "tx",
            op = tracing::field::debug(op),
        );
        let _guard = span.enter();
        match cmd {
            ReadWriteCommand::Update(cmd) => handle_update(cmd, state, tables),
            ReadWriteCommand::Set(cmd) => handle_set(cmd, state, tables),
            ReadWriteCommand::DeleteBlobw(cmd) => Self::delete(state, tables, cmd).await,
            ReadWriteCommand::SetTag(cmd) => Self::set_tag(tables, cmd).await,
            ReadWriteCommand::CreateTag(cmd) => Self::create_tag(tables, cmd).await,
            ReadWriteCommand::DeleteTags(cmd) => Self::delete_tags(tables, cmd).await,
            ReadWriteCommand::RenameTag(cmd) => Self::rename_tag(tables, cmd).await,
        }
    }

    async fn handle_non_toplevel(
        state: &mut Bookkeeping,
        tables: &mut Tables<'_>,
        cmd: NonTopLevelCommand,
        op: TxnNum,
    ) -> ActorResult<()> {
        match cmd {
            NonTopLevelCommand::ReadOnly(cmd) => {
                Self::handle_readonly(state, tables, cmd, op).await
            }
            NonTopLevelCommand::ReadWrite(cmd) => {
                Self::handle_readwrite(state, tables, cmd, op).await
            }
        }
    }

    async fn sync_db(_db: &mut Database, cmd: SyncDbMsg) -> ActorResult<()> {
        trace!("{cmd:?}");
        let SyncDbMsg { tx, .. } = cmd;
        // nothing to do here, since for a toplevel cmd we are outside a write transaction
        tx.send(Ok(())).await.ok();
        Ok(())
    }

    async fn handle_toplevel(
        db: &mut Database,
        cmd: TopLevelCommand,
        op: TxnNum,
    ) -> ActorResult<Option<ShutdownMsg>> {
        let span = info_span!(
            parent: cmd.parent_span(),
            "tx",
            op = tracing::field::debug(op),
        );
        let _guard = span.enter();
        Ok(match cmd {
            TopLevelCommand::SyncDb(cmd) => {
                Self::sync_db(db, cmd).await?;
                None
            }
            TopLevelCommand::Shutdown(cmd) => {
                trace!("{cmd:?}");
                Self::clear_run(db)?;
                Some(cmd)
            }
            TopLevelCommand::Snapshot(cmd) => {
                trace!("{cmd:?}");
                let txn = db
                    .begin_read()
                    .map_err(|e| e!(ActorError::Transaction, e))?;
                let snapshot = ReadOnlyTables::new(&txn).map_err(|e| e!(ActorError::Table, e))?;
                cmd.tx.send(snapshot).ok();
                None
            }
        })
    }

    /// Removes the run record, so the next open knows the store was shut down cleanly.
    fn clear_run(db: &Database) -> ActorResult<()> {
        let tx = db
            .begin_write()
            .map_err(|e| e!(ActorError::Transaction, e))?;
        let mut meta = tx
            .open_table(META_TABLE)
            .map_err(|e| e!(ActorError::Table, e))?;
        run::clear(&mut meta).map_err(|e| e!(ActorError::Storage, e))?;
        drop(meta);
        tx.commit().map_err(|e| e!(ActorError::Commit, e))?;
        Ok(())
    }

    pub async fn run(self) {
        if let Err(error) = self.run_inner().await {
            // Without this actor every store operation fails, so keep the cause.
            error!("metadata db actor stopped: {error:#}");
        }
    }

    async fn run_inner(mut self) -> ActorResult<()> {
        let mut db = DbWrapper::from(self.db);
        let options = &self.options;
        let mut op = 0u64;
        let shutdown = loop {
            op += 1;
            let Some(cmd) = self.cmds.recv().await else {
                break None;
            };
            match cmd {
                Command::TopLevel(cmd) => {
                    let op = TxnNum::TopLevel(op);
                    if let Some(shutdown) = Self::handle_toplevel(&mut db, cmd, op).await? {
                        break Some(shutdown);
                    }
                }
                Command::ReadOnly(cmd) => {
                    let op = TxnNum::Read(op);
                    self.cmds.push_back(cmd.into()).ok();
                    let tx = db
                        .begin_read()
                        .map_err(|e| e!(ActorError::Transaction, e))?;
                    let tables = ReadOnlyTables::new(&tx).map_err(|e| e!(ActorError::Table, e))?;
                    let timeout = n0_future::time::sleep(self.options.max_read_duration);
                    pin!(timeout);
                    let mut n = 0;
                    while let Some(cmd) = self.cmds.extract(Command::read_only, &mut timeout).await
                    {
                        Self::handle_readonly(&mut self.state, &tables, cmd, op).await?;
                        n += 1;
                        if n >= options.max_read_batch {
                            break;
                        }
                    }
                    // Everything is committed, so parked hashes whose entity is
                    // no longer live can be decided now.
                    if !self.state.parked.is_empty() {
                        let deletes = self.state.plan_file_deletes(&tables)?;
                        for (hash, parts) in deletes {
                            delete_files(&self.paths, &hash, &parts);
                        }
                    }
                }
                Command::ReadWrite(cmd) => {
                    let op = TxnNum::Write(op);
                    self.cmds.push_back(cmd.into()).ok();
                    let tx = db
                        .begin_write()
                        .map_err(|e| e!(ActorError::Transaction, e))?;
                    let mut tables = Tables::new(&tx).map_err(|e| e!(ActorError::Table, e))?;
                    let timeout = n0_future::time::sleep(self.options.max_write_duration);
                    pin!(timeout);
                    let mut n = 0;
                    while let Some(cmd) = self
                        .cmds
                        .extract(Command::non_top_level, &mut timeout)
                        .await
                    {
                        Self::handle_non_toplevel(&mut self.state, &mut tables, cmd, op).await?;
                        n += 1;
                        if n >= options.max_write_batch {
                            break;
                        }
                    }
                    let deletes = self.state.plan_file_deletes(&tables)?;
                    drop(tables);
                    tx.commit().map_err(|e| e!(ActorError::Commit, e))?;
                    // Delete only after the commit, and before handling the next
                    // command, so a load queued behind this batch sees the files gone.
                    for (hash, parts) in deletes {
                        delete_files(&self.paths, &hash, &parts);
                    }
                }
            }
        };
        if let Some(shutdown) = shutdown {
            drop(db);
            shutdown.tx.send(()).await.ok();
        }
        Ok(())
    }
}

/// Logs what recovery did.
fn report(recovered: &recovery::Recovered) {
    info!(
        "{} complete and {} partial entries are checked against their files when next loaded",
        recovered.complete, recovered.partial
    );
    if recovered.external > 0 {
        warn!(
            "{} entries with external data are not checked, since the data is not ours",
            recovered.external
        );
    }
}

/// Convert a redb StorageError into an api::Error
///
/// This can't be a From instance because that would require exposing redb::StorageError in the public API.
fn api_error_from_storage_error(e: redb::StorageError) -> api::Error {
    api::Error::Io(io::Error::other(e))
}

#[derive(Debug)]
struct DbWrapper(Option<Database>);

impl Deref for DbWrapper {
    type Target = Database;

    fn deref(&self) -> &Self::Target {
        self.0.as_ref().expect("database not open")
    }
}

impl DerefMut for DbWrapper {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.0.as_mut().expect("database not open")
    }
}

impl From<Database> for DbWrapper {
    fn from(db: Database) -> Self {
        Self(Some(db))
    }
}

impl Drop for DbWrapper {
    fn drop(&mut self) {
        if let Some(db) = self.0.take() {
            debug!("closing database");
            drop(db);
            debug!("database closed");
        }
    }
}

fn load_data(
    tables: &impl ReadableTables,
    location: DataLocation<(), u64>,
    hash: &Hash,
) -> ActorResult<DataLocation<Bytes, u64>> {
    Ok(match location {
        DataLocation::Inline(()) => {
            let Some(data) = tables
                .inline_data()
                .get(hash)
                .map_err(|e| e!(ActorError::Storage, e))?
            else {
                return Err(ActorError::inconsistent(format!(
                    "inconsistent database state: {} should have inline data but does not",
                    hash.to_hex()
                )));
            };
            DataLocation::Inline(Bytes::copy_from_slice(data.value()))
        }
        DataLocation::Owned(data_size) => DataLocation::Owned(data_size),
        DataLocation::External(paths, data_size) => DataLocation::External(paths, data_size),
    })
}

fn load_outboard(
    tables: &impl ReadableTables,
    location: OutboardLocation,
    hash: &Hash,
) -> ActorResult<OutboardLocation<Bytes>> {
    Ok(match location {
        OutboardLocation::NotNeeded => OutboardLocation::NotNeeded,
        OutboardLocation::Inline(_) => {
            let Some(outboard) = tables
                .inline_outboard()
                .get(hash)
                .map_err(|e| e!(ActorError::Storage, e))?
            else {
                return Err(ActorError::inconsistent(format!(
                    "inconsistent database state: {} should have inline outboard but does not",
                    hash.to_hex()
                )));
            };
            OutboardLocation::Inline(Bytes::copy_from_slice(outboard.value()))
        }
        OutboardLocation::Owned => OutboardLocation::Owned,
    })
}

pub(crate) fn raw_outboard_size(size: u64) -> u64 {
    BaoTree::new(size, IROH_BLOCK_SIZE).outboard_size()
}

pub async fn list_blobs(snapshot: ReadOnlyTables, cmd: ListBlobsMsg) {
    let ListBlobsMsg { mut tx, inner, .. } = cmd;
    if let Err(cause) = list_blobs_impl(snapshot, inner, &mut tx).await {
        error!("error listing blobs: {}", cause);
        tx.send(ListBlobsItem::Error(cause)).await.ok();
    }
}

async fn list_blobs_impl(
    snapshot: ReadOnlyTables,
    _cmd: ListRequest,
    tx: &mut mpsc::Sender<ListBlobsItem>,
) -> api::Result<()> {
    for item in snapshot
        .blobs
        .iter()
        .map_err(api_error_from_storage_error)?
    {
        let (k, _) = item.map_err(api_error_from_storage_error)?;
        if tx.send(ListBlobsItem::Hash(k.value())).await.is_err() {
            return Ok(());
        }
    }
    tx.send(ListBlobsItem::Done).await.ok();
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::{
        super::options::{CrashConsistency, Options, PathOptions},
        *,
    };

    fn a_run(boot_id: Option<&str>, syncs_files: bool) -> Run {
        Run {
            boot_id: boot_id.map(str::to_owned),
            syncs_files,
        }
    }

    /// Reads the recorded run from a closed store's database.
    fn recorded_run(db: &std::path::Path) -> testresult::TestResult<Option<Run>> {
        let db = Database::open(db)?;
        let tx = db.begin_read()?;
        Ok(run::read(&tx.open_table(META_TABLE)?))
    }

    /// Records `run` as the open run of a closed store's database.
    fn set_run(db: &std::path::Path, run: &Run) -> testresult::TestResult<()> {
        let db = Database::open(db)?;
        let tx = db.begin_write()?;
        run::write(&mut tx.open_table(META_TABLE)?, run)?;
        tx.commit()?;
        Ok(())
    }

    #[test]
    fn opening_records_the_run_until_a_clean_shutdown() -> testresult::TestResult<()> {
        let dir = tempfile::tempdir()?;
        let paths = Arc::new(PathOptions::new(dir.path()));
        let db = dir.path().join("db");
        let (_tx, rx) = tokio::sync::mpsc::channel(1);
        // dropped without a shutdown, as in a crash
        drop(Actor::new(
            db.clone(),
            rx,
            paths,
            BatchOptions::default(),
            false,
        )?);
        let expected = Run {
            boot_id: boot_id::current(),
            syncs_files: false,
        };
        assert_eq!(recorded_run(&db)?, Some(expected));
        Ok(())
    }

    /// Opens a store at `dir` with `crash_consistency`.
    async fn open(
        dir: &std::path::Path,
        crash_consistency: CrashConsistency,
    ) -> testresult::TestResult<crate::store::fs::FsStore> {
        let mut options = Options::new(dir);
        options.crash_consistency = crash_consistency;
        Ok(crate::store::fs::FsStore::load_with_opts(dir.join("blobs.db"), options).await?)
    }

    /// Simulates an OS crash during an unsynced run, and checks entries after reopening with `reopen`.
    async fn os_crash_recovery(reopen: CrashConsistency) -> testresult::TestResult<()> {
        use bao_tree::ChunkRanges;

        use crate::api::blobs::BlobStatus;

        let dir = tempfile::tempdir()?;
        let paths = PathOptions::new(dir.path());
        let data = |byte: u8, len: usize| (0..len).map(|i| byte ^ (i as u8)).collect::<Vec<_>>();
        // owned data, inline outboard
        let intact = data(1, 100_000);
        let truncated = data(2, 100_000);
        // owned data and outboard
        let no_outboard = data(3, 5_000_000);
        let store = open(dir.path(), CrashConsistency::ProcessCrash).await?;
        let intact_hash = store.add_bytes(intact.clone()).await?.hash;
        let truncated_hash = store.add_bytes(truncated).await?.hash;
        let no_outboard_hash = store.add_bytes(no_outboard).await?.hash;
        store.shutdown().await?;
        drop(store);

        // the machine went down while the store was open, and lost writes
        set_run(
            &dir.path().join("blobs.db"),
            &a_run(Some("another boot"), false),
        )?;
        let file = std::fs::OpenOptions::new()
            .write(true)
            .open(paths.data_path(&truncated_hash))?;
        file.set_len(50_000)?;
        std::fs::File::create(paths.outboard_path(&no_outboard_hash))?;

        let store = open(dir.path(), reopen).await?;
        // loading an entry checks it, an intact one is complete again
        assert_eq!(
            store.get_bytes(intact_hash).await?.as_ref(),
            intact.as_slice()
        );
        assert!(matches!(
            store.status(intact_hash).await?,
            BlobStatus::Complete { size: 100_000 }
        ));
        // a damaged one keeps what its files still hold, and nothing more
        let bitfield = store.observe(truncated_hash).await?;
        assert!(!bitfield.is_complete());
        assert!(bitfield.ranges.contains(&bao_tree::ChunkNum(0)));
        assert!(store.get_bytes(truncated_hash).await.is_err());
        let bitfield = store.observe(no_outboard_hash).await?;
        assert_eq!(bitfield.ranges, ChunkRanges::empty());
        store.shutdown().await?;
        Ok(())
    }

    #[tokio::test]
    async fn after_an_os_crash_entries_only_claim_what_their_files_hold(
    ) -> testresult::TestResult<()> {
        os_crash_recovery(CrashConsistency::ProcessCrash).await
    }

    #[tokio::test]
    async fn recovery_depends_on_the_last_run_not_this_one() -> testresult::TestResult<()> {
        os_crash_recovery(CrashConsistency::PowerLoss).await
    }

    #[tokio::test]
    async fn after_an_os_crash_a_synced_run_needs_no_recovery() -> testresult::TestResult<()> {
        use crate::api::blobs::BlobStatus;

        let dir = tempfile::tempdir()?;
        let data = vec![7u8; 100_000];
        let store = open(dir.path(), CrashConsistency::PowerLoss).await?;
        let hash = store.add_bytes(data).await?.hash;
        store.shutdown().await?;
        drop(store);
        set_run(
            &dir.path().join("blobs.db"),
            &a_run(Some("another boot"), true),
        )?;
        let store = open(dir.path(), CrashConsistency::ProcessCrash).await?;
        // recovery would have made it partial until it is loaded
        assert!(matches!(
            store.status(hash).await?,
            BlobStatus::Complete { size: 100_000 }
        ));
        store.shutdown().await?;
        Ok(())
    }

    #[tokio::test]
    async fn a_clean_shutdown_clears_the_run() -> testresult::TestResult<()> {
        let dir = tempfile::tempdir()?;
        let store = crate::store::fs::FsStore::load(dir.path()).await?;
        store.shutdown().await?;
        drop(store);
        assert_eq!(recorded_run(&dir.path().join("blobs.db"))?, None);
        Ok(())
    }

    fn setup() -> testresult::TestResult<(tempfile::TempDir, Arc<PathOptions>, Database)> {
        let dir = tempfile::tempdir()?;
        let paths = Arc::new(PathOptions::new(dir.path()));
        std::fs::create_dir_all(&paths.data_path)?;
        let db = Database::create(dir.path().join("db"))?;
        Ok((dir, paths, db))
    }

    /// A get marks a hash live only if the entity may touch its files.
    #[test]
    fn get_marks_live_by_mode() -> testresult::TestResult<()> {
        let (_dir, paths, db) = setup()?;
        let partial = Hash::new(b"partial");
        let complete = Hash::new(b"complete");
        let missing = Hash::new(b"missing");
        let mut state = Bookkeeping::default();
        write_batch(&db, &mut state, &paths, |tables, _| {
            tables
                .blobs
                .insert(partial, EntryState::Partial { size: None })?;
            tables.blobs.insert(
                complete,
                EntryState::Complete {
                    data_location: DataLocation::Owned(100_000),
                    outboard_location: OutboardLocation::Owned,
                },
            )?;
            Ok(())
        })?;
        let cases = [
            (partial, GetMode::Lookup, false),
            (partial, GetMode::Read, true),
            (partial, GetMode::Write, true),
            (complete, GetMode::Lookup, false),
            (complete, GetMode::Read, false),
            (complete, GetMode::Write, true),
            (missing, GetMode::Lookup, false),
            (missing, GetMode::Read, false),
            (missing, GetMode::Write, true),
        ];
        for (hash, mode, expected) in cases {
            let mut state = Bookkeeping::default();
            write_batch(&db, &mut state, &paths, |tables, state| {
                let (tx, rx) = oneshot::channel();
                let span = Span::current();
                handle_get(
                    Get {
                        hash,
                        mode,
                        tx,
                        span,
                    },
                    tables,
                    &mut state.live,
                )?;
                let oneshot::Receiver::Tokio(mut rx) = rx;
                let res = rx.try_recv()?;
                assert_eq!(res.live, expected, "{hash} {mode:?}");
                assert_eq!(state.live.is_live(&hash), expected, "{hash} {mode:?}");
                Ok(())
            })?;
        }
        Ok(())
    }

    /// Run one write batch the way the actor does, including the file deletes.
    fn write_batch(
        db: &Database,
        state: &mut Bookkeeping,
        paths: &PathOptions,
        f: impl FnOnce(&mut Tables, &mut Bookkeeping) -> testresult::TestResult<()>,
    ) -> testresult::TestResult<()> {
        let tx = db.begin_write()?;
        let mut tables = Tables::new(&tx)?;
        f(&mut tables, state)?;
        let deletes = state.plan_file_deletes(&tables)?;
        drop(tables);
        tx.commit()?;
        for (hash, parts) in deletes {
            delete_files(paths, &hash, &parts);
        }
        Ok(())
    }

    #[test]
    fn export_then_reimport_before_metadata_runs() -> testresult::TestResult<()> {
        let (dir, paths, db) = setup()?;
        let hash = Hash::new(b"content");
        let owned = paths.data_path(&hash);
        let external = dir.path().join("exported");
        std::fs::write(&owned, b"content")?;
        let mut state = Bookkeeping::default();
        write_batch(&db, &mut state, &paths, |tables, state| {
            tables.blobs.insert(
                hash,
                EntryState::Complete {
                    data_location: DataLocation::Owned(7),
                    outboard_location: OutboardLocation::NotNeeded,
                },
            )?;
            // Export queues Set; another import writes before the db handles it.
            std::fs::rename(&owned, &external)?;
            std::fs::write(&owned, b"content")?;
            let (reply, _rx) = oneshot::channel();
            handle_set(
                Set {
                    hash,
                    state: EntryState::Complete {
                        data_location: DataLocation::External(vec![external.clone()], 7),
                        outboard_location: OutboardLocation::NotNeeded,
                    },
                    tx: reply,
                    span: Span::none(),
                },
                state,
                tables,
            )?;
            handle_update(
                Update {
                    hash,
                    state: EntryState::Complete {
                        data_location: DataLocation::Owned(7),
                        outboard_location: OutboardLocation::NotNeeded,
                    },
                    tx: None,
                    span: Span::none(),
                },
                state,
                tables,
            )?;
            Ok(())
        })?;
        assert!(
            owned.exists(),
            "committed metadata still refers to this file"
        );
        Ok(())
    }

    #[test]
    fn deletes_wait_for_live_entity() -> testresult::TestResult<()> {
        let (_dir, paths, db) = setup()?;
        let hash = Hash::new(b"partial");
        let files = [
            paths.data_path(&hash),
            paths.outboard_path(&hash),
            paths.sizes_path(&hash),
            paths.bitfield_path(&hash),
        ];
        let mut state = Bookkeeping::default();
        // An entity loads the hash and writes its partial files.
        state.live.load(hash);
        for file in &files {
            std::fs::write(file, b"x")?;
        }
        // The entry is removed while the entity is live.
        write_batch(&db, &mut state, &paths, |tables, state| {
            tables
                .blobs
                .insert(hash, EntryState::Partial { size: None })?;
            tables.blobs.remove(hash)?;
            state.written(hash);
            Ok(())
        })?;
        assert!(files.iter().all(|f| f.exists()), "entity is live");
        assert!(state.parked.contains(&hash));
        // Once the entity is inactive, the next batch decides by the committed state.
        state.live.inactive(hash);
        write_batch(&db, &mut state, &paths, |_, _| Ok(()))?;
        assert!(
            files.iter().all(|f| !f.exists()),
            "no entry, no live entity"
        );
        assert!(state.parked.is_empty());
        Ok(())
    }
}

//! The metadata database
#![allow(clippy::result_large_err)]
use std::{
    collections::{HashMap, HashSet},
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
            BlobStatusMsg, BlobStatusRequest, CreateTagRequest, DeleteTagsRequest, ListBlobsItem,
            ListBlobsMsg, ListRequest, ListTagsItem, ListTagsMsg, ListTagsRequest,
            RenameTagRequest, SetTagRequest, ShutdownMsg, SyncDbMsg,
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
    file_deletes::{referenced_parts, BaoFilePart, FileNotice},
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
        let (tx, rx) = oneshot::channel();
        self.sender
            .send(
                Get {
                    hash,
                    tx,
                    span: tracing::Span::current(),
                }
                .into(),
            )
            .await
            .map_err(|_| io::Error::other("send get"))?;
        let res = rx.await.map_err(|_| io::Error::other("receive get"))?;
        Ok(res.state?)
    }

    /// Apply an unconditional delete. A file notice confirms the commit,
    /// even if there was no metadata entry.
    ///
    /// Only the hash's entity sends this, see [`Delete`].
    pub async fn delete(&self, hash: Hash) -> io::Result<()> {
        let (tx, rx) = oneshot::channel();
        let span = Span::current();
        self.sender
            .send(Delete { hash, tx, span }.into())
            .await
            .map_err(|_| io::Error::other("send delete"))?;
        rx.await.map_err(|_| io::Error::other("receive delete"))??;
        Ok(())
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
    /// Apply an unconditional delete; a file notice confirms its commit.
    pub async fn delete(&self) -> io::Result<()> {
        self.db.delete(self.hash).await
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

fn handle_get(cmd: Get, tables: &impl ReadableTables) -> ActorResult<()> {
    trace!("{cmd:?}");
    let Get { hash, tx, .. } = cmd;
    let Some(entry) = tables.blobs().get(hash)? else {
        tx.send(GetResult { state: Ok(None) });
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
    tx.send(GetResult {
        state: Ok(Some(entry)),
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
    trace!("updating hash {} to {}", hash.to_hex(), state.fmt_short());
    let old_entry_opt = tables.blobs.get(hash)?.map(|e| e.value());
    bookkeeping.written(hash, old_entry_opt.as_ref());
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
    bookkeeping.references(hash, &state);
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
    let old = tables.blobs.get(hash)?.map(|e| e.value());
    bookkeeping.written(hash, old.as_ref());
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
    bookkeeping.references(hash, &state);
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
    /// Hashes whose entry was written or removed in the current write batch,
    /// with every file their entry referenced in it: before the batch, or in
    /// any state the batch gave it. Files may exist for each of those.
    written: HashMap<Hash, Vec<BaoFilePart>>,
    /// Hashes whose entry was deleted in the current write batch.
    deleted: HashSet<Hash>,
}

impl Bookkeeping {
    /// Note a write to the entry of a hash, whose state before the write was `before`.
    fn written<I>(&mut self, hash: Hash, before: Option<&EntryState<I>>) {
        self.written
            .entry(hash)
            .or_insert_with(|| referenced_parts(before));
    }

    /// Note the state a write in this batch gave the entry of a hash.
    fn references<I>(&mut self, hash: Hash, state: &EntryState<I>) {
        if let Some(parts) = self.written.get_mut(&hash) {
            for part in referenced_parts(Some(state)) {
                if !parts.contains(&part) {
                    parts.push(part);
                }
            }
        }
    }

    /// Note that the entry of a hash was deleted.
    fn deleted<I>(&mut self, hash: Hash, before: Option<&EntryState<I>>) {
        self.written(hash, before);
        self.deleted.insert(hash);
    }

    /// What the batch means for files, once it is committed.
    ///
    /// For each hash written in the batch: whether its entry was deleted, and
    /// which files it referenced in the batch and no longer does.
    fn file_notices(&mut self, tables: &impl ReadableTables) -> ActorResult<Vec<FileNotice>> {
        let mut res = Vec::new();
        for (hash, before) in self.written.drain() {
            let deleted = self.deleted.remove(&hash);
            let after = tables.blobs().get(hash)?.map(|e| e.value());
            let still = referenced_parts(after.as_ref());
            let parts: Vec<_> = before
                .into_iter()
                .filter(|part| !still.contains(part))
                .collect();
            if deleted || !parts.is_empty() {
                res.push(FileNotice {
                    hash,
                    deleted,
                    parts,
                });
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
    db: DbWrapper,
    cmds: PeekableReceiver<Command>,
    /// Where the file consequences of each committed batch go, see [`FileNotice`].
    notices: tokio::sync::mpsc::UnboundedSender<FileNotice>,
    options: BatchOptions,
    state: Bookkeeping,
}

impl Actor {
    pub fn new(
        db_path: PathBuf,
        cmds: tokio::sync::mpsc::Receiver<Command>,
        notices: tokio::sync::mpsc::UnboundedSender<FileNotice>,
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
            db: db.into(),
            cmds,
            notices,
            options,
            state: Default::default(),
        })
    }

    async fn handle_readonly(
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
            ReadOnlyCommand::Get(cmd) => handle_get(cmd, tables),
            ReadOnlyCommand::Dump(cmd) => handle_dump(cmd, tables),
            ReadOnlyCommand::GetBlobStatus(cmd) => handle_get_blob_status(cmd, tables).await,
        }
    }

    fn delete(state: &mut Bookkeeping, tables: &mut Tables<'_>, cmd: Delete) -> ActorResult<()> {
        trace!("{cmd:?}");
        let Delete { hash, tx, .. } = cmd;
        let entry = tables.blobs.remove(hash)?.map(|entry| entry.value());
        // files are deleted after the commit, see Bookkeeping::file_notices
        state.deleted(hash, entry.as_ref());
        if let Some(EntryState::Complete {
            data_location,
            outboard_location,
        }) = entry
        {
            if let DataLocation::Inline(_) = data_location {
                tables.inline_data.remove(hash)?;
            }
            if let OutboardLocation::Inline(_) = outboard_location {
                tables.inline_outboard.remove(hash)?;
            }
        }
        tx.send(Ok(()));
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
            ReadWriteCommand::Delete(cmd) => Self::delete(state, tables, cmd),
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
            NonTopLevelCommand::ReadOnly(cmd) => Self::handle_readonly(tables, cmd, op).await,
            NonTopLevelCommand::ReadWrite(cmd) => {
                Self::handle_readwrite(state, tables, cmd, op).await
            }
        }
    }

    async fn sync_db(_db: &Database, cmd: SyncDbMsg) -> ActorResult<()> {
        trace!("{cmd:?}");
        let SyncDbMsg { tx, .. } = cmd;
        // nothing to do here, since for a toplevel cmd we are outside a write transaction
        tx.send(Ok(())).await.ok();
        Ok(())
    }

    async fn handle_toplevel(
        db: &Database,
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

    pub(super) async fn run(mut self) -> Result<(), Failure> {
        match self.run_inner().await {
            Ok(shutdown) => {
                drop(self);
                if let Some(shutdown) = shutdown {
                    shutdown.tx.send(()).await.ok();
                }
                Ok(())
            }
            Err(error) => Err(Failure {
                error,
                _db: self.db,
            }),
        }
    }

    async fn run_inner(&mut self) -> ActorResult<Option<ShutdownMsg>> {
        let db = &self.db;
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
                    if let Some(shutdown) = Self::handle_toplevel(db, cmd, op).await? {
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
                        Self::handle_readonly(&tables, cmd, op).await?;
                        n += 1;
                        if n >= options.max_read_batch {
                            break;
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
                    let notices = self.state.file_notices(&tables)?;
                    drop(tables);
                    tx.commit().map_err(|e| e!(ActorError::Commit, e))?;
                    // Files are only touched once the commit that stops
                    // referencing them is durable. The main actor hands each
                    // notice to the hash's entity, or acts on it itself if
                    // there is none. Unbounded, so this actor never waits on
                    // the main actor, which waits on this one.
                    for notice in notices {
                        self.notices.send(notice).ok();
                    }
                }
            }
        };
        Ok(shutdown)
    }
}

/// Keeps the database locked until the main actor has cancelled all workers.
#[derive(Debug)]
pub(super) struct Failure {
    pub error: ActorError,
    _db: DbWrapper,
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
        let (notices, _) = tokio::sync::mpsc::unbounded_channel();
        drop(Actor::new(
            db.clone(),
            rx,
            notices,
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

    /// Run one write batch the way the actor does, and return its file notices.
    fn write_batch(
        db: &Database,
        state: &mut Bookkeeping,
        f: impl FnOnce(&mut Tables, &mut Bookkeeping) -> testresult::TestResult<()>,
    ) -> testresult::TestResult<Vec<FileNotice>> {
        let tx = db.begin_write()?;
        let mut tables = Tables::new(&tx)?;
        f(&mut tables, state)?;
        let notices = state.file_notices(&tables)?;
        drop(tables);
        tx.commit()?;
        Ok(notices)
    }

    /// An update from an entity, without waiting for it to be applied.
    fn update(hash: Hash, state: EntryState<Bytes>) -> Update {
        Update {
            hash,
            state,
            tx: None,
            span: Span::none(),
        }
    }

    fn owned_complete(size: u64) -> EntryState<Bytes> {
        EntryState::Complete {
            data_location: DataLocation::Owned(size),
            outboard_location: OutboardLocation::Owned,
        }
    }

    #[test]
    fn export_then_reimport_before_metadata_runs() -> testresult::TestResult<()> {
        let (dir, paths, db) = setup()?;
        let hash = Hash::new(b"content");
        let owned = paths.data_path(&hash);
        let external = dir.path().join("exported");
        std::fs::write(&owned, b"content")?;
        let mut state = Bookkeeping::default();
        let notices = write_batch(&db, &mut state, |tables, state| {
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
            notices
                .iter()
                .all(|n| !n.parts.contains(&BaoFilePart::Data)),
            "committed metadata still refers to this file: {notices:?}"
        );
        Ok(())
    }

    /// A delete names every file its entry used.
    #[test]
    fn a_delete_names_the_files_of_the_entry() -> testresult::TestResult<()> {
        let (_dir, _paths, db) = setup()?;
        let hash = Hash::new(b"partial");
        let mut state = Bookkeeping::default();
        let notices = write_batch(&db, &mut state, |tables, state| {
            handle_update(
                update(
                    hash,
                    EntryState::Partial {
                        size: Some(100_000),
                    },
                ),
                state,
                tables,
            )?;
            Ok(())
        })?;
        assert!(notices.is_empty(), "a new entry leaves no files behind");
        let notices = write_batch(&db, &mut state, |tables, state| {
            let entry = tables.blobs.remove(hash)?.expect("an entry").value();
            state.deleted(hash, Some(&entry));
            Ok(())
        })?;
        assert_eq!(notices.len(), 1);
        assert!(notices[0].deleted);
        assert_eq!(notices[0].parts.len(), 4, "a partial entry uses every file");
        Ok(())
    }

    /// An absent entry still needs a commit notice to release its entity.
    #[test]
    fn deleting_an_absent_entry_is_confirmed() -> testresult::TestResult<()> {
        let (_dir, _paths, db) = setup()?;
        let hash = Hash::new(b"absent");
        let mut state = Bookkeeping::default();
        for _ in 0..2 {
            let notices = write_batch(&db, &mut state, |tables, state| {
                let (tx, _rx) = oneshot::channel();
                Actor::delete(
                    state,
                    tables,
                    Delete {
                        hash,
                        tx,
                        span: Span::none(),
                    },
                )?;
                Ok(())
            })?;
            assert_eq!(notices.len(), 1);
            assert_eq!(notices[0].hash, hash);
            assert!(notices[0].deleted);
        }
        Ok(())
    }

    #[derive(Debug)]
    struct FailSync {
        inner: redb::backends::FileBackend,
        fail: Arc<std::sync::atomic::AtomicBool>,
        closed: Arc<std::sync::atomic::AtomicBool>,
    }

    impl redb::StorageBackend for FailSync {
        fn len(&self) -> io::Result<u64> {
            self.inner.len()
        }
        fn read(&self, offset: u64, out: &mut [u8]) -> io::Result<()> {
            self.inner.read(offset, out)
        }
        fn set_len(&self, len: u64) -> io::Result<()> {
            self.inner.set_len(len)
        }
        fn write(&self, offset: u64, data: &[u8]) -> io::Result<()> {
            self.inner.write(offset, data)
        }
        fn sync_data(&self) -> io::Result<()> {
            if self.fail.swap(false, std::sync::atomic::Ordering::SeqCst) {
                return Err(io::Error::other("injected commit sync failure"));
            }
            self.inner.sync_data()
        }
        fn close(&self) -> io::Result<()> {
            self.closed.store(true, std::sync::atomic::Ordering::SeqCst);
            self.inner.close()
        }
    }

    /// Exercise a real redb commit failure while a delete waits for its notice.
    /// Cached observers and idle waiters must also stop, and old files must survive.
    #[tokio::test]
    async fn failed_commit_aborts_store_without_unlinking() -> testresult::TestResult<()> {
        use std::{
            sync::atomic::{AtomicBool, Ordering},
            time::Duration,
        };

        use n0_future::StreamExt;

        use crate::store::fs::{self, entity_manager::EntityManagerState};

        let dir = tempfile::tempdir()?;
        let options = Arc::new(Options::new(dir.path()));
        std::fs::create_dir_all(&options.path.data_path)?;
        std::fs::create_dir_all(&options.path.temp_path)?;
        let db_path = dir.path().join("blobs.db");
        let fail = Arc::new(AtomicBool::new(false));
        let closed = Arc::new(AtomicBool::new(false));
        let file = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create_new(true)
            .open(&db_path)?;
        let db = Database::builder().create_with_backend(FailSync {
            inner: redb::backends::FileBackend::new(file)?,
            fail: fail.clone(),
            closed: closed.clone(),
        })?;
        // Initialize the same tables and unclean-run marker as normal startup.
        let tx = db.begin_write()?;
        drop(Tables::new(&tx)?);
        run::record(
            &mut tx.open_table(META_TABLE)?,
            &a_run(boot_id::current().as_deref(), false),
        )?;
        tx.commit()?;

        let (db_tx, db_rx) = tokio::sync::mpsc::channel(100);
        let (notice_tx, notices) = tokio::sync::mpsc::unbounded_channel();
        let metadata = Actor {
            db: db.into(),
            cmds: PeekableReceiver::new(db_rx),
            notices: notice_tx,
            options: options.batch.clone(),
            state: Bookkeeping::default(),
        };
        let (cmd_tx, cmd_rx) = tokio::sync::mpsc::channel(100);
        let (fs_tx, fs_cmd_rx) = tokio::sync::mpsc::channel(100);
        let context = Arc::new(fs::TaskContext {
            options: options.clone(),
            db: Db::new(db_tx),
            internal_cmd_tx: fs_tx.clone(),
        });
        let mut actor = fs::Actor {
            context: context.clone(),
            cmd_rx,
            fs_cmd_rx,
            notices,
            metadata_task: Some(tokio::spawn(metadata.run())),
            deletes: Arc::new(tokio::sync::watch::Sender::new(0)),
            tasks: tokio::task::JoinSet::new(),
            handles: EntityManagerState::new(context, 16, 32, 2),
            temp_tags: Default::default(),
            gc: None,
            deferred: Default::default(),
            idle_waiters: Vec::new(),
            _rt: fs::RtWrapper(None),
        };
        struct Worker {
            closed: Arc<AtomicBool>,
            stopped: Arc<AtomicBool>,
        }
        impl Drop for Worker {
            fn drop(&mut self) {
                assert!(
                    !self.closed.load(Ordering::SeqCst),
                    "database released before worker cancellation"
                );
                self.stopped.store(true, Ordering::SeqCst);
            }
        }
        let stopped = Arc::new(AtomicBool::new(false));
        let worker = Worker {
            closed: closed.clone(),
            stopped: stopped.clone(),
        };
        actor.spawn(async move {
            let _worker = worker;
            std::future::pending::<()>().await;
        });
        let task = tokio::spawn(actor.run());
        let store = fs::FsStore::new(cmd_tx.into(), fs_tx);
        let data = fs::tests::test_data(100_000);
        let hash = store.add_bytes(data.clone()).await?.hash;
        let other = store.add_slice("cached observer").await?.hash;
        store.sync_db().await?;
        let mut observer = Box::pin(store.observe(other).stream().await?);
        assert!(observer.next().await.unwrap().is_complete());
        // This waiter cannot complete while the observer keeps its entity busy.
        let idle_store = store.clone();
        let idle = tokio::spawn(async move { idle_store.wait_idle().await });

        fail.store(true, Ordering::SeqCst);
        let result = tokio::time::timeout(
            Duration::from_secs(5),
            store
                .blobs()
                .delete_with_opts(crate::api::proto::BlobDeleteRequest {
                    hashes: vec![hash],
                    force: true,
                }),
        )
        .await?;
        assert!(
            result.is_err(),
            "failed commit must fail the pending delete"
        );
        tokio::time::timeout(Duration::from_secs(5), task).await??;
        assert!(tokio::time::timeout(Duration::from_secs(5), idle)
            .await??
            .is_err());
        assert!(
            tokio::time::timeout(Duration::from_secs(5), observer.next())
                .await?
                .is_none()
        );
        assert!(
            store.get_bytes(other).await.is_err(),
            "cached reads must stop too"
        );
        assert!(
            store.shutdown().await.is_err(),
            "failure cannot become a clean shutdown"
        );
        assert!(!fail.load(Ordering::SeqCst), "the failure was injected");
        assert!(
            closed.load(Ordering::SeqCst),
            "database was eventually released"
        );
        assert!(
            stopped.load(Ordering::SeqCst),
            "worker must be cancelled before releasing the database"
        );
        assert!(
            options.path.data_path(&hash).exists(),
            "uncommitted cleanup must not run"
        );
        assert!(
            recorded_run(&db_path)?.is_some(),
            "unclean marker must survive"
        );
        let reopened = fs::FsStore::load(dir.path()).await?;
        // A failed commit may have committed or rolled back. Any retained
        // metadata must still refer to readable, intact files.
        if reopened.has(hash).await? {
            assert_eq!(reopened.get_bytes(hash).await?, data);
        }
        assert_eq!(
            reopened.get_bytes(other).await?.as_ref(),
            b"cached observer"
        );
        reopened.shutdown().await?;
        Ok(())
    }

    /// Completing an entry names the files it no longer needs, and nothing else.
    #[test]
    fn completing_names_the_files_no_longer_needed() -> testresult::TestResult<()> {
        let (_dir, _paths, db) = setup()?;
        let hash = Hash::new(b"complete");
        let mut state = Bookkeeping::default();
        write_batch(&db, &mut state, |tables, state| {
            handle_update(
                update(
                    hash,
                    EntryState::Partial {
                        size: Some(100_000),
                    },
                ),
                state,
                tables,
            )?;
            Ok(())
        })?;
        let notices = write_batch(&db, &mut state, |tables, state| {
            handle_update(update(hash, owned_complete(100_000)), state, tables)?;
            Ok(())
        })?;
        assert_eq!(notices.len(), 1);
        assert!(!notices[0].deleted);
        let mut parts = notices[0].parts.clone();
        parts.sort();
        assert_eq!(parts, vec![BaoFilePart::Sizes, BaoFilePart::Bitfield]);
        Ok(())
    }
}

//! The metadata database
#![allow(clippy::result_large_err)]
use std::{
    io,
    ops::{Bound, Deref, DerefMut},
    path::PathBuf,
    time::SystemTime,
};

use bao_tree::BaoTree;
use bytes::Bytes;
use irpc::channel::mpsc;
use n0_error::{anyerr, e, stack_error, AnyError};
use redb::{Database, DatabaseError, ReadableDatabase, ReadableTable};
use tokio::pin;

use crate::{
    api::{
        self,
        blobs::BlobStatus,
        proto::{
            BlobDeleteRequest, BlobStatusMsg, BlobStatusRequest, CreateTagRequest, DeleteBlobsMsg,
            DeleteTagsRequest, FinishGcProtectionMsg, FinishGcProtectionRequest, ListBlobsMsg,
            ListRequest, ListTagsRequest, RenameTagRequest, SetTagRequest, ShutdownMsg,
            StartGcProtectionMsg, SyncDbMsg, TagCompareAndSwapOutcome, TagCompareAndSwapRequest,
        },
        tags::TagInfo,
        Tag,
    },
    util::channel::oneshot,
    Hash,
};
mod proto;
pub use proto::*;
pub(crate) mod tables;
use tables::{ReadOnlyTables, ReadableTables, Tables};
use tracing::{debug, error, info_span, trace, Span};

use super::{
    delete_set::DeleteHandle,
    entry_state::{DataLocation, EntryState, OutboardLocation},
    options::BatchOptions,
    util::PeekableReceiver,
    BaoFilePart,
};
use crate::store::GcProtectionSet;
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
    #[error("write transaction aborted before durable acknowledgement: {msg}")]
    WriteTransactionAborted { msg: String },
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

    /// Queue an intermediate entry-state update without waiting for commit.
    ///
    /// The metadata actor batches these updates. Callers that expose completion
    /// externally must finish with [`Self::update_await`] or [`Self::set`].
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

    pub async fn reconcile_external_reference(
        &self,
        hash: Hash,
        expected_paths: Vec<PathBuf>,
        expected_size: u64,
        expected_outboard: Option<OutboardLocation<()>>,
        retained_paths: Vec<PathBuf>,
    ) -> io::Result<ExternalReferenceReconcileOutcome> {
        let (tx, rx) = oneshot::channel();
        self.sender
            .send(
                ReconcileExternalReference {
                    hash,
                    expected_paths,
                    expected_size,
                    expected_outboard,
                    retained_paths,
                    tx,
                    span: Span::current(),
                }
                .into(),
            )
            .await
            .map_err(|_| io::Error::other("send external reference reconciliation"))?;
        rx.await
            .map_err(|_| io::Error::other("receive external reference reconciliation"))?
            .map_err(Into::into)
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

async fn handle_start_gc_protection(
    cmd: StartGcProtectionMsg,
    protected: &mut GcProtectionSet,
) -> ActorResult<()> {
    trace!("{cmd:?}");
    cmd.tx.send(protected.start()).await.ok();
    Ok(())
}

async fn handle_finish_gc_protection(
    cmd: FinishGcProtectionMsg,
    protected: &mut GcProtectionSet,
) -> ActorResult<()> {
    trace!("{cmd:?}");
    let FinishGcProtectionRequest { cycle } = cmd.inner;
    cmd.tx.send(Ok(protected.finish(cycle))).await.ok();
    Ok(())
}

fn handle_protect(cmd: proto::Protect, protected: &mut GcProtectionSet) -> ActorResult<()> {
    trace!("{cmd:?}");
    protected.protect(cmd.hash);
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

async fn handle_list_tags(msg: ListTagsMsg, tables: &impl ReadableTables) -> ActorResult<()> {
    trace!("{msg:?}");
    let ListTagsMsg {
        inner:
            ListTagsRequest {
                from,
                to,
                raw,
                hash_seq,
            },
        tx,
        ..
    } = msg;
    let from = from.map(Bound::Included).unwrap_or(Bound::Unbounded);
    let to = to.map(Bound::Excluded).unwrap_or(Bound::Unbounded);
    let mut res = Vec::new();
    for item in tables
        .tags()
        .range((from, to))
        .map_err(|e| e!(ActorError::Storage, e))?
    {
        match item {
            Ok((k, v)) => {
                let v = v.value();
                if raw && v.format.is_raw() || hash_seq && v.format.is_hash_seq() {
                    let info = TagInfo {
                        name: k.value(),
                        hash: v.hash,
                        format: v.format,
                    };
                    res.push(crate::api::Result::Ok(info));
                }
            }
            Err(e) => res.push(Err(api_error_from_storage_error(e))),
        }
    }
    tx.send(res).await.ok();
    Ok(())
}

fn handle_update(
    cmd: Update,
    protected: &mut GcProtectionSet,
    tables: &mut Tables,
) -> ActorResult<Option<PendingWriteReply>> {
    trace!("{cmd:?}");
    let Update {
        hash, state, tx, ..
    } = cmd;
    protected.protect(hash);
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
        Some(old) => {
            let partial_to_complete = old.is_partial() && state.is_complete();
            let res = EntryState::union(old, state)?;
            if partial_to_complete {
                tables
                    .ftx
                    .delete(hash, [BaoFilePart::Sizes, BaoFilePart::Bitfield]);
            }
            res
        }
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
    Ok(tx.map(PendingWriteReply::ActorUnit))
}

fn handle_reconcile_external_reference(
    cmd: ReconcileExternalReference,
    tables: &mut Tables,
) -> ActorResult<PendingWriteReply> {
    trace!("{cmd:?}");
    let ReconcileExternalReference {
        hash,
        expected_paths,
        expected_size,
        expected_outboard,
        retained_paths,
        tx,
        ..
    } = cmd;
    let current = tables.blobs.get(hash)?.map(|entry| entry.value());
    let outcome = match current {
        None => ExternalReferenceReconcileOutcome::AlreadyAbsent,
        Some(EntryState::Complete {
            data_location: DataLocation::External(paths, size),
            outboard_location,
        }) if paths == expected_paths
            && size == expected_size
            && expected_outboard
                .as_ref()
                .map(|expected| expected == &outboard_location)
                .unwrap_or(true) =>
        {
            if retained_paths.is_empty() {
                tables.blobs.remove(hash)?;
                match outboard_location {
                    OutboardLocation::Inline(()) => {
                        tables.inline_outboard.remove(hash)?;
                    }
                    OutboardLocation::Owned => {
                        tables.ftx.delete(hash, [BaoFilePart::Outboard]);
                    }
                    OutboardLocation::NotNeeded => {}
                }
                ExternalReferenceReconcileOutcome::Removed
            } else {
                tables.blobs.insert(
                    hash,
                    EntryState::Complete {
                        data_location: DataLocation::External(retained_paths, size),
                        outboard_location,
                    },
                )?;
                ExternalReferenceReconcileOutcome::Retained
            }
        }
        Some(_) => ExternalReferenceReconcileOutcome::Changed,
    };
    Ok(PendingWriteReply::ExternalReferenceReconcile { tx, outcome })
}

fn handle_set(
    cmd: Set,
    protected: &mut GcProtectionSet,
    tables: &mut Tables,
) -> ActorResult<PendingWriteReply> {
    trace!("{cmd:?}");
    let Set {
        state, hash, tx, ..
    } = cmd;
    protected.protect(hash);
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
    Ok(PendingWriteReply::ActorUnit(tx))
}

#[derive(Debug)]
enum PendingWriteReply {
    ActorUnit(oneshot::Sender<ActorResult<()>>),
    ExternalReferenceReconcile {
        tx: oneshot::Sender<ActorResult<ExternalReferenceReconcileOutcome>>,
        outcome: ExternalReferenceReconcileOutcome,
    },
    ApiUnit(irpc::channel::oneshot::Sender<api::Result<()>>),
    ApiTag {
        tx: irpc::channel::oneshot::Sender<api::Result<Tag>>,
        tag: Tag,
    },
    ApiU64 {
        tx: irpc::channel::oneshot::Sender<api::Result<u64>>,
        value: u64,
    },
    ApiTagCompareAndSwap {
        tx: irpc::channel::oneshot::Sender<api::Result<TagCompareAndSwapOutcome>>,
        outcome: TagCompareAndSwapOutcome,
    },
}

impl PendingWriteReply {
    async fn succeed(self) {
        match self {
            Self::ActorUnit(tx) => tx.send(Ok(())),
            Self::ExternalReferenceReconcile { tx, outcome } => tx.send(Ok(outcome)),
            Self::ApiUnit(tx) => {
                tx.send(Ok(())).await.ok();
            }
            Self::ApiTag { tx, tag } => {
                tx.send(Ok(tag)).await.ok();
            }
            Self::ApiU64 { tx, value } => {
                tx.send(Ok(value)).await.ok();
            }
            Self::ApiTagCompareAndSwap { tx, outcome } => {
                tx.send(Ok(outcome)).await.ok();
            }
        }
    }

    async fn fail(self, failure: &PendingWriteFailure) {
        match self {
            Self::ActorUnit(tx) => tx.send(Err(failure.actor_error())),
            Self::ExternalReferenceReconcile { tx, .. } => tx.send(Err(failure.actor_error())),
            Self::ApiUnit(tx) => {
                tx.send(Err(failure.api_error())).await.ok();
            }
            Self::ApiTag { tx, .. } => {
                tx.send(Err(failure.api_error())).await.ok();
            }
            Self::ApiU64 { tx, .. } => {
                tx.send(Err(failure.api_error())).await.ok();
            }
            Self::ApiTagCompareAndSwap { tx, .. } => {
                tx.send(Err(failure.api_error())).await.ok();
            }
        }
    }
}

#[derive(Debug, Default)]
struct PendingWriteReplies(Vec<PendingWriteReply>);

impl PendingWriteReplies {
    fn push(&mut self, reply: PendingWriteReply) {
        self.0.push(reply);
    }

    fn push_opt(&mut self, reply: Option<PendingWriteReply>) {
        if let Some(reply) = reply {
            self.push(reply);
        }
    }

    async fn succeed(self) {
        for reply in self.0 {
            reply.succeed().await;
        }
    }

    async fn fail(self, failure: &PendingWriteFailure) {
        for reply in self.0 {
            reply.fail(failure).await;
        }
    }
}

#[derive(Debug, Clone)]
struct PendingWriteFailure(String);

impl PendingWriteFailure {
    fn from_actor_error(error: &ActorError) -> Self {
        Self(error.to_string())
    }

    fn actor_error(&self) -> ActorError {
        e!(ActorError::WriteTransactionAborted {
            msg: self.0.clone()
        })
    }

    fn api_error(&self) -> api::Error {
        api::Error::other(self.0.clone())
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
    ds: DeleteHandle,
    options: BatchOptions,
    protected: GcProtectionSet,
}

impl Actor {
    pub fn new(
        db_path: PathBuf,
        cmds: tokio::sync::mpsc::Receiver<Command>,
        mut ds: DeleteHandle,
        options: BatchOptions,
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
        let ftx = ds.begin_write();
        Tables::new(&tx, &ftx)?;
        tx.commit()?;
        drop(ftx);
        let cmds = PeekableReceiver::new(cmds);
        Ok(Self {
            db,
            cmds,
            ds,
            options,
            protected: Default::default(),
        })
    }

    async fn handle_readonly(
        protected: &mut GcProtectionSet,
        tables: &impl ReadableTables,
        cmd: ReadOnlyCommand,
        op: TxnNum,
    ) -> ActorResult<()> {
        let span = info_span!(
            parent: &cmd.parent_span(),
            "tx",
            op = tracing::field::debug(op),
        );
        let _guard = span.enter();
        match cmd {
            ReadOnlyCommand::Get(cmd) => handle_get(cmd, tables),
            ReadOnlyCommand::Dump(cmd) => handle_dump(cmd, tables),
            ReadOnlyCommand::ListTags(cmd) => handle_list_tags(cmd, tables).await,
            ReadOnlyCommand::StartGcProtection(cmd) => {
                handle_start_gc_protection(cmd, protected).await
            }
            ReadOnlyCommand::FinishGcProtection(cmd) => {
                handle_finish_gc_protection(cmd, protected).await
            }
            ReadOnlyCommand::GetBlobStatus(cmd) => handle_get_blob_status(cmd, tables).await,
            ReadOnlyCommand::Protect(cmd) => handle_protect(cmd, protected),
        }
    }

    async fn delete(
        protected: &mut GcProtectionSet,
        tables: &mut Tables<'_>,
        cmd: DeleteBlobsMsg,
    ) -> ActorResult<PendingWriteReply> {
        let DeleteBlobsMsg {
            inner: BlobDeleteRequest { hashes, force },
            tx,
            ..
        } = cmd;
        for hash in hashes {
            if !force && protected.contains(&hash) {
                trace!("delete {hash}: skip (protected)");
                continue;
            }
            if let Some(entry) = tables.blobs.remove(hash)? {
                match entry.value() {
                    EntryState::Complete {
                        data_location,
                        outboard_location,
                    } => {
                        trace!("delete {hash}: currently complete. will be deleted.");
                        match data_location {
                            DataLocation::Inline(_) => {
                                tables.inline_data.remove(hash)?;
                            }
                            DataLocation::Owned(_) => {
                                // mark the data for deletion
                                tables.ftx.delete(hash, [BaoFilePart::Data]);
                            }
                            DataLocation::External(_, _) => {}
                        }
                        match outboard_location {
                            OutboardLocation::Inline(_) => {
                                tables.inline_outboard.remove(hash)?;
                            }
                            OutboardLocation::Owned => {
                                // mark the outboard for deletion
                                tables.ftx.delete(hash, [BaoFilePart::Outboard]);
                            }
                            OutboardLocation::NotNeeded => {}
                        }
                    }
                    EntryState::Partial { .. } => {
                        trace!("delete {hash}: currently partial. will be deleted.");
                        tables.ftx.delete(
                            hash,
                            [
                                BaoFilePart::Outboard,
                                BaoFilePart::Data,
                                BaoFilePart::Sizes,
                                BaoFilePart::Bitfield,
                            ],
                        );
                    }
                }
            }
        }
        Ok(PendingWriteReply::ApiUnit(tx))
    }

    async fn set_tag(
        protected: &mut GcProtectionSet,
        tables: &mut Tables<'_>,
        cmd: SetTagMsg,
    ) -> ActorResult<Option<PendingWriteReply>> {
        trace!("{cmd:?}");
        let SetTagMsg {
            inner: SetTagRequest { name: tag, value },
            tx,
            ..
        } = cmd;
        protected.protect(value.hash);
        match tables.tags.insert(tag, value) {
            Ok(_) => Ok(Some(PendingWriteReply::ApiUnit(tx))),
            Err(error) => {
                tx.send(Err(api_error_from_storage_error(error))).await.ok();
                Ok(None)
            }
        }
    }

    async fn compare_and_swap_tag(
        protected: &mut GcProtectionSet,
        tables: &mut Tables<'_>,
        cmd: CompareAndSwapTagMsg,
    ) -> ActorResult<PendingWriteReply> {
        trace!("{cmd:?}");
        let CompareAndSwapTagMsg {
            inner:
                TagCompareAndSwapRequest {
                    name,
                    expected,
                    value,
                },
            tx,
            ..
        } = cmd;
        let current = tables.tags.get(name.clone())?.map(|value| value.value());
        let outcome = if current == expected {
            match value {
                Some(value) => {
                    protected.protect(value.hash);
                    tables.tags.insert(name, value)?;
                }
                None => {
                    tables.tags.remove(name)?;
                }
            }
            TagCompareAndSwapOutcome::Applied
        } else {
            TagCompareAndSwapOutcome::Mismatch { current }
        };
        Ok(PendingWriteReply::ApiTagCompareAndSwap { tx, outcome })
    }

    async fn create_tag(
        protected: &mut GcProtectionSet,
        tables: &mut Tables<'_>,
        cmd: CreateTagMsg,
    ) -> ActorResult<PendingWriteReply> {
        trace!("{cmd:?}");
        let CreateTagMsg {
            inner: CreateTagRequest { value },
            tx,
            ..
        } = cmd;
        protected.protect(value.hash);
        let tag = {
            let tag = Tag::auto(SystemTime::now(), |x| {
                matches!(tables.tags.get(Tag(Bytes::copy_from_slice(x))), Ok(Some(_)))
            });
            tables.tags.insert(tag.clone(), value)?;
            tag
        };
        Ok(PendingWriteReply::ApiTag { tx, tag })
    }

    async fn delete_tags(
        tables: &mut Tables<'_>,
        cmd: DeleteTagsMsg,
    ) -> ActorResult<PendingWriteReply> {
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
        Ok(PendingWriteReply::ApiU64 { tx, value: deleted })
    }

    async fn rename_tag(
        tables: &mut Tables<'_>,
        cmd: RenameTagMsg,
    ) -> ActorResult<Option<PendingWriteReply>> {
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
                return Ok(None);
            }
        };
        tables.tags.insert(to, value)?;
        Ok(Some(PendingWriteReply::ApiUnit(tx)))
    }

    async fn handle_readwrite(
        protected: &mut GcProtectionSet,
        tables: &mut Tables<'_>,
        pending: &mut PendingWriteReplies,
        cmd: ReadWriteCommand,
        op: TxnNum,
    ) -> ActorResult<()> {
        let span = info_span!(
            parent: &cmd.parent_span(),
            "tx",
            op = tracing::field::debug(op),
        );
        let _guard = span.enter();
        match cmd {
            ReadWriteCommand::Update(cmd) => {
                pending.push_opt(handle_update(cmd, protected, tables)?)
            }
            ReadWriteCommand::ReconcileExternalReference(cmd) => {
                pending.push(handle_reconcile_external_reference(cmd, tables)?)
            }
            ReadWriteCommand::Set(cmd) => pending.push(handle_set(cmd, protected, tables)?),
            ReadWriteCommand::DeleteBlobw(cmd) => {
                pending.push(Self::delete(protected, tables, cmd).await?)
            }
            ReadWriteCommand::SetTag(cmd) => {
                pending.push_opt(Self::set_tag(protected, tables, cmd).await?)
            }
            ReadWriteCommand::CompareAndSwapTag(cmd) => {
                pending.push(Self::compare_and_swap_tag(protected, tables, cmd).await?)
            }
            ReadWriteCommand::CreateTag(cmd) => {
                pending.push(Self::create_tag(protected, tables, cmd).await?)
            }
            ReadWriteCommand::DeleteTags(cmd) => {
                pending.push(Self::delete_tags(tables, cmd).await?)
            }
            ReadWriteCommand::RenameTag(cmd) => {
                pending.push_opt(Self::rename_tag(tables, cmd).await?)
            }
            ReadWriteCommand::ProcessExit(cmd) => {
                std::process::exit(cmd.code);
            }
        }
        Ok(())
    }

    async fn handle_non_toplevel(
        protected: &mut GcProtectionSet,
        tables: &mut Tables<'_>,
        pending: &mut PendingWriteReplies,
        cmd: NonTopLevelCommand,
        op: TxnNum,
    ) -> ActorResult<()> {
        match cmd {
            NonTopLevelCommand::ReadOnly(cmd) => {
                Self::handle_readonly(protected, tables, cmd, op).await
            }
            NonTopLevelCommand::ReadWrite(cmd) => {
                Self::handle_readwrite(protected, tables, pending, cmd, op).await
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
            parent: &cmd.parent_span(),
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
                // nothing to do here, since the database will be dropped
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

    pub async fn run(mut self) -> ActorResult<()> {
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
                        Self::handle_readonly(&mut self.protected, &tables, cmd, op).await?;
                        n += 1;
                        if n >= options.max_read_batch {
                            break;
                        }
                    }
                }
                Command::ReadWrite(cmd) => {
                    let op = TxnNum::Write(op);
                    self.cmds.push_back(cmd.into()).ok();
                    let mut pending = PendingWriteReplies::default();
                    let batch_res: ActorResult<()> = async {
                        let ftx = self.ds.begin_write();
                        let tx = db
                            .begin_write()
                            .map_err(|e| e!(ActorError::Transaction, e))?;
                        let mut tables =
                            Tables::new(&tx, &ftx).map_err(|e| e!(ActorError::Table, e))?;
                        let timeout = n0_future::time::sleep(self.options.max_write_duration);
                        pin!(timeout);
                        let mut n = 0;
                        while let Some(cmd) = self
                            .cmds
                            .extract(Command::non_top_level, &mut timeout)
                            .await
                        {
                            Self::handle_non_toplevel(
                                &mut self.protected,
                                &mut tables,
                                &mut pending,
                                cmd,
                                op,
                            )
                            .await?;
                            n += 1;
                            if n >= options.max_write_batch {
                                break;
                            }
                        }
                        drop(tables);
                        tx.commit().map_err(|e| e!(ActorError::Commit, e))?;
                        ftx.commit();
                        Ok(())
                    }
                    .await;
                    match batch_res {
                        Ok(()) => pending.succeed().await,
                        Err(error) => {
                            pending
                                .fail(&PendingWriteFailure::from_actor_error(&error))
                                .await;
                            return Err(error);
                        }
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
    match list_blobs_impl(snapshot, inner, &mut tx).await {
        Ok(()) => {}
        Err(e) => {
            error!("error listing blobs: {}", e);
            tx.send(Err(e)).await.ok();
        }
    }
}

async fn list_blobs_impl(
    snapshot: ReadOnlyTables,
    _cmd: ListRequest,
    tx: &mut mpsc::Sender<api::Result<Hash>>,
) -> api::Result<()> {
    for item in snapshot
        .blobs
        .iter()
        .map_err(api_error_from_storage_error)?
    {
        let (k, _) = item.map_err(api_error_from_storage_error)?;
        let k = k.value();
        tx.send(Ok(k)).await.ok();
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use irpc::channel::oneshot as rpc_oneshot;

    use super::*;

    #[tokio::test]
    async fn write_reply_success_waits_for_commit_dispatch() {
        let (probe_tx, probe_rx) = oneshot::channel();
        let (committed_tx, committed_rx) = oneshot::channel();
        let pending = PendingWriteReplies(vec![
            PendingWriteReply::ActorUnit(probe_tx),
            PendingWriteReply::ActorUnit(committed_tx),
        ]);

        assert!(n0_future::future::now_or_never(probe_rx).is_none());

        pending.succeed().await;
        assert!(committed_rx
            .await
            .expect("reply should arrive after commit")
            .is_ok());
    }

    #[tokio::test]
    async fn write_reply_commit_failure_reaches_all_result_types() {
        let (actor_tx, actor_rx) = oneshot::channel();
        let (unit_tx, unit_rx) = rpc_oneshot::channel();
        let (tag_tx, tag_rx) = rpc_oneshot::channel();
        let (count_tx, count_rx) = rpc_oneshot::channel();
        let (reconcile_tx, reconcile_rx) = oneshot::channel();
        let pending = PendingWriteReplies(vec![
            PendingWriteReply::ActorUnit(actor_tx),
            PendingWriteReply::ExternalReferenceReconcile {
                tx: reconcile_tx,
                outcome: ExternalReferenceReconcileOutcome::Removed,
            },
            PendingWriteReply::ApiUnit(unit_tx),
            PendingWriteReply::ApiTag {
                tx: tag_tx,
                tag: Tag::from("created"),
            },
            PendingWriteReply::ApiU64 {
                tx: count_tx,
                value: 7,
            },
        ]);

        pending
            .fail(&PendingWriteFailure::from_actor_error(&e!(
                ActorError::Inconsistent {
                    msg: "commit failed".to_string()
                }
            )))
            .await;

        let actor_error = actor_rx
            .await
            .expect("actor reply should be delivered")
            .expect_err("actor reply should fail");
        assert!(actor_error.to_string().contains("commit failed"));
        assert!(matches!(
            actor_error,
            ActorError::WriteTransactionAborted { .. }
        ));
        assert!(matches!(
            reconcile_rx
                .await
                .expect("reconcile reply should be delivered")
                .expect_err("reconcile reply should fail"),
            ActorError::WriteTransactionAborted { .. }
        ));

        let unit_error = unit_rx
            .await
            .expect("api unit reply should be delivered")
            .expect_err("api unit reply should fail");
        let api::Error::Io(unit_error) = unit_error;
        assert!(unit_error.to_string().contains("commit failed"));

        let tag_error = tag_rx
            .await
            .expect("api tag reply should be delivered")
            .expect_err("api tag reply should fail");
        let api::Error::Io(tag_error) = tag_error;
        assert!(tag_error.to_string().contains("commit failed"));

        let count_error = count_rx
            .await
            .expect("api count reply should be delivered")
            .expect_err("api count reply should fail");
        let api::Error::Io(count_error) = count_error;
        assert!(count_error.to_string().contains("commit failed"));
    }

    #[tokio::test]
    async fn write_reply_success_preserves_each_result_type_once() {
        let (actor_tx, actor_rx) = oneshot::channel();
        let (unit_tx, unit_rx) = rpc_oneshot::channel();
        let (tag_tx, tag_rx) = rpc_oneshot::channel();
        let (count_tx, count_rx) = rpc_oneshot::channel();
        let (reconcile_tx, reconcile_rx) = oneshot::channel();
        let expected_tag = Tag::from("created");
        let pending = PendingWriteReplies(vec![
            PendingWriteReply::ActorUnit(actor_tx),
            PendingWriteReply::ExternalReferenceReconcile {
                tx: reconcile_tx,
                outcome: ExternalReferenceReconcileOutcome::Removed,
            },
            PendingWriteReply::ApiUnit(unit_tx),
            PendingWriteReply::ApiTag {
                tx: tag_tx,
                tag: expected_tag.clone(),
            },
            PendingWriteReply::ApiU64 {
                tx: count_tx,
                value: 7,
            },
        ]);

        pending.succeed().await;

        assert!(actor_rx
            .await
            .expect("actor reply should be delivered")
            .is_ok());
        assert!(unit_rx
            .await
            .expect("api unit reply should be delivered")
            .is_ok());
        assert_eq!(
            reconcile_rx
                .await
                .expect("reconcile reply should be delivered")
                .expect("reconcile reply should succeed"),
            ExternalReferenceReconcileOutcome::Removed
        );
        assert_eq!(
            tag_rx
                .await
                .expect("api tag reply should be delivered")
                .expect("api tag reply should succeed"),
            expected_tag
        );
        assert_eq!(
            count_rx
                .await
                .expect("api count reply should be delivered")
                .expect("api count reply should succeed"),
            7
        );
    }
}

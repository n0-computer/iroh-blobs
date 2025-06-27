//! The metadata database
#![allow(clippy::result_large_err)]
use std::{
    collections::HashSet,
    io,
    ops::{Bound, Deref, DerefMut},
    path::PathBuf,
    time::SystemTime,
};

use bao_tree::BaoTree;
use bytes::Bytes;
use irpc::channel::mpsc;
use n0_snafu::SpanTrace;
use nested_enum_utils::common_fields;
use redb::{Database, DatabaseError, ReadableTable};
use snafu::{Backtrace, ResultExt, Snafu};
use tokio::pin;

use crate::{
    api::{
        self,
        blobs::BlobStatus,
        proto::{
            BlobDeleteRequest, BlobStatusMsg, BlobStatusRequest, ClearProtectedMsg,
            CreateTagRequest, DeleteBlobsMsg, DeleteTagsRequest, ListBlobsMsg, ListRequest,
            ListTagsRequest, RenameTagRequest, SetTagRequest, ShutdownMsg, SyncDbMsg,
        },
        tags::TagInfo,
    },
    util::channel::oneshot,
};
mod proto;
pub use proto::*;
pub(crate) mod tables;
use tables::{ReadOnlyTables, ReadableTables, Tables};
use tracing::{debug, error, info_span, trace};

use super::{
    delete_set::DeleteHandle,
    entry_state::{DataLocation, EntryState, OutboardLocation},
    options::BatchOptions,
    util::PeekableReceiver,
    BaoFilePart,
};
use crate::store::{util::Tag, Hash, IROH_BLOCK_SIZE};

/// Error type for message handler functions of the redb actor.
///
/// What can go wrong are various things with redb, as well as io errors related
/// to files other than redb.
#[common_fields({
    backtrace: Option<Backtrace>,
    #[snafu(implicit)]
    span_trace: SpanTrace,
})]
#[allow(missing_docs)]
#[non_exhaustive]
#[derive(Debug, Snafu)]
pub enum ActorError {
    #[snafu(display("table error: {source}"))]
    Table { source: redb::TableError },
    #[snafu(display("database error: {source}"))]
    Database { source: redb::DatabaseError },
    #[snafu(display("transaction error: {source}"))]
    Transaction { source: redb::TransactionError },
    #[snafu(display("commit error: {source}"))]
    Commit { source: redb::CommitError },
    #[snafu(display("storage error: {source}"))]
    Storage { source: redb::StorageError },
    #[snafu(display("inconsistent database state: {msg}"))]
    Inconsistent { msg: String },
}

impl From<ActorError> for io::Error {
    fn from(e: ActorError) -> Self {
        io::Error::other(e)
    }
}

impl ActorError {
    pub(super) fn inconsistent(msg: String) -> Self {
        InconsistentSnafu { msg }.build()
    }
}

pub type ActorResult<T> = Result<T, ActorError>;

#[derive(Debug, Clone)]
pub struct Db {
    pub sender: tokio::sync::mpsc::Sender<Command>,
}

impl Db {
    pub fn new(sender: tokio::sync::mpsc::Sender<Command>) -> Self {
        Self { sender }
    }

    /// Get the entry state for a hash, if any.
    pub async fn get(&self, hash: Hash) -> anyhow::Result<Option<EntryState<Bytes>>> {
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
            .await?;
        let res = rx.await?;
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
    let Some(entry) = tables.blobs().get(hash).context(StorageSnafu)? else {
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
    for e in tables.blobs().iter().context(StorageSnafu)? {
        let (k, v) = e.context(StorageSnafu)?;
        let k = k.value();
        let v = v.value();
        println!("blobs: {} -> {:?}", k.to_hex(), v);
    }
    for e in tables.tags().iter().context(StorageSnafu)? {
        let (k, v) = e.context(StorageSnafu)?;
        let k = k.value();
        let v = v.value();
        println!("tags: {k} -> {v:?}");
    }
    for e in tables.inline_data().iter().context(StorageSnafu)? {
        let (k, v) = e.context(StorageSnafu)?;
        let k = k.value();
        let v = v.value();
        println!("inline_data: {} -> {:?}", k.to_hex(), v.len());
    }
    for e in tables.inline_outboard().iter().context(StorageSnafu)? {
        let (k, v) = e.context(StorageSnafu)?;
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
    let res = match tables.blobs().get(hash).context(StorageSnafu)? {
        Some(entry) => match entry.value() {
            EntryState::Complete { data_location, .. } => match data_location {
                DataLocation::Inline(_) => {
                    let Some(data) = tables.inline_data().get(hash).context(StorageSnafu)? else {
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
    for item in tables.tags().range((from, to)).context(StorageSnafu)? {
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
            Err(e) => {
                res.push(Err(crate::api::Error::other(e)));
            }
        }
    }
    tx.send(res).await.ok();
    Ok(())
}

fn handle_update(
    cmd: Update,
    protected: &mut HashSet<Hash>,
    tables: &mut Tables,
) -> ActorResult<()> {
    trace!("{cmd:?}");
    let Update {
        hash, state, tx, ..
    } = cmd;
    protected.insert(hash);
    trace!("updating hash {} to {}", hash.to_hex(), state.fmt_short());
    let old_entry_opt = tables
        .blobs
        .get(hash)
        .context(StorageSnafu)?
        .map(|e| e.value());
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
    tables.blobs.insert(hash, state).context(StorageSnafu)?;
    if let Some(data) = data {
        tables
            .inline_data
            .insert(hash, data.as_ref())
            .context(StorageSnafu)?;
    }
    if let Some(outboard) = outboard {
        tables
            .inline_outboard
            .insert(hash, outboard.as_ref())
            .context(StorageSnafu)?;
    }
    if let Some(tx) = tx {
        tx.send(Ok(()));
    }
    Ok(())
}

fn handle_set(cmd: Set, protected: &mut HashSet<Hash>, tables: &mut Tables) -> ActorResult<()> {
    trace!("{cmd:?}");
    let Set {
        state, hash, tx, ..
    } = cmd;
    protected.insert(hash);
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
    tables.blobs.insert(hash, state).context(StorageSnafu)?;
    if let Some(data) = data {
        tables
            .inline_data
            .insert(hash, data.as_ref())
            .context(StorageSnafu)?;
    }
    if let Some(outboard) = outboard {
        tables
            .inline_outboard
            .insert(hash, outboard.as_ref())
            .context(StorageSnafu)?;
    }
    tx.send(Ok(()));
    Ok(())
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
    protected: HashSet<Hash>,
}

impl Actor {
    pub fn new(
        db_path: PathBuf,
        cmds: tokio::sync::mpsc::Receiver<Command>,
        mut ds: DeleteHandle,
        options: BatchOptions,
    ) -> anyhow::Result<Self> {
        debug!("creating or opening meta database at {}", db_path.display());
        let db = match redb::Database::create(db_path) {
            Ok(db) => db,
            Err(DatabaseError::UpgradeRequired(1)) => {
                return Err(anyhow::anyhow!("migration from v1 no longer supported"));
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
        protected: &mut HashSet<Hash>,
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
            ReadOnlyCommand::ClearProtected(cmd) => handle_clear_protected(cmd, protected).await,
            ReadOnlyCommand::GetBlobStatus(cmd) => handle_get_blob_status(cmd, tables).await,
        }
    }

    async fn delete(
        protected: &mut HashSet<Hash>,
        tables: &mut Tables<'_>,
        cmd: DeleteBlobsMsg,
    ) -> ActorResult<()> {
        let DeleteBlobsMsg {
            inner: BlobDeleteRequest { hashes, force },
            ..
        } = cmd;
        for hash in hashes {
            if !force && protected.contains(&hash) {
                continue;
            }
            if let Some(entry) = tables.blobs.remove(hash).context(StorageSnafu)? {
                match entry.value() {
                    EntryState::Complete {
                        data_location,
                        outboard_location,
                    } => {
                        match data_location {
                            DataLocation::Inline(_) => {
                                tables.inline_data.remove(hash).context(StorageSnafu)?;
                            }
                            DataLocation::Owned(_) => {
                                // mark the data for deletion
                                tables.ftx.delete(hash, [BaoFilePart::Data]);
                            }
                            DataLocation::External(_, _) => {}
                        }
                        match outboard_location {
                            OutboardLocation::Inline(_) => {
                                tables.inline_outboard.remove(hash).context(StorageSnafu)?;
                            }
                            OutboardLocation::Owned => {
                                // mark the outboard for deletion
                                tables.ftx.delete(hash, [BaoFilePart::Outboard]);
                            }
                            OutboardLocation::NotNeeded => {}
                        }
                    }
                    EntryState::Partial { .. } => {
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
        let res = tables.tags.insert(tag, value).map(|_| ());
        tx.send(res.map_err(crate::api::Error::other)).await.ok();
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
            tables
                .tags
                .insert(tag.clone(), value)
                .context(StorageSnafu)?;
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
        let removing = tables
            .tags
            .extract_from_if((from, to), |_, _| true)
            .context(StorageSnafu)?;
        // drain the iterator to actually remove the tags
        for res in removing {
            res.context(StorageSnafu)?;
        }
        tx.send(Ok(())).await.ok();
        Ok(())
    }

    async fn rename_tag(tables: &mut Tables<'_>, cmd: RenameTagMsg) -> ActorResult<()> {
        trace!("{cmd:?}");
        let RenameTagMsg {
            inner: RenameTagRequest { from, to },
            tx,
            ..
        } = cmd;
        let value = match tables.tags.remove(from).context(StorageSnafu)? {
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
        tables.tags.insert(to, value).context(StorageSnafu)?;
        tx.send(Ok(())).await.ok();
        Ok(())
    }

    async fn handle_readwrite(
        protected: &mut HashSet<Hash>,
        tables: &mut Tables<'_>,
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
            ReadWriteCommand::Update(cmd) => handle_update(cmd, protected, tables),
            ReadWriteCommand::Set(cmd) => handle_set(cmd, protected, tables),
            ReadWriteCommand::DeleteBlobw(cmd) => Self::delete(protected, tables, cmd).await,
            ReadWriteCommand::SetTag(cmd) => Self::set_tag(tables, cmd).await,
            ReadWriteCommand::CreateTag(cmd) => Self::create_tag(tables, cmd).await,
            ReadWriteCommand::DeleteTags(cmd) => Self::delete_tags(tables, cmd).await,
            ReadWriteCommand::RenameTag(cmd) => Self::rename_tag(tables, cmd).await,
            ReadWriteCommand::ProcessExit(cmd) => {
                std::process::exit(cmd.code);
            }
        }
    }

    async fn handle_non_toplevel(
        protected: &mut HashSet<Hash>,
        tables: &mut Tables<'_>,
        cmd: NonTopLevelCommand,
        op: TxnNum,
    ) -> ActorResult<()> {
        match cmd {
            NonTopLevelCommand::ReadOnly(cmd) => {
                Self::handle_readonly(protected, tables, cmd, op).await
            }
            NonTopLevelCommand::ReadWrite(cmd) => {
                Self::handle_readwrite(protected, tables, cmd, op).await
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
                let txn = db.begin_read().context(TransactionSnafu)?;
                let snapshot = ReadOnlyTables::new(&txn).context(TableSnafu)?;
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
                    let tx = db.begin_read().context(TransactionSnafu)?;
                    let tables = ReadOnlyTables::new(&tx).context(TableSnafu)?;
                    let timeout = tokio::time::sleep(self.options.max_read_duration);
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
                    let ftx = self.ds.begin_write();
                    let tx = db.begin_write().context(TransactionSnafu)?;
                    let mut tables = Tables::new(&tx, &ftx).context(TableSnafu)?;
                    let timeout = tokio::time::sleep(self.options.max_read_duration);
                    pin!(timeout);
                    let mut n = 0;
                    while let Some(cmd) = self
                        .cmds
                        .extract(Command::non_top_level, &mut timeout)
                        .await
                    {
                        Self::handle_non_toplevel(&mut self.protected, &mut tables, cmd, op)
                            .await?;
                        n += 1;
                        if n >= options.max_write_batch {
                            break;
                        }
                    }
                    drop(tables);
                    tx.commit().context(CommitSnafu)?;
                    ftx.commit();
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
            let Some(data) = tables.inline_data().get(hash).context(StorageSnafu)? else {
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
            let Some(outboard) = tables.inline_outboard().get(hash).context(StorageSnafu)? else {
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
    for item in snapshot.blobs.iter().map_err(api::Error::other)? {
        let (k, _) = item.map_err(api::Error::other)?;
        let k = k.value();
        tx.send(Ok(k)).await.ok();
    }
    Ok(())
}

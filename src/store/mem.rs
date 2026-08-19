//! Mutable in-memory blob store.
//!
//! Being a memory store, this store has to import all data into memory before it can
//! serve it. So the amount of data you can serve is limited by your available memory.
//! Other than that this is a fully featured store that provides all features such as
//! tags and garbage collection.
//!
//! For many use cases this can be quite useful, since it does not require write access
//! to the file system.
use std::{
    collections::{BTreeMap, HashMap, HashSet},
    future::Future,
    io::{self, Write},
    num::NonZeroU64,
    ops::Deref,
    sync::Arc,
};

use bao_tree::{
    blake3,
    io::{
        mixed::{traverse_ranges_validated, EncodedItem, ReadBytesAt},
        outboard::PreOrderMemOutboard,
        sync::{Outboard, ReadAt, WriteAt},
        BaoContentItem, EncodeError, Leaf,
    },
    BaoTree, ChunkNum, ChunkRanges, TreeNode,
};
use bytes::Bytes;
use irpc::channel::mpsc;
use n0_error::{Result, StdResultExt};
use n0_future::{
    future::yield_now,
    task::{JoinError, JoinSet},
    time::SystemTime,
};
use range_collections::range_set::RangeSetRange;
use tokio::sync::watch;
use tracing::{error, info, instrument, trace, Instrument};

use super::util::{BaoTreeSender, PartialMemStorage};
use super::virtual_blob::{DynReadBytesAt, VirtualProviders};
use crate::{
    api::{
        self,
        blobs::{AddProgressItem, Bitfield, BlobStatus, ExportProgressItem},
        proto::{
            AddVirtualMsg, AddVirtualRequest, AddVirtualWithOutboardMsg,
            AddVirtualWithOutboardRequest, BatchMsg, BatchResponse, BlobDeleteRequest,
            BlobStatusMsg, BlobStatusRequest, BuildOutboardMsg, Command, CreateTagMsg,
            CreateTagRequest, CreateTempTagMsg, DeleteBlobsMsg, DeleteTagsMsg, DeleteTagsRequest,
            ExportBaoMsg, ExportBaoRequest, ExportPathMsg, ExportPathRequest, ExportRangesItem,
            ExportRangesMsg, ExportRangesRequest, ImportBaoMsg, ImportBaoRequest,
            ImportByteStreamMsg, ImportByteStreamUpdate, ImportBytesMsg, ImportBytesRequest,
            ImportPathMsg, ImportPathRequest, ListBlobsMsg, ListTagsMsg, ListTagsRequest,
            ObserveMsg, ObserveRequest, RenameTagMsg, RenameTagRequest, Scope, SetTagMsg,
            SetTagRequest, ShutdownMsg, SyncDbMsg, WaitIdleMsg,
        },
        tags::TagInfo,
        ApiClient,
    },
    protocol::ChunkRangesExt,
    store::{
        gc::{run_gc, GcConfig},
        util::{SizeInfo, SparseMemFile, Tag},
        IROH_BLOCK_SIZE,
    },
    util::temp_tag::{TagDrop, TempTagScope, TempTags},
    BlobFormat, Hash, HashAndFormat,
};

#[derive(Debug, Default)]
pub struct Options {
    pub gc_config: Option<GcConfig>,
}

#[derive(Debug, Clone)]
#[repr(transparent)]
pub struct MemStore {
    client: ApiClient,
}

impl From<MemStore> for crate::api::Store {
    fn from(value: MemStore) -> Self {
        crate::api::Store::from_sender(value.client)
    }
}

impl AsRef<crate::api::Store> for MemStore {
    fn as_ref(&self) -> &crate::api::Store {
        crate::api::Store::ref_from_sender(&self.client)
    }
}

impl Deref for MemStore {
    type Target = crate::api::Store;

    fn deref(&self) -> &Self::Target {
        crate::api::Store::ref_from_sender(&self.client)
    }
}

impl Default for MemStore {
    fn default() -> Self {
        Self::new()
    }
}

#[derive(derive_more::From)]
enum TaskResult {
    Unit(()),
    Import(Result<ImportEntry>),
    BuildOutboard(Result<BuildOutboardResult>),
    Scope(Scope),
}

/// The result of [`build_outboard`]: the computed hash, the outboard bytes, the
/// total size, and the channels needed to finish registration.
struct BuildOutboardResult {
    hash: Hash,
    outboard: Bytes,
    size: u64,
    scope: Scope,
    format: BlobFormat,
    tx: mpsc::Sender<AddProgressItem>,
}

impl MemStore {
    pub fn from_sender(client: ApiClient) -> Self {
        Self { client }
    }

    pub fn new() -> Self {
        Self::new_with_opts(Options::default())
    }

    pub fn new_with_virtuals(opts: Options) -> (Self, VirtualProviders) {
        let (sender, receiver) = tokio::sync::mpsc::channel(32);
        let virtuals = VirtualProviders::new();
        n0_future::task::spawn(
            Actor {
                commands: receiver,
                tasks: JoinSet::new(),
                state: State {
                    data: HashMap::new(),
                    tags: BTreeMap::new(),
                    empty_hash: BaoFileHandle::new_partial(Hash::EMPTY),
                },
                options: Arc::new(Options::default()),
                temp_tags: Default::default(),
                protected: Default::default(),
                idle_waiters: Default::default(),
                virtuals: virtuals.clone(),
            }
            .run(),
        );

        let store = Self::from_sender(sender.into());
        if let Some(gc_config) = opts.gc_config {
            n0_future::task::spawn(run_gc(store.deref().clone(), gc_config));
        }

        (store, virtuals)
    }

    pub fn new_with_opts(opts: Options) -> Self {
        Self::new_with_virtuals(opts).0
    }
}

struct Actor {
    commands: tokio::sync::mpsc::Receiver<Command>,
    tasks: JoinSet<TaskResult>,
    state: State,
    #[allow(dead_code)]
    options: Arc<Options>,
    // temp tags
    temp_tags: TempTags,
    // idle waiters
    idle_waiters: Vec<irpc::channel::oneshot::Sender<()>>,
    protected: HashSet<Hash>,
    /// Live providers for virtual blobs, keyed by provider name.
    virtuals: VirtualProviders,
}

impl Actor {
    fn spawn<F, T>(&mut self, f: F)
    where
        F: Future<Output = T> + Send + 'static,
        T: Into<TaskResult>,
    {
        let span = tracing::Span::current();
        let fut = async move { f.await.into() }.instrument(span);
        self.tasks.spawn(fut);
    }

    async fn handle_command(&mut self, cmd: Command) -> Option<ShutdownMsg> {
        match cmd {
            Command::ImportBao(ImportBaoMsg {
                inner: ImportBaoRequest { hash, size },
                rx: data,
                tx,
                ..
            }) => {
                let entry = self.get_or_create_entry(hash);
                self.spawn(import_bao(entry, size, data, tx));
            }
            Command::WaitIdle(WaitIdleMsg { tx, .. }) => {
                trace!("wait idle");
                if self.tasks.is_empty() {
                    // we are currently idle
                    tx.send(()).await.ok();
                } else {
                    // wait for idle state
                    self.idle_waiters.push(tx);
                }
            }
            Command::Observe(ObserveMsg {
                inner: ObserveRequest { hash },
                tx,
                ..
            }) => {
                let entry = self.get_or_create_entry(hash);
                self.spawn(observe(entry, tx));
            }
            Command::ImportBytes(ImportBytesMsg {
                inner:
                    ImportBytesRequest {
                        data,
                        scope,
                        format,
                        ..
                    },
                tx,
                ..
            }) => {
                self.spawn(import_bytes(data, scope, format, tx));
            }
            Command::ImportByteStream(ImportByteStreamMsg { inner, tx, rx, .. }) => {
                self.spawn(import_byte_stream(inner.scope, inner.format, rx, tx));
            }
            Command::BuildOutboard(BuildOutboardMsg { inner, tx, rx, .. }) => {
                self.spawn(build_outboard(inner.scope, inner.format, rx, tx));
            }
            Command::AddVirtual(AddVirtualMsg {
                inner: AddVirtualRequest { hash, provider },
                tx,
                ..
            }) => match self.get(&hash) {
                Some(entry) => {
                    let modified = entry
                        .0
                        .state
                        .send_if_modified(|state: &mut BaoFileStorage| {
                            if let BaoFileStorage::Virtual(v) = state {
                                v.provider = provider.clone();
                                true
                            } else {
                                false
                            }
                        });
                    let res = if modified {
                        Ok(())
                    } else {
                        Err(api::Error::io(
                            io::ErrorKind::InvalidInput,
                            "entry is not virtual",
                        ))
                    };
                    tx.send(res).await.ok();
                }
                None => {
                    tx.send(Err(api::Error::io(
                        io::ErrorKind::NotFound,
                        "no virtual entry for hash",
                    )))
                    .await
                    .ok();
                }
            },
            Command::AddVirtualWithOutboard(AddVirtualWithOutboardMsg {
                inner:
                    AddVirtualWithOutboardRequest {
                        hash,
                        size,
                        outboard,
                        provider,
                    },
                tx,
                ..
            }) => {
                // The outboard must match the size: one 64-byte hash pair per
                // non-root tree node, and nothing at all for a single-chunk blob.
                let res = if outboard.len() as u64 != BaoTree::new(size, IROH_BLOCK_SIZE).outboard_size()
                {
                    Err(api::Error::io(
                        io::ErrorKind::InvalidInput,
                        "outboard length does not match blob size",
                    ))
                } else {
                    let entry = self.get_or_create_entry(hash);
                    let modified = entry.0.state.send_if_modified(|state: &mut BaoFileStorage| {
                        // Stored data takes precedence over a virtual source.
                        if matches!(state.deref(), BaoFileStorage::Complete(_)) {
                            return false;
                        }
                        *state = BaoFileStorage::Virtual(VirtualStorage::new(
                            outboard.clone(),
                            size,
                            provider.clone(),
                        ));
                        true
                    });
                    if modified {
                        Ok(())
                    } else {
                        Err(api::Error::io(
                            io::ErrorKind::InvalidInput,
                            "entry already exists as stored data",
                        ))
                    }
                };
                tx.send(res).await.ok();
            }
            Command::ImportPath(cmd) => {
                self.spawn(import_path(cmd));
            }
            Command::ExportBao(ExportBaoMsg {
                inner: ExportBaoRequest { hash, ranges },
                tx,
                ..
            }) => {
                let entry = self.get(&hash);
                let virtuals = self.virtuals.clone();
                self.spawn(export_bao(entry, ranges, tx, virtuals))
            }
            Command::ExportPath(cmd) => {
                let entry = self.get(&cmd.hash);
                self.spawn(export_path(entry, cmd));
            }
            Command::DeleteTags(cmd) => {
                let DeleteTagsMsg {
                    inner: DeleteTagsRequest { from, to },
                    tx,
                    ..
                } = cmd;
                info!("deleting tags from {:?} to {:?}", from, to);
                // state.tags.remove(&from.unwrap());
                // todo: more efficient impl
                let mut deleted = 0;
                self.state.tags.retain(|tag, _| {
                    if let Some(from) = &from {
                        if tag < from {
                            return true;
                        }
                    }
                    if let Some(to) = &to {
                        if tag >= to {
                            return true;
                        }
                    }
                    info!("    removing {:?}", tag);
                    deleted += 1;
                    false
                });
                tx.send(Ok(deleted)).await.ok();
            }
            Command::RenameTag(cmd) => {
                let RenameTagMsg {
                    inner: RenameTagRequest { from, to },
                    tx,
                    ..
                } = cmd;
                let tags = &mut self.state.tags;
                let value = match tags.remove(&from) {
                    Some(value) => value,
                    None => {
                        tx.send(Err(api::Error::io(
                            io::ErrorKind::NotFound,
                            format!("tag not found: {from:?}"),
                        )))
                        .await
                        .ok();
                        return None;
                    }
                };
                tags.insert(to, value);
                tx.send(Ok(())).await.ok();
                return None;
            }
            Command::ListTags(cmd) => {
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
                } = cmd;
                let tags = self
                    .state
                    .tags
                    .iter()
                    .filter(move |(tag, value)| {
                        if let Some(from) = &from {
                            if tag < &from {
                                return false;
                            }
                        }
                        if let Some(to) = &to {
                            if tag >= &to {
                                return false;
                            }
                        }
                        raw && value.format.is_raw() || hash_seq && value.format.is_hash_seq()
                    })
                    .map(|(tag, value)| TagInfo {
                        name: tag.clone(),
                        hash: value.hash,
                        format: value.format,
                    })
                    .map(Ok);
                tx.send(tags.collect()).await.ok();
            }
            Command::SetTag(SetTagMsg {
                inner: SetTagRequest { name: tag, value },
                tx,
                ..
            }) => {
                self.state.tags.insert(tag, value);
                tx.send(Ok(())).await.ok();
            }
            Command::CreateTag(CreateTagMsg {
                inner: CreateTagRequest { value },
                tx,
                ..
            }) => {
                let tag = Tag::auto(SystemTime::now(), |tag| self.state.tags.contains_key(tag));
                self.state.tags.insert(tag.clone(), value);
                tx.send(Ok(tag)).await.ok();
            }
            Command::CreateTempTag(cmd) => {
                trace!("{cmd:?}");
                self.create_temp_tag(cmd).await;
            }
            Command::ListTempTags(cmd) => {
                trace!("{cmd:?}");
                let tts = self.temp_tags.list();
                cmd.tx.send(tts).await.ok();
            }
            Command::ListBlobs(cmd) => {
                let ListBlobsMsg { tx, .. } = cmd;
                let blobs = self.state.data.keys().cloned().collect::<Vec<Hash>>();
                self.spawn(async move {
                    for blob in blobs {
                        if tx.send(Ok(blob)).await.is_err() {
                            break;
                        }
                    }
                });
            }
            Command::BlobStatus(cmd) => {
                trace!("{cmd:?}");
                let BlobStatusMsg {
                    inner: BlobStatusRequest { hash },
                    tx,
                    ..
                } = cmd;
                let res = match self.get(&hash) {
                    None => api::blobs::BlobStatus::NotFound,
                    Some(x) => {
                        let bitfield = x.0.state.borrow().bitfield();
                        if bitfield.is_complete() {
                            BlobStatus::Complete {
                                size: bitfield.size,
                            }
                        } else {
                            BlobStatus::Partial {
                                size: bitfield.validated_size(),
                            }
                        }
                    }
                };
                tx.send(res).await.ok();
            }
            Command::DeleteBlobs(cmd) => {
                trace!("{cmd:?}");
                let DeleteBlobsMsg {
                    inner: BlobDeleteRequest { hashes, force },
                    tx,
                    ..
                } = cmd;
                for hash in hashes {
                    if !force && self.protected.contains(&hash) {
                        continue;
                    }
                    self.state.data.remove(&hash);
                }
                tx.send(Ok(())).await.ok();
            }
            Command::Batch(cmd) => {
                trace!("{cmd:?}");
                let (id, scope) = self.temp_tags.create_scope();
                self.spawn(handle_batch(cmd, id, scope));
            }
            Command::ClearProtected(cmd) => {
                self.protected.clear();
                cmd.tx.send(Ok(())).await.ok();
            }
            Command::ExportRanges(cmd) => {
                let entry = self.get(&cmd.hash);
                self.spawn(export_ranges(cmd, entry));
            }
            Command::SyncDb(SyncDbMsg { tx, .. }) => {
                tx.send(Ok(())).await.ok();
            }
            Command::Shutdown(cmd) => {
                return Some(cmd);
            }
        }
        None
    }

    fn get(&mut self, hash: &Hash) -> Option<BaoFileHandle> {
        if *hash == Hash::EMPTY {
            Some(self.state.empty_hash.clone())
        } else {
            self.state.data.get(hash).cloned()
        }
    }

    fn get_or_create_entry(&mut self, hash: Hash) -> BaoFileHandle {
        if hash == Hash::EMPTY {
            self.state.empty_hash.clone()
        } else {
            self.state
                .data
                .entry(hash)
                .or_insert_with(|| BaoFileHandle::new_partial(hash))
                .clone()
        }
    }

    async fn create_temp_tag(&mut self, cmd: CreateTempTagMsg) {
        let CreateTempTagMsg { tx, inner, .. } = cmd;
        let mut tt = self.temp_tags.create(inner.scope, inner.value);
        if tx.is_rpc() {
            tt.leak();
        }
        tx.send(tt).await.ok();
    }

    async fn finish_import(&mut self, res: Result<ImportEntry>) {
        let import_data = match res {
            Ok(entry) => entry,
            Err(e) => {
                error!("import failed: {e}");
                return;
            }
        };
        let hash = import_data.outboard.root().into();
        let entry = self.get_or_create_entry(hash);
        entry
            .0
            .state
            .send_if_modified(|state: &mut BaoFileStorage| {
                let BaoFileStorage::Partial(_) = state.deref() else {
                    return false;
                };
                *state =
                    CompleteStorage::new(import_data.data, import_data.outboard.data.into()).into();
                true
            });
        let tt = self.temp_tags.create(
            import_data.scope,
            HashAndFormat {
                hash,
                format: import_data.format,
            },
        );
        import_data.tx.send(AddProgressItem::Done(tt)).await.ok();
    }

    /// Finish a [`build_outboard`] task: store the computed outboard as a
    /// virtual entry (no data) and protect it with a temp tag.
    ///
    /// If an entry already exists as `Complete` or `Virtual`, it is left
    /// untouched (store-before-virtual precedence).
    async fn finish_build_outboard(&mut self, res: Result<BuildOutboardResult>) {
        let r = match res {
            Ok(r) => r,
            Err(e) => {
                error!("build_outboard failed: {e}");
                return;
            }
        };
        let entry = self.get_or_create_entry(r.hash);
        entry
            .0
            .state
            .send_if_modified(|state: &mut BaoFileStorage| {
                // Only convert a fresh or in-progress Partial entry. A Complete
                // entry (data already stored) or an existing Virtual entry is left
                // untouched: stored data takes precedence over a virtual source.
                if matches!(state.deref(), BaoFileStorage::Partial(_)) {
                    *state = BaoFileStorage::Virtual(VirtualStorage::new(
                        r.outboard.clone(),
                        r.size,
                        String::new(),
                    ));
                    return true;
                }
                false
            });
        let tt = self.temp_tags.create(
            r.scope,
            HashAndFormat {
                hash: r.hash,
                format: r.format,
            },
        );
        r.tx.send(AddProgressItem::Done(tt)).await.ok();
    }

    fn log_task_result(&self, res: Result<TaskResult, JoinError>) -> Option<TaskResult> {
        match res {
            Ok(x) => Some(x),
            Err(e) => {
                if e.is_cancelled() {
                    trace!("task cancelled: {e}");
                } else {
                    error!("task failed: {e}");
                }
                None
            }
        }
    }

    pub async fn run(mut self) {
        let shutdown = loop {
            tokio::select! {
                cmd = self.commands.recv() => {
                    let Some(cmd) = cmd else {
                        // last sender has been dropped.
                        // exit immediately.
                        break None;
                    };
                    if let Some(cmd) = self.handle_command(cmd).await {
                        break Some(cmd);
                    }
                }
                Some(res) = self.tasks.join_next(), if !self.tasks.is_empty() => {
                    let Some(res) = self.log_task_result(res) else {
                        continue;
                    };
                    match res {
                        TaskResult::Import(res) => {
                            self.finish_import(res).await;
                        }
                        TaskResult::BuildOutboard(res) => {
                            self.finish_build_outboard(res).await;
                        }
                        TaskResult::Scope(scope) => {
                            self.temp_tags.end_scope(scope);
                        }
                        TaskResult::Unit(_) => {}
                    }
                    if self.tasks.is_empty() {
                        // we are idle now
                        for tx in self.idle_waiters.drain(..) {
                            tx.send(()).await.ok();
                        }
                    }
                }
            }
        };
        if let Some(shutdown) = shutdown {
            shutdown.tx.send(()).await.ok();
        }
    }
}

async fn handle_batch(cmd: BatchMsg, id: Scope, scope: Arc<TempTagScope>) -> Scope {
    if let Err(cause) = handle_batch_impl(cmd, id, &scope).await {
        error!("batch failed: {cause}");
    }
    id
}

async fn handle_batch_impl(cmd: BatchMsg, id: Scope, scope: &Arc<TempTagScope>) -> api::Result<()> {
    let BatchMsg { tx, mut rx, .. } = cmd;
    trace!("created scope {}", id);
    tx.send(id).await?;
    while let Some(msg) = rx.recv().await? {
        match msg {
            BatchResponse::Drop(msg) => scope.on_drop(&msg),
            BatchResponse::Ping => {}
        }
    }
    Ok(())
}

async fn export_ranges(mut cmd: ExportRangesMsg, entry: Option<BaoFileHandle>) {
    let Some(entry) = entry else {
        let err = io::Error::new(io::ErrorKind::NotFound, "hash not found");
        cmd.tx.send(ExportRangesItem::Error(err.into())).await.ok();
        return;
    };
    if let Err(cause) = export_ranges_impl(cmd.inner, &mut cmd.tx, entry).await {
        cmd.tx
            .send(ExportRangesItem::Error(cause.into()))
            .await
            .ok();
    }
}

async fn export_ranges_impl(
    cmd: ExportRangesRequest,
    tx: &mut mpsc::Sender<ExportRangesItem>,
    entry: BaoFileHandle,
) -> io::Result<()> {
    let ExportRangesRequest { ranges, hash } = cmd;
    let bitfield = entry.bitfield();
    trace!(
        "exporting ranges: {hash} {ranges:?} size={}",
        bitfield.size()
    );
    debug_assert!(entry.hash() == hash, "hash mismatch");
    let data = entry.data_reader();
    let size = bitfield.size();
    for range in ranges.iter() {
        let range = match range {
            RangeSetRange::Range(range) => size.min(*range.start)..size.min(*range.end),
            RangeSetRange::RangeFrom(range) => size.min(*range.start)..size,
        };
        let requested = ChunkRanges::bytes(range.start..range.end);
        if !bitfield.ranges.is_superset(&requested) {
            return Err(io::Error::other(format!(
                "missing range: {requested:?}, present: {bitfield:?}",
            )));
        }
        let bs = 1024;
        let mut offset = range.start;
        loop {
            let end: u64 = (offset + bs).min(range.end);
            let size = (end - offset) as usize;
            tx.send(
                Leaf {
                    offset,
                    data: data.read_bytes_at(offset, size)?,
                }
                .into(),
            )
            .await?;
            offset = end;
            if offset >= range.end {
                break;
            }
        }
    }
    Ok(())
}

fn chunk_range(leaf: &Leaf) -> ChunkRanges {
    let start = ChunkNum::chunks(leaf.offset);
    let end = ChunkNum::chunks(leaf.offset + leaf.data.len() as u64);
    (start..end).into()
}

async fn import_bao(
    entry: BaoFileHandle,
    size: NonZeroU64,
    mut stream: mpsc::Receiver<BaoContentItem>,
    tx: irpc::channel::oneshot::Sender<api::Result<()>>,
) {
    let size = size.get();
    entry
        .0
        .state
        .send_if_modified(|state: &mut BaoFileStorage| {
            let BaoFileStorage::Partial(entry) = state else {
                // entry was already completed, no need to write
                return false;
            };
            entry.size.write(0, size);
            false
        });
    let tree = BaoTree::new(size, IROH_BLOCK_SIZE);
    while let Some(item) = stream.recv().await.unwrap() {
        entry.0.state.send_if_modified(|state| {
            let BaoFileStorage::Partial(partial) = state else {
                // entry was already completed, no need to write
                return false;
            };
            match item {
                BaoContentItem::Parent(parent) => {
                    if let Some(offset) = tree.pre_order_offset(parent.node) {
                        let mut pair = [0u8; 64];
                        pair[..32].copy_from_slice(parent.pair.0.as_bytes());
                        pair[32..].copy_from_slice(parent.pair.1.as_bytes());
                        partial
                            .outboard
                            .write_at(offset * 64, &pair)
                            .expect("writing to mem can never fail");
                    }
                    false
                }
                BaoContentItem::Leaf(leaf) => {
                    let start = leaf.offset;
                    partial
                        .data
                        .write_at(start, &leaf.data)
                        .expect("writing to mem can never fail");
                    let added = chunk_range(&leaf);
                    let update = partial.bitfield.update(&Bitfield::new(added.clone(), size));
                    if update.new_state().complete {
                        let data = std::mem::take(&mut partial.data);
                        let outboard = std::mem::take(&mut partial.outboard);
                        let data: Bytes = <Vec<u8>>::try_from(data).unwrap().into();
                        let outboard: Bytes = <Vec<u8>>::try_from(outboard).unwrap().into();
                        *state = CompleteStorage::new(data, outboard).into();
                    }
                    update.changed()
                }
            }
        });
    }
    tx.send(Ok(())).await.ok();
}

#[instrument(skip_all, fields(hash = tracing::field::Empty))]
async fn export_bao(
    entry: Option<BaoFileHandle>,
    ranges: ChunkRanges,
    mut sender: mpsc::Sender<EncodedItem>,
    virtuals: VirtualProviders,
) {
    let Some(entry) = entry else {
        let err = EncodeError::Io(io::Error::new(io::ErrorKind::NotFound, "hash not found"));
        sender.send(err.into()).await.ok();
        return;
    };
    tracing::Span::current().record("hash", tracing::field::display(entry.hash));
    let outboard = entry.outboard_reader();
    let is_virtual = matches!(entry.0.state.borrow().deref(), BaoFileStorage::Virtual(_));
    if is_virtual {
        // Serve a virtual entry via the provider named in the entry. The
        // outboard comes from the entry; the data comes from the provider via
        // `VirtualProviders`. An entry whose provider is not registered (or
        // whose provider has no data for this hash) is served as not found.
        let provider_name = match entry.0.state.borrow().deref() {
            BaoFileStorage::Virtual(v) => v.provider.clone(),
            _ => unreachable!("checked above"),
        };
        let Some(provider) = virtuals.get(&provider_name) else {
            let err = EncodeError::Io(io::Error::new(
                io::ErrorKind::NotFound,
                "no provider registered for virtual entry",
            ));
            sender.send(err.into()).await.ok();
            return;
        };
        let Some(source) = provider.reader_for(&entry.hash) else {
            let err = EncodeError::Io(io::Error::new(
                io::ErrorKind::NotFound,
                "provider has no data for virtual entry",
            ));
            sender.send(err.into()).await.ok();
            return;
        };
        let tx = BaoTreeSender::new(&mut sender);
        traverse_ranges_validated(DynReadBytesAt(source), outboard, &ranges, tx)
            .await
            .ok();
    } else {
        let data = entry.data_reader();
        let tx = BaoTreeSender::new(&mut sender);
        traverse_ranges_validated(data, outboard, &ranges, tx)
            .await
            .ok();
    }
}

/// Build the BLAKE3 outboard for a stream of bytes without storing the data.
///
/// The bytes are accumulated, the outboard and root hash are computed, and the
/// data is then discarded. The result is stored as a virtual entry by
/// [`Actor::finish_build_outboard`].
async fn build_outboard(
    scope: Scope,
    format: BlobFormat,
    mut rx: mpsc::Receiver<ImportByteStreamUpdate>,
    tx: mpsc::Sender<AddProgressItem>,
) -> Result<BuildOutboardResult> {
    let mut res = Vec::new();
    loop {
        match rx.recv().await {
            Ok(Some(ImportByteStreamUpdate::Bytes(data))) => {
                res.extend_from_slice(&data);
                tx.send(AddProgressItem::CopyProgress(res.len() as u64))
                    .await?;
            }
            Ok(Some(ImportByteStreamUpdate::Done)) => break,
            Ok(None) => {
                return Err(api::Error::io(
                    io::ErrorKind::UnexpectedEof,
                    "byte stream ended unexpectedly",
                )
                .into());
            }
            Err(e) => {
                return Err(e.into());
            }
        }
    }
    tx.send(AddProgressItem::Size(res.len() as u64)).await?;
    tx.send(AddProgressItem::CopyDone).await?;
    let outboard = PreOrderMemOutboard::create(&res, IROH_BLOCK_SIZE);
    let hash: Hash = outboard.root.into();
    let size = res.len() as u64;
    let outboard_bytes: Bytes = outboard.data.into();
    // The data is intentionally not retained; a virtual entry stores only the
    // outboard and is served from a registered live data source.
    drop(res);
    Ok(BuildOutboardResult {
        hash,
        outboard: outboard_bytes,
        size,
        scope,
        format,
        tx,
    })
}

#[instrument(skip_all, fields(hash = %entry.hash.fmt_short()))]
async fn observe(entry: BaoFileHandle, tx: mpsc::Sender<api::blobs::Bitfield>) {
    entry.subscribe().forward(tx).await.ok();
}

async fn import_bytes(
    data: Bytes,
    scope: Scope,
    format: BlobFormat,
    tx: mpsc::Sender<AddProgressItem>,
) -> Result<ImportEntry> {
    tx.send(AddProgressItem::Size(data.len() as u64)).await?;
    tx.send(AddProgressItem::CopyDone).await?;
    let outboard = PreOrderMemOutboard::create(&data, IROH_BLOCK_SIZE);
    Ok(ImportEntry {
        data,
        outboard,
        scope,
        format,
        tx,
    })
}

async fn import_byte_stream(
    scope: Scope,
    format: BlobFormat,
    mut rx: mpsc::Receiver<ImportByteStreamUpdate>,
    tx: mpsc::Sender<AddProgressItem>,
) -> Result<ImportEntry> {
    let mut res = Vec::new();
    loop {
        match rx.recv().await {
            Ok(Some(ImportByteStreamUpdate::Bytes(data))) => {
                res.extend_from_slice(&data);
                tx.send(AddProgressItem::CopyProgress(res.len() as u64))
                    .await?;
            }
            Ok(Some(ImportByteStreamUpdate::Done)) => {
                break;
            }
            Ok(None) => {
                return Err(api::Error::io(
                    io::ErrorKind::UnexpectedEof,
                    "byte stream ended unexpectedly",
                )
                .into());
            }
            Err(e) => {
                return Err(e.into());
            }
        }
    }
    import_bytes(res.into(), scope, format, tx).await
}

#[cfg(wasm_browser)]
async fn import_path(cmd: ImportPathMsg) -> Result<ImportEntry> {
    let _: ImportPathRequest = cmd.inner;
    Err(n0_error::anyerr!(
        "import_path is not supported in the browser"
    ))
}

#[instrument(skip_all, fields(path = %cmd.path.display()))]
#[cfg(not(wasm_browser))]
async fn import_path(cmd: ImportPathMsg) -> Result<ImportEntry> {
    use tokio::io::AsyncReadExt;
    let ImportPathMsg {
        inner:
            ImportPathRequest {
                path,
                scope,
                format,
                ..
            },
        tx,
        ..
    } = cmd;
    let mut res = Vec::new();
    let mut file = tokio::fs::File::open(path).await?;
    let mut buf = [0u8; 1024 * 64];
    loop {
        let size = file.read(&mut buf).await?;
        if size == 0 {
            break;
        }
        res.extend_from_slice(&buf[..size]);
        tx.send(AddProgressItem::CopyProgress(res.len() as u64))
            .await?;
    }
    import_bytes(res.into(), scope, format, tx).await
}

#[instrument(skip_all, fields(hash = %cmd.hash.fmt_short(), path = %cmd.target.display()))]
async fn export_path(entry: Option<BaoFileHandle>, cmd: ExportPathMsg) {
    let ExportPathMsg { inner, mut tx, .. } = cmd;
    let Some(entry) = entry else {
        tx.send(ExportProgressItem::Error(api::Error::io(
            io::ErrorKind::NotFound,
            "hash not found",
        )))
        .await
        .ok();
        return;
    };
    match export_path_impl(entry, inner, &mut tx).await {
        Ok(()) => tx.send(ExportProgressItem::Done).await.ok(),
        Err(e) => tx.send(ExportProgressItem::Error(e.into())).await.ok(),
    };
}

async fn export_path_impl(
    entry: BaoFileHandle,
    cmd: ExportPathRequest,
    tx: &mut mpsc::Sender<ExportProgressItem>,
) -> io::Result<()> {
    let ExportPathRequest { target, .. } = cmd;
    if !target.is_absolute() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "path is not absolute",
        ));
    }
    if let Some(parent) = target.parent() {
        std::fs::create_dir_all(parent)?;
    }
    // todo: for partial entries make sure to only write the part that is actually present
    let mut file = std::fs::File::create(target)?;
    let size = entry.0.state.borrow().size();
    tx.send(ExportProgressItem::Size(size)).await?;
    let mut buf = [0u8; 1024 * 64];
    for offset in (0..size).step_by(1024 * 64) {
        let len = std::cmp::min(size - offset, 1024 * 64) as usize;
        let buf = &mut buf[..len];
        entry.0.state.borrow().data().read_exact_at(offset, buf)?;
        file.write_all(buf)?;
        tx.try_send(ExportProgressItem::CopyProgress(offset))
            .await?;
        yield_now().await;
    }
    Ok(())
}

struct ImportEntry {
    scope: Scope,
    format: BlobFormat,
    data: Bytes,
    outboard: PreOrderMemOutboard,
    tx: mpsc::Sender<AddProgressItem>,
}

pub struct DataReader(BaoFileHandle);

impl ReadBytesAt for DataReader {
    fn read_bytes_at(&self, offset: u64, size: usize) -> std::io::Result<Bytes> {
        let entry = self.0 .0.state.borrow();
        entry.data().read_bytes_at(offset, size)
    }
}

pub struct OutboardReader {
    hash: blake3::Hash,
    tree: BaoTree,
    data: BaoFileHandle,
}

impl Outboard for OutboardReader {
    fn root(&self) -> blake3::Hash {
        self.hash
    }

    fn tree(&self) -> BaoTree {
        self.tree
    }

    fn load(&self, node: TreeNode) -> io::Result<Option<(blake3::Hash, blake3::Hash)>> {
        let Some(offset) = self.tree.pre_order_offset(node) else {
            return Ok(None);
        };
        let mut buf = [0u8; 64];
        let size = self
            .data
            .0
            .state
            .borrow()
            .outboard()
            .read_at(offset * 64, &mut buf)?;
        if size != 64 {
            return Err(io::Error::new(io::ErrorKind::UnexpectedEof, "short read"));
        }
        let left: [u8; 32] = buf[..32].try_into().unwrap();
        let right: [u8; 32] = buf[32..].try_into().unwrap();
        Ok(Some((left.into(), right.into())))
    }
}

struct State {
    data: HashMap<Hash, BaoFileHandle>,
    tags: BTreeMap<Tag, HashAndFormat>,
    empty_hash: BaoFileHandle,
}

#[derive(Debug, derive_more::From)]
pub enum BaoFileStorage {
    Partial(PartialMemStorage),
    Complete(CompleteStorage),
    /// A virtual entry: the outboard is stored, but the data is not. The data
    /// is served on demand by the provider named in the entry, registered in
    /// [`VirtualProviders`].
    Virtual(VirtualStorage),
}

/// Stored outboard for a virtual blob, with no data.
#[derive(Debug, Clone)]
pub struct VirtualStorage {
    pub(crate) outboard: Bytes,
    pub(crate) size: u64,
    /// The name of the provider that serves this entry's data.
    pub(crate) provider: String,
}

impl VirtualStorage {
    pub fn new(outboard: Bytes, size: u64, provider: String) -> Self {
        Self {
            outboard,
            size,
            provider,
        }
    }
    pub fn size(&self) -> u64 {
        self.size
    }
}

impl BaoFileStorage {
    /// Get the bitfield of the storage.
    pub fn bitfield(&self) -> Bitfield {
        match self {
            Self::Partial(entry) => entry.bitfield.clone(),
            Self::Complete(entry) => Bitfield::complete(entry.size()),
            Self::Virtual(entry) => Bitfield::complete(entry.size()),
        }
    }
}

#[derive(Debug)]
pub struct BaoFileHandleInner {
    state: watch::Sender<BaoFileStorage>,
    hash: Hash,
}

/// A cheaply cloneable handle to a bao file, including the hash
#[derive(Debug, Clone, derive_more::Deref)]
pub struct BaoFileHandle(Arc<BaoFileHandleInner>);

impl BaoFileHandle {
    pub fn new_partial(hash: Hash) -> Self {
        let (state, _) = watch::channel(BaoFileStorage::Partial(PartialMemStorage {
            data: SparseMemFile::new(),
            outboard: SparseMemFile::new(),
            size: SizeInfo::default(),
            bitfield: Bitfield::empty(),
        }));
        Self(Arc::new(BaoFileHandleInner { state, hash }))
    }

    /// Create a virtual entry handle: outboard stored, no data.
    pub fn new_virtual(hash: Hash, outboard: Bytes, size: u64) -> Self {
        let (state, _) = watch::channel(BaoFileStorage::Virtual(VirtualStorage::new(
            outboard,
            size,
            String::new(),
        )));
        Self(Arc::new(BaoFileHandleInner { state, hash }))
    }

    pub fn hash(&self) -> Hash {
        self.hash
    }

    pub fn bitfield(&self) -> Bitfield {
        self.0.state.borrow().bitfield()
    }

    pub fn subscribe(&self) -> BaoFileStorageSubscriber {
        BaoFileStorageSubscriber::new(self.0.state.subscribe())
    }

    pub fn data_reader(&self) -> DataReader {
        DataReader(self.clone())
    }

    pub fn outboard_reader(&self) -> OutboardReader {
        let entry = self.0.state.borrow();
        let hash = self.hash.into();
        let tree = BaoTree::new(entry.size(), IROH_BLOCK_SIZE);
        OutboardReader {
            hash,
            tree,
            data: self.clone(),
        }
    }
}

impl Default for BaoFileStorage {
    fn default() -> Self {
        Self::Partial(Default::default())
    }
}

impl BaoFileStorage {
    fn data(&self) -> &[u8] {
        match self {
            Self::Partial(entry) => entry.data.as_ref(),
            Self::Complete(entry) => &entry.data,
            // A virtual entry has no stored data; it is served from a live
            // source. This is only reached via `DataReader`, which is not used
            // for virtual entries (see `export_bao`).
            Self::Virtual(_) => &[],
        }
    }

    fn outboard(&self) -> &[u8] {
        match self {
            Self::Partial(entry) => entry.outboard.as_ref(),
            Self::Complete(entry) => &entry.outboard,
            Self::Virtual(entry) => &entry.outboard,
        }
    }

    fn size(&self) -> u64 {
        match self {
            Self::Partial(entry) => entry.current_size(),
            Self::Complete(entry) => entry.size(),
            Self::Virtual(entry) => entry.size(),
        }
    }
}

#[derive(Debug, Clone)]
pub struct CompleteStorage {
    pub(crate) data: Bytes,
    pub(crate) outboard: Bytes,
}

impl CompleteStorage {
    pub fn create(data: Bytes) -> (Hash, Self) {
        let outboard = PreOrderMemOutboard::create(&data, IROH_BLOCK_SIZE);
        let hash = outboard.root().into();
        let outboard = outboard.data.into();
        let entry = Self::new(data, outboard);
        (hash, entry)
    }

    pub fn new(data: Bytes, outboard: Bytes) -> Self {
        Self { data, outboard }
    }

    pub fn size(&self) -> u64 {
        self.data.len() as u64
    }
}

#[allow(dead_code)]
fn print_outboard(hashes: &[u8]) {
    assert!(hashes.len().is_multiple_of(64));
    for chunk in hashes.chunks(64) {
        let left: [u8; 32] = chunk[..32].try_into().unwrap();
        let right: [u8; 32] = chunk[32..].try_into().unwrap();
        let left = blake3::Hash::from(left);
        let right = blake3::Hash::from(right);
        println!("l: {left:?}, r: {right:?}");
    }
}

pub struct BaoFileStorageSubscriber {
    receiver: watch::Receiver<BaoFileStorage>,
}

impl BaoFileStorageSubscriber {
    pub fn new(receiver: watch::Receiver<BaoFileStorage>) -> Self {
        Self { receiver }
    }

    /// Forward observed *values* to the given sender
    ///
    /// Returns an error if sending fails, or if the last sender is dropped
    pub async fn forward(mut self, mut tx: mpsc::Sender<Bitfield>) -> Result<()> {
        let value = self.receiver.borrow().bitfield();
        tx.send(value).await?;
        loop {
            self.update_or_closed(&mut tx).await?;
            let value = self.receiver.borrow().bitfield();
            tx.send(value.clone()).await?;
        }
    }

    /// Forward observed *deltas* to the given sender
    ///
    /// Returns an error if sending fails, or if the last sender is dropped
    #[allow(dead_code)]
    pub async fn forward_delta(mut self, mut tx: mpsc::Sender<Bitfield>) -> Result<()> {
        let value = self.receiver.borrow().bitfield();
        let mut old = value.clone();
        tx.send(value).await?;
        loop {
            self.update_or_closed(&mut tx).await?;
            let new = self.receiver.borrow().bitfield();
            let diff = old.diff(&new);
            if diff.is_empty() {
                continue;
            }
            tx.send(diff).await?;
            old = new;
        }
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::store::virtual_blob::{DynVirtualSource, Provider};
    use n0_future::StreamExt;
    use testresult::TestResult;

    #[tokio::test]
    async fn smoke() -> TestResult<()> {
        let store = MemStore::new();
        let tt = store.add_bytes(vec![0u8; 1024 * 64]).temp_tag().await?;
        let hash = tt.hash();
        println!("hash: {hash:?}");
        let mut stream = store.export_bao(hash, ChunkRanges::all()).stream();
        while let Some(item) = stream.next().await {
            println!("item: {item:?}");
        }
        let stream = store.export_bao(hash, ChunkRanges::all());
        let exported = stream.bao_to_vec().await?;

        let store2 = MemStore::new();
        let mut or = store2.observe(hash).stream().await?;
        n0_future::task::spawn(async move {
            while let Some(event) = or.next().await {
                println!("event: {event:?}");
            }
        });
        store2
            .import_bao_bytes(hash, ChunkRanges::all(), exported.clone())
            .await?;

        let exported2 = store2
            .export_bao(hash, ChunkRanges::all())
            .bao_to_vec()
            .await?;
        assert_eq!(exported, exported2);

        Ok(())
    }

    /// A provider that serves a fixed set of bytes for any hash.
    #[derive(Clone)]
    struct TestProvider {
        data: Bytes,
    }

    impl TestProvider {
        fn new(data: Bytes) -> Self {
            Self { data }
        }
    }

    impl Provider for TestProvider {
        fn reader_for(&self, _hash: &Hash) -> Option<DynVirtualSource> {
            Some(Arc::new(self.data.clone()))
        }
    }

    /// A virtual blob: build the outboard from a stream without storing data,
    /// associate a provider, register it, and serve it. The served bytes must
    /// be byte-identical to a normally-stored blob of the same content, and
    /// must round-trip back to the original data through `import_bao`.
    #[tokio::test]
    async fn virtual_blob_roundtrip() -> TestResult<()> {
        use std::sync::Arc;

        let data = Bytes::from(
            (0..1024 * 64)
                .map(|i| (i & 0xff) as u8)
                .collect::<Vec<u8>>(),
        );
        let (store, virtuals) = MemStore::new_with_virtuals(Options::default());

        // Build the outboard from a stream of the data, without storing it.
        let stream = n0_future::stream::iter(vec![Ok::<_, io::Error>(data.clone())]);
        let tt = store.build_outboard(stream).await.temp_tag().await?;
        let hash = tt.hash();

        // An entry with no provider declared is served as not found.
        let err: crate::api::Error = store
            .export_bao(hash, ChunkRanges::all())
            .bao_to_vec()
            .await
            .unwrap_err()
            .into();
        assert!(
            matches!(err, crate::api::Error::Io(e) if e.kind() == io::ErrorKind::NotFound),
            "virtual entry without a provider must serve as not found"
        );

        // Declaring a provider that is not registered is still not found.
        store.add_virtual(hash, "test-provider").await?;
        let err: crate::api::Error = store
            .export_bao(hash, ChunkRanges::all())
            .bao_to_vec()
            .await
            .unwrap_err()
            .into();
        assert!(
            matches!(err, crate::api::Error::Io(e) if e.kind() == io::ErrorKind::NotFound),
            "virtual entry with an unregistered provider must serve as not found"
        );

        // Register the provider and serve.
        virtuals.register("test-provider", Arc::new(TestProvider::new(data.clone())))?;
        let exported_virtual = store
            .export_bao(hash, ChunkRanges::all())
            .bao_to_vec()
            .await?;

        // Serving the virtual entry must match a normal stored blob exactly.
        let store2 = MemStore::new();
        let tt2 = store2.add_bytes(data.clone()).temp_tag().await?;
        assert_eq!(tt2.hash(), hash, "hashes must match for identical content");
        let exported_stored = store2
            .export_bao(hash, ChunkRanges::all())
            .bao_to_vec()
            .await?;
        assert_eq!(
            exported_virtual, exported_stored,
            "virtual export must be byte-identical to stored export"
        );

        // The encoded bytes round-trip through import_bao back to the data.
        let store3 = MemStore::new();
        store3
            .import_bao_bytes(hash, ChunkRanges::all(), exported_virtual.clone())
            .await?;
        let got = store3.get_bytes(hash).await?;
        assert_eq!(got, data, "round-tripped data must match the original");

        Ok(())
    }
    /// A stored (Complete) blob must win over a virtual entry for the same hash.
    ///
    /// Storing a blob then building an outboard for the same hash is a no-op
    /// on the entry (it stays `Complete`), so attaching a provider correctly
    /// errors and serving uses the stored data.
    #[tokio::test]
    async fn virtual_entry_does_not_shadow_stored_blob() -> TestResult<()> {
        let data = Bytes::from(
            (0..1024 * 32)
                .map(|i| (i & 0xff) as u8)
                .collect::<Vec<u8>>(),
        );
        let (store, _virtuals) = MemStore::new_with_virtuals(Options::default());

        // Store the blob normally (Complete).
        let tt = store.add_bytes(data.clone()).temp_tag().await?;
        let hash = tt.hash();

        // build_outboard for the same hash is a no-op on the entry, which
        // stays Complete (store-before-virtual precedence).
        let stream = n0_future::stream::iter(vec![Ok::<_, io::Error>(data.clone())]);
        let _tt2 = store.build_outboard(stream).await.temp_tag().await?;

        // The entry is not virtual, so a provider cannot be attached.
        assert!(
            store.add_virtual(hash, "test-provider").await.is_err(),
            "add_virtual must fail for a stored (non-virtual) entry"
        );

        // Serving uses the stored data.
        let exported = store
            .export_bao(hash, ChunkRanges::all())
            .bao_to_vec()
            .await?;
        let store2 = MemStore::new();
        store2
            .import_bao_bytes(hash, ChunkRanges::all(), exported)
            .await?;
        let got = store2.get_bytes(hash).await?;
        assert_eq!(got, data, "stored data must be served");
        Ok(())
    }
}

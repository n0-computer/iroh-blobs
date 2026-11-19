//! A memory backed [`Backend`], to check that the abstraction carries a second implementation.
//!
//! The "storage" is a map shared by all entities through the backend value:
//! entities read their blob from it and write it back, and keep only
//! coordination state themselves (generation, freeze, delete). The metadata
//! store is an in-memory actor answering [`Request`]. Imports still stage in a
//! temp directory, since staging is not part of the backend yet.
use std::{
    collections::{BTreeMap, HashMap},
    io,
    ops::Bound,
    path::Path,
    sync::{Arc, Mutex},
    time::SystemTime,
};

use bao_tree::io::{
    mixed::ReadBytesAt,
    sync::{ReadAt, Size},
    BaoContentItem,
};
use bytes::Bytes;
use n0_future::stream;
use tokio::sync::watch;

use crate::{
    api::{
        self,
        blobs::{Bitfield, BlobStatus},
        proto::{
            AddProgressItem, BlobStatusMsg, BlobStatusRequest, CreateTagRequest, DeleteTagsRequest,
            ExportBaoMsg, ExportPathMsg, ExportPathRequest, ExportProgressItem, ExportRangesItem,
            ExportRangesMsg, ImportBaoMsg, ImportBaoRequest, ListTagsRequest, ObserveMsg,
            RenameTagRequest, SetTagRequest, ShutdownMsg, SyncDbMsg,
        },
        tags::TagInfo,
        Tag,
    },
    store::engine::{
        bao::{export_bao_impl, export_ranges_impl, import_bao_impl, BaoAccess},
        metadata::{
            entry_state::{DataLocation, EntryState, OutboardLocation},
            parts::UnreferencedParts,
            proto::{
                CreateTagMsg, Delete, DeleteTagsMsg, GcSnapshot, Get, GetResult, ListBlobs,
                ListTags, Listing, RenameTagMsg, Set, SetTagMsg, Update, UpdateAwait,
            },
            Failure, MetaChannels, MetaTask, Request,
        },
        Backend, EntityApi, EntityGlobal, ImportEntry, ImportEntryMsg, ImportSource, Reset,
        StoreOptions,
    },
    store::util::{MemOrFile, PartialMemStorage},
    util::temp_tag::TempTag,
    Hash, HashAndFormat,
};

/// A store on the memory backend, staging imports in `temp_dir`.
pub(crate) async fn store(temp_dir: &Path) -> n0_error::Result<api::Store> {
    let options = StoreOptions {
        inline: Default::default(),
        temp_path: temp_dir.to_path_buf(),
        gc: None,
    };
    let started = super::start(options, MemBackend::default()).await?;
    Ok(api::Store::from_client(started.commands))
}

/// One blob in the shared storage.
#[derive(Debug)]
enum Blob {
    Partial(PartialMemStorage),
    Complete { data: Bytes, outboard: Bytes },
}

impl Blob {
    fn bitfield(&self) -> Bitfield {
        match self {
            Blob::Partial(p) => p.bitfield().clone(),
            Blob::Complete { data, .. } => Bitfield::complete(data.len() as u64),
        }
    }
}

type Storage = Arc<Mutex<HashMap<Hash, Blob>>>;

/// The memory backend: the shared storage is the entities' configuration.
#[derive(Debug, Default)]
pub(crate) struct MemBackend {
    storage: Storage,
}

impl Backend for MemBackend {
    type Entity = MemEntity;

    fn start_meta(&self, channels: MetaChannels) -> n0_error::Result<impl MetaTask> {
        Ok(MemMeta::default().run(channels))
    }
}

/// The metadata of the memory backend.
#[derive(Debug, Default)]
struct MemMeta {
    blobs: BTreeMap<Hash, EntryState<Bytes>>,
    tags: BTreeMap<Tag, HashAndFormat>,
}

/// Merges a new entry state into an old one. Entries only grow.
fn merge(old: Option<EntryState<Bytes>>, new: EntryState<Bytes>) -> EntryState<Bytes> {
    match (old, new) {
        (Some(old @ EntryState::Complete { .. }), EntryState::Partial { .. }) => old,
        (Some(EntryState::Partial { size: old }), EntryState::Partial { size }) => {
            EntryState::Partial { size: size.or(old) }
        }
        (_, new) => new,
    }
}

impl MemMeta {
    async fn run(mut self, mut channels: MetaChannels) -> Result<(), Failure> {
        while let Some(request) = channels.requests.recv().await {
            if let Some(shutdown) = self.handle(request).await {
                shutdown.tx.send(()).await.ok();
                break;
            }
        }
        Ok(())
    }

    /// Handles one request. Returns the shutdown request, after which the store ends.
    async fn handle(&mut self, request: Request) -> Option<ShutdownMsg> {
        match request {
            Request::Get(Get { hash, tx, .. }) => tx.send(GetResult {
                state: Ok(self.blobs.get(&hash).cloned()),
            }),
            Request::Set(Set {
                hash, state, tx, ..
            }) => {
                self.blobs.insert(hash, state);
                tx.send(Ok(()));
            }
            Request::Update(Update { hash, state, .. }) => {
                let merged = merge(self.blobs.remove(&hash), state);
                self.blobs.insert(hash, merged);
            }
            Request::UpdateAwait(UpdateAwait {
                hash, state, tx, ..
            }) => {
                let merged = merge(self.blobs.remove(&hash), state);
                self.blobs.insert(hash, merged);
                tx.send(Ok(()));
            }
            Request::Delete(Delete { hash, tx, .. }) => {
                self.blobs.remove(&hash);
                tx.send(Ok(()));
            }
            Request::SetTag(SetTagMsg {
                inner: SetTagRequest { name, value },
                tx,
                ..
            }) => {
                self.tags.insert(name, value);
                tx.send(Ok(())).await.ok();
            }
            Request::CreateTag(CreateTagMsg {
                inner: CreateTagRequest { value },
                tx,
                ..
            }) => {
                let tag = Tag::auto(SystemTime::now(), |x| {
                    self.tags.contains_key(&Tag(Bytes::copy_from_slice(x)))
                });
                self.tags.insert(tag.clone(), value);
                tx.send(Ok(tag)).await.ok();
            }
            Request::DeleteTags(DeleteTagsMsg {
                inner: DeleteTagsRequest { from, to },
                tx,
                ..
            }) => {
                let range = (bound(from, Bound::Included), bound(to, Bound::Excluded));
                let names: Vec<Tag> = self.tags.range(range).map(|(k, _)| k.clone()).collect();
                for name in &names {
                    self.tags.remove(name);
                }
                tx.send(Ok(names.len() as u64)).await.ok();
            }
            Request::RenameTag(RenameTagMsg {
                inner: RenameTagRequest { from, to },
                tx,
                ..
            }) => {
                let res = match self.tags.remove(&from) {
                    Some(value) => {
                        self.tags.insert(to, value);
                        Ok(())
                    }
                    None => Err(api::Error::io(io::ErrorKind::NotFound, "tag not found")),
                };
                tx.send(res).await.ok();
            }
            Request::ListTags(ListTags { request, tx, .. }) => {
                let ListTagsRequest {
                    from,
                    to,
                    raw,
                    hash_seq,
                } = request;
                let range = (bound(from, Bound::Included), bound(to, Bound::Excluded));
                let tags: Vec<api::Result<TagInfo>> = self
                    .tags
                    .range(range)
                    .filter(|(_, v)| raw && v.format.is_raw() || hash_seq && v.format.is_hash_seq())
                    .map(|(name, v)| {
                        Ok(TagInfo {
                            name: name.clone(),
                            hash: v.hash,
                            format: v.format,
                        })
                    })
                    .collect();
                tx.send(Ok(Box::pin(stream::iter(tags)) as Listing<TagInfo>))
                    .ok();
            }
            Request::BlobStatus(BlobStatusMsg {
                inner: BlobStatusRequest { hash },
                tx,
                ..
            }) => {
                let status = match self.blobs.get(&hash) {
                    None => BlobStatus::NotFound,
                    Some(EntryState::Partial { size }) => BlobStatus::Partial { size: *size },
                    Some(EntryState::Complete { data_location, .. }) => BlobStatus::Complete {
                        size: match data_location {
                            DataLocation::Inline(data) => data.len() as u64,
                            DataLocation::Owned(size) | DataLocation::External(_, size) => *size,
                        },
                    },
                };
                tx.send(status).await.ok();
            }
            Request::ListBlobs(ListBlobs { tx, .. }) => {
                let hashes: Vec<api::Result<Hash>> = self.blobs.keys().copied().map(Ok).collect();
                tx.send(Ok(Box::pin(stream::iter(hashes)) as Listing<Hash>))
                    .ok();
            }
            Request::GcSnapshot(GcSnapshot { tx, .. }) => {
                let candidates: Vec<api::Result<Hash>> =
                    self.blobs.keys().copied().map(Ok).collect();
                let roots: Vec<api::Result<HashAndFormat>> =
                    self.tags.values().copied().map(Ok).collect();
                tx.send(Ok((
                    Box::pin(stream::iter(candidates)) as Listing<Hash>,
                    Box::pin(stream::iter(roots)) as Listing<HashAndFormat>,
                )))
                .ok();
            }
            Request::SyncDb(SyncDbMsg { tx, .. }) => {
                tx.send(Ok(())).await.ok();
            }
            Request::Shutdown(shutdown) => return Some(shutdown),
        }
        None
    }
}

fn bound(tag: Option<Tag>, f: fn(Tag) -> Bound<Tag>) -> Bound<Tag> {
    tag.map(f).unwrap_or(Bound::Unbounded)
}

/// What an entity coordinates itself; its data is in the shared storage.
#[derive(Debug, Default)]
struct MemState {
    /// Grows with every delete; observers end when it changes.
    generation: u64,
    /// Bumped with every write, to wake observers.
    version: u64,
    frozen: bool,
    deleting: bool,
}

/// The memory backend's entity.
#[derive(Debug, Clone)]
pub(crate) struct MemEntity {
    id: Hash,
    state: watch::Sender<MemState>,
}

impl Reset<Hash> for MemEntity {
    fn new(id: &Hash) -> Self {
        Self {
            id: *id,
            state: watch::Sender::new(MemState::default()),
        }
    }

    fn reset(&mut self, id: &Hash) {
        self.id = *id;
        // Keeps the generation, like the fs entity.
        self.state.send_modify(|s| {
            s.frozen = false;
            s.deleting = false;
        });
    }
}

impl MemEntity {
    /// Waits until writes may go on: not frozen, not deleting.
    async fn wait_writable(&self) {
        let mut rx = self.state.subscribe();
        rx.wait_for(|s| !s.frozen && !s.deleting).await.ok();
    }

    /// Waits until reads may go on: not deleting.
    async fn wait_readable(&self) {
        let mut rx = self.state.subscribe();
        rx.wait_for(|s| !s.deleting).await.ok();
    }

    fn ctx<'a>(&'a self, global: &'a EntityGlobal<MemBackend>) -> MemCtx<'a> {
        MemCtx {
            entity: self,
            global,
        }
    }

    fn bitfield(&self, storage: &Storage) -> Bitfield {
        storage
            .lock()
            .unwrap()
            .get(&self.id)
            .map(Blob::bitfield)
            .unwrap_or_else(Bitfield::empty)
    }
}

/// What the generic bao code works on for one request.
struct MemCtx<'a> {
    entity: &'a MemEntity,
    global: &'a EntityGlobal<MemBackend>,
}

impl MemCtx<'_> {
    fn storage(&self) -> &Storage {
        &self.global.backend.storage
    }
}

/// Reads one part of a blob from the shared storage.
struct MemReader {
    storage: Storage,
    hash: Hash,
    outboard: bool,
}

impl MemReader {
    fn with<T>(&self, f: impl FnOnce(&[u8]) -> T) -> io::Result<T> {
        let storage = self.storage.lock().unwrap();
        let blob = storage
            .get(&self.hash)
            .ok_or_else(|| io::Error::from(io::ErrorKind::NotFound))?;
        Ok(match (blob, self.outboard) {
            (Blob::Partial(p), false) => f(p.data.as_ref()),
            (Blob::Partial(p), true) => f(p.outboard.as_ref()),
            (Blob::Complete { data, .. }, false) => f(data),
            (Blob::Complete { outboard, .. }, true) => f(outboard),
        })
    }
}

fn read_at(buf: &[u8], offset: u64, out: &mut [u8]) -> usize {
    let start = (offset as usize).min(buf.len());
    let n = out.len().min(buf.len() - start);
    out[..n].copy_from_slice(&buf[start..start + n]);
    n
}

impl ReadAt for MemReader {
    fn read_at(&self, offset: u64, buf: &mut [u8]) -> io::Result<usize> {
        self.with(|data| read_at(data, offset, buf))
    }
}

impl ReadBytesAt for MemReader {
    /// Fails if not all of the range is there, like reading a file past its end.
    fn read_bytes_at(&self, offset: u64, size: usize) -> io::Result<Bytes> {
        self.with(|data| {
            let start = usize::try_from(offset).ok()?;
            let end = start.checked_add(size)?;
            data.get(start..end).map(Bytes::copy_from_slice)
        })?
        .ok_or_else(|| io::ErrorKind::UnexpectedEof.into())
    }
}

impl Size for MemReader {
    fn size(&self) -> io::Result<Option<u64>> {
        self.with(|data| Some(data.len() as u64))
    }
}

impl BaoAccess for MemCtx<'_> {
    fn id(&self) -> Hash {
        self.entity.id
    }

    fn data_reader(&self) -> impl ReadBytesAt {
        MemReader {
            storage: self.storage().clone(),
            hash: self.entity.id,
            outboard: false,
        }
    }

    fn outboard_reader(&self) -> impl ReadAt {
        MemReader {
            storage: self.storage().clone(),
            hash: self.entity.id,
            outboard: true,
        }
    }

    fn current_size(&self) -> io::Result<u64> {
        let storage = self.storage().lock().unwrap();
        match storage.get(&self.entity.id) {
            Some(Blob::Partial(p)) => Ok(p.current_size()),
            Some(Blob::Complete { data, .. }) => Ok(data.len() as u64),
            None => Err(io::ErrorKind::NotFound.into()),
        }
    }

    fn bitfield(&self) -> io::Result<Bitfield> {
        Ok(self.entity.bitfield(self.storage()))
    }

    async fn write_batch(&self, batch: &[BaoContentItem], bitfield: &Bitfield) -> io::Result<()> {
        self.entity.wait_writable().await;
        let update = {
            let mut storage = self.storage().lock().unwrap();
            let blob = storage
                .entry(self.entity.id)
                .or_insert_with(|| Blob::Partial(PartialMemStorage::default()));
            let Blob::Partial(p) = blob else {
                // Complete: the write is the same data again.
                return Ok(());
            };
            p.write_batch(bitfield.size(), batch)?;
            let changes = p.bitfield.update(bitfield);
            let new = changes.new_state();
            if new.complete {
                let size = new.validated_size.expect("complete means validated");
                let Blob::Partial(p) = std::mem::replace(
                    blob,
                    Blob::Complete {
                        data: Bytes::new(),
                        outboard: Bytes::new(),
                    },
                ) else {
                    unreachable!()
                };
                let (data, _) = p.data.into_parts();
                let (outboard, _) = p.outboard.into_parts();
                let mut data = data;
                data.truncate(size as usize);
                *blob = Blob::Complete {
                    data: data.into(),
                    outboard: outboard.into(),
                };
                Some(complete(size, &outboard_len(blob)))
            } else if changes.was_validated() || changes.changed() {
                Some(EntryState::Partial {
                    size: new.validated_size,
                })
            } else {
                None
            }
        };
        if let Some(state) = update {
            self.global.store.db.update(self.entity.id, state).await?;
        }
        self.entity.state.send_modify(|s| s.version += 1);
        Ok(())
    }
}

fn outboard_len(blob: &Blob) -> u64 {
    match blob {
        Blob::Complete { outboard, .. } => outboard.len() as u64,
        Blob::Partial(p) => p.outboard.as_ref().len() as u64,
    }
}

/// The metadata of a complete blob in the shared storage.
fn complete(size: u64, outboard_len: &u64) -> EntryState<Bytes> {
    EntryState::Complete {
        data_location: DataLocation::Owned(size),
        outboard_location: if *outboard_len == 0 {
            OutboardLocation::NotNeeded
        } else {
            OutboardLocation::Owned
        },
    }
}

/// Reads an import's part into memory; a temp file is removed when its path drops.
fn read_part(part: MemOrFile<Bytes, impl AsRef<Path>>) -> io::Result<Bytes> {
    Ok(match part {
        MemOrFile::Mem(bytes) => bytes,
        MemOrFile::File(path) => std::fs::read(path.as_ref())?.into(),
    })
}

impl EntityApi for MemEntity {
    type Backend = MemBackend;

    async fn import_bao(&self, global: &EntityGlobal<MemBackend>, cmd: ImportBaoMsg) {
        let ImportBaoMsg {
            inner: ImportBaoRequest { size, .. },
            mut rx,
            tx,
            ..
        } = cmd;
        let res = import_bao_impl(&self.ctx(global), size, &mut rx).await;
        tx.send(res).await.ok();
        drop(rx);
    }

    async fn finish_import(
        &self,
        global: &EntityGlobal<MemBackend>,
        cmd: ImportEntryMsg,
        mut tt: TempTag,
    ) {
        self.wait_writable().await;
        let res = async {
            let ImportEntry {
                source, outboard, ..
            } = cmd.inner;
            let data = match source {
                ImportSource::Memory(bytes) => bytes,
                ImportSource::TempFile(path, _, _) => read_part(MemOrFile::File(path))?,
                // The memory backend cannot reference files, so it copies.
                ImportSource::External(path, _, _) => read_part(MemOrFile::File(path))?,
            };
            let outboard = read_part(outboard)?;
            let state = complete(data.len() as u64, &(outboard.len() as u64));
            global
                .backend
                .storage
                .lock()
                .unwrap()
                .insert(self.id, Blob::Complete { data, outboard });
            global.store.db.update_await(self.id, state).await?;
            self.state.send_modify(|s| s.version += 1);
            io::Result::Ok(())
        }
        .await;
        let item = match res {
            Ok(()) => {
                if cmd.tx.is_rpc() {
                    tt.leak();
                }
                AddProgressItem::Done(tt)
            }
            Err(cause) => AddProgressItem::Error(cause),
        };
        cmd.tx.send(item).await.ok();
    }

    async fn observe(&self, global: &EntityGlobal<MemBackend>, cmd: ObserveMsg) {
        self.wait_readable().await;
        let storage = &global.backend.storage;
        let tx = cmd.tx;
        let mut rx = self.state.subscribe();
        let generation = rx.borrow_and_update().generation;
        let mut last = self.bitfield(storage);
        if tx.send(last.clone()).await.is_err() {
            return;
        }
        loop {
            if rx.changed().await.is_err() || rx.borrow_and_update().generation != generation {
                return;
            }
            let value = self.bitfield(storage);
            if !value.ranges.is_superset(&last.ranges) {
                return;
            }
            if value == last {
                continue;
            }
            last = value.clone();
            if tx.send(value).await.is_err() {
                return;
            }
        }
    }

    async fn export_ranges(&self, global: &EntityGlobal<MemBackend>, mut cmd: ExportRangesMsg) {
        self.wait_readable().await;
        if let Err(cause) = export_ranges_impl(&self.ctx(global), cmd.inner, &mut cmd.tx).await {
            cmd.tx
                .send(ExportRangesItem::Error(cause.into()))
                .await
                .ok();
        }
    }

    async fn export_bao(&self, global: &EntityGlobal<MemBackend>, mut cmd: ExportBaoMsg) {
        self.wait_readable().await;
        if let Err(cause) = export_bao_impl(&self.ctx(global), cmd.inner, &mut cmd.tx).await {
            cmd.tx
                .send(bao_tree::io::EncodeError::Io(cause).into())
                .await
                .ok();
        }
    }

    async fn export_path(&self, global: &EntityGlobal<MemBackend>, cmd: ExportPathMsg) {
        self.wait_readable().await;
        let ExportPathMsg { inner, tx, .. } = cmd;
        // The memory backend cannot hand out a file it owns, so it copies in both modes.
        let ExportPathRequest { target, .. } = inner;
        let data = match global.backend.storage.lock().unwrap().get(&self.id) {
            Some(Blob::Complete { data, .. }) => Ok(data.clone()),
            _ => Err(io::Error::new(io::ErrorKind::NotFound, "not complete")),
        };
        let res = async {
            let data = data?;
            tx.send(ExportProgressItem::Size(data.len() as u64)).await?;
            std::fs::write(&target, &data)?;
            tx.send(ExportProgressItem::Done).await?;
            api::Result::Ok(())
        }
        .await;
        if let Err(cause) = res {
            tx.send(cause.into()).await.ok();
        }
    }

    async fn delete(&self, global: &EntityGlobal<MemBackend>) -> io::Result<()> {
        self.wait_writable().await;
        self.state.send_modify(|s| {
            s.generation += 1;
            s.deleting = true;
        });
        let res = global.store.db.delete(self.id).await;
        global.backend.storage.lock().unwrap().remove(&self.id);
        self.state.send_modify(|s| s.deleting = false);
        res
    }

    async fn freeze(
        &self,
        _global: &EntityGlobal<MemBackend>,
        ack: tokio::sync::oneshot::Sender<()>,
        mut release: tokio::sync::watch::Receiver<()>,
    ) {
        self.wait_writable().await;
        self.state.send_modify(|s| s.frozen = true);
        ack.send(()).ok();
        while release.changed().await.is_ok() {}
        self.state.send_modify(|s| s.frozen = false);
    }

    async fn delete_unreferenced(
        &self,
        _global: &EntityGlobal<MemBackend>,
        _parts: UnreferencedParts,
    ) {
        // The memory metadata store never sends any.
    }

    async fn persist(&self, _global: &EntityGlobal<MemBackend>) {
        // The data is in the shared storage already.
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use bao_tree::{ChunkNum, ChunkRanges};
    use n0_future::StreamExt;
    use testresult::TestResult;

    use super::*;
    use crate::store::{fs::tests::test_data, util::tests::create_n0_bao};

    /// Bytes and files go in and come out unchanged, small and large.
    #[tokio::test]
    async fn bytes_and_paths_roundtrip() -> TestResult {
        let dir = tempfile::tempdir()?;
        let store = store(dir.path()).await?;
        for size in [1usize, 1024 * 16, 1024 * 16 + 1, 1024 * 1024] {
            let data = test_data(size);
            let hash = store.add_bytes(data.clone()).await?.hash;
            assert_eq!(store.get_bytes(hash).await?, data, "size={size}");
            let input = dir.path().join(format!("in-{size}"));
            std::fs::write(&input, &data)?;
            assert_eq!(store.add_path(&input).await?.hash, hash, "size={size}");
            let output = dir.path().join(format!("out-{size}"));
            store.export(hash, &output).await?;
            assert_eq!(std::fs::read(&output)?, data.to_vec(), "size={size}");
            assert_eq!(
                store.status(hash).await?,
                BlobStatus::Complete { size: size as u64 }
            );
        }
        store.shutdown().await?;
        Ok(())
    }

    /// A bao import in two parts is observed growing, completes, exports, and a delete ends the observer.
    #[tokio::test]
    async fn bao_import_observe_delete() -> TestResult {
        let dir = tempfile::tempdir()?;
        let store = store(dir.path()).await?;
        let data = test_data(1024 * 1024);
        let first = ChunkRanges::from(..ChunkNum(64));
        let rest = ChunkRanges::from(ChunkNum(64)..);
        let (hash, first_bao) = create_n0_bao(&data, &first)?;
        let (_, rest_bao) = create_n0_bao(&data, &rest)?;
        store
            .import_bao_bytes(hash, first.clone(), first_bao)
            .await?;
        assert!(matches!(
            store.status(hash).await?,
            BlobStatus::Partial { .. }
        ));
        let mut observer = Box::pin(store.observe(hash).stream().await?);
        let partial = observer.next().await.expect("initial bitfield");
        assert!(!partial.is_complete());
        store.import_bao_bytes(hash, rest, rest_bao).await?;
        let complete = loop {
            let bitfield = tokio::time::timeout(Duration::from_secs(5), observer.next())
                .await?
                .expect("observer ends only on delete");
            if bitfield.is_complete() {
                break bitfield;
            }
        };
        assert_eq!(complete.size(), data.len() as u64);
        assert_eq!(
            store.status(hash).await?,
            BlobStatus::Complete {
                size: data.len() as u64
            }
        );
        assert_eq!(store.get_bytes(hash).await?, data);
        let (_, expected) = create_n0_bao(&data, &ChunkRanges::all())?;
        let exported = store
            .export_bao(hash, ChunkRanges::all())
            .bao_to_vec()
            .await?;
        assert_eq!(exported, expected);
        store
            .blobs()
            .delete_with_opts(crate::api::proto::BlobDeleteRequest {
                hashes: vec![hash],
                force: true,
            })
            .await?;
        assert_eq!(store.status(hash).await?, BlobStatus::NotFound);
        let rest =
            tokio::time::timeout(Duration::from_secs(5), observer.collect::<Vec<_>>()).await?;
        assert!(rest.is_empty(), "the observer ends instead of seeing less");
        store.shutdown().await?;
        Ok(())
    }
}

//! A full in memory database for iroh-blobs
//!
//! Main entry point is [Store].
use std::{
    collections::{BTreeMap, BTreeSet},
    future::Future,
    io,
    path::PathBuf,
    sync::{Arc, RwLock, RwLockReadGuard, RwLockWriteGuard},
    time::SystemTime,
};

use bao_tree::{
    io::{fsm::Outboard, outboard::PreOrderOutboard, sync::WriteAt},
    BaoTree,
};
use bytes::{Bytes, BytesMut};
use futures_lite::{Stream, StreamExt};
use iroh_base::hash::{BlobFormat, Hash, HashAndFormat};
use iroh_io::AsyncSliceReader;

use super::{
    temp_name, BaoBatchWriter, ConsistencyCheckProgress, ExportMode, ExportProgressCb, ImportMode,
    ImportProgress, Map, TempCounterMap,
};
use crate::{
    store::{
        mutable_mem_storage::MutableMemStorage, BaoBlobSize, MapEntry, MapEntryMut, ReadableStore,
    },
    util::{
        progress::{BoxedProgressSender, IdGenerator, IgnoreProgressSender, ProgressSender},
        TagCounter, TagDrop,
    },
    Tag, TempTag, IROH_BLOCK_SIZE,
};

/// A fully featured in memory database for iroh-blobs, including support for
/// partial blobs.
#[derive(Debug, Clone, Default)]
pub struct Store {
    inner: Arc<StoreInner>,
}

#[derive(Debug, Default)]
struct StoreInner(RwLock<StateInner>);

impl TagDrop for StoreInner {
    fn on_drop(&self, inner: &HashAndFormat) {
        tracing::trace!("temp tag drop: {:?}", inner);
        let mut state = self.0.write().unwrap();
        state.temp.dec(inner);
    }
}

impl TagCounter for StoreInner {
    fn on_create(&self, inner: &HashAndFormat) {
        tracing::trace!("temp tagging: {:?}", inner);
        let mut state = self.0.write().unwrap();
        state.temp.inc(inner);
    }
}

impl Store {
    /// Create a new in memory store
    pub fn new() -> Self {
        Self::default()
    }

    /// Take a write lock on the store
    fn write_lock(&self) -> RwLockWriteGuard<'_, StateInner> {
        self.inner.0.write().unwrap()
    }

    /// Take a read lock on the store
    fn read_lock(&self) -> RwLockReadGuard<'_, StateInner> {
        self.inner.0.read().unwrap()
    }

    fn import_bytes_sync(
        &self,
        id: u64,
        bytes: Bytes,
        format: BlobFormat,
        progress: impl ProgressSender<Msg = ImportProgress> + IdGenerator,
    ) -> io::Result<TempTag> {
        progress.blocking_send(ImportProgress::OutboardProgress { id, offset: 0 })?;
        let progress2 = progress.clone();
        let cb = move |offset| {
            progress2
                .try_send(ImportProgress::OutboardProgress { id, offset })
                .ok();
        };
        let (storage, hash) = MutableMemStorage::complete(bytes, cb);
        progress.blocking_send(ImportProgress::OutboardDone { id, hash })?;
        use super::Store;
        let tag = self.temp_tag(HashAndFormat { hash, format });
        let entry = Entry {
            inner: Arc::new(EntryInner {
                hash,
                data: RwLock::new(storage),
            }),
            complete: true,
        };
        self.write_lock().entries.insert(hash, entry);
        Ok(tag)
    }

    fn export_sync(
        &self,
        hash: Hash,
        target: PathBuf,
        _mode: ExportMode,
        progress: impl Fn(u64) -> io::Result<()> + Send + Sync + 'static,
    ) -> io::Result<()> {
        tracing::trace!("exporting {} to {}", hash, target.display());

        if !target.is_absolute() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "target path must be absolute",
            ));
        }
        let parent = target.parent().ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                "target path has no parent directory",
            )
        })?;
        // create the directory in which the target file is
        std::fs::create_dir_all(parent)?;
        let state = self.read_lock();
        let entry = state
            .entries
            .get(&hash)
            .ok_or_else(|| io::Error::new(io::ErrorKind::NotFound, "hash not found"))?;
        let reader = &entry.inner.data;
        let size = reader.read().unwrap().current_size();
        let mut file = std::fs::File::create(target)?;
        for offset in (0..size).step_by(1024 * 1024) {
            let bytes = reader.read().unwrap().read_data_at(offset, 1024 * 1024);
            file.write_at(offset, &bytes)?;
            progress(offset)?;
        }
        std::io::Write::flush(&mut file)?;
        drop(file);
        Ok(())
    }
}

impl super::Store for Store {
    async fn import_file(
        &self,
        path: std::path::PathBuf,
        _mode: ImportMode,
        format: BlobFormat,
        progress: impl ProgressSender<Msg = ImportProgress> + IdGenerator,
    ) -> io::Result<(TempTag, u64)> {
        let this = self.clone();
        tokio::task::spawn_blocking(move || {
            let id = progress.new_id();
            progress.blocking_send(ImportProgress::Found {
                id,
                name: path.to_string_lossy().to_string(),
            })?;
            progress.try_send(ImportProgress::CopyProgress { id, offset: 0 })?;
            // todo: provide progress for reading into mem
            let bytes: Bytes = std::fs::read(path)?.into();
            let size = bytes.len() as u64;
            progress.blocking_send(ImportProgress::Size { id, size })?;
            let tag = this.import_bytes_sync(id, bytes, format, progress)?;
            Ok((tag, size))
        })
        .await?
    }

    async fn import_stream(
        &self,
        mut data: impl Stream<Item = io::Result<Bytes>> + Unpin + Send + 'static,
        format: BlobFormat,
        progress: impl ProgressSender<Msg = ImportProgress> + IdGenerator,
    ) -> io::Result<(TempTag, u64)> {
        let this = self.clone();
        let id = progress.new_id();
        let name = temp_name();
        progress.send(ImportProgress::Found { id, name }).await?;
        let mut bytes = BytesMut::new();
        while let Some(chunk) = data.next().await {
            bytes.extend_from_slice(&chunk?);
            progress
                .try_send(ImportProgress::CopyProgress {
                    id,
                    offset: bytes.len() as u64,
                })
                .ok();
        }
        let bytes = bytes.freeze();
        let size = bytes.len() as u64;
        progress.blocking_send(ImportProgress::Size { id, size })?;
        let tag = this.import_bytes_sync(id, bytes, format, progress)?;
        Ok((tag, size))
    }

    async fn import_bytes(&self, bytes: Bytes, format: BlobFormat) -> io::Result<TempTag> {
        let this = self.clone();
        tokio::task::spawn_blocking(move || {
            this.import_bytes_sync(0, bytes, format, IgnoreProgressSender::default())
        })
        .await?
    }

    async fn set_tag(&self, name: Tag, value: Option<HashAndFormat>) -> io::Result<()> {
        let mut state = self.write_lock();
        if let Some(value) = value {
            state.tags.insert(name, value);
        } else {
            state.tags.remove(&name);
        }
        Ok(())
    }

    async fn create_tag(&self, hash: HashAndFormat) -> io::Result<Tag> {
        let mut state = self.write_lock();
        let tag = Tag::auto(SystemTime::now(), |x| state.tags.contains_key(x));
        state.tags.insert(tag.clone(), hash);
        Ok(tag)
    }

    fn temp_tag(&self, tag: HashAndFormat) -> TempTag {
        self.inner.temp_tag(tag)
    }

    async fn gc_run<G, Gut>(&self, config: super::GcConfig, protected_cb: G)
    where
        G: Fn() -> Gut,
        Gut: Future<Output = BTreeSet<Hash>> + Send,
    {
        super::gc_run_loop(self, config, move || async { Ok(()) }, protected_cb).await
    }

    async fn delete(&self, hashes: Vec<Hash>) -> io::Result<()> {
        let mut state = self.write_lock();
        for hash in hashes {
            if !state.temp.contains(&hash) {
                state.entries.remove(&hash);
            }
        }
        Ok(())
    }

    async fn shutdown(&self) {}

    async fn sync(&self) -> io::Result<()> {
        Ok(())
    }
}

#[derive(Debug, Default)]
struct StateInner {
    entries: BTreeMap<Hash, Entry>,
    tags: BTreeMap<Tag, HashAndFormat>,
    temp: TempCounterMap,
}

/// An in memory entry
#[derive(Debug, Clone)]
pub struct Entry {
    inner: Arc<EntryInner>,
    complete: bool,
}

#[derive(Debug)]
struct EntryInner {
    hash: Hash,
    data: RwLock<MutableMemStorage>,
}

impl MapEntry for Entry {
    fn hash(&self) -> Hash {
        self.inner.hash
    }

    fn size(&self) -> BaoBlobSize {
        let size = self.inner.data.read().unwrap().current_size();
        BaoBlobSize::new(size, self.complete)
    }

    fn is_complete(&self) -> bool {
        self.complete
    }

    async fn outboard(&self) -> io::Result<impl Outboard> {
        let size = self.inner.data.read().unwrap().current_size();
        Ok(PreOrderOutboard {
            root: self.hash().into(),
            tree: BaoTree::new(size, IROH_BLOCK_SIZE),
            data: OutboardReader(self.inner.clone()),
        })
    }

    async fn data_reader(&self) -> io::Result<impl AsyncSliceReader> {
        Ok(DataReader(self.inner.clone()))
    }
}

impl MapEntryMut for Entry {
    async fn batch_writer(&self) -> io::Result<impl BaoBatchWriter> {
        Ok(BatchWriter(self.inner.clone()))
    }
}

struct DataReader(Arc<EntryInner>);

impl AsyncSliceReader for DataReader {
    async fn read_at(&mut self, offset: u64, len: usize) -> std::io::Result<Bytes> {
        Ok(self.0.data.read().unwrap().read_data_at(offset, len))
    }

    async fn size(&mut self) -> std::io::Result<u64> {
        Ok(self.0.data.read().unwrap().data_len())
    }
}

struct OutboardReader(Arc<EntryInner>);

impl AsyncSliceReader for OutboardReader {
    async fn read_at(&mut self, offset: u64, len: usize) -> std::io::Result<Bytes> {
        Ok(self.0.data.read().unwrap().read_outboard_at(offset, len))
    }

    async fn size(&mut self) -> std::io::Result<u64> {
        Ok(self.0.data.read().unwrap().outboard_len())
    }
}

struct BatchWriter(Arc<EntryInner>);

impl super::BaoBatchWriter for BatchWriter {
    async fn write_batch(
        &mut self,
        size: u64,
        batch: Vec<bao_tree::io::fsm::BaoContentItem>,
    ) -> io::Result<()> {
        self.0.data.write().unwrap().write_batch(size, &batch)
    }

    async fn sync(&mut self) -> io::Result<()> {
        Ok(())
    }
}

impl super::Map for Store {
    type Entry = Entry;

    async fn get(&self, hash: &Hash) -> std::io::Result<Option<Self::Entry>> {
        Ok(self.inner.0.read().unwrap().entries.get(hash).cloned())
    }
}

impl super::MapMut for Store {
    type EntryMut = Entry;

    async fn get_mut(&self, hash: &Hash) -> std::io::Result<Option<Self::EntryMut>> {
        self.get(hash).await
    }

    async fn get_or_create(&self, hash: Hash, _size: u64) -> std::io::Result<Entry> {
        let entry = Entry {
            inner: Arc::new(EntryInner {
                hash,
                data: RwLock::new(MutableMemStorage::default()),
            }),
            complete: false,
        };
        Ok(entry)
    }

    async fn entry_status(&self, hash: &Hash) -> std::io::Result<crate::store::EntryStatus> {
        self.entry_status_sync(hash)
    }

    fn entry_status_sync(&self, hash: &Hash) -> std::io::Result<crate::store::EntryStatus> {
        Ok(match self.inner.0.read().unwrap().entries.get(hash) {
            Some(entry) => {
                if entry.complete {
                    crate::store::EntryStatus::Complete
                } else {
                    crate::store::EntryStatus::Partial
                }
            }
            None => crate::store::EntryStatus::NotFound,
        })
    }

    async fn insert_complete(&self, mut entry: Entry) -> std::io::Result<()> {
        let hash = entry.hash();
        let mut inner = self.inner.0.write().unwrap();
        let complete = inner
            .entries
            .get(&hash)
            .map(|x| x.complete)
            .unwrap_or_default();
        if !complete {
            entry.complete = true;
            inner.entries.insert(hash, entry);
        }
        Ok(())
    }
}

impl ReadableStore for Store {
    async fn blobs(&self) -> io::Result<crate::store::DbIter<Hash>> {
        let entries = self.read_lock().entries.clone();
        Ok(Box::new(
            entries
                .into_values()
                .filter(|x| x.complete)
                .map(|x| Ok(x.hash())),
        ))
    }

    async fn partial_blobs(&self) -> io::Result<crate::store::DbIter<Hash>> {
        let entries = self.read_lock().entries.clone();
        Ok(Box::new(
            entries
                .into_values()
                .filter(|x| !x.complete)
                .map(|x| Ok(x.hash())),
        ))
    }

    async fn tags(
        &self,
    ) -> io::Result<crate::store::DbIter<(crate::Tag, iroh_base::hash::HashAndFormat)>> {
        #[allow(clippy::mutable_key_type)]
        let tags = self.read_lock().tags.clone();
        Ok(Box::new(tags.into_iter().map(Ok)))
    }

    fn temp_tags(
        &self,
    ) -> Box<dyn Iterator<Item = iroh_base::hash::HashAndFormat> + Send + Sync + 'static> {
        let tags = self.read_lock().temp.keys();
        Box::new(tags)
    }

    async fn consistency_check(
        &self,
        _repair: bool,
        _tx: BoxedProgressSender<ConsistencyCheckProgress>,
    ) -> io::Result<()> {
        todo!()
    }

    async fn export(
        &self,
        hash: Hash,
        target: std::path::PathBuf,
        mode: crate::store::ExportMode,
        progress: ExportProgressCb,
    ) -> io::Result<()> {
        let this = self.clone();
        tokio::task::spawn_blocking(move || this.export_sync(hash, target, mode, progress)).await?
    }
}

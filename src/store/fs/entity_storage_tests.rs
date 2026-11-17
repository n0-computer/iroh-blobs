//! A second, independent storage implementation exercising the shared actors.
//! Complete entries live inline in metadata; partial entries live in Config.
use std::{
    collections::HashMap,
    sync::{
        atomic::{AtomicBool, AtomicUsize, Ordering},
        Arc, Mutex,
    },
    time::Duration,
};

use super::entity_storage::StorageChange;
use super::*;
use super::{
    entity::SyncEntityApi,
    entry_state::{DataLocation, EntryState},
    file_deletes::ALL_PARTS,
    import::ImportEntry,
};
use crate::store::fs::entry_state::OutboardLocation;
use crate::store::fs::file_deletes::BaoFilePart;
use crate::store::fs::import::ImportSource;
use crate::store::util::PartialMemStorage;
use crate::{api::blobs::Bitfield, store::util::MemOrFile};
use bao_tree::{
    io::{mixed::ReadBytesAt, sync::ReadAt, BaoContentItem, Leaf},
    ChunkNum, ChunkRanges,
};
use bytes::Bytes;

#[derive(Debug, Default, Clone)]
struct MemoryStorage(Option<Arc<Mutex<PartialMemStorage>>>);

#[derive(Debug, Default, Clone)]
struct Config {
    partial: Arc<Mutex<HashMap<Hash, MemoryStorage>>>,
    writes: Arc<AtomicUsize>,
    persists: Arc<AtomicUsize>,
    deletes: Arc<AtomicUsize>,
    fail_write: Arc<AtomicBool>,
}

impl MemoryStorage {
    fn entry(storage: &PartialMemStorage) -> EntryState<Bytes> {
        if storage.bitfield.is_complete() {
            EntryState::Complete {
                data_location: DataLocation::Inline(storage.data.to_vec().into()),
                outboard_location: OutboardLocation::Inline(storage.outboard.to_vec().into()),
            }
        } else {
            EntryState::Partial {
                size: storage.bitfield.validated_size(),
            }
        }
    }

    fn from_bytes(data: Bytes, outboard: Bytes) -> io::Result<Self> {
        use bao_tree::io::sync::WriteAt;
        let mut storage = PartialMemStorage::default();
        storage.data.write_all_at(0, &data)?;
        storage.outboard.write_all_at(0, &outboard)?;
        storage.bitfield = Bitfield::complete(data.len() as u64);
        Ok(Self(Some(Arc::new(Mutex::new(storage)))))
    }
}

impl EntityStorage for MemoryStorage {
    type Config = Config;

    fn empty() -> Self {
        Self(None)
    }
    fn is_complete(&self) -> bool {
        self.0
            .as_ref()
            .is_some_and(|x| x.lock().unwrap().bitfield.is_complete())
    }
    fn open(
        config: &Config,
        hash: &Hash,
        entry: Option<EntryState<Bytes>>,
    ) -> io::Result<StorageChange<Self>> {
        let storage =
            match entry {
                None => Self::empty(),
                Some(EntryState::Partial { .. }) => config
                    .partial
                    .lock()
                    .unwrap()
                    .get(hash)
                    .cloned()
                    .ok_or_else(|| io::Error::other("missing memory storage"))?,
                Some(EntryState::Complete {
                    data_location: DataLocation::Inline(data),
                    outboard_location,
                }) => {
                    let outboard = match outboard_location {
                        OutboardLocation::Inline(bytes) => bytes,
                        OutboardLocation::NotNeeded => Bytes::new(),
                        _ => return Err(io::Error::other("expected inline outboard")),
                    };
                    Self::from_bytes(data, outboard)?
                }
                _ => return Err(io::Error::other("expected inline data")),
            };
        Ok((storage, None))
    }
    fn write_batch(
        mut self,
        batch: &[BaoContentItem],
        bitfield: &Bitfield,
        config: &Config,
        hash: &Hash,
    ) -> io::Result<StorageChange<Self>> {
        config.writes.fetch_add(1, Ordering::SeqCst);
        if config.fail_write.swap(false, Ordering::SeqCst) {
            return Err(io::Error::other("injected write failure"));
        }
        if self.is_complete() {
            return Ok((self, None));
        }
        let entry = {
            let mut storage = self.0.get_or_insert_with(Default::default).lock().unwrap();
            storage.write_batch(bitfield.size(), batch)?;
            storage.bitfield.update(bitfield);
            Self::entry(&storage)
        };
        config.partial.lock().unwrap().insert(*hash, self.clone());
        Ok((self, Some(entry)))
    }
    fn finish_import(
        _: &Config,
        _: &Hash,
        import: ImportEntry,
    ) -> io::Result<(Self, EntryState<Bytes>)> {
        let data = match import.source {
            ImportSource::Memory(data) => data,
            ImportSource::TempFile(path, _, _) => fs::read(path.as_ref() as &Path)?.into(),
            ImportSource::External(path, _, _) => fs::read(path)?.into(),
        };
        let outboard = match import.outboard {
            MemOrFile::Mem(bytes) => bytes,
            MemOrFile::File(path) => fs::read(path.as_ref() as &Path)?.into(),
        };
        let storage = Self::from_bytes(data, outboard)?;
        let entry = Self::entry(&storage.0.as_ref().unwrap().lock().unwrap());
        Ok((storage, entry))
    }
    fn read_data_at(&self, offset: u64, len: usize) -> io::Result<Bytes> {
        let storage = self
            .0
            .as_ref()
            .ok_or(io::ErrorKind::NotFound)?
            .lock()
            .unwrap();
        let data = storage.data.to_vec();
        data.as_slice().read_bytes_at(offset, len)
    }
    fn read_outboard_at(&self, offset: u64, buf: &mut [u8]) -> io::Result<usize> {
        let storage = self
            .0
            .as_ref()
            .ok_or(io::ErrorKind::NotFound)?
            .lock()
            .unwrap();
        storage.outboard.to_vec().as_slice().read_at(offset, buf)
    }
    fn current_size(&self) -> io::Result<u64> {
        Ok(self.bitfield()?.size())
    }
    fn bitfield(&self) -> io::Result<Bitfield> {
        Ok(self
            .0
            .as_ref()
            .ok_or(io::ErrorKind::NotFound)?
            .lock()
            .unwrap()
            .bitfield
            .clone())
    }
    fn persist(&self, config: &Config, _: &Hash) -> io::Result<()> {
        config.persists.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }
    fn references(&self, _: BaoFilePart) -> bool {
        false
    }
    fn delete_parts(config: &Config, hash: &Hash, parts: &[BaoFilePart]) {
        config.deletes.fetch_add(1, Ordering::SeqCst);
        if parts.len() == ALL_PARTS.len() {
            config.partial.lock().unwrap().remove(hash);
        }
    }
}

#[tokio::test]
async fn alternate_storage_roundtrip_and_delete() -> testresult::TestResult<()> {
    let dir = tempfile::tempdir()?;
    let config = Config::default();
    let options = Options::new(dir.path());
    let db_path = dir.path().join("blobs.db");
    let store = FsStore::load_with_storage::<MemoryStorage>(
        db_path.clone(),
        options.clone(),
        config.clone(),
    )
    .await?;
    // Local imports and path export work with independently implemented storage.
    let data = tests::test_data(100_000);
    let tag = store.add_bytes(data.clone()).await?;
    let hash = tag.hash;
    assert_eq!(store.get_bytes(hash).await?, data);
    let target = dir.path().join("exported");
    store.export(hash, &target).await?;
    assert_eq!(fs::read(target)?, data);
    let bao = store
        .export_bao(hash, ChunkRanges::all())
        .bao_to_vec()
        .await?;
    let tail = ChunkRanges::from(ChunkNum(96)..);
    let tail_bao = store.export_bao(hash, tail.clone()).bao_to_vec().await?;
    store.wait_idle().await?;
    assert!(config.persists.load(Ordering::SeqCst) > 0);
    store.shutdown().await?;

    // Reload from metadata, then delete while observed and import through batches.
    let store =
        FsStore::load_with_storage::<MemoryStorage>(db_path, options, config.clone()).await?;
    assert_eq!(store.get_bytes(hash).await?, data);
    let mut observed = std::pin::pin!(store.observe(hash).stream().await?);
    assert!(observed.next().await.unwrap().is_complete());
    store
        .blobs()
        .delete_with_opts(api::blobs::DeleteOptions {
            hashes: vec![hash],
            force: true,
        })
        .await?;
    assert!(
        tokio::time::timeout(Duration::from_secs(5), observed.next())
            .await?
            .is_none()
    );
    assert!(!store.has(hash).await?);
    store.import_bao_bytes(hash, tail, tail_bao).await?;
    store.wait_idle().await?;
    assert!(!store.observe(hash).await?.is_complete());
    store
        .import_bao_bytes(hash, ChunkRanges::all(), bao)
        .await?;
    assert_eq!(store.get_bytes(hash).await?, data);
    assert!(config.writes.load(Ordering::SeqCst) > 1);
    assert!(config.deletes.load(Ordering::SeqCst) > 0);
    store.shutdown().await?;
    Ok(())
}

#[tokio::test]
async fn alternate_storage_freeze_and_write_failure() -> testresult::TestResult<()> {
    tokio::time::timeout(Duration::from_secs(10), async {
        let dir = tempfile::tempdir()?;
        let config = Config::default();
        let (cmd_tx, cmd_rx) = tokio::sync::mpsc::channel(32);
        let (fs_tx, fs_rx) = tokio::sync::mpsc::channel(32);
        let mut actor = Actor::<MemoryStorage>::new(
            dir.path().join("blobs.db"),
            cmd_rx,
            fs_rx,
            fs_tx,
            Arc::new(Options::new(dir.path())),
            Arc::new(config.clone()),
        )
        .await?;
        let data = Bytes::from(vec![42; 1024]);
        let hash = Hash::new(&data);
        // Run through an actual entity actor, with no backend-specific coordinator.
        let (ctx_tx, ctx_rx) = tokio::sync::oneshot::channel();
        let (hold, held) = tokio::sync::oneshot::channel::<()>();
        if let Some(task) = actor
            .handles
            .spawn(hash, |arg| async move {
                let SpawnArg::Active(ctx) = arg else {
                    panic!("entity dead")
                };
                ctx_tx.send(ctx).unwrap();
                held.await.ok();
            })
            .await
        {
            actor.spawn(task);
        }
        let task = tokio::spawn(actor.run());
        let ctx = ctx_rx.await?;
        let (ack, frozen) = tokio::sync::oneshot::channel();
        let (release, release_rx) = tokio::sync::watch::channel(());
        let freeze = ctx.freeze(ack, release_rx);
        let work = async {
            frozen.await?;
            let batch = [BaoContentItem::Leaf(Leaf {
                offset: 0,
                data: data.clone(),
            })];
            let bitfield = Bitfield::complete(1024);
            let write = ctx.write_batch(&batch, &bitfield);
            tokio::pin!(write);
            assert!(tokio::time::timeout(Duration::from_millis(30), &mut write)
                .await
                .is_err());
            assert_eq!(config.writes.load(Ordering::SeqCst), 0);
            drop(release);
            write.await?;
            assert_eq!(ctx.data_reader().read_bytes_at(0, 1024)?, data);
            config.fail_write.store(true, Ordering::SeqCst);
            assert!(ctx.write_batch(&batch, &bitfield).await.is_err());
            assert!(ctx.current_size().is_err());
            assert!(ctx.state.borrow().storage.bitfield().is_none());
            ctx.delete().await?;
            ctx.write_batch(&batch, &bitfield).await?;
            assert_eq!(ctx.data_reader().read_bytes_at(0, 1024)?, data);
            testresult::TestResult::Ok(())
        };
        let (_, res) = tokio::join!(freeze, work);
        res?;
        drop(ctx);
        drop(hold);
        Store::from_client(cmd_tx).shutdown().await?;
        task.await?;
        testresult::TestResult::Ok(())
    })
    .await??;
    Ok(())
}

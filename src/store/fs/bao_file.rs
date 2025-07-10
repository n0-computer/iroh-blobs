use core::fmt;
use std::{
    fs::{File, OpenOptions},
    io,
    ops::Deref,
    path::Path,
};

use bao_tree::{
    blake3,
    io::{
        fsm::BaoContentItem,
        mixed::ReadBytesAt,
        outboard::PreOrderOutboard,
        sync::{ReadAt, WriteAt},
    },
    BaoTree, ChunkRanges,
};
use bytes::{Bytes, BytesMut};
use derive_more::Debug;
use irpc::channel::mpsc;
use tokio::sync::watch;
use tracing::{debug, error, info, trace};

use super::{
    entry_state::{DataLocation, EntryState, OutboardLocation},
    options::{Options, PathOptions},
    BaoFilePart,
};
use crate::{
    api::blobs::Bitfield,
    store::{
        fs::{meta::raw_outboard_size, util::entity_manager, HashContext},
        util::{
            read_checksummed_and_truncate, write_checksummed, FixedSize, MemOrFile,
            PartialMemStorage, DD,
        },
        Hash, IROH_BLOCK_SIZE,
    },
};

/// Storage for complete blobs. There is no longer any uncertainty about the
/// size, so we don't need a sizes file.
///
/// Writing is not possible but also not needed, since the file is complete.
/// This covers all combinations of data and outboard being in memory or on
/// disk.
///
/// For the memory variant, it does reading in a zero copy way, since storage
/// is already a `Bytes`.
#[derive(Default)]
pub struct CompleteStorage {
    /// data part, which can be in memory or on disk.
    pub data: MemOrFile<Bytes, FixedSize<File>>,
    /// outboard part, which can be in memory or on disk.
    pub outboard: MemOrFile<Bytes, File>,
}

impl fmt::Debug for CompleteStorage {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CompleteStorage")
            .field("data", &DD(self.data.fmt_short()))
            .field("outboard", &DD(self.outboard.fmt_short()))
            .finish()
    }
}

impl CompleteStorage {
    /// The size of the data file.
    pub fn size(&self) -> u64 {
        match &self.data {
            MemOrFile::Mem(mem) => mem.len() as u64,
            MemOrFile::File(file) => file.size,
        }
    }

    pub fn bitfield(&self) -> Bitfield {
        Bitfield::complete(self.size())
    }
}

/// Create a file for reading and writing, but *without* truncating the existing
/// file.
fn create_read_write(path: impl AsRef<Path>) -> io::Result<File> {
    OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(false)
        .open(path)
}

/// Read from the given file at the given offset, until end of file or max bytes.
fn read_to_end(file: impl ReadAt, offset: u64, max: usize) -> io::Result<Bytes> {
    let mut res = BytesMut::new();
    let mut buf = [0u8; 4096];
    let mut remaining = max;
    let mut offset = offset;
    while remaining > 0 {
        let end = buf.len().min(remaining);
        let read = file.read_at(offset, &mut buf[..end])?;
        if read == 0 {
            // eof
            break;
        }
        res.extend_from_slice(&buf[..read]);
        offset += read as u64;
        remaining -= read;
    }
    Ok(res.freeze())
}

fn max_offset(batch: &[BaoContentItem]) -> u64 {
    batch
        .iter()
        .filter_map(|item| match item {
            BaoContentItem::Leaf(leaf) => {
                let len = leaf.data.len().try_into().unwrap();
                let end = leaf
                    .offset
                    .checked_add(len)
                    .expect("u64 overflow for leaf end");
                Some(end)
            }
            _ => None,
        })
        .max()
        .unwrap_or(0)
}

/// A file storage for an incomplete bao file.
#[derive(Debug)]
pub struct PartialFileStorage {
    data: std::fs::File,
    outboard: std::fs::File,
    sizes: std::fs::File,
    bitfield: Bitfield,
}

impl PartialFileStorage {
    pub fn bitfield(&self) -> &Bitfield {
        &self.bitfield
    }

    fn sync_all(&self, bitfield_path: &Path) -> io::Result<()> {
        self.data.sync_all()?;
        self.outboard.sync_all()?;
        self.sizes.sync_all()?;
        // only write the bitfield if the syncs were successful
        trace!(
            "writing bitfield {:?} to {}",
            self.bitfield,
            bitfield_path.display()
        );
        write_checksummed(bitfield_path, &self.bitfield)?;
        Ok(())
    }

    fn load(hash: &Hash, options: &PathOptions) -> io::Result<Self> {
        let bitfield_path = options.bitfield_path(hash);
        let data = create_read_write(options.data_path(hash))?;
        let outboard = create_read_write(options.outboard_path(hash))?;
        let sizes = create_read_write(options.sizes_path(hash))?;
        let bitfield = match read_checksummed_and_truncate(&bitfield_path) {
            Ok(bitfield) => bitfield,
            Err(cause) => {
                trace!(
                    "failed to read bitfield for {} at {}: {:?}",
                    hash.to_hex(),
                    bitfield_path.display(),
                    cause
                );
                trace!("reconstructing bitfield from outboard");
                let size = read_size(&sizes).ok().unwrap_or_default();
                let outboard = PreOrderOutboard {
                    data: &outboard,
                    tree: BaoTree::new(size, IROH_BLOCK_SIZE),
                    root: blake3::Hash::from(*hash),
                };
                let mut ranges = ChunkRanges::empty();
                for range in bao_tree::io::sync::valid_ranges(outboard, &data, &ChunkRanges::all())
                    .into_iter()
                    .flatten()
                {
                    ranges |= ChunkRanges::from(range);
                }
                info!("reconstructed range is {:?}", ranges);
                Bitfield::new(ranges, size)
            }
        };
        Ok(Self {
            data,
            outboard,
            sizes,
            bitfield,
        })
    }

    fn into_complete(
        self,
        size: u64,
        options: &Options,
    ) -> io::Result<(CompleteStorage, EntryState<Bytes>)> {
        let outboard_size = raw_outboard_size(size);
        let (data, data_location) = if options.is_inlined_data(size) {
            let data = read_to_end(&self.data, 0, size as usize)?;
            (MemOrFile::Mem(data.clone()), DataLocation::Inline(data))
        } else {
            (
                MemOrFile::File(FixedSize::new(self.data, size)),
                DataLocation::Owned(size),
            )
        };
        let (outboard, outboard_location) = if options.is_inlined_outboard(outboard_size) {
            if outboard_size == 0 {
                (MemOrFile::empty(), OutboardLocation::NotNeeded)
            } else {
                let outboard = read_to_end(&self.outboard, 0, outboard_size as usize)?;
                trace!("read outboard from file: {:?}", outboard.len());
                (
                    MemOrFile::Mem(outboard.clone()),
                    OutboardLocation::Inline(outboard),
                )
            }
        } else {
            (MemOrFile::File(self.outboard), OutboardLocation::Owned)
        };
        // todo: notify the store that the state has changed to complete
        Ok((
            CompleteStorage { data, outboard },
            EntryState::Complete {
                data_location,
                outboard_location,
            },
        ))
    }

    fn current_size(&self) -> io::Result<u64> {
        read_size(&self.sizes)
    }

    fn write_batch(&mut self, size: u64, batch: &[BaoContentItem]) -> io::Result<()> {
        let tree = BaoTree::new(size, IROH_BLOCK_SIZE);
        for item in batch {
            match item {
                BaoContentItem::Parent(parent) => {
                    if let Some(offset) = tree.pre_order_offset(parent.node) {
                        let o0 = offset * 64;
                        self.outboard
                            .write_all_at(o0, parent.pair.0.as_bytes().as_slice())?;
                        self.outboard
                            .write_all_at(o0 + 32, parent.pair.1.as_bytes().as_slice())?;
                    }
                }
                BaoContentItem::Leaf(leaf) => {
                    let o0 = leaf.offset;
                    // divide by chunk size, multiply by 8
                    let index = (leaf.offset >> (tree.block_size().chunk_log() + 10)) << 3;
                    trace!(
                        "write_batch f={:?} o={} l={}",
                        self.data,
                        o0,
                        leaf.data.len()
                    );
                    self.data.write_all_at(o0, leaf.data.as_ref())?;
                    let size = tree.size();
                    self.sizes.write_all_at(index, &size.to_le_bytes())?;
                }
            }
        }
        Ok(())
    }
}

fn read_size(size_file: &File) -> io::Result<u64> {
    let len = size_file.metadata()?.len();
    if len < 8 {
        Ok(0)
    } else {
        let len = len & !7;
        let mut buf = [0u8; 8];
        size_file.read_exact_at(len - 8, &mut buf)?;
        Ok(u64::from_le_bytes(buf))
    }
}

/// The storage for a bao file. This can be either in memory or on disk.
///
/// The two initial states `Initial` and `Loading` are used to coordinate the
/// loading of the entry from the metadata database. Once that is complete,
/// you should never see these states again.
///
/// From the remaining states you can get into `Poisoned` if there is an
/// IO error during an operation.
///
/// `Poisioned` is also used once the handle is persisted and no longer usable.
#[derive(derive_more::From, Default)]
pub(crate) enum BaoFileStorage {
    /// Initial state, we don't know anything yet.
    #[default]
    Initial,
    /// Currently loading the entry from the metadata.
    Loading,
    /// There is no info about this hash in the metadata db.
    NonExisting,
    /// The entry is incomplete and in memory.
    ///
    /// Since it is incomplete, it must be writeable.
    ///
    /// This is used mostly for tiny entries, <= 16 KiB. But in principle it
    /// can be used for larger sizes.
    ///
    /// Incomplete mem entries are *not* persisted at all. So if the store
    /// crashes they will be gone.
    PartialMem(PartialMemStorage),
    /// The entry is incomplete and on disk.
    Partial(PartialFileStorage),
    /// The entry is complete. Outboard and data can come from different sources
    /// (memory or file).
    ///
    /// Writing to this is a no-op, since it is already complete.
    Complete(CompleteStorage),
    /// We will get into that state if there is an io error in the middle of an operation,
    /// or after the handle is persisted and no longer usable.
    Poisoned,
}

impl fmt::Debug for BaoFileStorage {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            BaoFileStorage::PartialMem(x) => x.fmt(f),
            BaoFileStorage::Partial(x) => x.fmt(f),
            BaoFileStorage::Complete(x) => x.fmt(f),
            BaoFileStorage::Poisoned => f.debug_struct("Poisoned").finish(),
            BaoFileStorage::Initial => f.debug_struct("Initial").finish(),
            BaoFileStorage::Loading => f.debug_struct("Loading").finish(),
            BaoFileStorage::NonExisting => f.debug_struct("NonExisting").finish(),
        }
    }
}

impl PartialMemStorage {
    /// Converts this storage into a complete storage, using the given hash for
    /// path names and the given options for decisions about inlining.
    fn into_complete(self, ctx: &HashContext) -> io::Result<(CompleteStorage, EntryState<Bytes>)> {
        let options = &ctx.global.options;
        let hash = &ctx.id;
        let size = self.current_size();
        let outboard_size = raw_outboard_size(size);
        let (data, data_location) = if options.is_inlined_data(size) {
            let data: Bytes = self.data.to_vec().into();
            (MemOrFile::Mem(data.clone()), DataLocation::Inline(data))
        } else {
            let data_path = options.path.data_path(hash);
            let mut data_file = create_read_write(&data_path)?;
            self.data.persist(&mut data_file)?;
            (
                MemOrFile::File(FixedSize::new(data_file, size)),
                DataLocation::Owned(size),
            )
        };
        let (outboard, outboard_location) = if ctx.global.options.is_inlined_outboard(outboard_size)
        {
            if outboard_size > 0 {
                let outboard: Bytes = self.outboard.to_vec().into();
                (
                    MemOrFile::Mem(outboard.clone()),
                    OutboardLocation::Inline(outboard),
                )
            } else {
                (MemOrFile::empty(), OutboardLocation::NotNeeded)
            }
        } else {
            let outboard_path = ctx.global.options.path.outboard_path(hash);
            let mut outboard_file = create_read_write(&outboard_path)?;
            self.outboard.persist(&mut outboard_file)?;
            let outboard_location = if outboard_size == 0 {
                OutboardLocation::NotNeeded
            } else {
                OutboardLocation::Owned
            };
            (MemOrFile::File(outboard_file), outboard_location)
        };
        Ok((
            CompleteStorage { data, outboard },
            EntryState::Complete {
                data_location,
                outboard_location,
            },
        ))
    }
}

impl BaoFileStorage {
    pub fn bitfield(&self) -> Bitfield {
        match self {
            BaoFileStorage::Initial => {
                panic!("initial storage should not be used")
            }
            BaoFileStorage::Loading => {
                panic!("loading storage should not be used")
            }
            BaoFileStorage::NonExisting => Bitfield::empty(),
            BaoFileStorage::PartialMem(x) => x.bitfield.clone(),
            BaoFileStorage::Partial(x) => x.bitfield.clone(),
            BaoFileStorage::Complete(x) => Bitfield::complete(x.data.size()),
            BaoFileStorage::Poisoned => {
                panic!("poisoned storage should not be used")
            }
        }
    }

    fn write_batch(
        self,
        batch: &[BaoContentItem],
        bitfield: &Bitfield,
        ctx: &HashContext,
    ) -> io::Result<(Self, Option<EntryState<bytes::Bytes>>)> {
        Ok(match self {
            BaoFileStorage::NonExisting => {
                Self::new_partial_mem().write_batch(batch, bitfield, ctx)?
            }
            BaoFileStorage::PartialMem(mut ms) => {
                // check if we need to switch to file mode, otherwise write to memory
                if max_offset(batch) <= ctx.global.options.inline.max_data_inlined {
                    ms.write_batch(bitfield.size(), batch)?;
                    let changes = ms.bitfield.update(bitfield);
                    let new = changes.new_state();
                    if new.complete {
                        let (cs, update) = ms.into_complete(ctx)?;
                        (cs.into(), Some(update))
                    } else {
                        let fs = ms.persist(ctx)?;
                        let update = EntryState::Partial {
                            size: new.validated_size,
                        };
                        (fs.into(), Some(update))
                    }
                } else {
                    // *first* switch to file mode, *then* write the batch.
                    //
                    // otherwise we might allocate a lot of memory if we get
                    // a write at the end of a very large file.
                    //
                    // opt: we should check if we become complete to avoid going from mem to partial to complete
                    let mut fs = ms.persist(ctx)?;
                    fs.write_batch(bitfield.size(), batch)?;
                    let changes = fs.bitfield.update(bitfield);
                    let new = changes.new_state();
                    if new.complete {
                        let size = new.validated_size.unwrap();
                        let (cs, update) = fs.into_complete(size, &ctx.global.options)?;
                        (cs.into(), Some(update))
                    } else {
                        let update = EntryState::Partial {
                            size: new.validated_size,
                        };
                        (fs.into(), Some(update))
                    }
                }
            }
            BaoFileStorage::Partial(mut fs) => {
                fs.write_batch(bitfield.size(), batch)?;
                let changes = fs.bitfield.update(bitfield);
                let new = changes.new_state();
                if new.complete {
                    let size = new.validated_size.unwrap();
                    let (cs, update) = fs.into_complete(size, &ctx.global.options)?;
                    (cs.into(), Some(update))
                } else if changes.was_validated() {
                    // we are still partial, but now we know the size
                    let update = EntryState::Partial {
                        size: new.validated_size,
                    };
                    (fs.into(), Some(update))
                } else {
                    (fs.into(), None)
                }
            }
            BaoFileStorage::Complete(_) => {
                // we are complete, so just ignore the write
                // unless there is a bug, this would just write the exact same data
                (self, None)
            }
            _ => {
                // we are poisoned, so just ignore the write
                (self, None)
            }
        })
    }

    /// Create a new mutable mem storage.
    pub fn new_partial_mem() -> Self {
        Self::PartialMem(Default::default())
    }

    /// Call sync_all on all the files.
    #[allow(dead_code)]
    fn sync_all(&self) -> io::Result<()> {
        match self {
            Self::Complete(_) => Ok(()),
            Self::PartialMem(_) => Ok(()),
            Self::NonExisting => Ok(()),
            Self::Partial(file) => {
                file.data.sync_all()?;
                file.outboard.sync_all()?;
                file.sizes.sync_all()?;
                Ok(())
            }
            Self::Poisoned | Self::Initial | Self::Loading => {
                // we are poisoned, so just ignore the sync
                Ok(())
            }
        }
    }

    pub fn take(&mut self) -> Self {
        std::mem::replace(self, BaoFileStorage::Poisoned)
    }
}

/// A cheaply cloneable handle to a bao file, including the hash and the configuration.
#[derive(Debug, Clone, Default, derive_more::Deref)]
pub(crate) struct BaoFileHandle(pub(super) watch::Sender<BaoFileStorage>);

impl entity_manager::Reset for BaoFileHandle {
    fn reset(&mut self) {
        self.send_replace(BaoFileStorage::Initial);
    }

    fn ref_count(&self) -> usize {
        self.0.receiver_count() + self.0.sender_count()
    }
}

/// A reader for a bao file, reading just the data.
#[derive(Debug)]
pub struct DataReader(BaoFileHandle);

impl ReadBytesAt for DataReader {
    fn read_bytes_at(&self, offset: u64, size: usize) -> std::io::Result<Bytes> {
        let guard = self.0.borrow();
        match guard.deref() {
            BaoFileStorage::PartialMem(x) => x.data.read_bytes_at(offset, size),
            BaoFileStorage::Partial(x) => x.data.read_bytes_at(offset, size),
            BaoFileStorage::Complete(x) => x.data.read_bytes_at(offset, size),
            BaoFileStorage::Poisoned => io::Result::Err(io::Error::other("poisoned storage")),
            BaoFileStorage::Initial => io::Result::Err(io::Error::other("initial")),
            BaoFileStorage::Loading => io::Result::Err(io::Error::other("loading")),
            BaoFileStorage::NonExisting => io::Result::Err(io::ErrorKind::NotFound.into()),
        }
    }
}

/// A reader for the outboard part of a bao file.
#[derive(Debug)]
pub struct OutboardReader(BaoFileHandle);

impl ReadAt for OutboardReader {
    fn read_at(&self, offset: u64, buf: &mut [u8]) -> io::Result<usize> {
        let guard = self.0.borrow();
        match guard.deref() {
            BaoFileStorage::Complete(x) => x.outboard.read_at(offset, buf),
            BaoFileStorage::PartialMem(x) => x.outboard.read_at(offset, buf),
            BaoFileStorage::Partial(x) => x.outboard.read_at(offset, buf),
            BaoFileStorage::Poisoned => io::Result::Err(io::Error::other("poisoned storage")),
            BaoFileStorage::Initial => io::Result::Err(io::Error::other("initial")),
            BaoFileStorage::Loading => io::Result::Err(io::Error::other("loading")),
            BaoFileStorage::NonExisting => io::Result::Err(io::ErrorKind::NotFound.into()),
        }
    }
}

impl BaoFileStorage {
    pub async fn open(state: Option<EntryState<Bytes>>, ctx: &HashContext) -> io::Result<Self> {
        let hash = &ctx.id;
        let options = &ctx.global.options;
        Ok(match state {
            Some(EntryState::Complete {
                data_location,
                outboard_location,
            }) => {
                let data = match data_location {
                    DataLocation::Inline(data) => MemOrFile::Mem(data),
                    DataLocation::Owned(size) => {
                        let path = options.path.data_path(hash);
                        let file = std::fs::File::open(&path)?;
                        MemOrFile::File(FixedSize::new(file, size))
                    }
                    DataLocation::External(paths, size) => {
                        let Some(path) = paths.into_iter().next() else {
                            return Err(io::Error::other("no external data path"));
                        };
                        let file = std::fs::File::open(&path)?;
                        MemOrFile::File(FixedSize::new(file, size))
                    }
                };
                let outboard = match outboard_location {
                    OutboardLocation::NotNeeded => MemOrFile::empty(),
                    OutboardLocation::Inline(data) => MemOrFile::Mem(data),
                    OutboardLocation::Owned => {
                        let path = options.path.outboard_path(hash);
                        let file = std::fs::File::open(&path)?;
                        MemOrFile::File(file)
                    }
                };
                Self::new_complete(data, outboard)
            }
            Some(EntryState::Partial { .. }) => Self::new_partial_file(ctx).await?,
            None => Self::NonExisting,
        })
    }

    /// Create a new bao file handle with a partial file.
    pub(super) async fn new_partial_file(ctx: &HashContext) -> io::Result<Self> {
        let hash = &ctx.id;
        let options = ctx.global.options.clone();
        let storage = PartialFileStorage::load(hash, &options.path)?;
        Ok(if storage.bitfield.is_complete() {
            let size = storage.bitfield.size;
            let (storage, entry_state) = storage.into_complete(size, &options)?;
            debug!("File was reconstructed as complete");
            ctx.global.db.set(*hash, entry_state).await?;
            storage.into()
        } else {
            storage.into()
        })
    }

    /// Create a new complete bao file handle.
    pub fn new_complete(
        data: MemOrFile<Bytes, FixedSize<File>>,
        outboard: MemOrFile<Bytes, File>,
    ) -> Self {
        CompleteStorage { data, outboard }.into()
    }
}

impl BaoFileHandle {
    pub(super) fn persist(&mut self, ctx: &HashContext) {
        self.send_if_modified(|guard| {
            let hash = &ctx.id;
            // if Arc::strong_count(&self.0) > 1 {
            //     // no one is listening, so we don't need to persist
            //     println!("abort persist - not unique {} {} {}", Arc::strong_count(&self.0), self.0.receiver_count(), self.0.sender_count());
            //     return false;
            // }
            let BaoFileStorage::Partial(fs) = guard.take() else {
                return false;
            };
            let path = ctx.global.options.path.bitfield_path(hash);
            trace!("writing bitfield for hash {} to {}", hash, path.display());
            if let Err(cause) = fs.sync_all(&path) {
                error!(
                    "failed to write bitfield for {} at {}: {:?}",
                    hash,
                    path.display(),
                    cause
                );
            }
            false
        });
    }

    /// Complete the handle
    pub fn complete(
        &self,
        data: MemOrFile<Bytes, FixedSize<File>>,
        outboard: MemOrFile<Bytes, File>,
    ) {
        self.send_if_modified(|guard| {
            let needs_complete = match guard {
                BaoFileStorage::NonExisting => true,
                BaoFileStorage::Complete(_) => false,
                BaoFileStorage::PartialMem(_) => true,
                BaoFileStorage::Partial(_) => true,
                _ => false,
            };
            if needs_complete {
                *guard = BaoFileStorage::Complete(CompleteStorage { data, outboard });
                true
            } else {
                true
            }
        });
    }

    pub fn subscribe(&self) -> BaoFileStorageSubscriber {
        BaoFileStorageSubscriber::new(self.0.subscribe())
    }

    /// True if the file is complete.
    #[allow(dead_code)]
    pub fn is_complete(&self) -> bool {
        matches!(self.borrow().deref(), BaoFileStorage::Complete(_))
    }

    /// An AsyncSliceReader for the data file.
    ///
    /// Caution: this is a reader for the unvalidated data file. Reading this
    /// can produce data that does not match the hash.
    pub fn data_reader(&self) -> DataReader {
        DataReader(self.clone())
    }

    /// An AsyncSliceReader for the outboard file.
    ///
    /// The outboard file is used to validate the data file. It is not guaranteed
    /// to be complete.
    pub fn outboard_reader(&self) -> OutboardReader {
        OutboardReader(self.clone())
    }

    /// The most precise known total size of the data file.
    pub fn current_size(&self) -> io::Result<u64> {
        match self.borrow().deref() {
            BaoFileStorage::Complete(mem) => Ok(mem.size()),
            BaoFileStorage::PartialMem(mem) => Ok(mem.current_size()),
            BaoFileStorage::Partial(file) => file.current_size(),
            BaoFileStorage::Poisoned => io::Result::Err(io::Error::other("poisoned storage")),
            BaoFileStorage::Initial => io::Result::Err(io::Error::other("initial")),
            BaoFileStorage::Loading => io::Result::Err(io::Error::other("loading")),
            BaoFileStorage::NonExisting => io::Result::Err(io::ErrorKind::NotFound.into()),
        }
    }

    /// The most precise known total size of the data file.
    pub fn bitfield(&self) -> io::Result<Bitfield> {
        match self.borrow().deref() {
            BaoFileStorage::Complete(mem) => Ok(mem.bitfield()),
            BaoFileStorage::PartialMem(mem) => Ok(mem.bitfield().clone()),
            BaoFileStorage::Partial(file) => Ok(file.bitfield().clone()),
            BaoFileStorage::Poisoned => io::Result::Err(io::Error::other("poisoned storage")),
            BaoFileStorage::Initial => io::Result::Err(io::Error::other("initial")),
            BaoFileStorage::Loading => io::Result::Err(io::Error::other("loading")),
            BaoFileStorage::NonExisting => io::Result::Err(io::ErrorKind::NotFound.into()),
        }
    }

    /// The outboard for the file.
    pub fn outboard(&self, hash: &Hash) -> io::Result<PreOrderOutboard<OutboardReader>> {
        let tree = BaoTree::new(self.current_size()?, IROH_BLOCK_SIZE);
        let outboard = self.outboard_reader();
        Ok(PreOrderOutboard {
            root: blake3::Hash::from(*hash),
            tree,
            data: outboard,
        })
    }

    /// Write a batch and notify the db
    pub(super) async fn write_batch(
        &self,
        batch: &[BaoContentItem],
        bitfield: &Bitfield,
        ctx: &HashContext,
    ) -> io::Result<()> {
        trace!("write_batch bitfield={:?} batch={}", bitfield, batch.len());
        let mut res = Ok(None);
        self.send_if_modified(|state| {
            let Ok((state1, update)) = state.take().write_batch(batch, bitfield, ctx) else {
                res = Err(io::Error::other("write batch failed"));
                return false;
            };
            res = Ok(update);
            *state = state1;
            true
        });
        if let Some(update) = res? {
            ctx.global.db.update(ctx.id, update).await?;
        }
        Ok(())
    }
}

impl PartialMemStorage {
    /// Persist the batch to disk.
    fn persist(self, ctx: &HashContext) -> io::Result<PartialFileStorage> {
        let options = &ctx.global.options.path;
        let hash = &ctx.id;
        ctx.global.protect.protect(
            *hash,
            [
                BaoFilePart::Data,
                BaoFilePart::Outboard,
                BaoFilePart::Sizes,
                BaoFilePart::Bitfield,
            ],
        );
        let mut data = create_read_write(options.data_path(hash))?;
        let mut outboard = create_read_write(options.outboard_path(hash))?;
        let mut sizes = create_read_write(options.sizes_path(hash))?;
        self.data.persist(&mut data)?;
        self.outboard.persist(&mut outboard)?;
        self.size.persist(&mut sizes)?;
        data.sync_all()?;
        outboard.sync_all()?;
        sizes.sync_all()?;
        Ok(PartialFileStorage {
            data,
            outboard,
            sizes,
            bitfield: self.bitfield,
        })
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
    pub async fn forward(mut self, mut tx: mpsc::Sender<Bitfield>) -> anyhow::Result<()> {
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
    pub async fn forward_delta(mut self, mut tx: mpsc::Sender<Bitfield>) -> anyhow::Result<()> {
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

    async fn update_or_closed(&mut self, tx: &mut mpsc::Sender<Bitfield>) -> anyhow::Result<()> {
        tokio::select! {
            _ = tx.closed() => {
                // the sender is closed, we are done
                Err(irpc::channel::SendError::ReceiverClosed.into())
            }
            e = self.receiver.changed() => Ok(e?),
        }
    }
}

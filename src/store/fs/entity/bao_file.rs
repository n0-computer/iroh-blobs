use core::fmt;
use std::{
    fs::{File, OpenOptions},
    io,
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
use tracing::{debug, info, trace};

use crate::store::fs::options::FsConfig;
use crate::{
    api::blobs::Bitfield,
    store::host::metadata::{
        entry_state::{DataLocation, EntryState, OutboardLocation},
        parts::BaoFilePart,
    },
    store::host::{Location, Phase, StateCell, WriteGuard},
    store::{
        fs::entity::HashContext,
        host::{bao::raw_outboard_size, StoreOptions},
        util::{
            read_checksummed_and_truncate, size_slot, write_checksummed, FixedSize, MemOrFile,
            PartialMemStorage, DD,
        },
        IROH_BLOCK_SIZE,
    },
    Hash,
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

/// Opens the first external data file that can be opened.
///
/// Fails with `NotFound` only if all files are missing. Otherwise the first
/// error that is not `NotFound` is returned, so real IO faults stay visible.
/// An empty list never happens in normal use and is reported as corrupt metadata.
fn open_external(paths: &[Location]) -> io::Result<File> {
    let mut error: Option<io::Error> = None;
    for location in paths {
        match location.to_path().and_then(File::open) {
            Ok(file) => return Ok(file),
            Err(cause) => {
                debug!("failed to open external data file {location:?}: {cause}");
                if error
                    .as_ref()
                    .is_none_or(|e| e.kind() == io::ErrorKind::NotFound)
                {
                    error = Some(cause);
                }
            }
        }
    }
    Err(error.unwrap_or_else(|| io::Error::other("no external data path")))
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

    /// Writes the bitfield, after the files it describes, syncing them first if `sync` is set.
    pub(super) fn write_bitfield(&self, bitfield_path: &Path, sync: bool) -> io::Result<()> {
        if sync {
            self.data.sync_all()?;
            self.outboard.sync_all()?;
            self.sizes.sync_all()?;
        }
        // only write the bitfield if the syncs were successful
        trace!(
            "writing bitfield {:?} to {}",
            self.bitfield,
            bitfield_path.display()
        );
        // The bitfield itself is not synced, not even for PowerLoss. It is a cache
        // of what valid_ranges computes from the files, so it only must never
        // claim more than they hold, which the syncs above ensure. If it is lost
        // or torn in a crash, it comes back empty or fails its checksum, and the
        // next load rebuilds it from the files.
        write_checksummed(bitfield_path, &self.bitfield)?;
        Ok(())
    }

    fn load(hash: &Hash, config: &FsConfig) -> io::Result<Self> {
        let paths = &config.path;
        let bitfield_path = paths.bitfield_path(hash);
        let data = create_read_write(paths.data_path(hash))?;
        let outboard = create_read_write(paths.outboard_path(hash))?;
        let sizes = create_read_write(paths.sizes_path(hash))?;
        let bitfield = match read_checksummed_and_truncate(&bitfield_path, config.syncs_files()) {
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
        options: &StoreOptions,
        config: &FsConfig,
    ) -> io::Result<(CompleteStorage, EntryState<Bytes>)> {
        let outboard_size = raw_outboard_size(size);
        let (data, data_location) = if options.is_inlined_data(size) {
            let data = read_to_end(&self.data, 0, size as usize)?;
            (MemOrFile::Mem(data.clone()), DataLocation::Inline(data))
        } else {
            if config.syncs_files() {
                self.data.sync_all()?;
            }
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
            if config.syncs_files() {
                self.outboard.sync_all()?;
            }
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

    pub(super) fn current_size(&self) -> io::Result<u64> {
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
                    // one size slot per chunk group
                    let index = size_slot(leaf.offset);
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

/// A loaded entry.
#[derive(derive_more::From)]
pub(crate) enum BaoStorage {
    /// There is no entry for this hash in the metadata.
    NonExisting,
    /// The entry is incomplete and on disk.
    Partial(PartialFileStorage),
    /// The entry is complete. Outboard and data can come from different sources
    /// (memory or file).
    ///
    /// Writing to this is a no-op, since it is already complete.
    Complete(CompleteStorage),
    /// The in-memory state cannot be trusted: an IO error happened during an
    /// operation, or the entity was persisted. Writes fail. A finished import
    /// or a delete replaces it, and recycling the entity resets it.
    Invalid,
}

impl fmt::Debug for BaoStorage {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            BaoStorage::NonExisting => f.debug_struct("NonExisting").finish(),
            BaoStorage::Partial(x) => x.fmt(f),
            BaoStorage::Complete(x) => x.fmt(f),
            BaoStorage::Invalid => f.debug_struct("Invalid").finish(),
        }
    }
}

impl PartialMemStorage {
    /// Converts this storage into a complete storage, using the given hash for
    /// path names and the given options for decisions about inlining.
    fn into_complete(
        self,
        ctx: &HashContext<'_>,
    ) -> io::Result<(CompleteStorage, EntryState<Bytes>)> {
        let options = &ctx.global.options;
        let hash = &ctx.id;
        let size = self.current_size();
        let outboard_size = raw_outboard_size(size);
        let (data, data_location) = if options.is_inlined_data(size) {
            let data: Bytes = self.data.to_vec().into();
            (MemOrFile::Mem(data.clone()), DataLocation::Inline(data))
        } else {
            let data_path = ctx.config.path.data_path(hash);
            let mut data_file = create_read_write(&data_path)?;
            self.data.persist(&mut data_file)?;
            if ctx.config.syncs_files() {
                data_file.sync_all()?;
            }
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
            let outboard_path = ctx.config.path.outboard_path(hash);
            let mut outboard_file = create_read_write(&outboard_path)?;
            self.outboard.persist(&mut outboard_file)?;
            if ctx.config.syncs_files() {
                outboard_file.sync_all()?;
            }
            let outboard_location = if outboard_size == 0 {
                OutboardLocation::NotNeeded
            } else {
                OutboardLocation::Owned
            };
            (MemOrFile::File(outboard_file), outboard_location)
        };
        let created = matches!(data, MemOrFile::File(_)) || matches!(outboard, MemOrFile::File(_));
        if ctx.config.syncs_files() && created {
            super::sync_dir(&ctx.config.path.data_path)?;
        }
        Ok((
            CompleteStorage { data, outboard },
            EntryState::Complete {
                data_location,
                outboard_location,
            },
        ))
    }
}

impl BaoStorage {
    /// The bitfield an observer sees, or `None` if the entry is invalid.
    ///
    /// Observers end the stream on `None`, since there is no error channel.
    pub(super) fn observed_bitfield(&self) -> Option<Bitfield> {
        match self {
            BaoStorage::Invalid => None,
            _ => Some(self.bitfield()),
        }
    }

    pub fn bitfield(&self) -> Bitfield {
        match self {
            BaoStorage::NonExisting => Bitfield::empty(),
            BaoStorage::Partial(x) => x.bitfield.clone(),
            BaoStorage::Complete(x) => Bitfield::complete(x.data.size()),
            BaoStorage::Invalid => panic!("invalid storage should not be used"),
        }
    }

    pub(super) fn write_batch(
        self,
        batch: &[BaoContentItem],
        bitfield: &Bitfield,
        ctx: &HashContext<'_>,
    ) -> io::Result<(Self, Option<EntryState<bytes::Bytes>>)> {
        Ok(match self {
            // A new entry starts in memory, but never stays there: it leaves
            // this call partial on disk, or complete.
            BaoStorage::NonExisting => {
                write_new_entry(PartialMemStorage::default(), batch, bitfield, ctx)?
            }
            BaoStorage::Partial(mut fs) => {
                fs.write_batch(bitfield.size(), batch)?;
                let changes = fs.bitfield.update(bitfield);
                let new = changes.new_state();
                if new.complete {
                    let size = new.validated_size.unwrap();
                    let (cs, update) = fs.into_complete(size, &ctx.global.options, ctx.config)?;
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
            BaoStorage::Complete(_) => {
                // we are complete, so just ignore the write
                // unless there is a bug, this would just write the exact same data
                (self, None)
            }
            BaoStorage::Invalid => return Err(io::Error::other("invalid storage")),
        })
    }

    /// Whether this state uses the given file of its entry.
    pub(super) fn references(&self, part: BaoFilePart) -> bool {
        match self {
            BaoStorage::NonExisting => false,
            BaoStorage::Partial(_) => true,
            BaoStorage::Complete(x) => match part {
                BaoFilePart::Data => matches!(x.data, MemOrFile::File(_)),
                BaoFilePart::Outboard => matches!(x.outboard, MemOrFile::File(_)),
                BaoFilePart::Sizes | BaoFilePart::Bitfield => false,
            },
            // Cannot tell.
            BaoStorage::Invalid => true,
        }
    }
}

/// Writes the first batch of a new entry, which starts in memory.
fn write_new_entry(
    mut ms: PartialMemStorage,
    batch: &[BaoContentItem],
    bitfield: &Bitfield,
    ctx: &HashContext<'_>,
) -> io::Result<(BaoStorage, Option<EntryState<bytes::Bytes>>)> {
    // check if we need to switch to file mode, otherwise write to memory
    Ok(
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
                let (cs, update) = fs.into_complete(size, &ctx.global.options, ctx.config)?;
                (cs.into(), Some(update))
            } else {
                let update = EntryState::Partial {
                    size: new.validated_size,
                };
                (fs.into(), Some(update))
            }
        },
    )
}

impl Phase<BaoStorage> {
    /// Whether this state uses the given file of its entry.
    ///
    /// Says yes when it cannot tell, since a file kept too long only leaks,
    /// while a file deleted too early loses data.
    pub(super) fn references(&self, part: BaoFilePart) -> bool {
        match self {
            // Not loaded: the entity knows nothing about its files.
            Self::Initial | Self::Loading | Self::Deleting => true,
            Self::ReadWrite(s) | Self::ReadOnly(s) => s.references(part),
        }
    }

    /// Takes the state, leaving an invalid one behind.
    pub fn take(&mut self) -> Self {
        std::mem::replace(self, Self::ReadWrite(BaoStorage::Invalid))
    }
}

/// A reader for a bao file, reading just the data.
#[derive(Debug)]
pub struct DataReader(pub(super) StateCell<BaoStorage>);

impl ReadBytesAt for DataReader {
    fn read_bytes_at(&self, offset: u64, size: usize) -> std::io::Result<Bytes> {
        self.0.peek(|state| match state.try_settled()? {
            BaoStorage::Partial(x) => x.data.read_bytes_at(offset, size),
            BaoStorage::Complete(x) => x.data.read_bytes_at(offset, size),
            BaoStorage::Invalid => Err(io::Error::other("invalid storage")),
            BaoStorage::NonExisting => Err(io::ErrorKind::NotFound.into()),
        })
    }
}

/// A reader for the outboard part of a bao file.
#[derive(Debug)]
pub struct OutboardReader(pub(super) StateCell<BaoStorage>);

impl ReadAt for OutboardReader {
    fn read_at(&self, offset: u64, buf: &mut [u8]) -> io::Result<usize> {
        self.0.peek(|state| match state.try_settled()? {
            BaoStorage::Partial(x) => x.outboard.read_at(offset, buf),
            BaoStorage::Complete(x) => x.outboard.read_at(offset, buf),
            BaoStorage::Invalid => Err(io::Error::other("invalid storage")),
            BaoStorage::NonExisting => Err(io::ErrorKind::NotFound.into()),
        })
    }
}

impl BaoStorage {
    pub(super) async fn open(
        state: Option<EntryState<Bytes>>,
        ctx: &HashContext<'_>,
    ) -> io::Result<Self> {
        let hash = &ctx.id;
        Ok(match state {
            Some(EntryState::Complete {
                data_location,
                outboard_location,
            }) => {
                let data = match data_location {
                    DataLocation::Inline(data) => MemOrFile::Mem(data),
                    DataLocation::Owned(size) => {
                        let path = ctx.config.path.data_path(hash);
                        let file = std::fs::File::open(&path)?;
                        MemOrFile::File(FixedSize::new(file, size))
                    }
                    DataLocation::External(paths, size) => {
                        let file = open_external(&paths)?;
                        MemOrFile::File(FixedSize::new(file, size))
                    }
                };
                let outboard = match outboard_location {
                    OutboardLocation::NotNeeded => MemOrFile::empty(),
                    OutboardLocation::Inline(data) => MemOrFile::Mem(data),
                    OutboardLocation::Owned => {
                        let path = ctx.config.path.outboard_path(hash);
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
    pub(super) async fn new_partial_file(ctx: &HashContext<'_>) -> io::Result<Self> {
        let hash = &ctx.id;
        let storage = PartialFileStorage::load(hash, ctx.config)?;
        Ok(if storage.bitfield.is_complete() {
            let size = storage.bitfield.size;
            let (storage, entry_state) =
                storage.into_complete(size, &ctx.global.options, ctx.config)?;
            debug!("File was reconstructed as complete");
            ctx.meta().set(entry_state).await?;
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

/// Completes the entry, for a finished import.
pub(super) fn complete(
    write: &WriteGuard<'_, BaoStorage>,
    data: MemOrFile<Bytes, FixedSize<File>>,
    outboard: MemOrFile<Bytes, File>,
) {
    write.modify(|state| {
        // A finished import does not depend on the old content, so it
        // completes any writable state, an invalid one included.
        let needs_complete = match state {
            Phase::ReadWrite(BaoStorage::Complete(_)) => false,
            Phase::ReadWrite(_) => true,
            _ => false,
        };
        if needs_complete {
            *state = Phase::ReadWrite(BaoStorage::Complete(CompleteStorage { data, outboard }));
        }
        true
    });
}

impl PartialMemStorage {
    /// Persist the batch to disk.
    fn persist(self, ctx: &HashContext<'_>) -> io::Result<PartialFileStorage> {
        let options = &ctx.config.path;
        let sync = ctx.config.syncs_files();
        let hash = &ctx.id;
        let mut data = create_read_write(options.data_path(hash))?;
        let mut outboard = create_read_write(options.outboard_path(hash))?;
        let mut sizes = create_read_write(options.sizes_path(hash))?;
        self.data.persist(&mut data)?;
        self.outboard.persist(&mut outboard)?;
        self.size.persist(&mut sizes)?;
        if sync {
            data.sync_all()?;
            outboard.sync_all()?;
            sizes.sync_all()?;
            super::sync_dir(&options.data_path)?;
        }
        Ok(PartialFileStorage {
            data,
            outboard,
            sizes,
            bitfield: self.bitfield,
        })
    }
}

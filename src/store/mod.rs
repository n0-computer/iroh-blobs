//! Store implementations
//!
//! Use the [`mem`] store for sharing a small amount of mutable data,
//! the [`readonly_mem`] store for sharing static data, and the [`fs`] store
//! for when you want to efficiently share more than the available memory and
//! have access to a writeable filesystem.
use std::{fmt, fs::File, path::PathBuf};

use bao_tree::BlockSize;
#[cfg(feature = "fs-store")]
pub mod fs;
mod gc;
pub mod mem;
pub mod readonly_mem;
mod test;
pub(crate) mod util;

/// Block size used by iroh, 2^4*1024 = 16KiB
pub const IROH_BLOCK_SIZE: BlockSize = BlockSize::from_chunk_log(4);

use bytes::Bytes;
pub use gc::{GcConfig, ProtectCb, ProtectOutcome};
use irpc::WithChannels;

use crate::{
    api::{
        proto::{
            ExportBaoMsg, ExportPathMsg, ExportRangesMsg, ImportBaoMsg, ObserveMsg, Request, Scope,
        },
        TempTag,
    },
    store::util::{MemOrFile, DD},
    BlobFormat, Hash,
};

/// The minimal API you need to implement for an entity for a store to work.
#[allow(async_fn_in_trait)]
pub trait EntityApi {
    /// Import from a stream of n0 bao encoded data.
    async fn import_bao(&self, cmd: ImportBaoMsg);
    /// Finish an import from a local file or memory.
    async fn finish_import(&self, cmd: ImportEntryMsg, tt: TempTag);
    /// Observe the bitfield of the entry.
    async fn observe(&self, cmd: ObserveMsg);
    /// Export byte ranges of the entry as data
    async fn export_ranges(&self, cmd: ExportRangesMsg);
    /// Export chunk ranges of the entry as a n0 bao encoded stream.
    async fn export_bao(&self, cmd: ExportBaoMsg);
    /// Export the entry to a local file.
    async fn export_path(&self, cmd: ExportPathMsg);
    /// Persist the entry at the end of its lifecycle.
    async fn persist(&self);
}

pub type ImportEntryMsg = WithChannels<ImportEntry, Request>;

/// An import entry.
///
/// This is the final result of an import operation. It gets passed to the store
/// for integration.
///
/// The store can assume that the outboard, if on disk, is in a location where
/// it can be moved to the final location (basically it needs to be on the same device).
pub struct ImportEntry {
    pub hash: Hash,
    pub format: BlobFormat,
    pub scope: Scope,
    pub source: ImportSource,
    pub outboard: MemOrFile<Bytes, PathBuf>,
}

impl std::fmt::Debug for ImportEntry {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ImportEntry")
            .field("hash", &self.hash)
            .field("format", &self.format)
            .field("scope", &self.scope)
            .field("source", &DD(self.source.fmt_short()))
            .field("outboard", &DD(self.outboard.fmt_short()))
            .finish()
    }
}

/// An import source.
///
/// It must provide a way to read the data synchronously, as well as the size
/// and the file location.
///
/// This serves as an intermediate result between copying and outboard computation.
pub enum ImportSource {
    TempFile(PathBuf, File, u64),
    External(PathBuf, File, u64),
    Memory(Bytes),
}

impl std::fmt::Debug for ImportSource {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::TempFile(path, _, size) => {
                f.debug_tuple("TempFile").field(path).field(size).finish()
            }
            Self::External(path, _, size) => {
                f.debug_tuple("External").field(path).field(size).finish()
            }
            Self::Memory(data) => f.debug_tuple("Memory").field(&data.len()).finish(),
        }
    }
}

impl ImportSource {
    pub fn fmt_short(&self) -> String {
        match self {
            Self::TempFile(path, _, _) => format!("TempFile({})", path.display()),
            Self::External(path, _, _) => format!("External({})", path.display()),
            Self::Memory(data) => format!("Memory({})", data.len()),
        }
    }

    fn is_mem(&self) -> bool {
        matches!(self, Self::Memory(_))
    }

    /// A reader for the import source.
    fn read(&self) -> MemOrFile<std::io::Cursor<&[u8]>, &File> {
        match self {
            Self::TempFile(_, file, _) => MemOrFile::File(file),
            Self::External(_, file, _) => MemOrFile::File(file),
            Self::Memory(data) => MemOrFile::Mem(std::io::Cursor::new(data.as_ref())),
        }
    }

    /// The size of the import source.
    fn size(&self) -> u64 {
        match self {
            Self::TempFile(_, _, size) => *size,
            Self::External(_, _, size) => *size,
            Self::Memory(data) => data.len() as u64,
        }
    }
}

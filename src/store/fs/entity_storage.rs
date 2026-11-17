//! Storage I/O hooks for the entity coordinator.
//!
//! Hooks run synchronously between the coordinator's await points. They do not
//! access metadata or manage loading, freezes, generations, or notifications.
//! The coordinator supplies the hash and configuration from its fixed binding.
//!
//! Backends share the filesystem store's metadata format and file layout:
//! import preparation, path export, and startup recovery still use that format.
//! This supports alternative access to the same files (such as mappings), not
//! arbitrary remote storage. Runtime bitfield persistence belongs to the backend;
//! invalidation, generations, and GC freezes belong to the coordinator.

use std::io;

use bao_tree::io::BaoContentItem;
use bytes::Bytes;

use super::{entry_state::EntryState, file_deletes::BaoFilePart, import::ImportEntry};
use crate::{api::blobs::Bitfield, Hash};

/// Resulting storage and an optional metadata correction or merge.
pub(super) type StorageChange<S> = (S, Option<EntryState<Bytes>>);

pub(super) trait EntityStorage: Sized + std::fmt::Debug + Send + Sync + 'static {
    type Config: std::fmt::Debug + Send + Sync + 'static;

    /// Empty storage after a committed deletion, ready for new writes.
    fn empty() -> Self;
    /// Whether an import can preserve this storage and a path export can reference it.
    fn is_complete(&self) -> bool;

    /// Open/recover storage. An optional correction replaces the metadata
    /// before the coordinator publishes the loaded storage.
    fn open(
        config: &Self::Config,
        hash: &Hash,
        entry: Option<EntryState<Bytes>>,
    ) -> io::Result<StorageChange<Self>>;

    /// Write a batch and return storage plus an optional metadata merge.
    /// Consumes the old storage: on failure the coordinator leaves it invalid.
    fn write_batch(
        self,
        batch: &[BaoContentItem],
        bitfield: &Bitfield,
        config: &Self::Config,
        hash: &Hash,
    ) -> io::Result<StorageChange<Self>>;

    /// Install an import. The coordinator publishes storage, merges metadata,
    /// and waits for that merge before reporting success.
    fn finish_import(
        config: &Self::Config,
        hash: &Hash,
        import: ImportEntry,
    ) -> io::Result<(Self, EntryState<Bytes>)>;

    fn read_data_at(&self, offset: u64, len: usize) -> io::Result<Bytes>;
    fn read_outboard_at(&self, offset: u64, buf: &mut [u8]) -> io::Result<usize>;
    fn current_size(&self) -> io::Result<u64>;
    /// Return NotFound for absent storage. Observers translate that to empty.
    fn bitfield(&self) -> io::Result<Bitfield>;

    /// Save backend state, including any bitfield cache, before the entity unloads.
    /// Called for every valid storage state, including empty and complete entries.
    fn persist(&self, config: &Self::Config, hash: &Hash) -> io::Result<()>;
    /// Return true when uncertain: retaining a file is safer than losing data.
    fn references(&self, part: BaoFilePart) -> bool;
    /// Best-effort cleanup, logging failures. The coordinator calls this only
    /// after a committed delete or checking unreferenced parts against storage.
    fn delete_parts(config: &Self::Config, hash: &Hash, parts: &[BaoFilePart]);
}

//! Options for configuring the file store.
use std::path::{Path, PathBuf};

use n0_future::time::Duration;

use super::{meta::raw_outboard_size, temp_name};
use crate::{store::gc::GcConfig, Hash};

/// Options for directories used by the file store.
#[derive(Debug, Clone)]
pub struct PathOptions {
    /// Path to the directory where data and outboard files are stored.
    pub data_path: PathBuf,
    /// Path to the directory where temp files are stored.
    /// This *must* be on the same device as `data_path`, since we need to
    /// atomically move temp files into place.
    pub temp_path: PathBuf,
}

impl PathOptions {
    pub fn new(root: &Path) -> Self {
        Self {
            data_path: root.join("data"),
            temp_path: root.join("temp"),
        }
    }

    pub fn data_path(&self, hash: &Hash) -> PathBuf {
        self.data_path.join(format!("{}.data", hash.to_hex()))
    }

    pub fn outboard_path(&self, hash: &Hash) -> PathBuf {
        self.data_path.join(format!("{}.obao4", hash.to_hex()))
    }

    pub fn sizes_path(&self, hash: &Hash) -> PathBuf {
        self.data_path.join(format!("{}.sizes4", hash.to_hex()))
    }

    pub fn bitfield_path(&self, hash: &Hash) -> PathBuf {
        self.data_path.join(format!("{}.bitfield", hash.to_hex()))
    }

    pub fn temp_file_name(&self) -> PathBuf {
        self.temp_path.join(temp_name())
    }
}

/// Options for inlining small complete data or outboards.
#[derive(Debug, Clone)]
pub struct InlineOptions {
    /// Maximum data size to inline.
    pub max_data_inlined: u64,
    /// Maximum outboard size to inline.
    pub max_outboard_inlined: u64,
}

impl InlineOptions {
    /// Do not inline anything, ever.
    pub const NO_INLINE: Self = Self {
        max_data_inlined: 0,
        max_outboard_inlined: 0,
    };
    /// Always inline everything
    pub const ALWAYS_INLINE: Self = Self {
        max_data_inlined: u64::MAX,
        max_outboard_inlined: u64::MAX,
    };
}

impl Default for InlineOptions {
    fn default() -> Self {
        Self {
            max_data_inlined: 1024 * 16,
            max_outboard_inlined: 1024 * 16,
        }
    }
}

/// Options for transaction batching.
#[derive(Debug, Clone)]
pub struct BatchOptions {
    /// Maximum number of actor messages to batch before creating a new read transaction.
    pub max_read_batch: usize,
    /// Maximum duration to wait before committing a read transaction.
    pub max_read_duration: Duration,
    /// Maximum number of actor messages to batch before committing write transaction.
    pub max_write_batch: usize,
    /// Maximum duration to wait before committing a write transaction.
    pub max_write_duration: Duration,
}

impl Default for BatchOptions {
    fn default() -> Self {
        Self {
            max_read_batch: 10000,
            max_read_duration: Duration::from_secs(1),
            max_write_batch: 1000,
            max_write_duration: Duration::from_millis(500),
        }
    }
}

/// What kind of crash the file store stays consistent through.
///
/// This is about consistency, not durability. After a crash the store holds
/// no entry that claims data it does not have, but recent writes can be lost
/// in every mode, even acknowledged ones. A write is only durable after
/// [`Store::sync_db`](crate::api::Store::sync_db) or a clean shutdown, and
/// only against the kind of crash the chosen level covers.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
#[non_exhaustive]
pub enum CrashConsistency {
    /// Consistent after a process crash; recovers from an OS crash or power loss on the next open.
    ///
    /// No data files are synced, so an OS crash or power loss can leave entries
    /// that claim data that never reached the disk. The store records the boot
    /// id while it is open, and if it finds one from another boot session on
    /// open, every entry with files checks them again when it is next loaded.
    /// Until then it may report less data than it has, never more.
    ///
    /// A clean shutdown does not sync, so an OS crash within seconds after it,
    /// before the OS wrote out its cache, goes unnoticed. Where there is no boot
    /// id, every unclean shutdown counts as a possible OS crash.
    #[default]
    ProcessCrash,
    /// Consistent after an OS crash or power loss too, at the cost of fsyncs.
    ///
    /// Data files and the data directory are synced before the metadata that
    /// depends on them is recorded, for imports as well as downloads. That
    /// costs a few fsyncs for every blob above the inline threshold, which
    /// slows down imports and downloads of many small blobs considerably.
    PowerLoss,
}

/// Options for the file store.
#[derive(Debug, Clone)]
pub struct Options {
    /// Path options.
    pub path: PathOptions,
    /// Inline storage options.
    pub inline: InlineOptions,
    /// Transaction batching options.
    pub batch: BatchOptions,
    /// Gc configuration.
    pub gc: Option<GcConfig>,
    /// What kind of crash the store stays consistent through.
    pub crash_consistency: CrashConsistency,
}

impl Options {
    /// Create new optinos with the given root path and everything else default.
    pub fn new(root: &Path) -> Self {
        Self {
            path: PathOptions::new(root),
            inline: InlineOptions::default(),
            batch: BatchOptions::default(),
            gc: None,
            crash_consistency: CrashConsistency::default(),
        }
    }

    /// True if files must reach the disk before metadata that depends on them is recorded.
    pub(crate) fn syncs_files(&self) -> bool {
        self.crash_consistency == CrashConsistency::PowerLoss
    }

    // check if the data will be inlined, based on the size of the data
    pub fn is_inlined_data(&self, data_size: u64) -> bool {
        data_size <= self.inline.max_data_inlined
    }

    // check if the outboard will be inlined, based on the size of the *outboard*
    pub fn is_inlined_outboard(&self, outboard_size: u64) -> bool {
        outboard_size <= self.inline.max_outboard_inlined
    }

    // check if both the data and outboard will be inlined, based on the size of the data
    pub fn is_inlined_all(&self, data_size: u64) -> bool {
        let outboard_size = raw_outboard_size(data_size);
        self.is_inlined_data(data_size) && self.is_inlined_outboard(outboard_size)
    }
}

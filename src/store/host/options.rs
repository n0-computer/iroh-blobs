//! The options of the store itself, apart from its backend.
use std::path::PathBuf;

use super::bao::raw_outboard_size;
use crate::store::gc::GcConfig;

/// The part of the fs store's `Options` the store itself uses.
///
/// The rest goes to the parts the store is built from: the fs handler gets its
/// paths and sync mode as `FsConfig`, and the redb metadata store gets the
/// options it needs when it is created.
#[derive(Debug, Clone)]
pub(crate) struct StoreOptions {
    /// Inlining, which every handler implementation shares.
    pub inline: InlineOptions,
    /// Where imports stage their files.
    ///
    /// Imports stage and hash their data before the hash is known, so outside
    /// any handler, and the fs handler then renames the files into place. So
    /// this belongs to the fs handler really, and must be on the same device as
    /// its data directory. It moves there once import staging is part of the
    /// handler implementation.
    pub temp_path: PathBuf,
    /// Gc configuration.
    pub gc: Option<GcConfig>,
}

impl StoreOptions {
    /// Check if the data will be inlined, based on the size of the data.
    pub fn is_inlined_data(&self, data_size: u64) -> bool {
        data_size <= self.inline.max_data_inlined
    }

    /// Check if the outboard will be inlined, based on the size of the *outboard*.
    pub fn is_inlined_outboard(&self, outboard_size: u64) -> bool {
        outboard_size <= self.inline.max_outboard_inlined
    }

    /// Check if both the data and outboard will be inlined, based on the size of the data.
    pub fn is_inlined_all(&self, data_size: u64) -> bool {
        let outboard_size = raw_outboard_size(data_size);
        self.is_inlined_data(data_size) && self.is_inlined_outboard(outboard_size)
    }

    /// A new, unique file name in the temp directory.
    pub fn temp_file_name(&self) -> PathBuf {
        self.temp_path.join(temp_name())
    }
}

/// Create a 16 byte unique ID.
fn new_uuid() -> [u8; 16] {
    use rand::Rng;
    let mut rng = rand::rng();
    let mut bytes = [0u8; 16];
    rng.fill_bytes(&mut bytes);
    bytes
}

/// Create temp file name based on a 16 byte UUID.
pub(crate) fn temp_name() -> String {
    format!("{}.temp", hex::encode(new_uuid()))
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

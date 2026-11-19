//! Deleting an entry's files.
use tracing::warn;

use super::options::PathOptions;
use crate::{store::engine::metadata::parts::BaoFilePart, Hash};

/// Delete the given files of an entry, ignoring files that do not exist.
pub(super) fn delete_files(options: &PathOptions, hash: &Hash, parts: &[BaoFilePart]) {
    for part in parts {
        let path = match part {
            BaoFilePart::Data => options.data_path(hash),
            BaoFilePart::Outboard => options.outboard_path(hash),
            BaoFilePart::Sizes => options.sizes_path(hash),
            BaoFilePart::Bitfield => options.bitfield_path(hash),
        };
        if let Err(cause) = std::fs::remove_file(&path) {
            // Ignore NotFound errors, if the file is already gone that's fine.
            if cause.kind() != std::io::ErrorKind::NotFound {
                warn!("failed to delete {part:?} {}: {cause}", path.display());
            }
        }
    }
}

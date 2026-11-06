use std::collections::HashSet;

use tracing::warn;

use super::{
    entry_state::{DataLocation, EntryState, OutboardLocation},
    options::PathOptions,
};
use crate::Hash;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub(super) enum BaoFilePart {
    Outboard,
    Data,
    Sizes,
    Bitfield,
}

const ALL_PARTS: [BaoFilePart; 4] = [
    BaoFilePart::Outboard,
    BaoFilePart::Data,
    BaoFilePart::Sizes,
    BaoFilePart::Bitfield,
];

/// The owned files of an entry that the given state does not reference.
///
/// This may include files that do not exist. Deleting those is a no-op.
pub(super) fn unreferenced_parts<I>(state: Option<&EntryState<I>>) -> Vec<BaoFilePart> {
    match state {
        None => ALL_PARTS.to_vec(),
        Some(EntryState::Partial { .. }) => Vec::new(),
        Some(EntryState::Complete {
            data_location,
            outboard_location,
        }) => {
            let mut parts = vec![BaoFilePart::Sizes, BaoFilePart::Bitfield];
            if !matches!(data_location, DataLocation::Owned(_)) {
                parts.push(BaoFilePart::Data);
            }
            if !matches!(outboard_location, OutboardLocation::Owned) {
                parts.push(BaoFilePart::Outboard);
            }
            parts
        }
    }
}

/// Which hashes have a live entity, as reported by the entities themselves.
///
/// An entity reports itself live with its load, and not live at the end of its
/// shutdown. Both go through the metadata FIFO, so:
///
/// - an entity only touches files of its hash after the metadata actor has
///   answered its load, so after it was marked live, and
/// - when its inactive is processed, all its metadata writes have been
///   processed before, and it touches no more files.
///
/// So at the end of a batch, the entry state of a hash that is not live
/// reflects all files of that hash.
#[derive(Debug, Default)]
pub(super) struct LiveEntities(HashSet<Hash>);

impl LiveEntities {
    pub fn load(&mut self, hash: Hash) {
        self.0.insert(hash);
    }

    pub fn inactive(&mut self, hash: Hash) {
        self.0.remove(&hash);
    }

    pub fn is_live(&self, hash: &Hash) -> bool {
        self.0.contains(hash)
    }
}

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

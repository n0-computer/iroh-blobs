use tracing::warn;

use super::{
    entry_state::{DataLocation, EntryState, OutboardLocation},
    options::PathOptions,
};
use crate::Hash;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) enum BaoFilePart {
    Outboard,
    Data,
    Sizes,
    Bitfield,
}

pub(super) const ALL_PARTS: [BaoFilePart; 4] = [
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

/// The owned files of an entry that the given state references.
pub(super) fn referenced_parts<I>(state: Option<&EntryState<I>>) -> Vec<BaoFilePart> {
    let unreferenced = unreferenced_parts(state);
    ALL_PARTS
        .into_iter()
        .filter(|part| !unreferenced.contains(part))
        .collect()
}

/// What a committed write batch means for the files of one hash.
///
/// The metadata actor sends this after the commit, so the files it names are
/// no longer referenced by any durable state. Files are only touched by the
/// hash's entity, or by the main actor while no entity exists for the hash.
#[derive(Debug)]
pub(crate) struct FileNotice {
    pub hash: Hash,
    /// A delete committed, even if no entry existed. The entity must finish cleanup.
    pub deleted: bool,
    /// Files the entry referenced before or during the batch, and no longer does.
    pub parts: Vec<BaoFilePart>,
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

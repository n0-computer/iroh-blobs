use tracing::warn;

use super::{
    entry_state::{DataLocation, EntryState, OutboardLocation},
    storage::Storage,
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

/// Some of the four files of an entry. Never allocates.
pub(crate) type BaoFileParts = smallvec::SmallVec<[BaoFilePart; 4]>;

/// The owned files of an entry that the given state does not reference.
///
/// This may include files that do not exist. Deleting those is a no-op.
pub(super) fn unreferenced_parts<I>(state: Option<&EntryState<I>>) -> BaoFileParts {
    match state {
        None => BaoFileParts::from(ALL_PARTS),
        Some(EntryState::Partial { .. }) => BaoFileParts::new(),
        Some(EntryState::Complete {
            data_location,
            outboard_location,
        }) => {
            let mut parts = smallvec::smallvec![BaoFilePart::Sizes, BaoFilePart::Bitfield];
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
pub(super) fn referenced_parts<I>(state: Option<&EntryState<I>>) -> BaoFileParts {
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
    pub parts: BaoFileParts,
}

/// Delete the given files of an entry, ignoring files that do not exist.
pub(super) fn delete_files(storage: &impl Storage, hash: &Hash, parts: &[BaoFilePart]) {
    for part in parts {
        if let Err(cause) = storage.delete_part(hash, *part) {
            warn!("failed to delete {part:?} of {}: {cause}", hash.fmt_short());
        }
    }
}

use tracing::warn;

use super::{
    entry_state::{DataLocation, EntryState, OutboardLocation},
    options::PathOptions,
};
use crate::Hash;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum BaoFilePart {
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

/// Files of an entry that a committed write batch stopped referencing.
///
/// The metadata actor sends these with the entry's hash after the commit, so
/// no durable state references them anymore. That does not make them garbage:
/// the entry's entity may still use them, since its state is never behind its
/// entry. So before deleting, the entity must check each part against its own
/// state, and only delete those it does not use. Files are only touched by the
/// hash's entity, or by the main actor while no entity exists for the hash.
///
/// Deletes do not come through here: the entity removes its files once its
/// delete is answered, after the commit.
#[derive(Debug)]
pub(crate) struct UnreferencedParts(pub BaoFileParts);

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

//! The parts of an entry's data, and which of them a state references.
use super::entry_state::{DataLocation, EntryState, OutboardLocation};

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) enum BaoFilePart {
    Outboard,
    Data,
    Sizes,
    Bitfield,
}

pub(crate) const ALL_PARTS: [BaoFilePart; 4] = [
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
pub(crate) fn unreferenced_parts<I>(state: Option<&EntryState<I>>) -> BaoFileParts {
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
pub(crate) fn referenced_parts<I>(state: Option<&EntryState<I>>) -> BaoFileParts {
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
/// state, and only delete those it does not use. Only the hash's entity
/// touches its files.
///
/// Deletes do not come through here: the entity removes its files once its
/// delete is answered, after the commit.
#[derive(Debug)]
pub(crate) struct UnreferencedParts(pub BaoFileParts);

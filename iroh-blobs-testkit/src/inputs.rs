//! Generated inputs: proptest strategies for blobs and ranges.
use std::fmt;

use bao_tree::{ChunkNum, ChunkRanges};
use bytes::Bytes;
use proptest::prelude::*;
use range_collections::RangeSet2;

use crate::reference::Reference;

/// Size of a chunk, the unit of the bitfield.
pub const CHUNK: usize = 1024;

/// Size of a chunk group, the unit of the outboard.
pub const GROUP: usize = 16 * CHUNK;

/// Largest generated blob. Large enough for a tree with several levels.
const MAX_SIZE: usize = 16 * GROUP;

/// Sizes at the chunk and chunk group boundaries.
const INTERESTING_SIZES: [usize; 11] = [
    1,
    CHUNK - 1,
    CHUNK,
    CHUNK + 1,
    GROUP - 1,
    GROUP,
    GROUP + 1,
    2 * GROUP - 1,
    2 * GROUP,
    2 * GROUP + 1,
    3 * GROUP + CHUNK + 1,
];

/// Blob content: `size` bytes of pseudo random data derived from `seed`.
///
/// Only size and seed are generated and printed, so failures stay readable
/// and shrinking is cheap.
#[derive(Clone, PartialEq, Eq)]
pub struct Blob {
    size: usize,
    seed: u64,
}

impl Blob {
    pub fn size(&self) -> usize {
        self.size
    }

    pub fn data(&self) -> Bytes {
        // splitmix64: random looking data, so that zeroed or misplaced
        // regions never match by accident.
        let mut state = self.seed;
        let mut data = Vec::with_capacity(self.size + 8);
        while data.len() < self.size {
            state = state.wrapping_add(0x9e3779b97f4a7c15);
            let mut z = state;
            z = (z ^ (z >> 30)).wrapping_mul(0xbf58476d1ce4e5b9);
            z = (z ^ (z >> 27)).wrapping_mul(0x94d049bb133111eb);
            data.extend_from_slice(&(z ^ (z >> 31)).to_le_bytes());
        }
        data.truncate(self.size);
        data.into()
    }

    /// The data together with its expected protocol values.
    pub fn reference(&self) -> Reference {
        Reference::new(self.data())
    }
}

impl fmt::Debug for Blob {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Blob {{ size: {}, seed: {} }}", self.size, self.seed)
    }
}

/// A blob of any size, including empty.
pub fn blob() -> impl Strategy<Value = Blob> {
    prop_oneof![
        1 => Just(0usize),
        4 => proptest::sample::select(&INTERESTING_SIZES[..]),
        4 => 0..=MAX_SIZE,
    ]
    .prop_flat_map(blob_of_size)
}

/// A blob of at least one byte, e.g. for bao import, which needs a non-zero
/// size.
pub fn non_empty_blob() -> impl Strategy<Value = Blob> {
    prop_oneof![
        1 => proptest::sample::select(&INTERESTING_SIZES[..]),
        1 => 1..=MAX_SIZE,
    ]
    .prop_flat_map(blob_of_size)
}

fn blob_of_size(size: usize) -> impl Strategy<Value = Blob> {
    any::<u64>().prop_map(move |seed| Blob { size, seed })
}

/// A non-empty set of chunk ranges for a blob of `size` bytes.
///
/// Ranges may extend past the end of the blob. A store must treat that like
/// a request up to the end.
pub fn chunk_ranges(size: usize) -> impl Strategy<Value = ChunkRanges> {
    let limit = ChunkNum::chunks(size as u64).0 + 2;
    prop_oneof![
        1 => Just(ChunkRanges::all()),
        1 => (0..limit).prop_map(|start| ChunkRanges::from(ChunkNum(start)..)),
        6 => proptest::collection::vec((0..limit, 1..=limit), 1..4).prop_map(|ranges| {
            ranges
                .into_iter()
                .map(|(start, len)| ChunkRanges::from(ChunkNum(start)..ChunkNum(start + len)))
                .fold(ChunkRanges::empty(), |acc, range| acc | range)
        }),
    ]
}

/// A non-empty set of byte ranges for a blob of `size` bytes.
///
/// Ranges may extend past the end of the blob.
pub fn byte_ranges(size: usize) -> impl Strategy<Value = RangeSet2<u64>> {
    let limit = size as u64 + 2 * CHUNK as u64;
    prop_oneof![
        1 => Just(RangeSet2::all()),
        1 => (0..limit).prop_map(|start| RangeSet2::from(start..)),
        6 => proptest::collection::vec((0..limit, 1..=limit), 1..4).prop_map(|ranges| {
            ranges
                .into_iter()
                .map(|(start, len)| RangeSet2::from(start..start + len))
                .fold(RangeSet2::empty(), |acc, range| acc | range)
        }),
    ]
}

/// Pairs each blob with chunk ranges for it.
pub fn with_chunk_ranges(
    blobs: impl Strategy<Value = Blob>,
) -> impl Strategy<Value = (Blob, ChunkRanges)> {
    blobs.prop_flat_map(|blob| {
        let ranges = chunk_ranges(blob.size());
        (Just(blob), ranges)
    })
}

/// Pairs each blob with 0 to 3 sets of chunk ranges for it, e.g. to import
/// in steps.
pub fn with_chunk_range_steps(
    blobs: impl Strategy<Value = Blob>,
) -> impl Strategy<Value = (Blob, Vec<ChunkRanges>)> {
    blobs.prop_flat_map(|blob| {
        let steps = proptest::collection::vec(chunk_ranges(blob.size()), 0..4);
        (Just(blob), steps)
    })
}

/// Pairs each blob with byte ranges for it.
pub fn with_byte_ranges(
    blobs: impl Strategy<Value = Blob>,
) -> impl Strategy<Value = (Blob, RangeSet2<u64>)> {
    blobs.prop_flat_map(|blob| {
        let ranges = byte_ranges(blob.size());
        (Just(blob), ranges)
    })
}

/// Sizes to split data into, used cyclically. Not aligned to chunks on
/// purpose.
pub fn piece_sizes() -> impl Strategy<Value = Vec<usize>> {
    proptest::collection::vec(1..=2 * GROUP, 1..4)
}

/// Splits `data` into pieces with sizes taken cyclically from `sizes`.
pub fn split(data: &Bytes, sizes: &[usize]) -> Vec<Bytes> {
    let mut pieces = Vec::new();
    let mut offset = 0;
    for size in sizes.iter().cycle() {
        if offset >= data.len() {
            break;
        }
        let end = (offset + size).min(data.len());
        pieces.push(data.slice(offset..end));
        offset = end;
    }
    pieces
}

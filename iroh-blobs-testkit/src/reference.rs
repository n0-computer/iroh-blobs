//! Expected values for a blob, computed with bao-tree.
use std::{io::Cursor, num::NonZeroU64};

use bao_tree::{
    io::{
        outboard::PreOrderMemOutboard,
        sync::{encode_ranges_validated, DecodeResponseIter},
        BaoContentItem,
    },
    ChunkNum, ChunkRanges,
};
use bytes::Bytes;
use iroh_blobs::{store::IROH_BLOCK_SIZE, Hash};
use range_collections::{range_set::RangeSetRange, RangeSet2};

/// A blob together with its outboard.
pub struct Reference {
    data: Bytes,
    outboard: PreOrderMemOutboard,
}

impl Reference {
    pub fn new(data: Bytes) -> Self {
        let outboard = PreOrderMemOutboard::create(&data, IROH_BLOCK_SIZE);
        Self { data, outboard }
    }

    pub fn data(&self) -> &Bytes {
        &self.data
    }

    pub fn hash(&self) -> Hash {
        self.outboard.root.into()
    }

    pub fn size(&self) -> u64 {
        self.data.len() as u64
    }

    /// The size for a bao import, which needs a non-empty blob.
    pub fn bao_size(&self) -> NonZeroU64 {
        NonZeroU64::new(self.size()).expect("bao import needs a non-empty blob")
    }

    /// All chunks of the blob.
    pub fn all_chunks(&self) -> ChunkRanges {
        ChunkRanges::from(..ChunkNum::chunks(self.size()))
    }

    /// `ranges`, limited to the chunks of the blob.
    pub fn clip(&self, ranges: &ChunkRanges) -> ChunkRanges {
        ranges & &self.all_chunks()
    }

    /// The bao encoding of `ranges`, as a sequence of items.
    pub fn items(&self, ranges: &ChunkRanges) -> Vec<BaoContentItem> {
        let mut encoded = Vec::new();
        encode_ranges_validated(&self.data[..], &self.outboard, ranges, &mut encoded)
            .expect("encoding from complete data and outboard can not fail");
        DecodeResponseIter::new(
            self.outboard.root,
            self.outboard.tree,
            Cursor::new(encoded),
            ranges,
        )
        .collect::<Result<_, _>>()
        .expect("a valid encoding always decodes")
    }

    /// `ranges`, limited to the bytes of the blob.
    pub fn clip_bytes(&self, ranges: &RangeSet2<u64>) -> RangeSet2<u64> {
        ranges & &RangeSet2::from(0..self.size())
    }
}

/// The byte ranges covered by `ranges`, limited to `size`.
pub fn chunks_to_bytes(ranges: &ChunkRanges, size: u64) -> RangeSet2<u64> {
    let mut bytes = RangeSet2::empty();
    for range in ranges.iter() {
        let (start, end) = match range {
            RangeSetRange::Range(range) => (range.start.to_bytes(), range.end.to_bytes()),
            RangeSetRange::RangeFrom(range) => (range.start.to_bytes(), size),
        };
        let end = end.min(size);
        if start < end {
            bytes |= RangeSet2::from(start..end);
        }
    }
    bytes
}
